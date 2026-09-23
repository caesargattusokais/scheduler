// config/WorkerConfig.java —— 镜像 server Beans 的执行侧装配(需 worker 的仓库 bean)
package dev.scheduler.worker.config;

import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.JdbcWorkerRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.worker.execute.ExecutorWorker;
import dev.scheduler.worker.handler.DemoHandler;
import dev.scheduler.worker.handler.EchoHandler;
import dev.scheduler.worker.handler.ExecutionHandler;
import dev.scheduler.worker.handler.HandlerRegistry;
import dev.scheduler.worker.handler.MapHandlerRegistry;
import dev.scheduler.worker.handler.MyJobHandler;
import dev.scheduler.worker.handler.SyncDemoHandler;
import dev.scheduler.worker.registration.WorkerRegistrar;
import dev.scheduler.persistence.retry.FailureResolver;
import dev.scheduler.persistence.retry.RetryPolicy;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class WorkerConfig {
  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(WorkerConfig.class);
  // ---- 仓库 bean(worker 独立上下文,不能依赖 server Beans) ----
  @Bean TaskRepository taskRepository(JdbcTemplate jdbc) { return new JdbcTaskRepository(jdbc); }
  @Bean ShardRepository shardRepository(JdbcTemplate jdbc) { return new JdbcShardRepository(jdbc); }
  @Bean WorkerRepository workerRepository(JdbcTemplate jdbc) { return new JdbcWorkerRepository(jdbc); }
  @Bean Clock clock() { return Clock.systemUTC(); }

  @Bean ExecutionHandler demoHandler() { return new DemoHandler(); }
  @Bean ExecutionHandler echoHandler(Clock clock) { return new EchoHandler(clock); }
  @Bean ExecutionHandler myJobHandler(Clock clock) { return new MyJobHandler(clock); }
  @Bean ExecutionHandler syncDemoHandler(JdbcTemplate jdbc) { return new SyncDemoHandler(jdbc); }

  @Bean
  HandlerRegistry handlerRegistry(List<ExecutionHandler> handlers) {
    List<Supplier<ExecutionHandler>> suppliers =
        handlers.stream().map(h -> (Supplier<ExecutionHandler>) () -> h).toList();
    return new MapHandlerRegistry(suppliers);
  }

  @Bean RetryPolicy retryPolicy() { return new RetryPolicy(); }
  @Bean FailureResolver failureResolver(ShardRepository shards, RetryPolicy retryPolicy, Clock clock) {
    return new FailureResolver(shards, retryPolicy, clock);
  }

  @Bean
  String schedulerWorkerId(@Value("${scheduler.worker-id:}") String configured) {
    return configured.isBlank() ? defaultWorkerId() : configured;
  }

  /** 稳定 worker 标识,进程重启后不换 id(在途租约可被接续;避免累积陈旧 ALIVE 行)。与 server 的
   *  defaultWorkerId(<os>:<hostname>) 不同前缀,注册表不冲突。 */
  static String defaultWorkerId() {
    try {
      return "worker@" + java.net.InetAddress.getLocalHost().getHostName()
          + ":" + java.lang.ProcessHandle.current().pid();
    } catch (Throwable t) {
      log.warn("could not resolve hostname for worker-id, falling back to uuid", t);
      return "worker@" + java.util.UUID.randomUUID();
    }
  }

  @Bean
  ExecutorWorker executorWorker(TaskRepository tasks, ShardRepository shards,
                                HandlerRegistry handlers, String schedulerWorkerId,
                                FailureResolver failureResolver, Clock clock,
                                @Value("${scheduler.lease.seconds:60}") int leaseSeconds,
                                @Value("${scheduler.lease.renew-seconds:15}") int renewSeconds) {
    return new ExecutorWorker(tasks, shards, handlers, schedulerWorkerId, failureResolver, clock,
        leaseSeconds, renewSeconds);
  }

  @Bean
  WorkerRegistrar workerRegistrar(WorkerRepository repo, HandlerRegistry registry,
                                  String schedulerWorkerId, Clock clock) {
    return new WorkerRegistrar(repo, registry, schedulerWorkerId, clock);
  }

  /** 启动即注册一次,随后按 interval 周期心跳。 */
  @Bean
  ApplicationRunner registerOnStart(WorkerRegistrar registrar) {
    return args -> registrar.heartbeat();
  }

  /** 执行认领环:共享 DB 原子认领,支持多 worker 并发分摊;capacity>1 时单进程并行认领多片。 */
  @Bean
  WorkLoop workLoop(ExecutorWorker worker,
                    @Value("${scheduler.worker.capacity:1}") int capacity,
                    @Value("${scheduler.worker.idle-ms:200}") long idleMs) {
    return new WorkLoop(worker, capacity, idleMs);
  }

  /** 心跳环:专用守护线程解耦执行环,长运行 handler 阻塞 WorkLoop 时心跳仍推进(见 HeartbeatLoop 注释)。 */
  @Bean
  HeartbeatLoop heartbeatLoop(WorkerRegistrar registrar,
                              @Value("${scheduler.heartbeat.interval-ms:10000}") long intervalMs) {
    return new HeartbeatLoop(registrar, intervalMs);
  }

  /** 执行环(4c 扩容):capacity 个自持守护调度线程,各自循环认领并阻塞执行一个分片。
   *   scheduleWithFixedDelay 保证同一执行线程不重叠 → 时刻至多 capacity 分片在途(认领天然不超 capacity——
   *   只有 capacity 个线程在认领,各领一个);无活时 workOne 旋即返回,经 idleMs 延后重试。
   *   长运行 handler 会占满其一槽位(少一拍认领能力),符合「capacity=并发在途数」语义。与心跳环解耦
   *   (见 HeartbeatLoop),长运行期间心跳照常推进,owner 活性不被误判。无 @Scheduled,由本池自持驱动。 */
  public static final class WorkLoop implements DisposableBean {
    private final ExecutorWorker worker;
    /** 专用 daemon 调度池:容量=并发认领数;daemon 不阻塞 JVM 退出。 */
    private final ScheduledExecutorService pool;

    WorkLoop(ExecutorWorker worker, int capacity, long idleMs) {
      this.worker = worker;
      if (capacity < 1) throw new IllegalArgumentException("scheduler.worker.capacity must be >= 1");
      this.pool = Executors.newScheduledThreadPool(capacity, r -> {
        Thread t = new Thread(r, "worker-exec");
        t.setDaemon(true);
        return t;
      });
      for (int i = 0; i < capacity; i++) {
        pool.scheduleWithFixedDelay(this::tick, 0, idleMs, TimeUnit.MILLISECONDS);
      }
    }

    private void tick() {
      try {
        worker.workOne();
      } catch (Throwable t) {
        log.warn("worker claim loop tick failed; continuing next tick", t);
      }
    }

    @Override public void destroy() {
      pool.shutdownNow();
    }
  }

  /** 心跳环。与认领/执行环(WorkLoop,共享 Spring 默认单调度线程)解耦:运行在自持的单线程守护调度器上。
   *  否则长运行 handler 同步阻塞 WorkLoop.tick 时,同线程的心跳也无法触发,owner last_seen 冻结,会被 server
   *  活性优先回收(findExpiredRunning)误判为死 worker,夺走健康 worker 的在途分片。此解耦是 renewer(租约)
   *  之外的活性保障——长运行期间心跳仍推进,owner 活性不被误判。 */
  public static final class HeartbeatLoop implements DisposableBean {
    private final WorkerRegistrar registrar;
    /** 专用守护线程:长运行 handler 阻塞执行环时心跳照常触发。daemon 不阻塞 JVM 退出。 */
    private final ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(r -> {
          Thread t = new Thread(r, "worker-heartbeat");
          t.setDaemon(true);
          return t;
        });

    HeartbeatLoop(WorkerRegistrar registrar, long intervalMs) {
      this.registrar = registrar;
      scheduler.scheduleWithFixedDelay(this::tick, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void tick() {
      try {
        registrar.heartbeat();
      } catch (Throwable t) {
        log.warn("worker heartbeat loop tick failed; continuing next tick", t);
      }
    }

    @Override public void destroy() {
      scheduler.shutdownNow();
    }
  }
}