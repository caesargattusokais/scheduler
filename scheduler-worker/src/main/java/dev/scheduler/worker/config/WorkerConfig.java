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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class WorkerConfig {
  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(WorkerConfig.class);
  // ---- 仓库 bean(worker 独立上下文,不能依赖 server Beans) ----
  @Bean TaskRepository taskRepository(JdbcTemplate jdbc) { return new JdbcTaskRepository(jdbc); }
  @Bean ShardRepository shardRepository(JdbcTemplate jdbc) { return new JdbcShardRepository(jdbc); }
  @Bean WorkerRepository workerRepository(JdbcTemplate jdbc) { return new JdbcWorkerRepository(jdbc); }
  @Bean Clock clock() { return Clock.systemUTC(); }
  /** worker 侧运行时配置只读器:同一张 app_runtime_config 表,读 capacity 等热键(只读,无审计/白名单)。 */
  @Bean WorkerRuntimeConfig workerRuntimeConfig(JdbcTemplate jdbc) { return new WorkerRuntimeConfig(jdbc); }

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

  /** 执行认领环:共享 DB 原子认领,支持多 worker 并发分摊;capacity>1 时单进程并行认领多片。
   *   max-capacity 为线程池上界(重启级,非热键),capacity 为启动并发并可热改的运行时目标。 */
  @Bean
  WorkLoop workLoop(ExecutorWorker worker,
                    @Value("${scheduler.worker.capacity:1}") int capacity,
                    @Value("${scheduler.worker.max-capacity:32}") int maxCapacity,
                    @Value("${scheduler.worker.idle-ms:200}") long idleMs,
                    WorkerRuntimeConfig runtime) {
    return new WorkLoop(worker, capacity, maxCapacity, idleMs, runtime);
  }

  /** 心跳环:专用守护线程解耦执行环,长运行 handler 阻塞 WorkLoop 时心跳仍推进(见 HeartbeatLoop 注释)。 */
  @Bean
  HeartbeatLoop heartbeatLoop(WorkerRegistrar registrar,
                              @Value("${scheduler.heartbeat.interval-ms:10000}") long intervalMs) {
    return new HeartbeatLoop(registrar, intervalMs);
  }

  /** 停机编排:容器关闭时 排空 → 停心跳 → deregister(WorkerConfig 内引用 workLoop/heartbeatLoop)。
   *   gracefulSec 读自运行时热键 worker.shutdown-grace-sec(默认 30)。
   *   @DependsOn 把创建顺序钉死在 workLoop/heartbeatLoop 之后 → 销毁顺序在其之前:先排空+停心跳+deregister,
   *   再轮到二者 destroy()(幂等 no-op)。避免中途心跳先死、server 活性在排空期误回收在途分片的双重执行。 */
  @Bean
  @DependsOn({"workLoop", "heartbeatLoop"})
  DisposableBean gracefulShutdown(WorkLoop workLoop, HeartbeatLoop heartbeatLoop,
                                  WorkerRegistrar registrar, WorkerRuntimeConfig runtime,
                                  String schedulerWorkerId) {
    return () -> {
      long grace = runtime.getLong("worker.shutdown-grace-sec", 30L);
      workLoop.stopAndDrain(grace);
      heartbeatLoop.stop();
      registrar.deregister();
      log.info("worker {} drained and deregistered", schedulerWorkerId);
    };
  }

  /** 执行环(4c 扩容):守护调度池负责并发认领并阻塞执行分片。
   *   池按 max-capacity 定长(线程池上界,只建不缩),start 由 capacity 钳制到 [0, max-capacity];
   *   运行时并发目标由 {@code target}(启动值 = capacity)经 tick 内 CAS 闸控制,capacity 热改不重建线程池。
   *   scheduleWithFixedDelay 保证同一执行线程不重叠;无活时 workOne 旋即返回,经 idleMs 延后重试。
   *   长运行 handler 会占满其一槽位(少一拍认领能力),符合「capacity=并发在途数」语义。与心跳环解耦
   *   (见 HeartbeatLoop),长运行期间心跳照常推进,owner 活性不被误判。无 @Scheduled,由本池自持驱动。
   *   行为变更:旧构造 capacity<1 抛 IllegalArgumentException;现在 capacity<=0 不排 tick(0 槽 idle),
   *   与「capacity=0 暂停认领」语义一致。 */
  public static final class WorkLoop implements DisposableBean {
    /** 全局暂停热键(与 server RuntimeConfigKeys.SUSPEND 同 key;worker 独立上下文不 import server,故本地定义)。 */
    private static final String SUSPEND_KEY = "suspend";
    private final ExecutorWorker worker;
    /** 专用 daemon 调度池:定长 max-capacity(上界);daemon 不阻塞 JVM 退出。 */
    private final ScheduledExecutorService pool;
    private final long idleMs;
    private final int maxCapacity;
    private final WorkerRuntimeConfig runtime;
    private final AtomicBoolean running = new AtomicBoolean(true);
    /** 当期认领槽计数:capacity 热改时 CAS 控流。 */
    private final AtomicInteger target = new AtomicInteger(1);
    private final AtomicInteger activeClaims = new AtomicInteger(0);
    private final AtomicInteger inflight = new AtomicInteger(0);

    WorkLoop(ExecutorWorker worker, int capacity, int maxCapacity, long idleMs,
             WorkerRuntimeConfig runtime) {
      this.worker = worker;
      this.idleMs = idleMs;
      this.maxCapacity = Math.max(1, maxCapacity);
      this.runtime = runtime;
      // 池按 maxCapacity 定长(上界只建不缩)→ 并发并行性有界、预留热扩空间;热改只动 target+排拍数。
      this.pool = Executors.newScheduledThreadPool(this.maxCapacity, r -> {
        Thread t = new Thread(r, "worker-exec");
        t.setDaemon(true);
        return t;
      });
      // capacity<=0 → 0 个排拍(worker 暂停认领);clamp 到 maxCapacity。
      int start = Math.max(0, Math.min(capacity, this.maxCapacity));
      this.target.set(start);
      for (int i = 0; i < start; i++) {
        pool.scheduleWithFixedDelay(this::tick, 0, idleMs, TimeUnit.MILLISECONDS);
      }
    }

    private void tick() {
      if (!running.get()) return;
      try {
        // 全局 suspend:停新认领(server 与 worker 一致),心跳环独立继续 → 在途不被活性误回收;清位后下一拍自动恢复。
        if (runtime.getFlag(SUSPEND_KEY, false)) return;
        // 每 tick 前自读容量热键并 self-resize;DB 宕机读到异常 → 降级保持当前容量,不杀执行线程。
        this.resize(Math.toIntExact(runtime.getLong("worker.capacity", target.get())));
      } catch (Throwable t) {
        log.warn("worker.capacity/suspend hot-read failed; keeping current capacity {}", target.get(), t);
      }
      // 进闸先计入在途:stopAndDrain 以 inflight 为排空依据,凡进得本 tick 的认领必被计入待其完成,
      // 关门瞬间不会漏排一条在途。让位(activeClaims 超 target)的瞬时计数随即在 finally 递减,无害。
      inflight.incrementAndGet();
      try {
        if (activeClaims.incrementAndGet() > target.get()) {
          activeClaims.decrementAndGet(); // 满了,让位
          return;
        }
        try {
          worker.workOne();
        } finally {
          activeClaims.decrementAndGet();
        }
      } catch (Throwable t) {
        log.warn("worker claim loop tick failed; continuing next tick", t);
      } finally {
        inflight.decrementAndGet();
      }
    }

    /** 双向热改并行度:n clamp 到 [1, maxCapacity];增则补排 fixed-delay 拍,减则仅降 target(CAS 多余槽让位)。 */
    void resize(int n) {
      n = Math.max(1, Math.min(n, maxCapacity));
      int prev = target.getAndSet(n);
      if (n > prev) {
        for (int i = prev; i < n; i++) {
          pool.scheduleWithFixedDelay(this::tick, 0, idleMs, TimeUnit.MILLISECONDS);
        }
        log.info("worker capacity hot-resized {} -> {}", prev, n);
      } else if (n < prev) {
        log.info("worker capacity hot-resized {} -> {} (CAS 回收多余槽)", prev, n);
      }
    }

    @Override public void destroy() { stop(); }

    void stop() { running.set(false); }

    /** 停机:停新认领,排空在途 handler(≤ graceSec),随后关闭调度线程。幂等:二次 no-op。 */
    public void stopAndDrain(long graceSec) {
      running.set(false);
      pool.shutdown(); // 不再排新拍
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(graceSec);
      while (inflight.get() > 0 && System.nanoTime() < deadline) {
        try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
      }
      if (inflight.get() > 0) {
        log.warn("drain timeout after {}s; {} inflight left for liveness recovery", graceSec, inflight.get());
      }
      pool.shutdownNow(); // 兜底:daemon 不阻塞 JVM,剩余由 server 活性回收
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

    /** 停机编排用:停掉心跳调度线程(不再上报活性,配合此后 deregister 离开存活视界)。 */
    void stop() {
      scheduler.shutdownNow();
    }

    @Override public void destroy() {
      stop();
    }
  }
}