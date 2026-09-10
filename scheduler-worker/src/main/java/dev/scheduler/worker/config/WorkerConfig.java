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
import dev.scheduler.worker.handler.ExecutionHandler;
import dev.scheduler.worker.handler.HandlerRegistry;
import dev.scheduler.worker.handler.MapHandlerRegistry;
import dev.scheduler.worker.registration.WorkerRegistrar;
import dev.scheduler.worker.retry.FailureResolver;
import dev.scheduler.worker.retry.RetryPolicy;
import java.time.Clock;
import java.util.List;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
public class WorkerConfig {
  // ---- 仓库 bean(worker 独立上下文,不能依赖 server Beans) ----
  @Bean TaskRepository taskRepository(JdbcTemplate jdbc) { return new JdbcTaskRepository(jdbc); }
  @Bean ShardRepository shardRepository(JdbcTemplate jdbc) { return new JdbcShardRepository(jdbc); }
  @Bean WorkerRepository workerRepository(JdbcTemplate jdbc) { return new JdbcWorkerRepository(jdbc); }
  @Bean Clock clock() { return Clock.systemUTC(); }

  @Bean ExecutionHandler demoHandler() { return new DemoHandler(); }

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
    return configured.isBlank() ? "worker@" + java.util.UUID.randomUUID() : configured;
  }

  @Bean
  ExecutorWorker executorWorker(TaskRepository tasks, ShardRepository shards,
                                HandlerRegistry handlers, String schedulerWorkerId,
                                FailureResolver failureResolver, Clock clock) {
    return new ExecutorWorker(tasks, shards, handlers, schedulerWorkerId, failureResolver, clock);
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

  /** 执行认领环:共享 DB 原子认领,支持多 worker 并发分摊。 */
  @Bean
  WorkLoop workLoop(ExecutorWorker worker) {
    return new WorkLoop(worker);
  }

  /** 心跳环。 */
  @Bean
  HeartbeatLoop heartbeatLoop(WorkerRegistrar registrar) {
    return new HeartbeatLoop(registrar);
  }

  public static final class WorkLoop {
    private final ExecutorWorker worker;
    WorkLoop(ExecutorWorker worker) { this.worker = worker; }
    @Scheduled(fixedDelayString = "${scheduler.loop.work-delay-ms:100}")
    public void tick() { worker.workOne(); }
  }

  public static final class HeartbeatLoop {
    private final WorkerRegistrar registrar;
    HeartbeatLoop(WorkerRegistrar registrar) { this.registrar = registrar; }
    @Scheduled(fixedDelayString = "${scheduler.heartbeat.interval-ms:10000}")
    public void tick() { registrar.heartbeat(); }
  }
}