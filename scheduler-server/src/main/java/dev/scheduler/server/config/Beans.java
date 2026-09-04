package dev.scheduler.server.config;

import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.JdbcExecutionRepository;
import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.execute.ExecutorWorker;
import dev.scheduler.server.handler.DemoHandler;
import dev.scheduler.server.handler.ExecutionHandler;
import dev.scheduler.server.handler.HandlerRegistry;
import dev.scheduler.server.handler.MapHandlerRegistry;
import dev.scheduler.server.leader.AdvisoryLockLeaderElection;
import dev.scheduler.server.leader.LeaderElection;
import dev.scheduler.server.reconcile.Reconciler;
import dev.scheduler.server.retry.FailureResolver;
import dev.scheduler.server.retry.RetryPolicy;
import dev.scheduler.server.trigger.TriggerEngine;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.net.InetAddress;
import java.time.Clock;
import java.util.List;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

/** 装配 Task 1-9 的既有零件为可运行 Bean 集。 */
@Configuration
public class Beans {
  private static final Logger log = LoggerFactory.getLogger(Beans.class);

  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  JdbcTemplate jdbcTemplate(DataSource ds) {
    return new JdbcTemplate(ds);
  }

  @Bean
  TaskRepository taskRepository(JdbcTemplate jdbc) {
    return new JdbcTaskRepository(jdbc);
  }

  @Bean
  ExecutionRepository executionRepository(JdbcTemplate jdbc) {
    return new JdbcExecutionRepository(jdbc);
  }

  @Bean
  ShardRepository shardRepository(JdbcTemplate jdbc) {
    return new JdbcShardRepository(jdbc);
  }

  @Bean
  ExecutionHandler demoHandler() {
    return new DemoHandler();
  }

  /** 收集全部 ExecutionHandler Bean 并路由 task.handlerRef → handler 实例;handler-ref 未注册在派发时暴露。 */
  @Bean
  HandlerRegistry handlerRegistry(List<ExecutionHandler> handlers) {
    List<Supplier<ExecutionHandler>> suppliers = handlers.stream()
        .map(h -> (Supplier<ExecutionHandler>) () -> h)
        .toList();
    return new MapHandlerRegistry(suppliers);
  }

  /** 会话级 advisory lock 选主;destroyMethod=close 保证停机时释锁并归还专属连接。 */
  @Bean(destroyMethod = "close")
  LeaderElection leaderElection(JdbcTemplate jdbc) {
    return new AdvisoryLockLeaderElection(jdbc);
  }

  @Bean
  TriggerEngine triggerEngine(TaskRepository tasks, ExecutionRepository execs,
                              ShardRepository shards, LeaderElection leader, Clock clock) {
    return new TriggerEngine(tasks, execs, shards, leader, clock);
  }

  /** 重试决策纯类:只判定"应否重试/退避多久",不含 DB 与时钟。 */
  @Bean
  RetryPolicy retryPolicy() {
    return new RetryPolicy();
  }

  /** 失败判定(FAILED 落库 + 重试/死信分流);单例,Task 5 reconciler 复用同一实例。M3:作用对象为 shard。 */
  @Bean
  FailureResolver failureResolver(ShardRepository shards, RetryPolicy retryPolicy, Clock clock) {
    return new FailureResolver(shards, retryPolicy, clock);
  }

  /** 本节点稳定的执行者标识,流入认领与每次 markStatus(审计"谁做的")。 */
  @Bean
  String schedulerWorkerId(@Value("${scheduler.worker-id:}") String configured) {
    return configured.isBlank() ? defaultWorkerId() : configured;
  }

  @Bean
  ExecutorWorker executorWorker(TaskRepository tasks, ShardRepository shards,
                                HandlerRegistry handlers, String schedulerWorkerId,
                                FailureResolver failureResolver, Clock clock) {
    return new ExecutorWorker(tasks, shards, handlers, schedulerWorkerId, failureResolver, clock);
  }

  /**
   * 每任务指标(§5.2):为每个任务各注册一条 series,任一带 task_id 标签,
   * 避免单个热点任务掩盖被饿死的任务的可见性。
   * 已知限制:新创建的任务要等下一次重启才出现 series(M1 接受此重启边界)。
   */
  @Bean
  MeterBinder schedulerMetrics(TaskRepository tasks, ExecutionRepository execs, JdbcTemplate jdbc) {
    return registry -> {
      for (Task t : tasks.findAll()) {
        long taskId = t.id();
        String name = t.name();
        Gauge.builder("scheduler_active_runs", () -> execs.countActive(taskId))
            .tag("task_id", Long.toString(taskId))
            .tag("task_name", name)
            .register(registry);
        Gauge.builder("scheduler_due_queue_max_age_seconds", () -> dueQueueMaxAgeSeconds(jdbc, taskId))
            .tag("task_id", Long.toString(taskId))
            .tag("task_name", name)
            .register(registry);
      }
    };
  }

  /** 固定周期触发扫描;#5:每 tick 均兜底,DB 抖动只杀一拍不杀调度线程。 */
  @Bean
  @ConditionalOnProperty(name = "scheduler.loop.enabled", havingValue = "true", matchIfMissing = true)
  ScanLoop scanLoop(TriggerEngine engine) {
    return new ScanLoop(engine);
  }

  @Bean
  @ConditionalOnProperty(name = "scheduler.loop.enabled", havingValue = "true", matchIfMissing = true)
  WorkLoop workLoop(ExecutorWorker worker) {
    return new WorkLoop(worker);
  }

  /** 对账器:单例,复用共享 FailureResolver(同一重试判定,worker 与 reconciler 无漂移)。M3:作用对象为 shard。 */
  @Bean
  Reconciler reconciler(TaskRepository tasks, ShardRepository shards,
                        FailureResolver failureResolver) {
    return new Reconciler(tasks, shards, failureResolver, "reconciler");
  }

  @Bean
  @ConditionalOnProperty(name = "scheduler.reconcile.enabled", havingValue = "true", matchIfMissing = true)
  ReconcileLoop reconcileLoop(Reconciler reconciler, LeaderElection leader) {
    return new ReconcileLoop(reconciler, leader);
  }

  public static final class ScanLoop {
    private static final Logger log = LoggerFactory.getLogger(ScanLoop.class);
    private final TriggerEngine engine;

    ScanLoop(TriggerEngine engine) {
      this.engine = engine;
    }

    @Scheduled(fixedDelayString = "${scheduler.loop.scan-delay-ms:5000}")
    public void tick() {
      try {
        engine.scanOnce();
      } catch (Throwable t) {
        log.warn("trigger scan loop tick failed; continuing next tick", t);
      }
    }
  }

  public static final class WorkLoop {
    private static final Logger log = LoggerFactory.getLogger(WorkLoop.class);
    private final ExecutorWorker worker;

    WorkLoop(ExecutorWorker worker) {
      this.worker = worker;
    }

    @Scheduled(fixedDelayString = "${scheduler.loop.work-delay-ms:100}")
    public void tick() {
      try {
        worker.workOne();
      } catch (Throwable t) {
        log.warn("executor work loop tick failed; continuing next tick", t);
      }
    }
  }

  /** 对账循环:leader 门控在 scan 之前(与 TriggerEngine 同),失败兜底不杀线程。 */
  public static final class ReconcileLoop {
    private static final Logger log = LoggerFactory.getLogger(ReconcileLoop.class);
    private final Reconciler reconciler;
    private final LeaderElection leader;

    ReconcileLoop(Reconciler reconciler, LeaderElection leader) {
      this.reconciler = reconciler;
      this.leader = leader;
    }

    @Scheduled(fixedDelayString = "${scheduler.reconcile.delay-ms:30000}")
    public void tick() {
      if (!leader.isLeader()) return;
      try {
        reconciler.scanOnce();
      } catch (Throwable t) {
        log.warn("reconcile loop tick failed; continuing next tick", t);
      }
    }
  }

  private static double dueQueueMaxAgeSeconds(JdbcTemplate jdbc, long taskId) {
    Double v = jdbc.queryForObject(
        "SELECT EXTRACT(EPOCH FROM (now() - COALESCE(MAX(created_at), now())))::float8 "
            + "FROM execution WHERE status='DUE' AND task_id=?", Double.class, taskId);
    return v == null ? 0.0 : v;
  }

  private static String defaultWorkerId() {
    try {
      return System.getProperty("os.name") + ":" + InetAddress.getLocalHost().getHostName();
    } catch (Throwable t) {
      log.warn("could not resolve hostname for worker-id, falling back to nano id", t);
      return "fallback:" + Long.toUnsignedString(System.nanoTime());
    }
  }
}
