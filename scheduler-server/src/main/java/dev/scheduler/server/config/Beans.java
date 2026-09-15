package dev.scheduler.server.config;

import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.JdbcDagRepository;
import dev.scheduler.persistence.JdbcExecutionRepository;
import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.JdbcWorkerRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.server.dag.DagEngine;
import dev.scheduler.server.leader.AdvisoryLockLeaderElection;
import dev.scheduler.server.leader.LeaderElection;
import dev.scheduler.server.reconcile.Reconciler;
import dev.scheduler.server.web.AvailableHandlerRefs;
import dev.scheduler.persistence.retry.FailureResolver;
import dev.scheduler.persistence.retry.RetryPolicy;
import dev.scheduler.server.trigger.TriggerEngine;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.time.Clock;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
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
  DagRepository dagRepository(JdbcTemplate jdbc) {
    return new JdbcDagRepository(jdbc);
  }

  /** M6:控制面读共享 DB 的 worker 注册表,供存活-refs 视图(findAllAlive)校验——M6.2 建/改任务时引用 worker 是否存活。 */
  @Bean
  WorkerRepository workerRepository(JdbcTemplate jdbc) {
    return new JdbcWorkerRepository(jdbc);
  }

  /** M6.3:控制面校验的可用 handlerRef = 存活 worker 注册表 refs 并集(纯 worker 视角);
   *  TaskController 建/改 + handlers 下拉框同源。 */
  @Bean
  AvailableHandlerRefs availableHandlerRefs(WorkerRepository workerRepository, Clock clock) {
    return new AvailableHandlerRefs(workerRepository, clock);
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

  @Bean
  DagEngine dagEngine(DagRepository dags, TaskRepository tasks, ShardRepository shards,
                      LeaderElection leader, Clock clock) {
    return new DagEngine(dags, tasks, shards, leader, clock);
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

  /**
   * 全局执行指标(§5.2):活跃 shard 数 + DUE 最深队龄。全局 gauge lazy 查 DB(scrape 时求值)→ 新建任务无需重启即计入,修正原 per-task 启动快照局限。
   * 口径:active 按 RUNNING shard 计数;队龄按 shard.queued_at(每次入队置 now,重试/重排不虚高)。
   */
  @Bean
  MeterBinder schedulerMetrics(ShardRepository shards) {
    return registry -> {
      Gauge.builder("scheduler_active_runs", () -> (double) shards.countActive()).register(registry);
      Gauge.builder("scheduler_due_queue_max_age_seconds", () -> (double) shards.maxDueQueueAgeSeconds()).register(registry);
    };
  }

  /** 活跃 DAG 批次(全部未终态 run 计数)。全局 gauge lazy 查 DB(scrape 时求值)→ 新建的 DAG 无需重启立即可见,修正原 per-dag 系列启动快照局限。 */
  @Bean
  MeterBinder dagMetrics(DagRepository dags) {
    return registry -> {
      Gauge.builder("scheduler_dag_runs_active", () -> (double) dags.countActiveRuns()).register(registry);
    };
  }

  /** M5.3 §1.5 全局指标:DLQ 深度(全局 gauge) + 存活 worker 数(读 M6.1 worker 心跳表)。
   *  死信计数 lazy 读 DB;worker 存活 = worker 表 last_seen 距今 ≤ 30s 且 status=ALIVE 的进程数(与 AvailableHandlerRefs 同窗),
   *  而非旧的"本进程是否持选主锁"(M6.1 拆执行到独立 worker 后,选主≠执行存活)。 */
  @Bean
  MeterBinder schedulerGlobalMetrics(ShardRepository shards, WorkerRepository workers, Clock clock) {
    return registry -> {
      Gauge.builder("scheduler_dlq_depth", () -> (double) shards.countDeadLetter()).register(registry);
      Gauge.builder("scheduler_worker_active",
          () -> (double) workers.findAllAlive(clock.instant().minus(WORKER_LIVE_WINDOW)).size())
          .register(registry);
    };
  }

  /** worker 存活判定窗口:last_seen 距今 ≤ 30s 视为存活(与 AvailableHandlerRefs 同窗)。 */
  private static final java.time.Duration WORKER_LIVE_WINDOW = java.time.Duration.ofSeconds(30);

  /**
   * V7 demo-sync 数据种子(运行时、默认关闭):仅当 {@code scheduler.demo.sync-seed-rows > 0} 且 demo_sync_source
   * 为空时一次性播种(幂等)。默认 0 → 测试与默认启动不灌数;开发栈由 start-dev.sh 设为 10000000,保留
   * 首次启动即灌千万行的演示能力,又不让每次全新 Testcontainers 测试库重灌拖慢套件。 */
  @Bean
  ApplicationRunner seedDemoSync(JdbcTemplate jdbc,
                                 @Value("${scheduler.demo.sync-seed-rows:0}") long rows) {
    return args -> {
      if (rows <= 0) return;
      Integer existing = jdbc.queryForObject("SELECT count(*) FROM demo_sync_source", Integer.class);
      if (existing != null && existing > 0) {
        log.info("demo_sync_source already has {} rows; skipping seed", existing);
        return;
      }
      log.info("seeding demo_sync_source with {} rows (demo sync)...", rows);
      long t0 = System.currentTimeMillis();
      jdbc.update("""
          INSERT INTO demo_sync_source
            (id, batch_no, acct_no, amount, quantity, rate, active, category, email, region, memo, check_flag, cfg_json, created_at, updated_at)
          SELECT gs,
                 (gs / 10000)::bigint,
                 'acct-' || lpad((gs % 500000)::text, 8, '0'),
                 (gs % 2000000 + 1)::numeric / 100,
                  gs % 5000,
                (gs % 1000)::double precision / 10,
                (gs % 2)::int = 1,
                chr(65 + (gs % 6)) || '-cat',
                'user' || (gs % 800000)::text || '@corp.example.com',
                CASE gs % 4 WHEN 0 THEN 'CN' WHEN 1 THEN 'US' WHEN 2 THEN 'EU' ELSE 'JP' END,
                'row-' || gs,
                 gs % 7,
                jsonb_build_object('seq', gs, 'mod97', gs % 97),
                now(), now()
          FROM generate_series(1::bigint, ?::bigint) AS gs""", rows);
      log.info("seeded demo_sync_source in {} ms", System.currentTimeMillis() - t0);
    };
  }

  /** 固定周期触发扫描;#5:每 tick 均兜底,DB 抖动只杀一拍不杀调度线程。 */
  @Bean
  @ConditionalOnProperty(name = "scheduler.loop.enabled", havingValue = "true", matchIfMissing = true)
  ScanLoop scanLoop(TriggerEngine engine) {
    return new ScanLoop(engine);
  }

  /** 对账器:单例,复用共享 FailureResolver(同一重试判定,worker 与 reconciler 无漂移)。M3:作用对象为 shard。 */
  @Bean
  Reconciler reconciler(TaskRepository tasks, ShardRepository shards,
                        FailureResolver failureResolver,
                        @Value("${scheduler.worker.stale-after-seconds:30}") int staleAfterSeconds) {
    return new Reconciler(tasks, shards, failureResolver, "reconciler", staleAfterSeconds);
  }

  @Bean
  @ConditionalOnProperty(name = "scheduler.reconcile.enabled", havingValue = "true", matchIfMissing = true)
  ReconcileLoop reconcileLoop(Reconciler reconciler, LeaderElection leader) {
    return new ReconcileLoop(reconciler, leader);
  }

  @Bean
  @ConditionalOnProperty(name = "scheduler.dag.enabled", havingValue = "true", matchIfMissing = true)
  DagLoop dagLoop(DagEngine engine) {
    return new DagLoop(engine);
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

  /** 对账循环:leader 门控在 scan 之前(与 TriggerEngine 同),失败兜底不杀线程。 */
  public static final class ReconcileLoop {
    private static final Logger log = LoggerFactory.getLogger(ReconcileLoop.class);
    private final Reconciler reconciler;
    private final LeaderElection leader;

    ReconcileLoop(Reconciler reconciler, LeaderElection leader) {
      this.reconciler = reconciler;
      this.leader = leader;
    }

    @Scheduled(fixedDelayString = "${scheduler.reconcile.delay-ms:15000}")
    public void tick() {
      if (!leader.isLeader()) return;
      try {
        reconciler.scanOnce();
      } catch (Throwable t) {
        log.warn("reconcile loop tick failed; continuing next tick", t);
      }
    }
  }

  /** DAG 调度循环:固定周期触发 DagEngine.scanOnce();失败兜底不杀线程(镜像 ScanLoop)。 */
  public static final class DagLoop {
    private static final Logger log = LoggerFactory.getLogger(DagLoop.class);
    private final DagEngine engine;

    DagLoop(DagEngine engine) {
      this.engine = engine;
    }

    @Scheduled(fixedDelayString = "${scheduler.dag.delay-ms:5000}")
    public void tick() {
      try {
        engine.scanOnce();
      } catch (Throwable t) {
        log.warn("dag scan loop tick failed; continuing next tick", t);
      }
    }
  }

  }
