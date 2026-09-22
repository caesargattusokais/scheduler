package dev.scheduler.server.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.core.OperatorRole;
import dev.scheduler.persistence.AuditRepository;
import dev.scheduler.persistence.AuthRepository;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.JdbcAuditRepository;
import dev.scheduler.persistence.JdbcDagRepository;
import dev.scheduler.persistence.JdbcExecutionRepository;
import dev.scheduler.persistence.JdbcOperatorRepository;
import dev.scheduler.persistence.JdbcAuthRepository;
import dev.scheduler.persistence.JdbcNotificationRepository;
import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.JdbcWorkerRepository;
import dev.scheduler.persistence.NotificationRepository;
import dev.scheduler.persistence.OperatorRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.server.dag.DagEngine;
import dev.scheduler.server.leader.AdvisoryLockLeaderElection;
import dev.scheduler.server.leader.LeaderElection;
import dev.scheduler.server.reconcile.Reconciler;
import dev.scheduler.server.service.AuditRecorder;
import dev.scheduler.server.service.AuditRetentionService;
import dev.scheduler.server.service.NotificationDispatcher;
import dev.scheduler.server.service.NotificationFirer;
import dev.scheduler.server.service.NotificationHub;
import dev.scheduler.server.service.NotificationProperties;
import dev.scheduler.server.service.OperatorPasswordService;
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
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.client.RestTemplate;

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
  AuditRepository auditRepository(JdbcTemplate jdbc) {
    return new JdbcAuditRepository(jdbc);
  }

  /** 操作者目录:写端点授权拦截器读白名单/角色的数据源。 */
  @Bean
  OperatorRepository operatorRepository(JdbcTemplate jdbc) {
    return new JdbcOperatorRepository(jdbc);
  }

  /** 强认证会话仓储:口令改/停用撤销会话、Task3 登录/锁退避共用。 */
  @Bean
  AuthRepository authRepository(JdbcTemplate jdbc) {
    return new JdbcAuthRepository(jdbc);
  }

  /**
   * 操作者引导:启动时把 {@code scheduler.operators}(形如 {@code alice:ADMIN,bob:OPERATOR})幂等 upsert 进目录,
   * 保证目录恒有可登记的 ADMIN(否则第一次部署建不出管家)。属性为空 → no-op(测试逐个播种)。
   * 实现为 {@link Ordered}(order 1),确保先于 seedOperatorPasswords(order 2)执行:Spring 的
   * ApplicationRunner 排序只读 bean 对象类上的摘要/Ordered,不读 @Bean 工厂方法上的 @Order。 */
  @Bean
  ApplicationRunner seedOperators(OperatorRepository operators,
                                  @Value("${scheduler.operators:}") String spec) {
    return new OrderedApplicationRunner(1, args -> {
      if (spec == null || spec.isBlank()) return;
      for (String pair : spec.split(",")) {
        String[] kv = pair.trim().split(":", 2);
        if (kv.length != 2 || kv[0].isBlank()) {
          log.warn("skip malformed scheduler.operators entry: '{}'", pair);
          continue;
        }
        OperatorRole role;
        try {
          role = OperatorRole.valueOf(kv[1].trim().toUpperCase());
        } catch (IllegalArgumentException e) {
          log.warn("skip entry '{}' with unknown role '{}'", kv[0].trim(), kv[1].trim());
          continue;
        }
        operators.upsert(kv[0].trim(), role, true);
      }
    });
  }

  /**
   * 操作者默认口令引导:读取 {@code scheduler.operators.default-password},对当前无口令(password_hash IS NULL)
   * 的操作者经 OperatorPasswordService.bootstrap 应用 BCrypt 默认口令(不覆盖已设口令)。属性为空 → no-op。
   * 实现为 {@link Ordered}(order 2),保证在 seedOperators(order 1)已 upsert 目录后才对无密者应用默认口令。 */
  @Bean
  ApplicationRunner seedOperatorPasswords(OperatorRepository ops, OperatorPasswordService svc,
                                          @Value("${scheduler.operators.default-password:}") String defaultPwd) {
    return new OrderedApplicationRunner(2, args -> {
      if (defaultPwd == null || defaultPwd.isBlank()) return;
      for (String n : ops.namesWithoutPassword()) svc.bootstrap(n, defaultPwd);
    });
  }

  @Bean
  AuditRecorder auditRecorder(AuditRepository audits, ObjectMapper json) {
    return new AuditRecorder(audits, json);
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
    return new TriggerEngine(tasks, execs, shards, leader, clock, 200); // §4 单 tick 批上限(5s 周期 ×200)
  }

  @Bean
  DagEngine dagEngine(DagRepository dags, TaskRepository tasks, ShardRepository shards,
                      LeaderElection leader, Clock clock) {
    return new DagEngine(dags, tasks, shards, leader, clock, 200); // §4 单 tick 批上限(5s 周期 ×200,触发/传播各 200)
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

  /** SLI(2b):执行服务水平指标——近 1h 完成/失败量、失败率、近 24h 完成延迟 p95。全部 lazy 查 DB(scrape 时求值),
   *  与 {@link #schedulerMetrics} 同款惰性 gauge;失败率在已完成总量(完成+失败)上算,无样本→0。 */
  @Bean
  MeterBinder schedulerSliMetrics(ExecutionRepository executions) {
    return registry -> {
      Gauge.builder("scheduler_execution_completed_1h",
          () -> (double) executions.sli().completed1h()).register(registry);
      Gauge.builder("scheduler_execution_failed_1h",
          () -> (double) executions.sli().failed1h()).register(registry);
      Gauge.builder("scheduler_execution_latency_p95_ms",
          () -> executions.sli().p95LatencyMs()).register(registry);
      Gauge.builder("scheduler_execution_failure_rate_1h", () -> {
        var sli = executions.sli();
        double done = sli.completed1h() + sli.failed1h();
        return done == 0 ? 0.0 : sli.failed1h() / done;
      }).register(registry);
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
                        FailureResolver failureResolver, NotificationFirer firer,
                        @Value("${scheduler.worker.stale-after-seconds:30}") int staleAfterSeconds) {
    return new Reconciler(tasks, shards, failureResolver, firer, "reconciler", staleAfterSeconds);
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

  /** 审计保留循环:leader 门控,按 {@code scheduler.audit.retention-days} 定期归档过期审计行。
   *  retention-days<=0 → 不归档(不开策略)。operator 以 'retention' 占位,追记一条 audit.archive。 */
  @Bean
  @ConditionalOnProperty(name = "scheduler.audit.retention.enabled", havingValue = "true", matchIfMissing = true)
  AuditRetentionLoop auditRetentionLoop(AuditRetentionService retention, LeaderElection leader,
                                        @Value("${scheduler.audit.retention-days:0}") int retentionDays) {
    return new AuditRetentionLoop(retention, leader, retentionDays);
  }

  /** 通知底座:outbox 仓储。当期 fire 落库由 NotificationHub(非阻断)驱动,投递由 NotificationLoop 拉取。 */
  @Bean
  NotificationRepository notificationRepository(JdbcTemplate jdbc) {
    return new JdbcNotificationRepository(jdbc);
  }

  /** 通知投递专用 RestTemplate(命名注入,避免与未来其它 Rest 客户端混淆):超时防止慢/挂水滴端点挂死投递循环。 */
  @Bean
  RestTemplate notificationRestTemplate() {
    SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
    f.setConnectTimeout(2000);
    f.setReadTimeout(10000);
    return new RestTemplate(f);
  }

  /** 通知入点半:kernel 事件 fire 落库 outbox(非阻断,入库失败仅告警不打断调用方)。 */
  @Bean
  NotificationHub notificationHub(NotificationRepository notifications, ObjectMapper json) {
    return new NotificationHub(notifications, json);
  }

  /** 通知投递器:foreground 拉取到期行、HMAC 签名投递、退避/重试/终态。由 NotificationLoop 周期驱动。 */
  @Bean
  NotificationDispatcher notificationDispatcher(NotificationRepository notifications,
                                                NotificationProperties props,
                                                RestTemplate notificationRestTemplate,
                                                ObjectMapper json) {
    return new NotificationDispatcher(notifications, props, notificationRestTemplate, json);
  }

  /** 通知投递循环:leader 门控,周期把到期通知投给已订阅 webhook;失败兜底不杀线程。webhooks 空 → no-op。 */
  @Bean
  @ConditionalOnProperty(name = "scheduler.notifications.enabled", havingValue = "true", matchIfMissing = true)
  NotificationLoop notificationLoop(NotificationDispatcher dispatcher, LeaderElection leader) {
    return new NotificationLoop(dispatcher, leader);
  }

  /** ApplicationRunner + Ordered:使启动引导按 order 升序确定性执行(Spring 的 runner 排序读对象类的
   *  Ordered/@Order,不读 @Bean 工厂方法注解——lambda 无法承载得靠实体包装)。 */
  private static final class OrderedApplicationRunner implements ApplicationRunner, Ordered {
    private final int order;
    private final ApplicationRunner delegate;

    OrderedApplicationRunner(int order, ApplicationRunner delegate) {
      this.order = order;
      this.delegate = delegate;
    }

    @Override public int getOrder() { return order; }

    @Override public void run(ApplicationArguments args) throws Exception { delegate.run(args); }
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

  /** 审计保留循环:按 retention-days 归档 occurred_at 早于 now-days 的行,单次 1000 条。leader 门控 + 失败兜底。 */
  public static final class AuditRetentionLoop {
    private static final Logger log = LoggerFactory.getLogger(AuditRetentionLoop.class);
    private final AuditRetentionService retention;
    private final LeaderElection leader;
    private final int retentionDays;

    AuditRetentionLoop(AuditRetentionService retention, LeaderElection leader, int retentionDays) {
      this.retention = retention;
      this.leader = leader;
      this.retentionDays = retentionDays;
    }

    @Scheduled(fixedDelayString = "${scheduler.audit.retention.delay-ms:3600000}")
    public void tick() {
      if (retentionDays <= 0) return;
      if (!leader.isLeader()) return;
      try {
        retention.archiveOlderThan("retention",
            java.time.Instant.now().minus(java.time.Duration.ofDays(retentionDays)), 1000);
      } catch (Throwable t) {
        log.warn("audit retention tick failed; continuing next tick", t);
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

  /** 通知投递循环:leader 门控,周期把到期通知投给已订阅 webhook;失败兜底不杀线程(镜像 ReconcileLoop)。 */
  public static final class NotificationLoop {
    private static final Logger log = LoggerFactory.getLogger(NotificationLoop.class);
    private final NotificationDispatcher dispatcher;
    private final LeaderElection leader;

    NotificationLoop(NotificationDispatcher dispatcher, LeaderElection leader) {
      this.dispatcher = dispatcher;
      this.leader = leader;
    }

    @Scheduled(fixedDelayString = "${scheduler.notifications.dispatch-delay-ms:1000}")
    public void tick() {
      if (!leader.isLeader()) return;
      try {
        dispatcher.dispatchOnce();
      } catch (Throwable t) {
        log.warn("notification dispatch tick failed; continuing next tick", t);
      }
    }
  }

  }
