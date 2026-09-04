package dev.scheduler.server.web;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.server.execute.ExecutorWorker;
import dev.scheduler.server.handler.ExecutionHandler;
import dev.scheduler.server.handler.HandlerContext;
import dev.scheduler.server.reconcile.Reconciler;
import dev.scheduler.server.trigger.TriggerEngine;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * 真 PG + 完整 Boot 上下文 + MockMvc 的端到端集成测试。
 *
 * <p>确定性策略:
 * <ol>
 *   <li>通过 {@code scheduler.loop.enabled=false} 关闭两个固定的 {@code @Scheduled} 循环 —— 消除异步
 *       扫描/执行线程对断言的竞态;故障容忍(@Scheduled 兜底 try/catch)在装配 bean 中由代码保证。</li>
 *   <li>{@code @TestConfiguration + @Primary} 把 {@link Clock} 换成可控 {@link MutableClock},钉在
 *       10:05:00Z —— 与配置的 6 字段 cron 恰为一个 tick 边界,使
 *       {@code TriggerEngine.scanOnce()} 确定性命中并登记一条 DUE,不依赖墙钟。</li>
 *   <li>触发扫描({@link TriggerEngine#scanOnce()})与执行({@link ExecutorWorker#workOne()})由测试
 *       同步直接驱动;选主由 Boot 装配的 {@code AdvisoryLockLeaderElection} Bean 持锁,scan 的
 *       leader 守卫恒真。</li>
 *   <li>每个用例 {@code @BeforeEach} 清库,互不影响。</li>
 * </ol>
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK,
    properties = {
        "scheduler.loop.enabled=false",
        "management.endpoints.web.exposure.include=health,info,prometheus",
        "management.prometheus.metrics.export.enabled=true"
    })
@AutoConfigureMockMvc
class ApiIntegrationTest {

  @Container static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine");

  private static final String CRON = "0 */5 * * * *"; // 6 字段(强制不外泄 5 字段)
  private static final Instant BASE = Instant.parse("2026-01-01T10:05:00Z");
  static final MutableClock CLOCK = new MutableClock(BASE);

  @DynamicPropertySource
  static void datasource(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", PG::getJdbcUrl);
    registry.add("spring.datasource.username", PG::getUsername);
    registry.add("spring.datasource.password", PG::getPassword);
  }

  @Autowired MockMvc mvc;
  @Autowired ObjectMapper objectMapper;
  @Autowired JdbcTemplate jdbc;
  @Autowired ShardRepository shards;
  @Autowired TriggerEngine triggerEngine;
  @Autowired ExecutorWorker executorWorker;
  /** 共享 Reconciler Bean(M3:回收孤儿 shard + 父级终态汇聚),由测试同步驱动以推进父终态。 */
  @Autowired Reconciler reconciler;
  /** 装配的 flaky handler 单例:"flaky" 任务首调抛错、次调成功;@BeforeEach 重装成 failNext 保持确定性。 */
  @Autowired FlakyHandler flakyHandler;

  /** 上下文启动(绑定时刻)前就存在的任务 id,用于断言 per-task 指标 series。 */
  private static long METRICS_TASK_ID;

  @TestConfiguration
  static class TestClockConfig {
    @Bean
    @Primary
    Clock testClock() {
      return ApiIntegrationTest.CLOCK;
    }

    /** 状态化测试 handler:"flaky" 任务首调抛错、次调成功。Beans#handlerRegistry 收集全部 ExecutionHandler
     *  Bean,故该实例被自动路由进共享 registry,worker 派发到的即本单例(测试字段与之同一实例)。 */
    @Bean
    FlakyHandler flakyHandler() {
      return new FlakyHandler();
    }
  }

  /**
   * 指标在上下文启动(绑定时刻)为当时的每个任务各注册一条 series;此后新建的任务要等下次重启才有
   * series(§5.2 已知重启边界)。因此这里在共享上下文创建前(静态 @BeforeAll,镜像 AbstractPostgresTest
   * 的 Flyway+裸 JDBC)先落一条 disabled 任务,保证其 tagged series 在绑定时刻确定存在。
   */
  @BeforeAll
  static void seedMetricsTaskBeforeContext() {
    Flyway.configure().dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()).load().migrate();
    // JdbcTemplate over DriverManagerDataSource 每次操作自借自还连接,无需(也不能)关闭 DataSource 本身。
    JdbcTemplate j = new JdbcTemplate(
        new DriverManagerDataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()));
      j.update("""
        INSERT INTO app_task (name, kind, handler_ref, cron, shard_count, timeout_seconds,
          max_retries, backoff_ms, max_active_concurrent, enabled, paused)
        VALUES (?, 'cron', 'demo', ?, 1, 300, 0, 1000, 1, false, false)""",
          "metrics-seed-task", "0 */5 * * * *");
      METRICS_TASK_ID = j.queryForObject("SELECT id FROM app_task WHERE name=?",
          Long.class, "metrics-seed-task");
  }

  @BeforeEach
  void resetDb() {
    jdbc.execute("TRUNCATE execution, execution_outcome, execution_shard, execution_shard_outcome,"
        + " app_task RESTART IDENTITY CASCADE");
    CLOCK.now = BASE;
    flakyHandler.failNext = true; // 每个用例从"下一调抛错"确定性起步
  }

  @Test
  void createTask_appearsInList_andGet() throws Exception {
    long id = postTask("create-list-task");

    mvc.perform(get("/api/v1/tasks"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[*].name", hasItem("create-list-task")));

    mvc.perform(get("/api/v1/tasks/" + id))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.name").value("create-list-task"));
  }

  @Test
  void manualTrigger_createsDueExecution() throws Exception {
    long id = postTask("trigger-task");

    MvcResult r = mvc.perform(post("/api/v1/tasks/" + id + "/trigger"))
        .andExpect(status().isCreated())
        .andReturn();
    long execId = objectMapper.readTree(r.getResponse().getContentAsString()).get("id").asLong();

    mvc.perform(get("/api/v1/executions"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[*].id", hasItem((int) execId)))
        .andExpect(jsonPath("$[?(@.id == " + execId + ")].status").value("DUE")); // worker 循环已关,保持 DUE
  }

  @Test
  void leadershipHeldScanOnce_thenWorkOne_reachesSuccess() throws Exception {
    long id = postTask("scan-success-task");

    // 持有领导权(装配的 advisory-lock Bean)时,同步扫描一次 → 命中 tick,登记父 + 1 DUE shard
    triggerEngine.scanOnce();
    long parentId = parentIdFor(id);
    assertEquals("DUE", parentStatus(parentId), "触发扇出 → 父 header DUE");
    assertEquals("DUE", shardStatus(parentId), "触发扇出 → 1 条 DUE shard");
    mvc.perform(get("/api/v1/executions"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[*].status", hasItem("DUE")));

    // 执行器工作一步:认领并运行已注册的 DemoHandler → 回写 shard SUCCESS;父仍 DUE(终态由 reconciler 汇聚)
    boolean processed = executorWorker.workOne();
    assertTrue(processed, "expected the seeded handling loop to claim and run one work item");
    assertEquals("SUCCESS", shardStatus(parentId), "workOne → shard 实时 SUCCESS");
    assertEquals("DUE", parentStatus(parentId), "worker 只写 shard,父 header 保持 DUE 直至 reconciler 汇聚");

    // 对账器扫描一次 → 父级终态汇聚 → 父 SUCCESS
    reconciler.scanOnce();
    assertEquals("SUCCESS", parentStatus(parentId), "reconciler 汇聚 → 父 SUCCESS");

    mvc.perform(get("/api/v1/executions").param("taskId", String.valueOf(id)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].status").value("SUCCESS"));
  }

  @Test
  void prometheus_shardMetrics() throws Exception {
    // 每用例 @BeforeEach 已 TRUNCATE app_task;重载 METRICS_TASK_ID 行使 FK 允许挂 execution/shard,
    // 以便确定性断言该绑定时刻已注册 series 的 gauge 采样值(指标口径切到 RUNNING shard 计数)。
    jdbc.update("INSERT INTO app_task (id, name, kind, handler_ref, cron, shard_count, enabled, paused)"
        + " VALUES (?, 'metrics-seed-task', 'cron', 'demo', ?, 1, false, false)",
        METRICS_TASK_ID, "0 */5 * * * *");
    long parentId = shards.createParentWithShards(METRICS_TASK_ID, "m:" + System.nanoTime(), 1).id();
    long shardId = shards.findShards(parentId).get(0).id();
    // 确定性直插 1 条有效租约 RUNNING shard → scheduler_active_runs 按 RUNNING shard 计数 = 1。
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w2',"
        + " lease_until=now() + interval '60 seconds' WHERE id=?", shardId);

    String prom = mvc.perform(get("/actuator/prometheus"))
        .andExpect(status().isOk())
        .andReturn().getResponse().getContentAsString();
    // 断言绑定时刻存活任务的 tagged series(task_id 标签存在),而非仅裸指标名。
    assertTrue(prom.contains("scheduler_active_runs{task_id=\"" + METRICS_TASK_ID + "\","),
        "missing per-task scheduler_active_runs{task_id=...} series in prometheus:\n" + prom);
    assertTrue(prom.contains("scheduler_due_queue_max_age_seconds{task_id=\"" + METRICS_TASK_ID + "\","),
        "missing per-task scheduler_due_queue_max_age_seconds{task_id=...} series in prometheus:\n" + prom);
    assertEquals(1.0, promGauge(prom, "scheduler_active_runs", METRICS_TASK_ID), 0.0,
        "scheduler_active_runs 应按 RUNNING shard 计数(1 条 RUNNING)");
  }

  @Test
  void cancelDue_setsCanceled() throws Exception {
    long id = postTask("cancel-task");
    long parentId = triggerParent(id); // 触发扇出 → 父(DUE) + 1 DUE shard,无 RUNNING
    long shardId = shards.findShards(parentId).get(0).id();

    mvc.perform(post("/api/v1/executions/" + parentId + "/cancel"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("CANCELED")); // 无 RUNNING → 直取消,派生父 CANCELED
    assertEquals("CANCELED", jdbc.queryForObject(
        "SELECT status FROM execution_shard WHERE id=?", String.class, shardId),
        "父直取消 → DUE shard 级联编 CANCELED");
    assertEquals("CANCELED", jdbc.queryForObject(
        "SELECT status FROM execution WHERE id=?", String.class, parentId));
  }

  @Test
  void cancelRunning_setsCancelRequestedFlag_returns202() throws Exception {
    long id = postTask("cancel-running-task");
    long parentId = triggerParent(id); // 父 header 只存 DUE
    long shardId = shards.findShards(parentId).get(0).id();
    // 确定性直插 RUNNING shard(父保持 DUE header):有 RUNNING 片 → 协作取消。
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w1',"
        + " lease_until=now() + interval '60 seconds' WHERE id=?", shardId);

    mvc.perform(post("/api/v1/executions/" + parentId + "/cancel"))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.status").value("RUNNING")); // 派生:父 DUE + ≥1 RUNNING shard → 读作 RUNNING
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT cancel_requested FROM execution_shard WHERE id=?", Boolean.class, shardId),
        "RUNNING shard → cancel 置 cancel_requested=true,供 worker 协作退出");
  }

  @Test
  void cancelSuccess_returnsConflict() throws Exception {
    long id = postTask("cancel-success-task");
    long parentId = shards.createParentWithShards(id, "succ-p:" + System.nanoTime(), 1).id();
    // 预置父 header 终态 SUCCESS(父只经汇聚 DUE → 终态;自此不可取消)。
    jdbc.update("UPDATE execution SET status='SUCCESS', worker_id='w1', finished_at=now() WHERE id=?",
        parentId);

    mvc.perform(post("/api/v1/executions/" + parentId + "/cancel"))
        .andExpect(status().isConflict()); // 终态父不可取消
  }

  @Test
  void dlqGetAndRequeue_roundTrip() throws Exception {
    long id = postTask("dlq-task");
    Shard dlq = seedDeadLetter(id); // 建父 + 1 FAILED dead_letter shard
    long shardId = dlq.id();

    // GET /dlq → 含该死信 shard(status FAILED,带父 id + shard_index)
    mvc.perform(get("/api/v1/executions/dlq"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[*].id", hasItem((int) shardId)))
        .andExpect(jsonPath("$[0].status").value("FAILED"))
        .andExpect(jsonPath("$[0].executionId").value(dlq.executionId().intValue()));

    // POST /shards/{id}/requeue → 200 + 现态(DUE, attempt=0)
    mvc.perform(post("/api/v1/executions/shards/" + shardId + "/requeue"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.id").value((int) shardId))
        .andExpect(jsonPath("$.status").value("DUE"));
    assertEquals(0, jdbc.queryForObject(
        "SELECT attempt FROM execution_shard WHERE id=?", Integer.class, shardId));
    assertEquals(Boolean.FALSE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=?", Boolean.class, shardId));

    // GET /dlq → 不再含它
    mvc.perform(get("/api/v1/executions/dlq"))
        .andExpect(status().isOk())
        .andExpect(result -> assertTrue(
            !new String(result.getResponse().getContentAsByteArray())
                .contains("\"id\":" + shardId),
            "requeue 后该 shard 不应再出现在 /dlq"));
    assertEquals(0, jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard WHERE id=? AND status='FAILED' AND dead_letter",
        Integer.class, shardId),
        "requeue 后该 shard 不再是死信");

    // workOne 可成功跑(requeue 后该 shard DUE 被认领 → shard SUCCESS)
    boolean processed = executorWorker.workOne();
    assertTrue(processed, "requeued shard should be claimable by the worker");
    assertEquals("SUCCESS", jdbc.queryForObject(
        "SELECT status FROM execution_shard WHERE id=?", String.class, shardId));
  }

  @Test
  void requeueNonFailed_returnsConflict() throws Exception {
    long id = postTask("requeue-conflict-task");
    long parentId = shards.createParentWithShards(id, "req-p:" + System.nanoTime(), 1).id();
    long shardId = shards.findShards(parentId).get(0).id();
    jdbc.update("UPDATE execution_shard SET status='SUCCESS' WHERE id=?", shardId);

    mvc.perform(post("/api/v1/executions/shards/" + shardId + "/requeue"))
        .andExpect(status().isConflict());
    assertEquals("SUCCESS", jdbc.queryForObject(
        "SELECT status FROM execution_shard WHERE id=?", String.class, shardId),
        "非 FAILED 的 requeue 不得改动行");
  }

  @Test
  void requeueMissingShard_returnsNotFound() throws Exception {
    mvc.perform(post("/api/v1/executions/shards/999999/requeue"))
        .andExpect(status().isNotFound());
  }

  /** 详情 = 父 header + 其 shard,派生父 status(非终态父 + RUNNING shard → 读作 RUNNING),且只读不写库。 */
  @Test
  void getExecutionDetail_returnsParentAndShards_withDerivedRunningStatus() throws Exception {
    long id = postTask("detail-task");
    long parentId = triggerParent(id);
    long shardId = shards.findShards(parentId).get(0).id();
    // 预置 1 RUNNING shard → 派生父 RUNNING(父 header 只存 DUE)
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w1',"
        + " lease_until=now() + interval '60 seconds' WHERE id=?", shardId);

    mvc.perform(get("/api/v1/executions/" + parentId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.id").value((int) parentId))
        .andExpect(jsonPath("$.status").value("RUNNING")) // 派生:父 DUE + RUNNING shard
        .andExpect(jsonPath("$.shardCount").value(1))
        .andExpect(jsonPath("$.shards.length()").value(1));
    assertEquals("DUE", jdbc.queryForObject(
        "SELECT status FROM execution WHERE id=?", String.class, parentId),
        "派生 RUNNING 仅读取映射,不写库:父仍存 DUE header");
  }

  /** M2 重试闭环(DB 时钟控闸):flaky 首调抛错 → 重试落 DUE + next_retry_at;拨后 gate 排除,调回过去重现。 */
  @Test
  void retryAfterFailure_thenBackoffGate_thenRerunReachesSuccess() throws Exception {
    long id = postTaskWithRetry("retry-task", "flaky", 1, 1000, ""); // maxRetries=1, pattern 空=恒可重试
    long parentId = triggerParent(id);
    long shardId = shards.findShards(parentId).get(0).id();

    // 第 1 次:flaky 首调抛 RuntimeException(the generic failure,非 CancellationException)→ 进重试路径
    assertTrue(executorWorker.workOne(), "第 1 次 workOne 应认领 shard(触发 DUE)");
    assertEquals("DUE", jdbc.queryForObject("SELECT status FROM execution_shard WHERE id=?", String.class, shardId),
        "maxRetries=1 → 首失败可重试,shard 回到 DUE 待再次认领");
    assertEquals("DUE", parentStatus(parentId), "worker 只写 shard,父 header 保持 DUE");
    // 注入时钟钉在 2026-01-01(远早于 DB now()),scheduleRetry 写回的 next_retry_at 由注入时钟推得;
    // 由 DB now() 维度断言其在 shard 落一条 DUE outcome 即可证明重试已调度(不靠墙钟断言 next_retry_at 绝对值)。
    assertTrue(jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='DUE'",
        Long.class, shardId) > 0, "重试必须在 shard 落 DUE outcome");

    // 证闸:把 shard 的 next_retry_at 拨到 DB now() 之后 1 小时 → findCandidate 的 now() 门栅排除,无候选可认领。
    jdbc.update("UPDATE execution_shard SET next_retry_at = now() + interval '1 hour' WHERE id=?", shardId);
    assertTrue(!executorWorker.workOne(), "未来闸 → 候选被门栅排除,workOne 不得认领任何 shard");
    assertEquals("DUE", jdbc.queryForObject("SELECT status FROM execution_shard WHERE id=?", String.class, shardId),
        "被未来闸挡下 → shard 仍 DUE,未被重认领/回写");

    // 调回过去:next_retry_at 早于 now() 1 秒 → 候选放行;flaky 现次调成功(已消耗首败)→ shard SUCCESS。
    jdbc.update("UPDATE execution_shard SET next_retry_at = now() - interval '1 second' WHERE id=?", shardId);
    assertTrue(executorWorker.workOne(), "过去闸 → 候选放行,workOne 应再次认领并跑成功");
    assertEquals("SUCCESS", jdbc.queryForObject("SELECT status FROM execution_shard WHERE id=?", String.class, shardId),
        "重试后 rerun → handler 次调成功 → shard SUCCESS");
  }

  /** M2 DLQ 闭环(真失败 → 死信而非预置 FAILED):真实耗尽失败落 DLQ,requeue 后再跑转 SUCCESS。 */
  @Test
  void exhaustedFailure_landsInDlq_thenRequeueRunsSucceeds() throws Exception {
    long id = postTaskWithRetry("dlq-real-task", "flaky", 0, 1000, ""); // maxRetries=0 → 首失败即耗尽
    long parentId = triggerParent(id);
    long shardId = shards.findShards(parentId).get(0).id();

    // flaky 首调抛错 → attempt 1 > maxRetries 0 → 不可重试 → shard FAILED + 死信
    assertTrue(executorWorker.workOne(), "第 1 次 workOne 应认领 shard 并让 flaky 失败");
    assertEquals("FAILED", jdbc.queryForObject("SELECT status FROM execution_shard WHERE id=?",
        String.class, shardId), "耗尽失败 → shard FAILED");
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=?", Boolean.class, shardId),
        "耗尽失败 → dead_letter=true");

    // GET /dlq 含它(shard 口径,真实 FAILED 死信,非 seedDeadLetter 预置)
    mvc.perform(get("/api/v1/executions/dlq"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[*].id", hasItem((int) shardId)))
        .andExpect(jsonPath("$[?(@.id == " + shardId + ")].status").value("FAILED"));

    // requeue → 200 + DUE;flaky 已消耗首败,次调成功 → workOne → shard SUCCESS
    mvc.perform(post("/api/v1/executions/shards/" + shardId + "/requeue"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("DUE"));
    assertTrue(executorWorker.workOne(), "requeue 后之 shard 应被 worker 认领并跑成功");
    assertEquals("SUCCESS", jdbc.queryForObject("SELECT status FROM execution_shard WHERE id=?",
        String.class, shardId), "requeue 后 rerun → flaky 次调成功 → shard SUCCESS");

    // 父终态 = reconciler 汇聚
    reconciler.scanOnce();
    assertEquals("SUCCESS", parentStatus(parentId), "reconciler 汇聚 → 父 SUCCESS");
  }

  /** M2 协作取消:worker 认领时前置检查 cancel_requested → CANCELED(恒真的 worker 侧验证,走真实 Boot 装配
   *  handler/db/worker,非 SQL 直置终态)。API requestCancel 仅对 RUNNING 置位(已由既有 cancelRunning_* 覆盖),
   *  此处直接在 DUE 行上写标志,使认领+运行前检查这段真实路径被驱动。 */
  @Test
  void workerReclaimsCancelRequestedDue_asCanceled() throws Exception {
    long id = postTask("cancel-coop-task"); // handlerRef=demo 即可,取消发生在运行 handler 之前
    long parentId = triggerParent(id);
    long shardId = shards.findShards(parentId).get(0).id();

    // 直接 DB 在 DUE shard 上置 cancel_requested:没有 RUNNING 兄弟可被 API requestCancel 置位(已另有
    // cancelRunning_* 端到端用例),此处模拟"已请求取消"的 DUE shard,使 workOne 认领后运行前检查命中。
    jdbc.update("UPDATE execution_shard SET cancel_requested=true WHERE id=?", shardId);

    assertTrue(executorWorker.workOne(), "带 cancel_requested 的 DUE shard 候选应被认领(运行前取消也属处理)");
    assertEquals("CANCELED", jdbc.queryForObject(
        "SELECT status FROM execution_shard WHERE id=?", String.class, shardId),
        "运行前检查命中取消请求 → shard CANCELED(cancelled before run)");
    assertEquals("DUE", jdbc.queryForObject(
        "SELECT status FROM execution WHERE id=?", String.class, parentId),
        "worker 只写 shard,父 header 保持 DUE");
    assertTrue(jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='CANCELED'",
        Long.class, shardId) > 0, "协作取消必须在 shard 落 CANCELED outcome");
  }

  /** M3 并发扇出端到端:shardCount=3 任务,三次 workOne → 三 shard 全 SUCCESS,父经 reconciler 汇聚为 SUCCESS;
   *  详情(父 + 全 3 shard)完整呈现扇出形状。 */
  @Test
  void craftShardFanOut_allSuccess_parentSuccess() throws Exception {
    long id = postTaskWithRetry("fanout-task", "demo", 0, 1000, null, 3); // shardCount=3, demo 恒成功
    triggerEngine.scanOnce(); // 命中 tick → 父 + 3 DUE shard
    long parentId = parentIdFor(id);
    List<Shard> seeded = shards.findShards(parentId);
    assertEquals(3, seeded.size(), "扇出 → 父 + 3 DUE shard");
    for (Shard s : seeded) {
      assertEquals("DUE", s.status().name(), "扇出后 shard 全 DUE");
    }
    assertEquals("DUE", parentStatus(parentId), "父 header 只存 DUE");

    // 三次 workOne → 三 shard 全 SUCCESS,父仍 DUE(worker 不写父)
    for (int i = 0; i < 3; i++) {
      assertTrue(executorWorker.workOne(), "第 " + (i + 1) + " 次 workOne 应认领一枚 DUE shard");
    }
    List<Shard> done = shards.findShards(parentId);
    for (Shard s : done) {
      assertEquals("SUCCESS", s.status().name(), "全部 shard → SUCCESS");
    }
    assertEquals("DUE", parentStatus(parentId), "父终态由 reconciler 汇聚,worker 不写父");

    reconciler.scanOnce();
    assertEquals("SUCCESS", parentStatus(parentId), "reconciler 汇聚 → 父 SUCCESS");

    // GET /executions/{id} 详情:父 + 全 3 shard
    mvc.perform(get("/api/v1/executions/" + parentId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.id").value((int) parentId))
        .andExpect(jsonPath("$.status").value("SUCCESS"))
        .andExpect(jsonPath("$.shardCount").value(3))
        .andExpect(jsonPath("$.shards.length()").value(3));
  }

  /** M3 FAIL_FAST 端到端:shardCount=3,maxRetries=0,handler=flaky。首个被认领的 shard(按 id 升序,即 shard0)
   *  首调耗尽失败 → DLQ → cancelSiblings 将两条 DUE 兄弟直编 CANCELED(+outcome);父经 reconciler 汇聚 → FAILED。 */
  @Test
  void failFast_endToEnd_oneShardFails_cancelsSiblings_parentFailed() throws Exception {
    long id = postTaskWithRetry("failfast-task", "flaky", 0, 1000, null, 3); // shardCount=3, maxRetries=0
    long parentId = triggerParent(id); // 手动触发扇出 → 父 + 3 DUE shard
    assertEquals(3, shards.findShards(parentId).size());

    // 首次 workOne 认领首个 DUE(按 id 升序,即 shard0)→ flaky 首调抛错 → 耗尽 FAILED + DLQ → FAIL_FAST
    assertTrue(executorWorker.workOne(), "首个 workOne 应认领并耗尽失败");

    List<Shard> ss = shards.findShards(parentId);
    long failedCnt = ss.stream().filter(s -> s.status() == ExecutionStatus.FAILED).count();
    long canceledCnt = ss.stream().filter(s -> s.status() == ExecutionStatus.CANCELED).count();
    assertEquals(1, failedCnt, "恰好一条 shard FAILED");
    assertEquals(2, canceledCnt, "FAIL_FAST 将两条 DUE 兄弟直编 CANCELED");

    // 唯一 FAILED 死信(shard0)在 /dlq
    mvc.perform(get("/api/v1/executions/dlq"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.length()").value(1));

    // 两条 CANCELED 兄弟各落 CANCELED outcome(FAIL_FAST detail=fail_fast)
    List<Shard> canceled = ss.stream().filter(s -> s.status() == ExecutionStatus.CANCELED).toList();
    for (Shard s : canceled) {
      assertEquals(1L, jdbc.queryForObject(
          "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='CANCELED'",
          Long.class, s.id()), "FAIL_FAST 取消的兄弟必须落 CANCELED outcome");
    }
    assertEquals("fail_fast", jdbc.queryForObject(
        "SELECT detail FROM execution_shard_outcome WHERE shard_id=? AND status='CANCELED'"
            + " ORDER BY id LIMIT 1", String.class, canceled.get(0).id()),
        "DUE 兄弟由 FAIL_FAST 取消,detail=fail_fast");

    // 父仍 DUE:全部兄弟已终态,但父终态由 reconciler 汇聚(any-FAILED → 父 FAILED)
    assertEquals("DUE", parentStatus(parentId), "汇聚前父保持 DUE");

    reconciler.scanOnce();
    assertEquals("FAILED", parentStatus(parentId), "any-FAILED 且全部终态 → 父 FAILED");
  }

  /** M3 父级取消级联:shardCount=3,无 RUNNING shard → cancelParentImmediate 直取消 → 父 CANCELED + 全 DUE shard 级联 CANCELED。 */
  @Test
  void cooperativeCancel_parentLevel_cascadesToShards() throws Exception {
    long id = postTaskWithRetry("cancel-parent-task", "demo", 0, 1000, null, 3); // shardCount=3
    long parentId = triggerParent(id); // 父 + 3 DUE shard,无 RUNNING
    assertEquals(3, shards.findShards(parentId).size());

    // 无 RUNNING shard → cancelParentImmediate 直取消 → 200 + 派生 CANCELED,全部 shard 级联 CANCELED
    mvc.perform(post("/api/v1/executions/" + parentId + "/cancel"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("CANCELED"));
    assertEquals("CANCELED", parentStatus(parentId), "父直取消 → 父 CANCELED");
    assertEquals(3, shards.findShards(parentId).stream()
        .filter(s -> s.status() == ExecutionStatus.CANCELED).count(),
        "父直取消 → 全部 DUE shard 级联 CANCELED");
  }

  /** 触发一次任务(扇出 → 父 header + N 个 DUE shard,shard_count 默认 1),返回父 execution id。 */
  private long triggerParent(long taskId) throws Exception {
    MvcResult r = mvc.perform(post("/api/v1/tasks/" + taskId + "/trigger"))
        .andExpect(status().isCreated()).andReturn();
    return objectMapper.readTree(r.getResponse().getContentAsString()).get("id").asLong();
  }

  /** 确定性预置一条死信 shard:建父 + 1 DUE shard(经 createParentWithShards 单事务),再直改 FAILED + dead_letter。
   *  返回该 shard(带 executionId/父 id)。邻类 ExecutorWorkerTest/ReconcilerTest 用同法(JdbcShardRepository + SQL 变更器)。 */
  private Shard seedDeadLetter(long taskId) {
    long parentId = shards.createParentWithShards(taskId, "dlq-p:" + System.nanoTime(), 1).id();
    long sid = shards.findShards(parentId).get(0).id();
    jdbc.update("UPDATE execution_shard SET status='FAILED' WHERE id=?", sid);
    jdbc.update("UPDATE execution_shard SET dead_letter=true WHERE id=?", sid);
    return shards.findShard(sid).orElseThrow();
  }

  private long postTask(String name) throws Exception {
    return postTaskWithRetry(name, "demo", 0, 1000, null);
  }

  /** 建任务(带重试参数,shard_count 默认 1):maxRetries/backoffMs/retryableFailurePattern;pattern 传 null 归一为空串(恒可重试)。 */
  private long postTaskWithRetry(String name, String handlerRef, int maxRetries, long backoffMs,
                                 String pattern) throws Exception {
    return postTaskWithRetry(name, handlerRef, maxRetries, backoffMs, pattern, 1);
  }

  /** 建任务(带重试参数 + shardCount):扇出 E2E 用它物化 shard_count=3。maxActiveConcurrent 随 shardCount 抬升,免除串行认领配额争用。 */
  private long postTaskWithRetry(String name, String handlerRef, int maxRetries, long backoffMs,
                                 String pattern, int shardCount) throws Exception {
    String body = "{\"name\":\"" + name + "\",\"kind\":\"cron\",\"handlerRef\":\"" + handlerRef + "\","
        + "\"cron\":\"" + CRON + "\",\"shardCount\":" + shardCount + ",\"maxRetries\":" + maxRetries
        + ",\"backoffMs\":" + backoffMs + ",\"retryableFailurePattern\":\""
        + (pattern == null ? "" : pattern) + "\",\"maxActiveConcurrent\":" + Math.max(1, shardCount) + "}";
    MvcResult r = mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isCreated())
        .andReturn();
    return objectMapper.readTree(r.getResponse().getContentAsString()).get("id").asLong();
  }

  /** 按任务取唯一父 execution id(单父场景:即指定任务本用例只触发一次)。 */
  private long parentIdFor(long taskId) {
    return jdbc.queryForObject("SELECT id FROM execution WHERE task_id=?", Long.class, taskId);
  }

  /** 父 execution 行存态(M3:父只 DUE→终态,由 reconciler 汇聚,从不存 RUNNING)。 */
  private String parentStatus(long parentId) {
    return jdbc.queryForObject("SELECT status FROM execution WHERE id=?", String.class, parentId);
  }

  /** 单 shard 父下该 shard 的现态(shard_count=1 场景用)。 */
  private String shardStatus(long parentId) {
    return jdbc.queryForObject(
        "SELECT status FROM execution_shard WHERE execution_id=?", String.class, parentId);
  }

  /** 解析 prometheus 文本中某 per-task gauge 系列行尾的采样值(ruling R2 口径即经此断言 shard 计数)。 */
  private double promGauge(String prom, String metric, long taskId) {
    for (String line : prom.split("\n")) {
      if (line.startsWith(metric + "{task_id=\"" + taskId + "\"")) {
        int sp = line.lastIndexOf(' ');
        return Double.parseDouble(line.substring(sp + 1));
      }
    }
    return Double.NaN;
  }

  /** M2 状态化测试 handler:"flaky" 任务首调抛普通异常(走重试/死信路径,非 CancellationException),次调成功。 */
  static class FlakyHandler implements ExecutionHandler {
    boolean failNext = true;

    @Override public String ref() { return "flaky"; }

    @Override public void handle(HandlerContext ctx) {
      if (failNext) {
        failNext = false; // 一次性失败:消耗后即恢复成功
        throw new RuntimeException("flaky fail");
      }
      // no-op success on subsequent runs
    }
  }

  /** 可复写的皮时钟:instant 由测试控制,getZone 固定 UTC。 */
  static final class MutableClock extends Clock {
    Instant now;

    MutableClock(Instant now) {
      this.now = now;
    }

    @Override public Instant instant() { return now; }
    @Override public ZoneId getZone() { return ZoneOffset.UTC; }
    @Override public Clock withZone(ZoneId zone) { return this; }
  }
}
