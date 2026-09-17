package dev.scheduler.server.web;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.WorkerRegistration;
import dev.scheduler.persistence.WorkerRepository;
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
 *   <li>触发扫描({@link TriggerEngine#scanOnce()})由测试同步直接驱动;选主由 Boot 装配的
 *       {@code AdvisoryLockLeaderElection} Bean 持锁,scan 的 leader 守卫恒真。M6.3 已无进程内执行器,
 *       服务器测试只断言控制面;执行平面语义收归 worker 模块测试。</li>
 *   <li>每个用例 {@code @BeforeEach} 清库,互不影响。</li>
 * </ol>
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK,
    properties = {
        "scheduler.loop.enabled=false",
        "scheduler.dag.enabled=false", // 关闭 DagLoop,消除异步扫描对同步驱动断言(dagEngine.scanOnce)的竞态
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
  /** M6:装配的 worker 表读写 bean——控制面据此读共享 DB 的存活 worker 注册视图(refs),供 M6.2 校验。 */
  @Autowired WorkerRepository workerRepository;
  @Autowired TriggerEngine triggerEngine;
  /** 共享 Reconciler Bean(M3:回收孤儿 shard + 父级终态汇聚),由测试同步驱动以推进父终态。 */
  @Autowired Reconciler reconciler;
  /** M4:工作流 DAG 引擎,由测试同步驱动 scanOnce() 推进节点 spawn/终态派生(镜像 triggerEngine)。 */
  @Autowired dev.scheduler.server.dag.DagEngine dagEngine;
  /** M4:工作流 DAG 仓储(建 DAG/触发,以及指标断言复用)。 */
  @Autowired dev.scheduler.persistence.DagRepository dagRepository;
  /** M5.3 §1.5:选主锁持有者,worker 活性 gauge(scheduler_worker_active)判据。 */
  @Autowired dev.scheduler.server.leader.LeaderElection leader;

  /** 上下文启动(绑定时刻)前就存在的任务 id,用于断言 per-task 指标 series。 */
  private static long METRICS_TASK_ID;
  /** 上下文启动(绑定时刻)前就存在的 DAG id(M4:断言 per-dag scheduler_dag_runs_active series)。 */
  private static long METRICS_DAG_ID;

  @TestConfiguration
  static class TestClockConfig {
    @Bean
    @Primary
    Clock testClock() {
      return ApiIntegrationTest.CLOCK;
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
      // M4:同样在绑定时刻前落一条(引用新指标任务的)PENDING 可达 DAG,保证 scheduler_dag_runs_active 的
      // tagged series 在上下文绑定那一刻确定存在(镜像 per-task 的重启边界)。disabled 以免被扫描误触发。
      j.update("""
        INSERT INTO app_dag (id, name, cron, enabled, paused)
        VALUES (?, 'metrics-seed-dag', ?, false, false)""",
          Long.MAX_VALUE / 2, "0 */5 * * * *");
      METRICS_DAG_ID = j.queryForObject("SELECT id FROM app_dag WHERE name=?",
          Long.class, "metrics-seed-dag");
      j.update("INSERT INTO app_dag_node (dag_id, node_key, task_id, sort_order) VALUES (?, 'A', ?, 0)",
          METRICS_DAG_ID, METRICS_TASK_ID);
  }

  @BeforeEach
  void resetDb() {
    jdbc.execute("TRUNCATE app_dag CASCADE; TRUNCATE execution, execution_outcome, execution_shard,"
        + " execution_shard_outcome, app_task, app_audit, worker RESTART IDENTITY CASCADE");
    CLOCK.now = BASE;
    // M6.3:server 上下文无进程内 handler——测试建任务须先注册一个存活 worker 提供 handlerRef(demo);
    //  该 worker 在未拨动 CLOCK.now 的用例中恒存活(见 handlersEndpoint 用例拨钟后须重播种)。
    jdbc.update("INSERT INTO worker(id, refs, last_seen, status) VALUES('w-demo','demo',?,'ALIVE')",
        java.sql.Timestamp.from(CLOCK.now));
    // w-flaky 供仍用 "flaky" ref 建任务的用例通过 M6.2 校验(其 FAILED 由 completeNodeShards 直写,handler 不实跑)。
    jdbc.update("INSERT INTO worker(id, refs, last_seen, status) VALUES('w-flaky','flaky',?,'ALIVE')",
        java.sql.Timestamp.from(CLOCK.now));
  }

  /** M6:控制面应能从共享 DB 读存活 worker 注册视图(refs)。@Autowired 装配的 workerRepository bean 直接读,
   *  既验查询逻辑、又证 bean 真被接线进 server 上下文(镜像既有 shards/jdbc 用法)。 */
  @Test
  void readsLiveWorkerRegistryView() throws Exception {
    Instant base = Instant.parse("2026-09-10T00:00:00Z");
    jdbc.update("INSERT INTO worker(id, refs, last_seen, status) "
        + "VALUES('w1','demo',?,'ALIVE')", java.sql.Timestamp.from(base));
    var live = workerRepository.findAllAlive(base.minusSeconds(10));
    assertEquals(List.of("w1"), live.stream().map(WorkerRegistration::id).toList());
    assertEquals(List.of("demo"), live.get(0).refs());
  }

  @Test
  void createTask_appearsInList_andGet() throws Exception {
    long id = postTask("create-list-task");

    mvc.perform(get("/api/v1/tasks"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][*].name", hasItem("create-list-task")))
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$.limit").value(100))
        .andExpect(jsonPath("$.offset").value(0));

    mvc.perform(get("/api/v1/tasks/" + id))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.name").value("create-list-task"));
  }

  /** M6.3:表单下拉框的数据源 = 纯存活 worker 注册表并集(与 create/update 校验同源;无进程内 handler)。 */
  @Test
  void handlersEndpoint_listsRegisteredRefs() throws Exception {
    Instant base = Instant.parse("2026-09-10T00:00:00Z");
    CLOCK.now = base;
    // demo 与 ping 都来自存活 worker(@BeforeEach 里的 w-demo 是 BASE 时刻,拨钟后已 stale,须在此重播种)。
    // w-demo 行已由 @BeforeEach 建立(主键唯一),拨钟后仅刷新 last_seen 使其在 base 重新存活。
    jdbc.update("UPDATE worker SET last_seen=? WHERE id='w-demo'", java.sql.Timestamp.from(base));
    jdbc.update("INSERT INTO worker(id, refs, last_seen, status) "
        + "VALUES('w-ping','ping',?,'ALIVE')", java.sql.Timestamp.from(base));
    mvc.perform(get("/api/v1/handlers"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[*]", hasItem("demo")))    // 存活 worker
        .andExpect(jsonPath("$[*]", hasItem("ping")));   // 存活 worker
  }

  /** M6.2:入口即拒绝孤儿 ref——无存活 worker、非进程内 handler 的 handlerRef 建不进去。 */
  @Test
  void createTask_orphanHandlerRef_returnsBadRequest() throws Exception {
    String body = "{\"name\":\"orphan-task\",\"kind\":\"cron\",\"handlerRef\":\"no-such-handler\","
        + "\"cron\":\"" + CRON + "\"}";
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("no-such-handler")));
  }

  /** M6.2:注册到 worker 表且有新鲜 last_seen 的 worker 的 ref,是合法可创建目标(控制面只读 DB 注册视图)。 */
  @Test
  void createTask_liveWorkerRef_isAccepted() throws Exception {
    Instant base = Instant.parse("2026-09-10T00:00:00Z");
    CLOCK.now = base;
    jdbc.update("INSERT INTO worker(id, refs, last_seen, status) "
        + "VALUES('w-ping','ping',?,'ALIVE')", java.sql.Timestamp.from(base));
    String body = "{\"name\":\"live-ref-task\",\"kind\":\"cron\",\"handlerRef\":\"ping\","
        + "\"cron\":\"" + CRON + "\",\"shardCount\":1}";
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isCreated());
  }

  /** M6.2:同源 stale(>30s)worker 的 ref 不算存活,建任务仍被拒(校验只采信新鲜注册)。 */
  @Test
  void createTask_staleWorkerRef_isRejected() throws Exception {
    Instant base = Instant.parse("2026-09-10T00:00:00Z");
    CLOCK.now = base;
    jdbc.update("INSERT INTO worker(id, refs, last_seen, status) "
        + "VALUES('w-stale','ping',?,'ALIVE')", java.sql.Timestamp.from(base.minusSeconds(60)));
    String body = "{\"name\":\"stale-ref-task\",\"kind\":\"cron\",\"handlerRef\":\"ping\","
        + "\"cron\":\"" + CRON + "\",\"shardCount\":1}";
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isBadRequest());
  }

  /** M6.2:update 到孤儿 ref 同样 400(与 create 同一校验路径)。 */
  @Test
  void update_orphanHandlerRef_returnsBadRequest() throws Exception {
    long id = postTask("update-orphan"); // 先建一个合法任务(process-in ref demo)
    mvc.perform(put("/api/v1/tasks/" + id).contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"still\",\"kind\":\"cron\",\"handlerRef\":\"no-such-handler\","
                + "\"cron\":\"0 */6 * * * *\",\"shardCount\":1}"))
        .andExpect(status().isBadRequest());
  }

  /** M3 回归:shardCount=0 不得创建任务(否则扇出 0 个 shard → 恒 DUE、无法汇聚终态的父)。 */
  @Test
  void createTask_shardCountZero_returnsBadRequest() throws Exception {
    String body = "{\"name\":\"zero-shard-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
        + "\"cron\":\"" + CRON + "\",\"shardCount\":0}";
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isBadRequest()); // 镜像 maxRetries/timeout 等校验拒绝路径
  }

  /** 边界:maxActiveConcurrent 为 claim 并发配额,须 >= 1——0/负会让 claim 的 active.c < maxConcurrent 恒 false,
   *  该任务所有 shard 永久卡 DUE 且不被回收(对账器只管 RUNNING),故禁建。 */
  @Test
  void createTask_maxActiveConcurrentZero_returnsBadRequest() throws Exception {
    String body = "{\"name\":\"zero-quota-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
        + "\"cron\":\"" + CRON + "\",\"shardCount\":1,\"maxActiveConcurrent\":0}";
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isBadRequest());
    String neg = "{\"name\":\"neg-quota-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
        + "\"cron\":\"" + CRON + "\",\"shardCount\":1,\"maxActiveConcurrent\":-3}";
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(neg))
        .andExpect(status().isBadRequest());
  }

  /** 边界:cron 字段值非法(秒 >= 60,字段数仍是 6)不得建任务——否则坏 cron 落库,触发扫描线程
   *  解析时级联拖累本 tick 其它任务触发。 */
  @Test
  void createTask_badCronFieldValue_returnsBadRequest() throws Exception {
    String body = "{\"name\":\"bad-cron-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
        + "\"cron\":\"60 * * * * *\",\"shardCount\":1}";
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isBadRequest());
  }

  /** 重试策略化(§1):建任务可设 retry_mode/cap/budget,CREATE 响应复读;非法值 400。 */
  @Test
  void createTask_persistsRetryStrategyFields() throws Exception {
    String body = "{\"name\":\"strat-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
        + "\"cron\":\"" + CRON + "\",\"shardCount\":1,"
        + "\"timeoutSeconds\":0,\"retryMode\":\"linear\",\"retryCapMs\":60000,\"retryBudgetMs\":300000}";
    MvcResult r = mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isCreated()).andReturn();
    String got = r.getResponse().getContentAsString();
    assertTrue(got.contains("\"retryMode\":\"linear\""));
    assertTrue(got.contains("\"retryCapMs\":60000"));
    assertTrue(got.contains("\"retryBudgetMs\":300000"));

    String badMode = "{\"name\":\"bad-mode\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
        + "\"cron\":\"" + CRON + "\",\"shardCount\":1,\"retryMode\":\"quadratic\"}";
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(badMode))
        .andExpect(status().isBadRequest());
    String negCap = "{\"name\":\"neg-cap\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
        + "\"cron\":\"" + CRON + "\",\"shardCount\":1,\"retryCapMs\":-5}";
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(negCap))
        .andExpect(status().isBadRequest());
  }

  @Test
  void putEditsTask() throws Exception {
    long id = postTask("put-edit"); // 复用本类既有建 task 辅助
    MvcResult r = mvc.perform(put("/api/v1/tasks/" + id)
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {"name":"renamed","kind":"cron","handlerRef":"demo","cron":"0 */6 * * * *",
                 "shardCount":3,"timeoutSeconds":600,"maxRetries":2,"backoffMs":2000,
                 "maxActiveConcurrent":4,"paused":true}"""))
        .andExpect(status().isOk()).andReturn();
    String body = r.getResponse().getContentAsString();
    assertTrue(body.contains("\"name\":\"renamed\""));
    assertTrue(body.contains("\"shardCount\":3"));
    assertTrue(body.contains("\"cron\":\"0 */6 * * * *\""));

    mvc.perform(put("/api/v1/tasks/999999999")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{}")).andExpect(status().isNotFound());
    mvc.perform(put("/api/v1/tasks/" + id)
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {"name":"","kind":"cron","handlerRef":"x","cron":"0 */6 * * * *",
                 "shardCount":1}"""))
        .andExpect(status().isBadRequest());
  }

  /** 部分 PUT 省略 retryableFailurePattern 时,不得清空既有重试 pattern(其它可空字段同款 null-fallback)。 */
  @Test
  void putOmittingRetryPattern_keepsExistingPattern() throws Exception {
    long id = postTaskWithRetry("retain-pattern-task", "demo", 2, 1500, ".*custom-err.*");

    MvcResult r = mvc.perform(put("/api/v1/tasks/" + id)
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {"name":"renamed-pattern","kind":"cron","handlerRef":"demo","cron":"0 */6 * * * *",
                 "shardCount":2,"timeoutSeconds":900}"""))
        .andExpect(status().isOk()).andReturn();
    String body = r.getResponse().getContentAsString();
    assertTrue(body.contains("\"name\":\"renamed-pattern\""));
    assertTrue(body.contains(".*custom-err.*"), "省略 retryableFailurePattern 不得清空既有 pattern:\n" + body);

    String got = mvc.perform(get("/api/v1/tasks/" + id)).andExpect(status().isOk())
        .andReturn().getResponse().getContentAsString();
    assertTrue(got.contains(".*custom-err.*"), "GET 复读:pattern 应保留:\n" + got);
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
        .andExpect(jsonPath("$['items'][*].id", hasItem((int) execId)))
        .andExpect(jsonPath("$['items'][?(@.id == " + execId + ")].status").value("DUE")); // worker 循环已关,保持 DUE
  }

  /** M5.2:GET /executions from/to 时间窗过滤(started_at)。父 execution.started_at 在建父即回填 now()
   *  (createParentWithShards INSERT now()),故真实 POST /tasks/{id}/trigger 后时间窗应含该父。窗口含该帧
   *  → 1 条,更早窗口 → 0 条。 */
  @Test
  void executionsTimeWindowFilters() throws Exception {
    resetDb();
    long id = postTask("tw");
    triggerParent(id); // POST /tasks/{id}/trigger → 201,父 DUE 且 started_at=now()(建父即回填)
    String from = Instant.now().minusSeconds(60).toString();
    String to = Instant.now().plusSeconds(60).toString();
    mvc.perform(get("/api/v1/executions")
            .param("from", from).param("to", to))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items']", hasSize(1)))
        .andExpect(jsonPath("$['items'][0].taskId", is((int) id)))
        .andExpect(jsonPath("$.total").value(1));
    // 窗口排除:过去 1 小时之前的窗口 → 0 条
    String pastFrom = Instant.now().minusSeconds(7200).toString();
    String pastTo = Instant.now().minusSeconds(3600).toString();
    mvc.perform(get("/api/v1/executions")
            .param("from", pastFrom).param("to", pastTo))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items']", hasSize(0)))
        .andExpect(jsonPath("$.total").value(0));
  }

  /** M5.2:GET /executions limit/offset 分页(ORDER BY started_at DESC NULLS LAST, id DESC 稳定同毫秒 now() 平局);
   *  trigger 3 次 → 3 父,limit=2 → 2 条,再 offset=2 → 剩 1 条。 */
  @Test
  void executionsPagination() throws Exception {
    resetDb();
    long id = postTask("pg");
    mvc.perform(post("/api/v1/tasks/" + id + "/trigger")).andExpect(status().isCreated());
    mvc.perform(post("/api/v1/tasks/" + id + "/trigger")).andExpect(status().isCreated());
    mvc.perform(post("/api/v1/tasks/" + id + "/trigger")).andExpect(status().isCreated()); // 3 个 execution 父
    mvc.perform(get("/api/v1/executions").param("limit", "2"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items']", hasSize(2)))
        .andExpect(jsonPath("$.total").value(3));
    mvc.perform(get("/api/v1/executions")
            .param("limit", "2").param("offset", "2"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items']", hasSize(1))) // 剩 1 条(3-2)
        .andExpect(jsonPath("$.total").value(3));
  }

  @Test void rerunExecution_terminalSource_createsFreshTraceableRound() throws Exception {
    long id = postTask("rerun-e2e");
    long sourceId = triggerParent(id); // DUE 父 + 1 DUE shard
    // 确定性终结源轮(测试直接置 shard/父 SUCCESS;生产由 worker + 对账器完成)
    for (Long sid : jdbc.queryForList("SELECT id FROM execution_shard WHERE execution_id=?",
        Long.class, sourceId)) {
      jdbc.update("UPDATE execution_shard SET status='SUCCESS', finished_at=now() WHERE id=?", sid);
    }
    assertEquals(1, jdbc.update(
        "UPDATE execution SET status='SUCCESS', finished_at=now() WHERE id=? AND status='DUE'", sourceId));

    MvcResult r = mvc.perform(post("/api/v1/executions/" + sourceId + "/rerun"))
        .andExpect(status().isCreated()).andReturn();
    String bodyStr = r.getResponse().getContentAsString();
    long newId = objectMapper.readTree(bodyStr).path("id").asLong();
    assertTrue(newId != sourceId, "重跑应新建一轮而非复用源轮");
    assertEquals("DUE", objectMapper.readTree(bodyStr).path("status").asText());
    assertEquals(id, objectMapper.readTree(bodyStr).path("taskId").asLong());
    assertEquals(sourceId, objectMapper.readTree(bodyStr).path("rerunOf").asLong(),
        "重跑轮的 rerunOf 应指向源轮(溯源)");
    assertEquals(1, objectMapper.readTree(bodyStr).path("shardCount").asInt());

    // 溯源 READ:详情也带 rerunOf
    mvc.perform(get("/api/v1/executions/" + newId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.rerunOf").value(sourceId));

    // 该任务共 2 轮(源 + 重跑)
    String list = mvc.perform(get("/api/v1/executions?taskId=" + id)).andReturn()
        .getResponse().getContentAsString();
    assertEquals(2, objectMapper.readTree(list).path("items").size());
    assertEquals(2, objectMapper.readTree(list).path("total").asLong());
  }

  @Test void rerunExecution_nonTerminalSource_rejected409() throws Exception {
    long id = postTask("rerun-409");
    long sourceId = triggerParent(id); // 仍 DUE(非终态)
    mvc.perform(post("/api/v1/executions/" + sourceId + "/rerun"))
        .andExpect(status().isConflict());
  }

  @Test void rerunExecution_missing_404() throws Exception {
    mvc.perform(post("/api/v1/executions/999999/rerun"))
        .andExpect(status().isNotFound());
  }

  // ---------- 任务删除(仅删无子记录,否则 409) ----------

  @Test void deleteTask_unreferenced_removesAnd404s() throws Exception {
    long id = postTask("del-ok");
    mvc.perform(delete("/api/v1/tasks/" + id))
        .andExpect(status().isNoContent());
    mvc.perform(get("/api/v1/tasks/" + id))
        .andExpect(status().isNotFound());
  }

  @Test void deleteTask_hasExecution_rejected409() throws Exception {
    long id = postTask("del-blocked");
    triggerParent(id); // 建 1 轮执行 → 任务有了子记录
    mvc.perform(delete("/api/v1/tasks/" + id))
        .andExpect(status().isConflict())
        .andExpect(status().reason(org.hamcrest.Matchers.containsString("执行记录")));
    mvc.perform(get("/api/v1/tasks/" + id))
        .andExpect(status().isOk()); // 任务仍在
  }

  @Test void deleteTask_missing_404() throws Exception {
    mvc.perform(delete("/api/v1/tasks/999999"))
        .andExpect(status().isNotFound());
  }

  // ---------- 分页信封契约(所有列表统一 Page{items,total,offset,limit}) ----------

  @Test void tasksList_nameSubstringAndPausedFilterPagination() throws Exception {
    long a = postTask("alice-task");
    long b = postTask("bob-task");
    postTask("alice-other");
    mvc.perform(post("/api/v1/tasks/" + a + "/pause")).andExpect(status().isOk());

    // name 子串(ILIKE)过滤
    mvc.perform(get("/api/v1/tasks").param("name", "alice"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(2))
        .andExpect(jsonPath("$['items']", hasSize(2)));
    // paused 过滤 → 仅 alice-task
    mvc.perform(get("/api/v1/tasks").param("paused", "true"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].id").value(a));
    // 分页:limit=1 → items 1 条,total 恒为全量 3
    mvc.perform(get("/api/v1/tasks").param("limit", "1"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items']", hasSize(1)))
        .andExpect(jsonPath("$.total").value(3));
    // 无过滤空条件:返回全部但恒 Page 信封
    mvc.perform(get("/api/v1/tasks"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(3))
        .andExpect(jsonPath("$.limit").value(100))
        .andExpect(jsonPath("$.offset").value(0));
  }

  /** 审计读 API:GET /api/v1/audits 过滤 + 分页信封,occurred_at DESC。写端见 taskWriteActions_areAudited 等用例;此处直种子读端。 */
  @Test
  void auditReadApi_filtersAndPagination() throws Exception {
    // 用裸 jdbc 落任务(而非 postTask)取真实 id 作 targetId —— postTask 走写端现会记 task.create 审计,干扰本用例的绝对值计数。
    long t1 = seedTaskRow("audit-seed-a");
    long t2 = seedTaskRow("audit-seed-b");
    jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta, source)"
            + " VALUES ('alice','task.create','task',?,'{\"name\":\"audit-seed-a\"}','cli')", t1);
    jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta)"
            + " VALUES ('alice','task.update','task',?, null)", t1);
    jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta)"
            + " VALUES ('bob','task.create','task',?, '{}')", t2);

    // 全量信封
    mvc.perform(get("/api/v1/audits"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(3))
        .andExpect(jsonPath("$['items']", hasSize(3)))
        .andExpect(jsonPath("$.limit").value(100))
        .andExpect(jsonPath("$.offset").value(0));
    // operator 子串
    mvc.perform(get("/api/v1/audits").param("operator", "ali"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(2));
    // action 等值
    mvc.perform(get("/api/v1/audits").param("action", "task.update"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1));
    // targetId 等值
    mvc.perform(get("/api/v1/audits").param("targetId", String.valueOf(t1)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(2));
    // 分页:limit=1 offset=2 → 剩 1 条,total 恒 3
    mvc.perform(get("/api/v1/audits").param("limit", "1").param("offset", "2"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items']", hasSize(1)))
        .andExpect(jsonPath("$.total").value(3));
  }

  /** 审计读 API:occurred_at 时间窗过滤(from/to)。窗口用 ±1h 规避 JVM/容器时钟偏斜(见 ExecutorWorkerTest 教训)。 */
  @Test
  void auditReadApi_timeWindow() throws Exception {
    jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, occurred_at)"
        + " VALUES ('old','t','task',1, now() - interval '2 days')");
    jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, occurred_at)"
        + " VALUES ('new','t','task',2, now())");
    // from=1h 前 → 只含 'new'(now 行)
    mvc.perform(get("/api/v1/audits").param("from", Instant.now().minusSeconds(3600).toString()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].operator").value("new"));
    // to=1h 前 → 只含 'old'(2 天前行)
    mvc.perform(get("/api/v1/audits").param("to", Instant.now().minusSeconds(3600).toString()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].operator").value("old"));
  }

  // ---------- 审计写端:三控制器 15 个写端点接 X-Operator + AuditRecorder ----------

  /** 审计写端:TaskController create/update/pause/resume/trigger/delete;X-Operator 缺省记 anonymous;meta 为后态。 */
  @Test
  void taskWriteActions_areAudited() throws Exception {
    // create with X-Operator → operator 为该头,meta 含后态 name
    long id = objectMapper.readTree(mvc.perform(post("/api/v1/tasks").header("X-Operator", "alice")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"audited-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
    mvc.perform(get("/api/v1/audits").param("operator", "alice").param("action", "task.create")
            .param("targetId", String.valueOf(id)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].targetType").value("task"))
        .andExpect(jsonPath("$['items'][0].meta").value(org.hamcrest.Matchers.containsString("audited-task")));

    // update → task.update,targetId=id,meta 为后态(改名后)
    mvc.perform(put("/api/v1/tasks/" + id).header("X-Operator", "alice")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"renamed-audit\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"0 */6 * * * *\",\"shardCount\":1}"))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "task.update").param("targetId", String.valueOf(id)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].meta").value(org.hamcrest.Matchers.containsString("renamed-audit")));

    // 无 X-Operator 头 → operator 兜底 'anonymous'
    mvc.perform(post("/api/v1/tasks/" + id + "/pause")).andExpect(status().isOk());
    mvc.perform(post("/api/v1/tasks/" + id + "/resume")).andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "task.pause"))
        .andExpect(status().isOk()).andExpect(jsonPath("$['items'][0].operator").value("anonymous"));
    mvc.perform(get("/api/v1/audits").param("action", "task.resume"))
        .andExpect(status().isOk()).andExpect(jsonPath("$['items'][0].operator").value("anonymous"));

    // trigger → task.trigger(目标 = 任务 id)
    mvc.perform(post("/api/v1/tasks/" + id + "/trigger").header("X-Operator", "alice"))
        .andExpect(status().isCreated());
    mvc.perform(get("/api/v1/audits").param("action", "task.trigger").param("targetId", String.valueOf(id)))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));

    // 该 id 已被 trigger(有 execution)→ delete 409(不记 action);换个未触发的任务验 delete
    mvc.perform(delete("/api/v1/tasks/" + id)).andExpect(status().isConflict());
    long delId = objectMapper.readTree(mvc.perform(post("/api/v1/tasks").header("X-Operator", "bob")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"audit-del\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
    mvc.perform(delete("/api/v1/tasks/" + delId).header("X-Operator", "bob")).andExpect(status().isNoContent());
    mvc.perform(get("/api/v1/audits").param("action", "task.delete").param("targetId", String.valueOf(delId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][0].operator").value("bob"));
  }

  /** 审计写端:ExecutionController rerun/cancel + shard.requeue(目标均为操作者主动动作)。 */
  @Test
  void executionWriteActions_areAudited() throws Exception {
    long id = postTask("audit-exec");

    // rerun(确定性终结源轮后):execution.rerun,target = 新轮 id
    long execId = triggerParent(id);
    for (Long sid : jdbc.queryForList("SELECT id FROM execution_shard WHERE execution_id=?", Long.class, execId))
      jdbc.update("UPDATE execution_shard SET status='SUCCESS', finished_at=now() WHERE id=?", sid);
    jdbc.update("UPDATE execution SET status='SUCCESS', finished_at=now() WHERE id=? AND status='DUE'", execId);
    long newId = objectMapper.readTree(mvc.perform(post("/api/v1/executions/" + execId + "/rerun")
            .header("X-Operator", "carol")).andExpect(status().isCreated()).andReturn()
        .getResponse().getContentAsString()).get("id").asLong();
    mvc.perform(get("/api/v1/audits").param("action", "execution.rerun").param("targetId", String.valueOf(newId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][0].operator").value("carol"));

    // requeue FAILED+dead_letter shard → shard.requeue
    Shard dlq = seedDeadLetter(id);
    mvc.perform(post("/api/v1/executions/shards/" + dlq.id() + "/requeue").header("X-Operator", "carol"))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "shard.requeue").param("targetId", String.valueOf(dlq.id())))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));

    // cancel(无 RUNNING shard → 直取消)→ execution.cancel
    long cancelTarget = triggerParent(id);
    mvc.perform(post("/api/v1/executions/" + cancelTarget + "/cancel").header("X-Operator", "carol"))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "execution.cancel").param("targetId", String.valueOf(cancelTarget)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][0].operator").value("carol"));
  }

  /** 审计写端:DagController create/pause/resume/trigger + dag_run.cancel + dag_node.rerun(meta 含 nodeId)。 */
  @Test
  void dagWriteActions_areAudited() throws Exception {
    long t = postTask("audit-dag-task");
    long dagId = postDag("audit-dag", new long[]{t}, new String[]{"A"}, new String[][]{});
    // postDag 无 X-Operator → anonymous
    mvc.perform(get("/api/v1/audits").param("action", "dag.create").param("targetId", String.valueOf(dagId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][0].operator").value("anonymous"))
        .andExpect(jsonPath("$['items'][0].meta").value(org.hamcrest.Matchers.containsString("audit-dag")));

    // pause / resume
    mvc.perform(post("/api/v1/dags/" + dagId + "/pause")).andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "dag.pause")).andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));
    mvc.perform(post("/api/v1/dags/" + dagId + "/resume")).andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "dag.resume")).andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));

    // trigger → dag.trigger
    long runId = triggerDag(dagId);
    mvc.perform(get("/api/v1/audits").param("action", "dag.trigger").param("targetId", String.valueOf(dagId)))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));

    // dag_run.cancel(scanOnce 后 A RUNNING → cancel 全节点级联终态)→ 记录一次
    dagEngine.scanOnce();
    mvc.perform(post("/api/v1/dags/runs/" + runId + "/cancel").header("X-Operator", "dave"))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "dag_run.cancel").param("targetId", String.valueOf(runId)))
        .andExpect(status().isOk()).andExpect(jsonPath("$['items'][0].operator").value("dave"));

    // dag_node.rerun:终态节点可重跑(rerunNode 仅要求节点 isTerminal();A 已 CANCELED)→ targetId=runId, meta 含 nodeId
    long aId = dagNodeId(runId, "A");
    mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + aId + "/rerun").header("X-Operator", "dave"))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "dag_node.rerun").param("targetId", String.valueOf(runId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][0].operator").value("dave"))
        // Ruling(见 task-3-report):写端序列化 ObjectMapper 在 meta JSON 冒号后补空格({"nodeId": 1}),故用容空格正则而非 containsString("nodeId":1)。
        .andExpect(jsonPath("$['items'][0].meta").value(
            org.hamcrest.Matchers.matchesPattern(".*\"nodeId\":\\s*" + aId + ".*")));
  }

  @Test void listDags_nameFilter() throws Exception {
    long t = postTask("dagt");
    postDag("report-daily", new long[]{t}, new String[]{"A"}, new String[][]{});
    postDag("ingest-hourly", new long[]{t}, new String[]{"A"}, new String[][]{});

    mvc.perform(get("/api/v1/dags"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(2))
        .andExpect(jsonPath("$['items']", hasSize(2)));
    mvc.perform(get("/api/v1/dags").param("name", "report"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].name").value("report-daily"));
  }

  @Test void listRuns_statusFilter() throws Exception {
    long t = postTask("runt");
    long dagId = postDag("run-dag", new long[]{t}, new String[]{"A"}, new String[][]{});
    long r1 = triggerDag(dagId);

    mvc.perform(get("/api/v1/dags/runs").param("dagId", String.valueOf(dagId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1));
    mvc.perform(get("/api/v1/dags/runs").param("status", "PENDING"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].id").value(r1));
  }

  @Test void dlqList_taskIdFilter() throws Exception {
    long t1 = postTask("dlq1");
    long t2 = postTask("dlq2");
    Shard s1 = seedDeadLetter(t1);
    seedDeadLetter(t2);

    mvc.perform(get("/api/v1/executions/dlq"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(2));
    mvc.perform(get("/api/v1/executions/dlq").param("taskId", String.valueOf(t1)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items']", hasSize(1)))
        .andExpect(jsonPath("$['items'][0].id").value(s1.id()));
  }

  @Test
  void leadershipHeldScanOnce_shardStaysDue_absentWorker() throws Exception {
    long id = postTask("scan-success-task");

    // 持有领导权(装配的 advisory-lock Bean)时,同步扫描一次 → 命中 tick,登记父 + 1 DUE shard
    triggerEngine.scanOnce();
    long parentId = parentIdFor(id);
    assertEquals("DUE", parentStatus(parentId), "触发扇出 → 父 header DUE");
    assertEquals("DUE", shardStatus(parentId), "触发扇出 → 1 条 DUE shard");
    mvc.perform(get("/api/v1/executions"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][*].status", hasItem("DUE")));

    boolean processed = false; // 无执行器:不再有 workOne
    reconciler.scanOnce();
    assertEquals("DUE", shardStatus(parentId), "无 worker → shard 保持 DUE(汇聚不改变未执行分片)");
    assertEquals("DUE", parentStatus(parentId), "非全部终态 → 父仍 DUE");
    mvc.perform(get("/api/v1/executions").param("taskId", String.valueOf(id)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][0].status").value("DUE"));
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
    // 指标为全局无标签懒查询 gauge(新建任务无需重启即计入),断言裸指标名 + 全局采样值,不再按 task_id。
    assertTrue(prom.contains("scheduler_active_runs "),
        "missing global scheduler_active_runs series in prometheus:\n" + prom);
    assertTrue(prom.contains("scheduler_due_queue_max_age_seconds "),
        "missing global scheduler_due_queue_max_age_seconds series in prometheus:\n" + prom);
    assertEquals(1.0, promGauge(prom, "scheduler_active_runs"), 0.0,
        "scheduler_active_runs 应按全局 RUNNING shard 计数(1 条 RUNNING)");
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
        .andExpect(jsonPath("$['items'][*].id", hasItem((int) shardId)))
        .andExpect(jsonPath("$['items'][0].status").value("FAILED"))
        .andExpect(jsonPath("$['items'][0].executionId").value(dlq.executionId().intValue()))
        .andExpect(jsonPath("$['items'][0].taskName").value("dlq-task"))
        .andExpect(jsonPath("$['items'][0].handlerRef").value("demo"))
        .andExpect(jsonPath("$.total").value(1));

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

    // 外部 worker 认领并跑成功(server 无执行器;requeue 控制面已在上方断言 DUE + 不再出现在 /dlq)
    jdbc.update("UPDATE execution_shard SET status='SUCCESS', worker_id='w-demo' WHERE id=?", shardId);
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

  /** 失败日志:分片 FAILED 时,detail 里应带该分片最近一次 FAILED outcome 的 detail。 */
  @Test
  void getExecutionDetail_includesShardFailureDetail() throws Exception {
    long id = postTask("fail-detail-task");
    long parentId = triggerParent(id);
    long shardId = shards.findShards(parentId).get(0).id();
    String boom = "boom: things went wrong";
    jdbc.update("UPDATE execution_shard SET status='FAILED' WHERE id=?", shardId);
    jdbc.update("INSERT INTO execution_shard_outcome (shard_id, status, detail) VALUES (?, 'FAILED', ?)",
        shardId, boom);

    mvc.perform(get("/api/v1/executions/" + parentId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.shards[0].failureDetail").value(boom));
  }

  /** 重排复位:分片有旧 FAILED 记录但当前已非 FAILED(如 requeue 后成功)时,失败原因为 null,不挂过期日志。 */
  @Test
  void getExecutionDetail_hidesFailureDetailWhenShardNoLongerFailed() throws Exception {
    long id = postTask("requeued-detail-task");
    long parentId = triggerParent(id);
    long shardId = shards.findShards(parentId).get(0).id();
    // 旧 FAILED 记录仍在(requeue 重排只复位状态、不删历史 outcome),但当前状态已成功
    jdbc.update("UPDATE execution_shard SET status='FAILED' WHERE id=?", shardId);
    jdbc.update("INSERT INTO execution_shard_outcome (shard_id, status, detail) VALUES (?, 'FAILED', 'old boom')", shardId);
    jdbc.update("UPDATE execution_shard SET status='SUCCESS' WHERE id=?", shardId);

    mvc.perform(get("/api/v1/executions/" + parentId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.shards[0].status").value("SUCCESS"))
        .andExpect(jsonPath("$.shards[0].failureDetail").value(org.hamcrest.Matchers.nullValue()));
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

  // ---- M4:工作流 DAG 控制面 E2E ----

  /** M4:建含环 DAG(A→B→C→A)→ 仓储 DFS 校验拒绝 → 400。 */
  @Test
  void dag_cycleCreation_rejected400() throws Exception {
    long t = postTask("cycle-task");
    mvc.perform(post("/api/v1/dags").contentType(MediaType.APPLICATION_JSON)
        .content(dagBody("cycle-dag", new long[]{t, t, t}, new String[]{"A", "B", "C"},
            new String[][]{{"A", "B"}, {"B", "C"}, {"C", "A"}})))
        .andExpect(status().isBadRequest());
  }

  /** M4:线性 A→B 全成功端到端。真实触发 + 同步驱动(dagEngine.scanOnce → completeNodeShards → reconciler.scanOnce)。
   *  跨周期收敛:节点只在"其上游 SUCCESS 之后的下一次 scan"才 spawn,故 A 终态后需多一次 scan 才生成 B。 */
  @Test
  void dagLinear_e2e_runsToSuccess() throws Exception {
    long t = postTask("linear-task"); // demo handler, shardCount 1
    long dagId = postDag("linear-dag", new long[]{t, t}, new String[]{"A", "B"},
        new String[][]{{"A", "B"}});
    pauseDag(dagId);                 // 暂停:剔除 cron 触发(CLOCK 钉在 10:05 tick,否则 scanOnce 会再建一条调度 run)
    long runId = triggerDag(dagId);  // 仅手动 run,幂等可断言 1 条

    dagEngine.scanOnce();            // A 是根(无上游)→ spawn A → RUNNING;B 等上游(PENDING)
    completeNodeShards(runId, "A", "SUCCESS");
    reconciler.scanOnce();           // 汇聚 A 的父 execution → SUCCESS
    dagEngine.scanOnce();            // A 由 shards 派生 SUCCESS;B 仍 PENDING(本周期上游快照 RUNNING)
    dagEngine.scanOnce();            // B 上游 SUCCESS → spawn B → RUNNING
    completeNodeShards(runId, "B", "SUCCESS");
    reconciler.scanOnce();           // 汇聚 B 的父 execution → SUCCESS
    dagEngine.scanOnce();            // B 派生 SUCCESS → 全节点终态 → dag_run SUCCESS

    mvc.perform(get("/api/v1/dags/runs").param("dagId", String.valueOf(dagId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items']", hasSize(1)))
        .andExpect(jsonPath("$.total").value(1));

    mvc.perform(get("/api/v1/dags/runs/" + runId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("SUCCESS"))
        .andExpect(jsonPath("$.nodes.length()").value(2))
        .andExpect(jsonPath("$.nodes[0].node.status").value("SUCCESS"))
        .andExpect(jsonPath("$.nodes[1].node.status").value("SUCCESS"));
  }

  /** M4:上游 A(flaky,maxRetries=0)首调耗尽失败 → 下游 B 被 SKIPPED(绝不 spawn,惰性验证)→ dag_run FAILED。 */
  @Test
  void dagLinear_failedUpstream_skipsDownstream_failed() throws Exception {
    long t = postTaskWithRetry("dag-fail-task", "flaky", 0, 1000, ""); // 首调抛错 → 耗尽 FAILED
    long dagId = postDag("fail-dag", new long[]{t, t}, new String[]{"A", "B"},
        new String[][]{{"A", "B"}});
    pauseDag(dagId);                 // 暂停:剔除 cron 触发,只留本次手动 run
    long runId = triggerDag(dagId);

    dagEngine.scanOnce();            // spawn A → RUNNING
    completeNodeShards(runId, "A", "FAILED");
    reconciler.scanOnce();           // 汇聚 A 的父 → FAILED
    dagEngine.scanOnce();            // A 派生 FAILED(本周期于 A 的 RUNNING 快照,不连锁)
    dagEngine.scanOnce();            // B 上游 FAILED → SKIPPED → 全节点终态 → dag_run FAILED

    mvc.perform(get("/api/v1/dags/runs/" + runId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("FAILED"))
        .andExpect(jsonPath("$.nodes.length()").value(2))
        .andExpect(jsonPath("$.nodes[0].node.status").value("FAILED"))
        .andExpect(jsonPath("$.nodes[1].node.status").value("SKIPPED"))
        .andExpect(jsonPath("$.nodes[1].shards.length()").value(0)); // B 从未 spawn → 空 shards
  }

  /** M4:取消级联端点。scanOnce 后:A 已 spawn(RUNNING,其父 DUE shard 无 RUNNING→直取消),B 仍 PENDING。
   *  cancel → A/B 全 CANCELED,A 的父 execution shard 级联 CANCELED,dag_run 终态 CANCELED。 */
  @Test
  void dagCancel_cascadesToNodesAndExecution() throws Exception {
    long t = postTask("cancel-dag-task");
    long dagId = postDag("cancel-dag", new long[]{t, t}, new String[]{"A", "B"},
        new String[][]{{"A", "B"}});
    pauseDag(dagId);                 // 暂停:剔除 cron 触发,只留本次手动 run
    long runId = triggerDag(dagId);
    dagEngine.scanOnce(); // spawn A → RUNNING;B 仍 PENDING

    mvc.perform(post("/api/v1/dags/runs/" + runId + "/cancel"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("CANCELED"))
        .andExpect(jsonPath("$.nodes.length()").value(2))
        .andExpect(jsonPath("$.nodes[0].node.status").value("CANCELED"))
        .andExpect(jsonPath("$.nodes[1].node.status").value("CANCELED"));

    // 已 spawn 节点 A 的父 execution 的最早 shard 被级联直取消 → CANCELED(无 RUNNING shard → cancelParentImmediate)
    assertEquals(0L, jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard es WHERE es.execution_id = ("
        + " SELECT execution_id FROM dag_run_node WHERE dag_run_id=? AND node_key='A')"
        + " AND es.status <> 'CANCELED'", Long.class, runId),
        "A 的父执行 shard 应全部级联 CANCELED");
  }

  /** M5.3:终态节点单节点重跑——节点回 RUNNING(新 execution_id) → worker 跑新 execution → 终态重派生;下游不自动复位。 */
  @Test
  void dagNodeRerun_terminalRestartsAndDerivesAgain() throws Exception {
    long t = postTask("rerun-success-task"); // demo handler, shardCount 1
    long dagId = postDag("rerun-success-dag", new long[]{t, t}, new String[]{"A", "B"},
        new String[][]{{"A", "B"}});
    pauseDag(dagId);
    long runId = triggerDag(dagId);
    dagEngine.scanOnce();              // spawn A → RUNNING
    completeNodeShards(runId, "A", "SUCCESS");
    reconciler.scanOnce();             // A 父 SUCCESS
    dagEngine.scanOnce();              // A 派生 SUCCESS
    dagEngine.scanOnce();              // B 上游 SUCCESS → spawn B
    completeNodeShards(runId, "B", "SUCCESS");
    reconciler.scanOnce();             // B 父 SUCCESS
    dagEngine.scanOnce();              // B 派生 SUCCESS → run 终态 SUCCESS

    long aNodeId = dagNodeId(runId, "A");
    long aExecBefore = dagNodeExecutionId(runId, "A");

    mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + aNodeId + "/rerun"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("RUNNING"))
        .andExpect(jsonPath("$.executionId").value(is(not(aExecBefore))));

    // 下游 B 不自动复位,仍 SUCCESS
    mvc.perform(get("/api/v1/dags/runs/" + runId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.nodes[1].node.status").value("SUCCESS"))
        .andExpect(jsonPath("$.nodes[0].node.executionId").value(is(not(aExecBefore))));

    // 新 execution 经引擎跑完 → 节点终态重派生(新 execution_id 不变)
    completeNodeShards(runId, "A", "SUCCESS");
    reconciler.scanOnce();
    dagEngine.scanOnce();
    mvc.perform(get("/api/v1/dags/runs/" + runId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("SUCCESS"))
        .andExpect(jsonPath("$.nodes[0].node.status").value("SUCCESS"))
        .andExpect(jsonPath("$.nodes[0].node.executionId").value(is(not(aExecBefore))));
  }

  /** M5.3:非终态(PENDING/RUNNING)节点重跑 → 409。 */
  @Test
  void dagNodeRerun_nonTerminal_rejected409() throws Exception {
    long t = postTask("rerun-terminal-task");
    long dagId = postDag("rerun-terminal-dag", new long[]{t, t}, new String[]{"A", "B"},
        new String[][]{{"A", "B"}});
    pauseDag(dagId);
    long runId = triggerDag(dagId);
    dagEngine.scanOnce(); // A RUNNING;B PENDING

    long aId = dagNodeId(runId, "A");   // RUNNING → 非终态
    long bId = dagNodeId(runId, "B");   // PENDING  → 非终态
    mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + aId + "/rerun"))
        .andExpect(status().isConflict());
    mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + bId + "/rerun"))
        .andExpect(status().isConflict());
  }

  /** M5.3:已失败(FAILED)终态节点允许重跑 → 200 RUNNING(新建 execution)。 */
  @Test
  void dagNodeRerun_failedNodeAllowed() throws Exception {
    long t = postTaskWithRetry("rerun-fail-task", "flaky", 0, 1000, ""); // 首调耗尽失败
    long dagId = postDag("rerun-fail-dag", new long[]{t}, new String[]{"A"}, new String[][]{});
    pauseDag(dagId);
    long runId = triggerDag(dagId);
    dagEngine.scanOnce();                 // spawn A
    completeNodeShards(runId, "A", "FAILED");
    reconciler.scanOnce();                // A 父 FAILED
    dagEngine.scanOnce();                 // A 派生 FAILED → run FAILED

    long aId = dagNodeId(runId, "A");
    mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + aId + "/rerun"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("RUNNING"));
    mvc.perform(get("/api/v1/dags/runs/" + runId)).andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("RUNNING")); // run 已重开
  }

  /** M4:每 DAG 指标。绑定时刻前 seed 的 METRICS_DAG 由 @BeforeAll 预注册 series(@BeforeEach 仅清 app_task /
   *  app_dag_node,app_dag 头是父表不受 CASCADE truncate);此处重载 task 行与节点引用,触发一条 PENDING
   *  dag_run → scheduler_dag_runs_active 按非终态 dag_run 全局计数 = 1(现为无标签全局懒查询 gauge)。 */
  @Test
  void prometheus_dagRunActiveMetric() throws Exception {
    jdbc.update("INSERT INTO app_task (id, name, kind, handler_ref, cron, shard_count, enabled, paused)"
        + " VALUES (?, 'metrics-seed-task', 'cron', 'demo', ?, 1, false, false)",
        METRICS_TASK_ID, "0 */5 * * * *");
    jdbc.update("INSERT INTO app_dag (id, name) VALUES (?, 'metrics-dag')", METRICS_DAG_ID);
    jdbc.update("INSERT INTO app_dag_node (dag_id, node_key, task_id, sort_order) VALUES (?, 'A', ?, 0)",
        METRICS_DAG_ID, METRICS_TASK_ID);
    dagRepository.createManualRun(METRICS_DAG_ID); // 一条 PENDING → active 计数 = 1

    String prom = mvc.perform(get("/actuator/prometheus"))
        .andExpect(status().isOk())
        .andReturn().getResponse().getContentAsString();
    assertTrue(prom.contains("scheduler_dag_runs_active "),
        "missing global scheduler_dag_runs_active series in prometheus:\n" + prom);
    assertEquals(1.0, promGauge(prom, "scheduler_dag_runs_active"), 0.0,
        "scheduler_dag_runs_active 应计全局非终态 dag_run 数(1 条 PENDING)");
  }

  /** M5.3 §1.5:全局指标 gauge scheduler_dlq_depth 与 scheduler_worker_active。种子一条 FAILED+dead_letter shard。 */
  @Test
  void prometheus_globalMetrics() throws Exception {
    long parentId = triggerParent(postTask("metrics-dlq-task"));
    long shardId = shards.findShards(parentId).get(0).id();
    jdbc.update("UPDATE execution_shard SET status='FAILED', dead_letter=true WHERE id=? AND status='DUE'", shardId);

    String prom = mvc.perform(get("/actuator/prometheus"))
        .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
    assertTrue(prom.contains("scheduler_dlq_depth"), "missing scheduler_dlq_depth in prometheus:\n" + prom);
    assertEquals(1.0, promGauge(prom, "scheduler_dlq_depth"), 0.0,
        "scheduler_dlq_depth 应计 FAILED+dead_letter shard 数(1)");
    assertTrue(prom.contains("scheduler_worker_active"), "missing scheduler_worker_active in prometheus");
    // 存活 worker = 读 worker 心跳表(非选主锁):@BeforeEach 恒种子 2 条 ALIVE worker(w-demo/w-flaky)。
    assertEquals(2.0, promGauge(prom, "scheduler_worker_active"), 0.0,
        "scheduler_worker_active 应等于存活 worker 数(2)");
  }

  /** 取 prometheus 文本中无标签 gauge 的数值(单 series)。 */
  private double promGauge(String prom, String name) {
    for (String line : prom.split("\n")) {
      if (line.startsWith(name + " ")) return Double.parseDouble(line.substring(line.indexOf(' ') + 1));
    }
    throw new AssertionError("no plain-valued gauge " + name + " in:\n" + prom);
  }

  /** 触发一次任务(扇出 → 父 header + N 个 DUE shard,shard_count 默认 1),返回父 execution id。 */
  private long triggerParent(long taskId) throws Exception {
    MvcResult r = mvc.perform(post("/api/v1/tasks/" + taskId + "/trigger"))
        .andExpect(status().isCreated()).andReturn();
    return objectMapper.readTree(r.getResponse().getContentAsString()).get("id").asLong();
  }

  /** 暂停一个 DAG:勿让 cron(CLOCK 钉在 10:05 tick)在 scanOnce 里额外建调度 run,只留手动 run(断言计数确定性)。 */
  private void pauseDag(long dagId) throws Exception {
    mvc.perform(post("/api/v1/dags/" + dagId + "/pause")).andExpect(status().isOk());
  }

  /** 手动触发一个 DAG:建 dag_run + 全 PENDING 节点,返回 run id(镜像 DagController.trigger)。 */
  private long triggerDag(long dagId) throws Exception {
    MvcResult r = mvc.perform(post("/api/v1/dags/" + dagId + "/trigger"))
        .andExpect(status().isCreated()).andReturn();
    return objectMapper.readTree(r.getResponse().getContentAsString()).get("id").asLong();
  }

  /** 取某 run 中指定 nodeKey 的 dag_run_node id(M5.3 rerun 用例)。 */
  private long dagNodeId(long runId, String key) throws Exception {
    String body = mvc.perform(get("/api/v1/dags/runs/" + runId))
        .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
    for (JsonNode n : objectMapper.readTree(body).get("nodes"))
      if (n.get("node").get("nodeKey").asText().equals(key)) return n.get("node").get("id").asLong();
    throw new AssertionError("no node " + key);
  }
  /** 取某 run 中指定 nodeKey 的 dag_run_node 当前 execution_id(M5.3 rerun 用例)。 */
  private long dagNodeExecutionId(long runId, String key) throws Exception {
    String body = mvc.perform(get("/api/v1/dags/runs/" + runId))
        .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
    for (JsonNode n : objectMapper.readTree(body).get("nodes"))
      if (n.get("node").get("nodeKey").asText().equals(key)) return n.get("node").get("executionId").asLong();
    throw new AssertionError("no node " + key);
  }

  /** M6.3:模拟外部 worker 完成某节点本次 execution 的全部分片(server 不再内嵌执行器)。
   *  status 传入 'SUCCESS' 或对失败路径 'FAILED' + dead_letter=true(cancel_siblings 不受影响)。
   *  对 rerun 用例,读的是该节点最新的 execution_id(即重跑新建的那条)。 */
  private void completeNodeShards(long runId, String nodeKey, String status) throws Exception {
    long execId = dagNodeExecutionId(runId, nodeKey);
    if ("FAILED".equals(status)) {
      jdbc.update("UPDATE execution_shard SET status='FAILED', dead_letter=true WHERE execution_id=?", execId);
    } else {
      jdbc.update("UPDATE execution_shard SET status=?, worker_id='w-demo' WHERE execution_id=?", status, execId);
    }
  }

  /** 建 DAG(nodeKeys 各引用 taskIds 对应任务;edges 为 from/to 键对),期望 201,返回 dag id。 */
  private long postDag(String name, long[] taskIds, String[] nodeKeys, String[][] edges) throws Exception {
    MvcResult r = mvc.perform(post("/api/v1/dags").contentType(MediaType.APPLICATION_JSON)
        .content(dagBody(name, taskIds, nodeKeys, edges)))
        .andExpect(status().isCreated()).andReturn();
    return objectMapper.readTree(r.getResponse().getContentAsString()).get("id").asLong();
  }

  /** POST /dags 请求体;edges 为 null 表示无边。 */
  private String dagBody(String name, long[] taskIds, String[] nodeKeys, String[][] edges) {
    StringBuilder nodes = new StringBuilder();
    for (int i = 0; i < nodeKeys.length; i++) {
      if (i > 0) nodes.append(",");
      nodes.append("{\"nodeKey\":\"").append(nodeKeys[i]).append("\",\"taskId\":").append(taskIds[i]).append("}");
    }
    StringBuilder eb = new StringBuilder();
    if (edges != null) {
      for (int i = 0; i < edges.length; i++) {
        if (i > 0) eb.append(",");
        eb.append("{\"from\":\"").append(edges[i][0]).append("\",\"to\":\"").append(edges[i][1]).append("\"}");
      }
    }
    return "{\"name\":\"" + name + "\",\"cron\":\"" + CRON + "\",\"nodes\":[" + nodes + "],\"edges\":[" + eb + "]}";
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

  /** 裸 jdbc 落一条任务行并返回 id(不经写端点,不产生 task.create 审计)——供审计读 API 用例布置 targetId 而不干扰计数。 */
  private long seedTaskRow(String name) {
    jdbc.update("INSERT INTO app_task (name, kind, handler_ref, cron, shard_count, enabled, paused)"
        + " VALUES (?, 'cron', 'demo', ?, 1, false, false)", name, CRON);
    return jdbc.queryForObject("SELECT id FROM app_task WHERE name=?", Long.class, name);
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
