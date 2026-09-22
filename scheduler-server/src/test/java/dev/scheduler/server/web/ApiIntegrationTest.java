package dev.scheduler.server.web;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import dev.scheduler.persistence.AuthHashing;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.WorkerRegistration;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.server.reconcile.Reconciler;
import dev.scheduler.server.service.AuthService;
import dev.scheduler.server.trigger.TriggerEngine;
import jakarta.servlet.http.Cookie;
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
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.boot.test.autoconfigure.web.servlet.MockMvcBuilderCustomizer;
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
        "management.prometheus.metrics.export.enabled=true",
        // 操作者目录引导:上下文启动时幂等 upsert(OperatorBootstrap);既有写用例经下方默认头 alice 授权零改动。
        "scheduler.operators=alice:ADMIN,bob:OPERATOR",
        // 默认口令引导:上下文启动时对无密操作者(alice/bob)应用 BCrypt 默认口令;Task3 用它登录 alice。
        "scheduler.operators.default-password=boot-pass"
    })
@AutoConfigureMockMvc
class ApiIntegrationTest {

  @Container static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine");

  private static final String CRON = "0 */5 * * * *"; // 6 字段(强制不外泄 5 字段)
  private static final Instant BASE = Instant.parse("2026-01-01T10:05:00Z");
  static final MutableClock CLOCK = new MutableClock(BASE);

  /** 会话令牌明文(64-char lower hex,与 AuthService.randomToken 同形):alice/bob/carol/dave 在 resetDb 播种落库;
   *  mallory 不播种 → 其 cookie 解析为空(匿名),用于 401 用例。 */
  private static final String ALICE_TOKEN = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  private static final String BOB_TOKEN = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  private static final String CAROL_TOKEN = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
  private static final String DAVE_TOKEN = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
  private static final String MALLORY_TOKEN = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

  @DynamicPropertySource
  static void datasource(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", PG::getJdbcUrl);
    registry.add("spring.datasource.username", PG::getUsername);
    registry.add("spring.datasource.password", PG::getPassword);
  }

  @Autowired MockMvc mvc;
  @Autowired ObjectMapper objectMapper;
  @Autowired JdbcTemplate jdbc;
  /** 强认证服务:轮换/解析断言(旧 token 失效、新 token 可解析)读它。 */
  @Autowired AuthService authService;
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
   * 给 MockMvc 请求默认带 session cookie(session=ALICE_TOKEN,ADMIN),使既有未显式断言权限的写用例零改动
   * 通过写端授权;拦截器按 cookie 解析身份。权限相关用例显式 .cookie(new Cookie("session", X_TOKEN)) 覆盖默认
   * (Spring 6.1 MockHttpServletRequestBuilder cookie 合并按 name 判重,默认 cookie 被同名显式 cookie 顶掉)。
   */
  @TestConfiguration
  static class DefaultOperatorConfig {
    @Bean
    MockMvcBuilderCustomizer defaultOperatorCookie() {
      return builder -> builder.defaultRequest(
          get("/").cookie(new Cookie("session", ALICE_TOKEN)));
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
        + " execution_shard_outcome, app_task, app_audit, app_audit_archive, worker,"
        + " app_auth_session, app_login_attempt, app_notification, app_task_event RESTART IDENTITY CASCADE");
    // app_operator 不在 TRUNCATE 之列(写端授权依赖其在引导/测试期间恒在;且不清 password_hash,保留上下文
    // 启动时 boot-pass 引导的口令,login(boot-pass) 恒可用):幂等确保 alice/bob/carol/dave 每用例都在,
    //  即便某用例 deactivate 过也不会让后续用例缺人。
    jdbc.update("INSERT INTO app_operator(name, role, active) VALUES "
        + "('alice','ADMIN',true),('bob','OPERATOR',true),('carol','ADMIN',true),('dave','OPERATOR',true)"
        + " ON CONFLICT (name) DO UPDATE SET role = EXCLUDED.role, active = true");
    // 为每个授权 actor 播种固定会话(SHA-256(token) 落库;明文不入库)。mallory 不播种 → 其 cookie 恒匿名(401)。
    jdbc.update("INSERT INTO app_auth_session(token_hash, operator_name, created_at, expires_at) VALUES"
            + " (?, 'alice', now(), now() + interval '8 hour'),"
            + " (?, 'bob', now(), now() + interval '8 hour'),"
            + " (?, 'carol', now(), now() + interval '8 hour'),"
            + " (?, 'dave', now(), now() + interval '8 hour')",
        AuthHashing.sha256(ALICE_TOKEN), AuthHashing.sha256(BOB_TOKEN),
        AuthHashing.sha256(CAROL_TOKEN), AuthHashing.sha256(DAVE_TOKEN));
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

  /** 3a:间隔触发任务(cron=null + intervalSeconds)可建;timezone 缺省回读 UTC;GET 读回新列。 */
  @Test
  void createTask_intervalTrigger_timezoneDefaultsUtc() throws Exception {
    String body = "{\"name\":\"interval-task\",\"kind\":\"interval\",\"handlerRef\":\"demo\","
        + "\"intervalSeconds\":60,\"timezone\":\"Asia/Shanghai\"}";
    MvcResult r = mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isCreated())
        .andReturn();
    long id = objectMapper.readTree(r.getResponse().getContentAsString()).get("id").asLong();

    mvc.perform(get("/api/v1/tasks/" + id))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.cron").value((String) null))
        .andExpect(jsonPath("$.intervalSeconds").value(60))
        .andExpect(jsonPath("$.timezone").value("Asia/Shanghai"));
  }

  /** 3a:cron 与 intervalSeconds 同给 → 400(cron 或 interval 恰具其一)。 */
  @Test
  void createTask_bothCronAndInterval_rejects400() throws Exception {
    String body = "{\"name\":\"both\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
        + "\"cron\":\"" + CRON + "\",\"intervalSeconds\":30}";
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isBadRequest());
  }

  /** 3b:事件触发任务(eventRoutes 非空,cron/interval 皆空)可建;kind 缺省 'event';GET 读回 eventRoutes。 */
  @Test
  void createTask_eventTrigger_persistsEventRoutes_andReadsBack() throws Exception {
    String body = "{\"name\":\"evt-task\",\"kind\":\"event\",\"handlerRef\":\"demo\","
        + "\"eventRoutes\":[\"order.created\",\"order.updated\"]}";
    MvcResult r = mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isCreated())
        .andReturn();
    long id = objectMapper.readTree(r.getResponse().getContentAsString()).get("id").asLong();

    mvc.perform(get("/api/v1/tasks/" + id))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.cron").value((String) null))
        .andExpect(jsonPath("$.intervalSeconds").value((Object) null))
        .andExpect(jsonPath("$.eventRoutes[0]").value("order.created"))
        .andExpect(jsonPath("$.eventRoutes[1]").value("order.updated"));

    // kind 缺省:用户未给时按事件触发推导为 'event'(与 cron 缺省 'cron' 对齐)。
    String body2 = "{\"name\":\"evt-default-kind\",\"handlerRef\":\"demo\","
        + "\"eventRoutes\":[\"order.created\"]}";
    MvcResult r2 = mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body2))
        .andExpect(status().isCreated())
        .andReturn();
    assertEquals("event", objectMapper.readTree(r2.getResponse().getContentAsString()).get("kind").asText());
  }

  /** 3b:cron 与 eventRoutes 同给 → 400(三种触发时钟恰具其一)。 */
  @Test
  void createTask_cronAndEventRoutes_rejects400() throws Exception {
    String body = "{\"name\":\"mixed-clock\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
        + "\"cron\":\"" + CRON + "\",\"eventRoutes\":[\"order.created\"]}";
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isBadRequest());
  }

  /** 3b:三种触发时钟全缺 → 400(坏任务,否则永不触发)。空路由数组(视为无事件时钟)同样拒绝。 */
  @Test
  void createTask_noTriggerClock_rejects400() throws Exception {
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON)
        .content("{\"name\":\"no-clock\",\"handlerRef\":\"demo\"}"))
        .andExpect(status().isBadRequest());
    mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON)
        .content("{\"name\":\"empty-routes\",\"handlerRef\":\"demo\",\"eventRoutes\":[]}"))
        .andExpect(status().isBadRequest());
  }

  /** 3b:POST /api/v1/events 落 PENDING 事件行并回显 id/routeKey/dedupeKey;缺 routeKey 或 dedupeKey → 400。 */
  @Test
  void postEvent_persistsPendingRow_andRequiresRouteAndDedupe() throws Exception {
    String body = "{\"routeKey\":\"order.created\",\"payload\":{\"oid\":7},\"dedupeKey\":\"evt-1\"}";
    MvcResult r = mvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isCreated())
        .andReturn();
    long id = objectMapper.readTree(r.getResponse().getContentAsString()).get("id").asLong();
    assertTrue(id > 0);
    // 分派循环在测试上下文被关闭(loop.enabled=false),事件保持 PENDING 待事件引擎。
    mvc.perform(get("/api/v1/events/" + id))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.routeKey").value("order.created"))
        .andExpect(jsonPath("$.dedupeKey").value("evt-1"))
        .andExpect(jsonPath("$.status").value("PENDING"));

    mvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON)
        .content("{\"payload\":{},\"dedupeKey\":\"x\"}"))
        .andExpect(status().isBadRequest());
    mvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON)
        .content("{\"routeKey\":\"r\"}"))
        .andExpect(status().isBadRequest());
  }

  /** 3b:相同 dedupeKey 重放 POST → 幂等返回既有行 id,不新建(防重放)。 */
  @Test
  void postEvent_replayedByDedupeKey_returnsExistingRow() throws Exception {
    String body = "{\"routeKey\":\"order.created\",\"payload\":{\"oid\":7},\"dedupeKey\":\"evt-dup\"}";
    MvcResult first = mvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isCreated()).andReturn();
    long id1 = objectMapper.readTree(first.getResponse().getContentAsString()).get("id").asLong();

    MvcResult replay = mvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isCreated()).andReturn();
    long id2 = objectMapper.readTree(replay.getResponse().getContentAsString()).get("id").asLong();
    assertEquals(id1, id2, "重放命中既有行");
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_task_event", Long.class));
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

  /** 审计读端联合检索:beforeField/metaField 过滤 before_meta/meta 顶层键,与 diffField 可跨列 AND;读端直种子。 */
  @Test
  void auditReadApi_jointJsonFilter_beforeAndMetaField() throws Exception {
    long a = seedTaskRow("jf-a"), b = seedTaskRow("jf-b"), c = seedTaskRow("jf-c");
    // A:before+meta 含 shardCount,diff 含 cron;  B:before+meta 含 cron,无 diff;  C:仅 meta name,无 diff/before。
    jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta, diff, before_meta, source)"
            + " VALUES ('opA','task.update','task',?, '{ \"name\":\"a\",\"shardCount\":1 }'::json,"
            + " '{ \"cron\":[\"x\"] }'::json, '{ \"name\":\"bA\",\"shardCount\":2 }'::json, 'cli')", a);
    jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta, diff, before_meta)"
            + " VALUES ('opB','dag.pause','dag',?, '{ \"name\":\"B\",\"cron\":\"0 *\" }'::json,"
            + " NULL, '{ \"name\":\"bB\",\"shardCount\":3,\"cron\":\"0 *\" }'::json)", b);
    jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta)"
            + " VALUES ('opC','execution.cancel','execution',?, '{ \"name\":\"C\" }')", c);

    // beforeField → 过滤 before_meta 顶层键存在(C 无 before → 排除)
    mvc.perform(get("/api/v1/audits").param("beforeField", "shardCount"))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(2));
    mvc.perform(get("/api/v1/audits").param("beforeField", "cron"))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].operator").value("opB"));
    mvc.perform(get("/api/v1/audits").param("beforeField", "name"))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(2)); // A,B(皆含 name);C 无 before
    // metaField → 过滤 meta 顶层键存在
    mvc.perform(get("/api/v1/audits").param("metaField", "cron"))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].operator").value("opB"));
    mvc.perform(get("/api/v1/audits").param("metaField", "name"))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(3));
    // 跨列 AND:diff 含 cron 且 before 含 shardCount → 仅 A
    mvc.perform(get("/api/v1/audits").param("diffField", "cron").param("beforeField", "shardCount"))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].operator").value("opA"));
    // 未命中组合 → 0(before 含 cron 的行 B,其 meta 无 shardCount;meta 含 shardCount 的行 A,其 before 无 cron → 无交叠)
    mvc.perform(get("/api/v1/audits").param("beforeField", "cron").param("metaField", "shardCount"))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(0));
  }

  /** 审计导出:GET /api/v1/audits/export 同过滤全量 CSV,attachment;BOM + 命中/排除 + CSV 转义 + 公式注入防护。 */
  @Test
  void auditExport_csv_allFilteredRows_escapedAndFormulaSafe() throws Exception {
    // 过滤 = beforeField shardCount:A(含 shardCount)命中;B(含 shardCount,operator 为公式向量)命中;C(无 shardCount)排除。
    long a = seedTaskRow("e-a"), b = seedTaskRow("e-b"), c = seedTaskRow("e-c");
    // A:命中;source 含双引号与逗号 → 验证单元格转义。
    String trickySource = "cli;v=abc\"def,ghi";
    jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta, diff, before_meta, source)"
            + " VALUES ('opA','task.update','task',?, '{ \"name\":\"a\" }'::json,"
            + " '{ \"cron\":[\"x\"] }'::json, '{ \"shardCount\":2 }'::json, ?)", a, trickySource);
    // B:命中;operator 以 '=' 开头 → 验证公式注入防护前置单引号。
    jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta, before_meta)"
            + " VALUES ('=1+1','dag.pause','dag',?,'{ \"name\":\"B\" }'::json, '{ \"shardCount\":3 }'::json)", b);
    // C:排除(无 shardCount)。
    jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta, before_meta)"
            + " VALUES ('opX','execution.cancel','execution',?,'{ \"name\":\"C\" }'::json, '{ \"name\":\"cC\" }'::json)", c);

    MvcResult r = mvc.perform(get("/api/v1/audits/export").param("beforeField", "shardCount"))
        .andExpect(status().isOk())
        .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.content().contentTypeCompatibleWith(
            MediaType.valueOf("text/csv")))
        .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.header()
            .string("Content-Disposition", "attachment; filename=\"audits.csv\""))
        .andReturn();
    byte[] raw = r.getResponse().getContentAsByteArray();
    assertTrue(raw.length >= 3 && (raw[0] & 0xFF) == 0xEF && (raw[1] & 0xFF) == 0xBB && (raw[2] & 0xFF) == 0xBF,
        "应带 UTF-8 BOM(0xEF 0xBB 0xBF)以便 Excel 解码中文");
    String body = r.getResponse().getContentAsString();
    assertTrue(body.contains("occurredAt,operator,action,targetType,targetId,source,meta,diff,before"), "表头行");
    assertTrue(body.contains("opA"), "命中行计入");
    assertTrue(body.contains("\"cli;v=abc\"\"def,ghi\""), "含逗号/引号单元格应双引号包裹且引号翻倍:" + body);
    assertTrue(body.contains("'=1+1"), "公式注入向量前置单引号防注入(应含 B):" + body);
    assertTrue(!body.contains("opX"), "过滤排除行(不带 shardCount)不应出现在导出行");
  }

  /** 审计取证链完整性:GET /api/v1/audits/integrity。写端产生真实链 → verified;篡改审计行 meta → 定位该行。 */
  @Test
  void auditIntegrity_api_cleanThenFlagsTamper() throws Exception {
    // 经写端创建任务 → AuditRecorder 落一条带链哈希的 task.create。
    objectMapper.readTree(mvc.perform(post("/api/v1/tasks").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"chain-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());

    mvc.perform(get("/api/v1/audits/integrity"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.verified").value(true))
        .andExpect(jsonPath("$.totalRecords").value(1))
        .andExpect(jsonPath("$.chainedRecords").value(1))
        .andExpect(jsonPath("$.firstTamperedId").value(org.hamcrest.Matchers.nullValue()));

    // 篡改该审计行 meta → 自哈希不再匹配 → verified=false,firstTamperedId 指向它。
    long auditId = jdbc.queryForObject("SELECT id FROM app_audit WHERE action='task.create'", Long.class);
    jdbc.update("UPDATE app_audit SET meta='{\"name\":\"evil\"}'::jsonb WHERE id=?", auditId);
    mvc.perform(get("/api/v1/audits/integrity"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.verified").value(false))
        .andExpect(jsonPath("$.chainedRecords").value(1))
        .andExpect(jsonPath("$.firstTamperedId").value(auditId));
  }

  /** 审计归档端点:POST /api/v1/audits/archive(ADMIN)。归档旧行即删即重链 → integrity 仍 verified;
   *  OPERATOR 调用 → 403 且落 access.denied。 */
  @Test
  void auditArchive_api_adminArchivesOldRows_thenIntegrityStillVerified() throws Exception {
    // 写端产生一条真实审计链行,再把其 occurred_at 回拨到过去 → eligible for archive。
    objectMapper.readTree(mvc.perform(post("/api/v1/tasks").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"archive-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
    long auditId = jdbc.queryForObject("SELECT id FROM app_audit WHERE action='task.create'", Long.class);
    jdbc.update("UPDATE app_audit SET occurred_at=? WHERE id=?",
        new java.sql.Timestamp(Instant.parse("2020-01-01T00:00:00Z").toEpochMilli()), auditId);

    // ADMIN(alice)归档 → 返回 archived=1;归档删除不透支取证链。
    mvc.perform(post("/api/v1/audits/archive")
            .cookie(session(ALICE_TOKEN))
            .param("olderThan", "2021-01-01T00:00:00Z"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.archived").value(1))
        .andExpect(jsonPath("$.olderThan").isNotEmpty());
    // 归档动作本身又追记一条 target=none 的 audit.archive(新行 occurred_at=now,不再 eligible)。
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_audit", Long.class));
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_audit WHERE action='audit.archive' AND target_type='none'", Long.class));
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_audit_archive", Long.class));
    // 归档删除 + 重链后,剩余 audit.archive 单行自成链头,integrity 仍 verified。
    mvc.perform(get("/api/v1/audits/integrity"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.verified").value(true))
        .andExpect(jsonPath("$.totalRecords").value(1))
        .andExpect(jsonPath("$.chainedRecords").value(1));

    // OPERATOR(bob)无权归档 → 403 + access.denied 审计。
    mvc.perform(post("/api/v1/audits/archive")
            .cookie(session(BOB_TOKEN))
            .param("olderThan", "2021-01-01T00:00:00Z"))
        .andExpect(status().isForbidden());
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_audit WHERE action='access.denied' AND operator='bob'", Long.class));
  }

  // ---------- 审计写端:三控制器 15 个写端点,审计operator取会话 cookie principal ----------

  /** 审计写端:TaskController create/update/pause/resume/trigger/delete;operator 取 CurrentOperator(会话 principal);meta 为后态。 */
  @Test
  void taskWriteActions_areAudited() throws Exception {
    // create(以 alice cookie)→ operator=alice,meta 含后态 name
    long id = objectMapper.readTree(mvc.perform(post("/api/v1/tasks").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"audited-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
    mvc.perform(get("/api/v1/audits").param("operator", "alice").param("action", "task.create")
            .param("targetId", String.valueOf(id)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].targetType").value("task"))
        .andExpect(jsonPath("$['items'][0].meta").value(org.hamcrest.Matchers.containsString("audited-task")))
        .andExpect(jsonPath("$['items'][0].diff").value(org.hamcrest.Matchers.nullValue()));

    // update → task.update,targetId=id,meta 为后态(改名后)
    mvc.perform(put("/api/v1/tasks/" + id).cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"renamed-audit\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"0 */6 * * * *\",\"shardCount\":1}"))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "task.update").param("targetId", String.valueOf(id)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].meta").value(org.hamcrest.Matchers.containsString("renamed-audit")))
        .andExpect(jsonPath("$['items'][0].diff").value(org.hamcrest.Matchers.containsString("renamed-audit")))
        .andExpect(jsonPath("$['items'][0].diff").value(org.hamcrest.Matchers.matchesPattern(
            ".*\"name\"\\s*:\\s*\\[\"audited-task\"\\s*,\\s*\"renamed-audit\"\\s*\\].*")))
        .andExpect(jsonPath("$['items'][0].diff").value(org.hamcrest.Matchers.not(
            org.hamcrest.Matchers.containsString("shardCount"))))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("audited-task")))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.not(
            org.hamcrest.Matchers.containsString("renamed-audit"))));

    // 写端点缺省头(defaultRequest=alice)→ 审计记录 alice;写必需已授权的操作者,不再兜底 anonymous
    mvc.perform(post("/api/v1/tasks/" + id + "/pause")).andExpect(status().isOk());
    mvc.perform(post("/api/v1/tasks/" + id + "/resume")).andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "task.pause"))
        .andExpect(status().isOk()).andExpect(jsonPath("$['items'][0].operator").value("alice"));
    mvc.perform(get("/api/v1/audits").param("action", "task.resume"))
        .andExpect(status().isOk()).andExpect(jsonPath("$['items'][0].operator").value("alice"));

    // trigger → task.trigger(目标 = 任务 id)
    mvc.perform(post("/api/v1/tasks/" + id + "/trigger").cookie(session(ALICE_TOKEN)))
        .andExpect(status().isCreated());
    mvc.perform(get("/api/v1/audits").param("action", "task.trigger").param("targetId", String.valueOf(id)))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));

    // 该 id 已被 trigger(有 execution)→ delete 409(不记 action);换个未触发的任务验 delete
    mvc.perform(delete("/api/v1/tasks/" + id)).andExpect(status().isConflict());
    // 409-conflict delete 记录为空:动作实际未发生
    mvc.perform(get("/api/v1/audits").param("action", "task.delete").param("targetId", String.valueOf(id)))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(0));
    long delId = objectMapper.readTree(mvc.perform(post("/api/v1/tasks").cookie(session(BOB_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"audit-del\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
    // task.delete 需 ADMIN:改用 carol(ADMIN),审计仍记录操作者 carol
    mvc.perform(delete("/api/v1/tasks/" + delId).cookie(session(CAROL_TOKEN))).andExpect(status().isNoContent());
    mvc.perform(get("/api/v1/audits").param("action", "task.delete").param("targetId", String.valueOf(delId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][0].operator").value("carol"));
  }

  /** 同值 update(不改任何字段)→ task.update,但 before/after 无差异 → diff 为空对象 {},meta 仍全量后态。 */
  @Test
  void taskUpdate_identicalFields_diffIsEmptyObject() throws Exception {
    long id = objectMapper.readTree(mvc.perform(post("/api/v1/tasks")            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"same-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
    // 完全相同的定义再 PUT 一次(不改任何字段)→ task.update,但 diff 应为空对象 {}
    mvc.perform(put("/api/v1/tasks/" + id)            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"same-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "task.update").param("targetId", String.valueOf(id)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].diff").value("{}"));
  }

  /** 取证:task.delete 物理删除后其完整定义只在 before 留存,可据此还原。 */
  @Test
  void taskDelete_beforeSnapshotRetainsDefinition() throws Exception {
    long delId = objectMapper.readTree(mvc.perform(post("/api/v1/tasks").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"snap-del\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":2}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
    mvc.perform(delete("/api/v1/tasks/" + delId).cookie(session(ALICE_TOKEN))).andExpect(status().isNoContent());
    // 任务已物理删除;审计行仍持 before 全量快照
    mvc.perform(get("/api/v1/audits").param("action", "task.delete").param("targetId", String.valueOf(delId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("snap-del")))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.matchesPattern(
            ".*\"shardCount\"\\s*:\\s*2.*")));
  }

  /** 同值 update 亦有 before(diff={} 但 before 为全量前态)。 */
  @Test
  void taskUpdate_identicalFields_hasBeforeThoEmptyDiff() throws Exception {
    long id = objectMapper.readTree(mvc.perform(post("/api/v1/tasks")            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"same-before\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
    mvc.perform(put("/api/v1/tasks/" + id)            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"same-before\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "task.update").param("targetId", String.valueOf(id)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].diff").value("{}"))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("same-before")));
  }

  // ---------- 审计 before:execution/shard/dag 动作快照(子项目3 扩展) ----------

  /** before 覆盖 execution.cancel / shard.requeue:操作前记录全量(取消前父 DUE、分片复位前 attempt)。 */
  @Test
  void executionCancelAndShardRequeue_beforeSnapshot() throws Exception {
    // execution.cancel:父 DUE 直取消 → before 为取消前父(状态 DUE, attempt 0)
    long t = postTask("bs-exec");
    long parentId = triggerParent(t); // DUE 父 + 1 DUE shard
    mvc.perform(post("/api/v1/executions/" + parentId + "/cancel")).andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "execution.cancel").param("targetId", String.valueOf(parentId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("DUE")));

    // shard.requeue:FAILED shard 复位前快照留存 attempt=3(已置高)/ FAILED;requeue 后 attempt 复位 0
    long t2 = postTask("bs-req");
    Shard dlq = seedDeadLetter(t2);
    jdbc.update("UPDATE execution_shard SET attempt=3 WHERE id=?", dlq.id());
    mvc.perform(post("/api/v1/executions/shards/" + dlq.id() + "/requeue"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.attempt").value(0));
    mvc.perform(get("/api/v1/audits").param("action", "shard.requeue").param("targetId", String.valueOf(dlq.id())))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("FAILED")))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.matchesPattern(
            ".*\"attempt\"\\s*:\\s*3.*")));
  }

  /** before 覆盖 execution.rerun:before = 被引用的源轮全量(source execution 的 id 进 before)。 */
  @Test
  void executionRerun_beforeIsSourceRound() throws Exception {
    long id = postTask("bs-rerun");
    long sourceId = triggerParent(id); // DUE 父 + 1 DUE shard
    for (Long sid : jdbc.queryForList("SELECT id FROM execution_shard WHERE execution_id=?",
        Long.class, sourceId)) {
      jdbc.update("UPDATE execution_shard SET status='SUCCESS', finished_at=now() WHERE id=?", sid);
    }
    jdbc.update("UPDATE execution SET status='SUCCESS', finished_at=now() WHERE id=? AND status='DUE'", sourceId);
    long newId = objectMapper.readTree(mvc.perform(post("/api/v1/executions/" + sourceId + "/rerun"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).path("id").asLong();
    mvc.perform(get("/api/v1/audits").param("action", "execution.rerun").param("targetId", String.valueOf(newId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        // before 是源轮全量:其 id = sourceId(而非 targetId=newId)
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.matchesPattern(
            ".*\"id\"\\s*:\\s*" + sourceId + ".*")));
  }

  /** before 覆盖 dag.pause / dag.trigger / dag_run.cancel:操作前 dag/run 全量。 */
  @Test
  void dagWriteActions_beforeSnapshot() throws Exception {
    long t = postTask("bs-dag-task");
    long dagId = postDag("bs-dag", new long[]{t}, new String[]{"A"}, null);
    // dag.pause:before 为暂停前 dag(含名称)
    mvc.perform(post("/api/v1/dags/" + dagId + "/pause")).andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "dag.pause").param("targetId", String.valueOf(dagId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("bs-dag")));
    // dag.trigger:before 为触发前 dag 定义
    long runId = triggerDag(dagId);
    mvc.perform(get("/api/v1/audits").param("action", "dag.trigger").param("targetId", String.valueOf(dagId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("bs-dag")));
    // dag_run.cancel:before 为取消前 run(PENDING)
    mvc.perform(post("/api/v1/dags/runs/" + runId + "/cancel")).andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "dag_run.cancel").param("targetId", String.valueOf(runId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("PENDING")));
  }

  /** before 覆盖 dag_node.rerun:before 为被重跑节点重跑前全量(终态 SUCCESS, nodeKey=A)。 */
  @Test
  void dagNodeRerun_beforeSnapshot() throws Exception {
    long t = postTask("bs-node");
    long dagId = postDag("bs-node-dag", new long[]{t}, new String[]{"A"}, null);
    pauseDag(dagId);
    long runId = triggerDag(dagId);
    dagEngine.scanOnce();              // spawn A → RUNNING
    completeNodeShards(runId, "A", "SUCCESS");
    reconciler.scanOnce();             // A 父 SUCCESS
    dagEngine.scanOnce();              // A 派生 SUCCESS → run 终态
    long aNodeId = dagNodeId(runId, "A");
    mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + aNodeId + "/rerun"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("RUNNING"));
    mvc.perform(get("/api/v1/audits").param("action", "dag_node.rerun").param("targetId", String.valueOf(runId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.matchesPattern(
            ".*\"nodeKey\"\\s*:\\s*\"A\".*")))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("SUCCESS")));
  }

  /** 审计读 diff 过滤:hasDiff=true 仅真变更行(排除 {} 与 NULL);diffField=cron 命中改过 cron 的行;组合生效。 */
  @Test
  void auditReadApi_diffFilters() throws Exception {
    // 真变更:create(create→diff NULL)后 PUT 改 cron → task.update 的非空 diff
    long changed = objectMapper.readTree(mvc.perform(post("/api/v1/tasks")            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"diff-c-1\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
    mvc.perform(put("/api/v1/tasks/" + changed)            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"diff-c-1\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"0 */9 * * * *\",\"shardCount\":1}"))
        .andExpect(status().isOk());
    // no-op:同值再 PUT → task.update 的 diff 为空对象 {}
    long same = objectMapper.readTree(mvc.perform(post("/api/v1/tasks")            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"diff-s-1\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
    mvc.perform(put("/api/v1/tasks/" + same)            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"diff-s-1\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isOk());

    // hasDiff=true → 仅真变更行(排除 {} 的 no-op update 与 NULL 的 create)
    mvc.perform(get("/api/v1/audits").param("hasDiff", "true").param("operator", "alice"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].targetId").value(changed))
        .andExpect(jsonPath("$['items'][0].diff").value(org.hamcrest.Matchers.containsString("cron")));

    // diffField=cron → 同样仅命中改过 cron 的行
    mvc.perform(get("/api/v1/audits").param("diffField", "cron").param("operator", "alice"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].targetId").value(changed));

    // 组合 hasDiff=true + diffField=cron
    mvc.perform(get("/api/v1/audits").param("hasDiff", "true").param("diffField", "cron").param("operator", "alice"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1));

    // 未改过的字段 shardCount → 0 命中(顶层键不在任何 diff 中)
    mvc.perform(get("/api/v1/audits").param("diffField", "shardCount").param("operator", "alice"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(0));
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
            .cookie(session(CAROL_TOKEN))).andExpect(status().isCreated()).andReturn()
        .getResponse().getContentAsString()).get("id").asLong();
    mvc.perform(get("/api/v1/audits").param("action", "execution.rerun").param("targetId", String.valueOf(newId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][0].operator").value("carol"));

    // requeue FAILED+dead_letter shard → shard.requeue
    Shard dlq = seedDeadLetter(id);
    mvc.perform(post("/api/v1/executions/shards/" + dlq.id() + "/requeue").cookie(session(CAROL_TOKEN)))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "shard.requeue").param("targetId", String.valueOf(dlq.id())))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));

    // cancel(无 RUNNING shard → 直取消)→ execution.cancel
    long cancelTarget = triggerParent(id);
    mvc.perform(post("/api/v1/executions/" + cancelTarget + "/cancel").cookie(session(CAROL_TOKEN)))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "execution.cancel").param("targetId", String.valueOf(cancelTarget)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][0].operator").value("carol"))
        .andExpect(jsonPath("$.total").value(1));

    // 幂等重取消已 CANCELED → 200 短路,不产生新审计(动作实际未发生):total 仍 1
    mvc.perform(post("/api/v1/executions/" + cancelTarget + "/cancel").cookie(session(CAROL_TOKEN)))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "execution.cancel").param("targetId", String.valueOf(cancelTarget)))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));
  }

  /** 审计写端:DagController create/pause/resume/trigger + dag_run.cancel + dag_node.rerun(meta 含 nodeId)。 */
  @Test
  void dagWriteActions_areAudited() throws Exception {
    long t = postTask("audit-dag-task");
    long dagId = postDag("audit-dag", new long[]{t}, new String[]{"A"}, new String[][]{});
    // postDag 缺省头(defaultRequest=alice)→ 审计记录 alice(写必需已授权操作者)
    mvc.perform(get("/api/v1/audits").param("action", "dag.create").param("targetId", String.valueOf(dagId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$['items'][0].operator").value("alice"))
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
    mvc.perform(post("/api/v1/dags/runs/" + runId + "/cancel").cookie(session(DAVE_TOKEN)))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "dag_run.cancel").param("targetId", String.valueOf(runId)))
        .andExpect(status().isOk()).andExpect(jsonPath("$['items'][0].operator").value("dave"));

    // 幂等重取消已 CANCELED run → 200 短路,不产生新审计:total 仍 1
    mvc.perform(post("/api/v1/dags/runs/" + runId + "/cancel").cookie(session(DAVE_TOKEN)))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "dag_run.cancel").param("targetId", String.valueOf(runId)))
        .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));

    // dag_node.rerun:终态节点可重跑(rerunNode 仅要求节点 isTerminal();A 已 CANCELED)→ targetId=runId, meta 含 nodeId
    long aId = dagNodeId(runId, "A");
    mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + aId + "/rerun").cookie(session(DAVE_TOKEN)))
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

  /** SLI(2b):近 1h 完成/失败量 + 失败率 + 近 24h 完成延迟 p95;seed 一条 SUCCESS + 一条 FAILED(均在窗口内)。 */
  @Test
  void prometheus_sliMetrics() throws Exception {
    jdbc.update("INSERT INTO app_task (id, name, kind, handler_ref, cron, shard_count, enabled, paused)"
        + " VALUES (?, 'sli-seed-task', 'cron', 'demo', ?, 1, false, false)",
        METRICS_TASK_ID, "0 */5 * * * *");
    jdbc.update("INSERT INTO execution (task_id, status, idempotency_key, shard_count, started_at, finished_at)"
        + " VALUES (?, 'SUCCESS', 'sli:done', 1, now() - interval '5 minutes', now() - interval '1 minute')",
        METRICS_TASK_ID);
    jdbc.update("INSERT INTO execution (task_id, status, idempotency_key, shard_count, started_at, finished_at)"
        + " VALUES (?, 'FAILED', 'sli:fail', 1, now() - interval '10 minutes', now() - interval '2 minutes')",
        METRICS_TASK_ID);

    String prom = mvc.perform(get("/actuator/prometheus"))
        .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
    assertTrue(prom.contains("scheduler_execution_completed_1h "), "missing completed SLI gauge");
    assertTrue(prom.contains("scheduler_execution_failed_1h "), "missing failed SLI gauge");
    assertTrue(prom.contains("scheduler_execution_failure_rate_1h "), "missing failure-rate SLI gauge");
    assertTrue(prom.contains("scheduler_execution_latency_p95_ms "), "missing p95-latency SLI gauge");
    assertEquals(1.0, promGauge(prom, "scheduler_execution_completed_1h"), 1e-9, "1h 内 1 条 SUCCESS");
    assertEquals(1.0, promGauge(prom, "scheduler_execution_failed_1h"), 1e-9, "1h 内 1 条 FAILED");
    assertEquals(0.5, promGauge(prom, "scheduler_execution_failure_rate_1h"), 1e-9,
        "失败率 = 1 / (1+1) = 0.5");
    assertTrue(promGauge(prom, "scheduler_execution_latency_p95_ms") > 0, "有完成延迟样本 → p95 > 0");
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

  /** 显式会话 cookie(session=token):用于覆盖默认 alice cookie 的权限相关用例。 */
  private static Cookie session(String token) {
    return new Cookie("session", token);
  }

  /** 从轮换响应的 Set-Cookie 提取新会话令牌明文(须已由调用方断言存在)。 */
  private static String rotationToken(MvcResult r) {
    String setCookie = r.getResponse().getHeader("Set-Cookie");
    assertTrue(setCookie != null && setCookie.startsWith("session="), "轮换应回写新会话 Set-Cookie: " + setCookie);
    String t = setCookie.substring("session=".length(), setCookie.indexOf(';')).trim();
    assertEquals(64, t.length());
    return t;
  }

  
  // ---- 操作者授权 / 审计访问控制 ----

  /** 无有效会话 cookie 的写 → 401(匿名);读端点仍开放(会话无关)。 */
  @Test
  void write_withoutSession_returns401_readStaysOpen() throws Exception {
    String body = "{\"name\":\"no-op\",\"kind\":\"cron\",\"handlerRef\":\"demo\",\"cron\":\"" + CRON + "\"}";
    mvc.perform(post("/api/v1/tasks").cookie(session(MALLORY_TOKEN))
            .contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isUnauthorized());
    // 读端点不受影响(开放)
    mvc.perform(get("/api/v1/audits").cookie(session(MALLORY_TOKEN)))
        .andExpect(status().isOk());
  }

  /** 未登记操作者(mallory 未播种会话)→ 匿名 401(cookie 会话必须由登录/播种建立,授权仅认已解析身份)。 */
  @Test
  void write_unknownOperator_withoutSession_returns401() throws Exception {
    String body = "{\"name\":\"mallory-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\",\"cron\":\"" + CRON + "\"}";
    mvc.perform(post("/api/v1/tasks").cookie(session(MALLORY_TOKEN))
            .contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isUnauthorized());
  }

  /** OPERATOR 可写常规端点(如 pause):能建任务(经默认 alice)后由 bob 暂停。 */
  @Test
  void operationalRole_routineWrite_isAllowed() throws Exception {
    long id = postTask("bob-routine");
    mvc.perform(post("/api/v1/tasks/" + id + "/pause").cookie(session(BOB_TOKEN)))
        .andExpect(status().isOk());
  }

  /** OPERATOR 删任务(提权到 ADMIN 的端点)→ 403;ADMIN 删 → 204。 */
  @Test
  void operationalRole_cannotDeleteTask_butAdminCan() throws Exception {
    long id = postTask("role-delete");
    mvc.perform(delete("/api/v1/tasks/" + id).cookie(session(BOB_TOKEN)))
        .andExpect(status().isForbidden());
    // alice(ADMIN)仍可删
    mvc.perform(delete("/api/v1/tasks/" + id).cookie(session(ALICE_TOKEN)))
        .andExpect(status().isNoContent());
  }

  /** 操作者管理整体 ADMIN:OPERATOR 连读都 403;ADMIN 可登记/停用。 */
  @Test
  void operatorManagement_isAdminOnly() throws Exception {
    mvc.perform(get("/api/v1/operators").cookie(session(BOB_TOKEN)))
        .andExpect(status().isForbidden());
    String upsert = "{\"name\":\"mallory\",\"role\":\"OPERATOR\",\"active\":true}";
    mvc.perform(post("/api/v1/operators").cookie(session(BOB_TOKEN))
            .contentType(MediaType.APPLICATION_JSON).content(upsert))
        .andExpect(status().isForbidden());
    // ADMIN 登记成功 → 目录含 mallory
    mvc.perform(post("/api/v1/operators").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON).content(upsert))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/operators").cookie(session(ALICE_TOKEN)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[*].name", hasItem("mallory")));
    // 停用后 mallory 再写被拒(mallory 无会话 → 匿名 401)
    mvc.perform(post("/api/v1/operators/mallory/deactivate").cookie(session(ALICE_TOKEN)))
        .andExpect(status().isOk());
    String body = "{\"name\":\"x\",\"kind\":\"cron\",\"handlerRef\":\"demo\",\"cron\":\"" + CRON + "\"}";
    mvc.perform(post("/api/v1/tasks").cookie(session(MALLORY_TOKEN))
            .contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isUnauthorized());
  }

  /** 停用操作者须撤销其现役会话(即使该操作者有活动 cookie):bob 有 BOB_TOKEN 会话 → 停用后该行 revoked + 再写 401。 */
  @Test
  void deactivate_revokesOperatorsActiveSession() throws Exception {
    // bob·OPERATOR 的活动会话在 resetDb 已播种(BOB_TOKEN)→ 应从 app_auth_session 读到未撤销行。
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_auth_session WHERE token_hash=? AND revoked_at IS NULL",
        Long.class, AuthHashing.sha256(BOB_TOKEN)));
    // alice(ADMIN)停用 bob
    mvc.perform(post("/api/v1/operators/bob/deactivate").cookie(session(ALICE_TOKEN)))
        .andExpect(status().isOk());
    // bob 会话被撤销(revoked_at 置非空);再用 BOB_TOKEN 写 → 401
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_auth_session WHERE token_hash=? AND revoked_at IS NOT NULL",
        Long.class, AuthHashing.sha256(BOB_TOKEN)));
    String body = "{\"name\":\"post-deact\",\"kind\":\"cron\",\"handlerRef\":\"demo\",\"cron\":\"" + CRON + "\"}";
    mvc.perform(post("/api/v1/tasks").cookie(session(BOB_TOKEN))
            .contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isUnauthorized());
  }

  // ---- 会话管理:活动会话视图 + 强制登出(ADMIN) ----

  /** 会话管理整体 ADMIN:ADMIN 可列/强制登出,OPERATOR 连读都 403;强制登出撤销活动会话并留 operator.sessions.revoke 审计。 */
  @Test
  void sessionManagement_isAdminOnly_listsAndRevokes() throws Exception {
    // bob 的 BOB_TOKEN 活动会话由 resetDb 播种 → 活动会话列表中应含其截断前缀。
    String bobPrefix = AuthHashing.sha256(BOB_TOKEN).substring(0, 10);
    mvc.perform(get("/api/v1/operators/bob/sessions").cookie(session(ALICE_TOKEN)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[*].tokenPrefix", hasItem(bobPrefix)))
        .andExpect(jsonPath("$[*].createdAt").isNotEmpty());
    // 非 ADMIN(OPERATOR bob)→ 列表/强制登出均 403。
    mvc.perform(get("/api/v1/operators/alice/sessions").cookie(session(BOB_TOKEN)))
        .andExpect(status().isForbidden());
    mvc.perform(post("/api/v1/operators/alice/sessions/revoke").cookie(session(BOB_TOKEN)))
        .andExpect(status().isForbidden());
    // ADMIN 强制登出 bob → 撤销其活动会话并记审计(归属 ADMIN)。
    mvc.perform(post("/api/v1/operators/bob/sessions/revoke").cookie(session(ALICE_TOKEN)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.revoked").value(org.hamcrest.Matchers.greaterThanOrEqualTo(1)));
    // bob 原 cookie 会话已撤 → 再写 → 401。
    String body = "{\"name\":\"post-sess-revoke\",\"kind\":\"cron\",\"handlerRef\":\"demo\",\"cron\":\"" + CRON + "\"}";
    mvc.perform(post("/api/v1/tasks").cookie(session(BOB_TOKEN))
            .contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isUnauthorized());
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_audit WHERE action='operator.sessions.revoke' AND operator='alice'",
        Long.class));
  }

  // ---- 强认证:操作者口令管理 + 默认口令引导 ----

  /** 默认口令引导:上下文启动时(application runner)对无密操作者 alice/bob 应用 boot-pass → password_hash 非空且为 BCrypt。 */
  @Test
  void bootstrapDefaultPassword_setsHashOnPasswordlessOperators() {
    String hash = jdbc.queryForObject(
        "SELECT password_hash FROM app_operator WHERE name='alice'", String.class);
    assertTrue(hash != null && hash.startsWith("$2"), "引导后 alice 应持 BCrypt 默认口令哈希,got: " + hash);
    String bobHash = jdbc.queryForObject(
        "SELECT password_hash FROM app_operator WHERE name='bob'", String.class);
    assertTrue(bobHash != null && bobHash.startsWith("$2"), "引导后 bob 也应持默认口令,got: " + bobHash);
  }

  /** 强认证:ADMIN 设/改操作者口令 → password_hash 落 BCrypt,并撤销该操作者既有活动会话(bob 旧会话 → 401)。 */
  @Test
  void adminSetsPassword_updatesHashAndRevokesSessions() throws Exception {
    // resetDb 已为 bob 播种 BOB_TOKEN 活动会话(模拟 bob 已登录)→ 改密后应被撤销。
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_auth_session WHERE token_hash=? AND revoked_at IS NULL",
        Long.class, AuthHashing.sha256(BOB_TOKEN)));

    mvc.perform(post("/api/v1/operators/bob/password").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"password\":\"new-secret-1\"}"))
        .andExpect(status().isOk());

    String hash = jdbc.queryForObject(
        "SELECT password_hash FROM app_operator WHERE name='bob'", String.class);
    assertTrue(hash != null && hash.startsWith("$2"), "改密后应落 BCrypt 哈希,got: " + hash);
    // 既有会话被撤销(revoked_at 非空),token 不再可解析。
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_auth_session WHERE operator_name='bob' AND revoked_at IS NOT NULL",
        Long.class));
    // bob 旧会话已撤 → 再以 BOB_TOKEN 写 → 401。
    String body = "{\"name\":\"post-pwd\",\"kind\":\"cron\",\"handlerRef\":\"demo\",\"cron\":\"" + CRON + "\"}";
    mvc.perform(post("/api/v1/tasks").cookie(session(BOB_TOKEN))
            .contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isUnauthorized());
  }

  /** 强认证:OPERATOR(bob)调改密端点 → 403(操作者管理整体 ADMIN)。 */
  @Test
  void operatorCannotSetPassword_returns403() throws Exception {
    mvc.perform(post("/api/v1/operators/carol/password").cookie(session(BOB_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"password\":\"new-secret-1\"}"))
        .andExpect(status().isForbidden());
  }

  /** 强认证:密码短于 8 → IllegalArgumentException → 400。 */
  @Test
  void setPassword_tooShort_returns400() throws Exception {
    mvc.perform(post("/api/v1/operators/bob/password").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"password\":\"short\"}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value(
            org.hamcrest.Matchers.containsString("at least 8")));
  }

  /** 强认证:为未登记操作者设密 → 400(避免"成功但没改到")。 */
  @Test
  void setPassword_unknownOperator_returns400() throws Exception {
    mvc.perform(post("/api/v1/operators/no-such-op/password").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"password\":\"new-secret-1\"}"))
        .andExpect(status().isBadRequest());
  }

  /** 权限拒绝会留一条 access.denied 审计(自带称操作者),自动进取证链。 */
  @Test
  void accessDenied_isAudited() throws Exception {
    long id = postTask("deny-audit");
    mvc.perform(delete("/api/v1/tasks/" + id).cookie(session(BOB_TOKEN)))
        .andExpect(status().isForbidden());
    var denied = jdbc.queryForList(
        "SELECT operator, action FROM app_audit WHERE action='access.denied'");
    assertFalse(denied.isEmpty(), "403 应落一条 access.denied 审计");
    assertEquals("bob", denied.get(0).get("operator"));
  }

  // ---- 强认证:登录 / me / 登出 / 轮换 / 写授权 ----

  /** 正确口令登录 → 200 {operator, expiresAt} + Set-Cookie(session,HttpOnly) + 落一条 SHA-256(token) 会话行。 */
  @Test
  void login_success_setsSessionCookieAndDBSessionRow() throws Exception {
    MvcResult r = mvc.perform(post("/api/v1/auth/login")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"alice\",\"password\":\"boot-pass\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.operator.name").value("alice"))
        .andExpect(jsonPath("$.operator.role").value("ADMIN"))
        .andExpect(jsonPath("$.expiresAt").isNotEmpty())
        .andReturn();
    String setCookie = r.getResponse().getHeader("Set-Cookie");
    assertTrue(setCookie != null && setCookie.startsWith("session=")
            && setCookie.contains("HttpOnly") && setCookie.contains("SameSite=Strict"),
        "登录应带回 HttpOnly 会话 Set-Cookie: " + setCookie);
    String token = setCookie.substring("session=".length(), setCookie.indexOf(';')).trim();
    assertEquals(64, token.length(), "会话令牌应为 64-char hex");
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_auth_session WHERE token_hash=? AND revoked_at IS NULL",
        Long.class, AuthHashing.sha256(token)));
  }

  /** 错误口令 → 401 且记一条 operator=提交名 的 access.denied。 */
  @Test
  void login_wrongPassword_401_auditsDenied() throws Exception {
    mvc.perform(post("/api/v1/auth/login")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"alice\",\"password\":\"wrong-pass\"}"))
        .andExpect(status().isUnauthorized());
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_audit WHERE action='access.denied' AND operator='alice'",
        Long.class));
  }

  /** 连续失败累计退避 → 锁定后正确口令也 401(locked 分支不验密直接拒)。 */
  @Test
  void login_repeatedFailures_backoffLocks() throws Exception {
    for (int i = 0; i < 6; i++) {
      mvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON)
              .content("{\"name\":\"alice\",\"password\":\"bad-" + i + "\"}"))
          .andExpect(status().isUnauthorized());
    }
    // 退避 cap 30s(DB now() 起)→ 现仍锁定,即便密码正确也 401
    mvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"alice\",\"password\":\"boot-pass\"}"))
        .andExpect(status().isUnauthorized());
  }

  /** 带 alice cookie 写 → 200(拦截器按 cookie 解析出 alice·ADMIN)。 */
  @Test
  void write_withAliceCookie_authorized() throws Exception {
    String body = "{\"name\":\"cookie-write\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
        + "\"cron\":\"" + CRON + "\",\"shardCount\":1}";
    mvc.perform(post("/api/v1/tasks").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isCreated());
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_task WHERE name='cookie-write'", Long.class));
  }

  /** 轮换:把 alice 会话 expires_at 置 now()(剩余 TTL≈0<8h/2)→ 该请求即轮换:旧 cookie 失效 + 响应新 Set-Cookie + 新 token 可解析。 */
  @Test
  void sessionRotation_remainingBelowHalf_rotatesCookieAndRevokesOld() throws Exception {
    jdbc.update("UPDATE app_auth_session SET expires_at = now() + interval '1 hour' WHERE token_hash=?",
        AuthHashing.sha256(ALICE_TOKEN));
    MvcResult r = mvc.perform(post("/api/v1/tasks").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"rot-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated())
        .andReturn();
    String setCookie = r.getResponse().getHeader("Set-Cookie");
    assertTrue(setCookie != null && setCookie.startsWith("session="),
        "轮换应回写新会话 Set-Cookie: " + setCookie);
    String newToken = setCookie.substring("session=".length(), setCookie.indexOf(';')).trim();
    assertEquals(64, newToken.length());
    assertTrue(!newToken.equals(ALICE_TOKEN), "轮换令牌应不同于旧令牌");
    // 旧 token 已轮换撤销 → 不可解析;新 token 可解析出 alice
    assertNull(authService.resolve(ALICE_TOKEN), "旧 token 轮换后失效");
    var nr = authService.resolve(newToken);
    assertNotNull(nr, "新 token 应可解析");
    assertEquals("alice", nr.operator(), "新 token 归属 alice");
  }

  /** 轮换必须保留绝对寿命基线(自首次登录 ≤5d),而非重置 created_at=now()。
   *  (a) 轮换后新行 created_at ≈ 原登录 created_at(≈5d 前),非近 now();(b) 血缘跨过 5d → 现有 SQL 拒绝 → 强制重登。 */
  @Test
  void sessionRotation_preservesAbsoluteLifetimeOrigin_andCapStillBinds() throws Exception {
    // 播种接近 5d 边缘的会话:created=5d-5h(仍 <5d 可解析),expires=1h(剩余<半程4h → 轮换)。
    jdbc.update("UPDATE app_auth_session SET created_at = now() - interval '4 days 19 hours',"
            + " expires_at = now() + interval '1 hour' WHERE token_hash=?",
        AuthHashing.sha256(ALICE_TOKEN));
    MvcResult r = mvc.perform(post("/api/v1/tasks").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"abs-cap-rot\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated())
        .andReturn();
    String newToken = rotationToken(r);
    // (a) 轮换行 created_at 应 ≈ 原登录(5d-5h=115h 前),而非重置为 now()(≈0h)。容差 ±20 分钟。
    long ageMinutes = jdbc.queryForObject(
        "SELECT extract(epoch FROM (now() - created_at)) / 60::int FROM app_auth_session WHERE token_hash=?",
        Long.class, AuthHashing.sha256(newToken));
    assertTrue(ageMinutes >= 115 * 60 - 20 && ageMinutes <= 115 * 60 + 20,
        "轮换行保留原 created_at(≈115h),实际分钟=" + ageMinutes);
    // (b) 血缘跨过 5d:现有 SQL now()-created_at<5d 拒绝 → resolve 空 → 强制重登(拿不到操作者)。
    jdbc.update("UPDATE app_auth_session SET created_at = now() - interval '6 days' WHERE token_hash=?",
        AuthHashing.sha256(newToken));
    assertNull(authService.resolve(newToken), "血缘≥5d 后轮换 token 应强制重登(空)");
  }

  /** GET /auth/me:无有效会话 → 401;alice cookie → 200 {operator:{name}}。 */
  @Test
  void authMe_noSession_401_withAlice_returnsOperator() throws Exception {
    mvc.perform(get("/api/v1/auth/me").cookie(session(MALLORY_TOKEN)))
        .andExpect(status().isUnauthorized());
    mvc.perform(get("/api/v1/auth/me").cookie(session(ALICE_TOKEN)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.operator.name").value("alice"))
        .andExpect(jsonPath("$.operator.role").value("ADMIN"));
  }

  /** deny 早退前必须清 CurrentOperator(preHandle 返回 false 时 Spring 不回调 afterCompletion):先触发 403(OPERATOR
   *  bob 写 ADMIN 门禁端点,resolve 已 set bob),再以无 cookie 请求 /me → 必须 401(不得因线程池残留而误报 bob)。 */
  @Test
  void deny_clearsCurrentOperator_soNextMeWithoutCookieIs401() throws Exception {
    mvc.perform(delete("/api/v1/tasks/1").cookie(session(BOB_TOKEN)))
        .andExpect(status().isForbidden()); // bob·OPERATOR 写 DELETE task(ADMIN 门禁)→ 403
    // defaultRequest 恒注入 alice cookie,故以无会话的 mallory cookie 模拟"无有效身份":
    // 若 deny 早退未清 ThreadLocal,此处会复用上一拒绝请求残留的 bob → 200;修正后 → 401。
    mvc.perform(get("/api/v1/auth/me").cookie(session(MALLORY_TOKEN)))
        .andExpect(status().isUnauthorized());
  }

  /** POST /auth/logout → 撤销该 cookie 会话 → 之后以同一 cookie 写 → 401。 */
  @Test
  void logout_revokesSession_subsequentWrite401() throws Exception {
    mvc.perform(post("/api/v1/auth/logout").cookie(session(ALICE_TOKEN)))
        .andExpect(status().isOk());
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_auth_session WHERE token_hash=? AND revoked_at IS NOT NULL",
        Long.class, AuthHashing.sha256(ALICE_TOKEN)));
    String body = "{\"name\":\"post-logout\",\"kind\":\"cron\",\"handlerRef\":\"demo\",\"cron\":\"" + CRON + "\"}";
    mvc.perform(post("/api/v1/tasks").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isUnauthorized());
  }

  /**
   * 登录/认证全量审计:成功登录(auth.login,+meta.expiresAt)、自助改密(auth.change_password)、登出(auth.logout)
   * 均入 app_audit;失败侧维持 access.denied(坏凭据)。各事件用不同操作者承载,避免改密撤会话干扰登出/登录;
   * 全部行走 append-only 取证链 → integrity 仍 verified,且可按动作过滤。
   */
  @Test
  void authFullAudit_loginChangeLogoutDenied_allChainedAndFilterable() throws Exception {
    // app_operator.password_hash 跨用例保留(不被 resetDb 重置),故先钉死 alice/bob 口令为已知值(镜像 loginAndMe 惯例)。
    pinOperatorPassword("alice", "boot-pass", false);
    // 1. 成功登录 → auth.login(operator=提交名 + meta 带 expiresAt)
    mvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"alice\",\"password\":\"boot-pass\"}"))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "auth.login"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$.items[0].operator").value("alice"))
        .andExpect(jsonPath("$.items[0].targetType").value("none"));
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_audit WHERE action='auth.login' AND operator='alice' AND meta ? 'expiresAt'",
        Long.class), "auth.login 的 meta 应带 expiresAt 观察窗口");

    // 2. 自助改密(bob 会话)→ auth.change_password(operator=bob)
    pinOperatorPassword("bob", "old-pass", true);
    mvc.perform(post("/api/v1/auth/change-password").cookie(session(BOB_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"currentPassword\":\"old-pass\",\"newPassword\":\"brand-new-1\"}"))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "auth.change_password"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$.items[0].operator").value("bob"));

    // 3. 登出(carol 会话)→ auth.logout(operator=carol)
    mvc.perform(post("/api/v1/auth/logout").cookie(session(CAROL_TOKEN)))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "auth.logout"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$.items[0].operator").value("carol"));

    // 4. 失败侧维持 access.denied(坏凭据登录)
    mvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"alice\",\"password\":\"wrong-pass\"}"))
        .andExpect(status().isUnauthorized());
    mvc.perform(get("/api/v1/audits").param("action", "access.denied"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$.items[0].operator").value("alice"));

    // 5. 上述行均经 AuditRecorder 落 append-only 取证链 → 完整性 verified
    mvc.perform(get("/api/v1/audits/integrity"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.verified").value(true))
        .andExpect(jsonPath("$.totalRecords").value(4));
  }

  /**
   * 口令策略:自助改密缺数字(复杂度)→ 400、复用当前口令 → 400;合规改密后旧口令入史,
   * ADMIN 设密再取回历史口令 → 400(防重用),合规设密仍通。
   */
  @Test
  void passwordPolicy_complexityReuseReject_compliantSucceeds() throws Exception {
    pinOperatorPassword("bob", "old-pass1", false);

    // 缺数字(复杂度)→ 400
    mvc.perform(post("/api/v1/auth/change-password").cookie(session(BOB_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"currentPassword\":\"old-pass1\",\"newPassword\":\"onlyletters\"}"))
        .andExpect(status().isBadRequest());
    assertEquals(0L, jdbc.queryForObject(
        "SELECT count(*) FROM app_password_history WHERE operator_name='bob'", Long.class),
        "复杂度拒绝不得压历史(无副作用)");
    // 复用当前口令 → 400;同样不得压历史
    mvc.perform(post("/api/v1/auth/change-password").cookie(session(BOB_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"currentPassword\":\"old-pass1\",\"newPassword\":\"old-pass1\"}"))
        .andExpect(status().isBadRequest());
    // 合规自助改密 → 200;旧口令 old-pass1 压入 bob 历史
    mvc.perform(post("/api/v1/auth/change-password").cookie(session(BOB_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"currentPassword\":\"old-pass1\",\"newPassword\":\"brand-new1\"}"))
        .andExpect(status().isOk());
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_password_history WHERE operator_name='bob'", Long.class),
        "改密成功后把旧活跃口令压入历史");
    // 自助改密已撤销 bob 全部会话;改 ADMIN(alice)继续测防重:
    //  ADMIN 把 bob 口令设回历史里曾有过的 old-pass1(含数字,复杂度通过)→ 400(防重用历史)
    mvc.perform(post("/api/v1/operators/bob/password").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"password\":\"old-pass1\"}"))
        .andExpect(status().isBadRequest());
    // 合规 ADMIN 设密 → 200
    mvc.perform(post("/api/v1/operators/bob/password").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"password\":\"final-pass1\"}"))
        .andExpect(status().isOk());
  }

  // ---- 自助改密 + 首登强制改密(必须改密标) ----

  /** app_operator 不在 resetDb truncate 之列(passwd/flag 跨用例保留),故每用例先用 jdbc 直接钉死要断言的口令/标。 */
  private void pinOperatorPassword(String name, String raw, boolean mustChange) {
    jdbc.update("UPDATE app_operator SET password_hash=?, must_change_password=? WHERE name=?",
        new BCryptPasswordEncoder().encode(raw), mustChange, name);
  }

  /** login 与 me 响应均带 mustChangePassword:引导置位(true:共享默认口令,须首登改密)可读到。 */
  @Test
  void loginAndMe_echoMustChangePasswordFlag() throws Exception {
    pinOperatorPassword("alice", "boot-pass", true); // 镜像 bootstrap 后状态
    mvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"alice\",\"password\":\"boot-pass\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.mustChangePassword").value(true));
    mvc.perform(get("/api/v1/auth/me").cookie(session(ALICE_TOKEN)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.mustChangePassword").value(true));
  }

  /** 自助改密全生命周期:正确当前密 → 200 + 无缝新 Set-Cookie + 清 must_change + 撤旧会话(alice 原 cookie → 401)
   *  + 新会话可解析(mustChangePassword=false);新密可登录、旧密失效(该失败登录置于末尾以免触发退避影响成功断言)。 */
  @Test
  void selfChangePassword_fullLifecycle_newSessionOldRevokedFlagCleared() throws Exception {
    pinOperatorPassword("alice", "old-pass", true);
    MvcResult r = mvc.perform(post("/api/v1/auth/change-password").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"currentPassword\":\"old-pass\",\"newPassword\":\"brand-new-1\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.operator.name").value("alice"))
        .andExpect(jsonPath("$.expiresAt").isNotEmpty())
        .andReturn();
    String newToken = rotationToken(r); // 无缝续期:响应回写新会话 Set-Cookie,前端无需重登

    // 旧会话被撤销 → alice 原 cookie 不再可解析
    assertNull(authService.resolve(ALICE_TOKEN), "自助改密应撤销该操作者全部旧会话");
    mvc.perform(get("/api/v1/auth/me").cookie(session(ALICE_TOKEN)))
        .andExpect(status().isUnauthorized());
    // 新会话可解析且须改密标已清
    assertNotNull(authService.resolve(newToken));
    mvc.perform(get("/api/v1/auth/me").cookie(session(newToken)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.mustChangePassword").value(false));
    // 口令已更替:新密可登录(成功后清零退避),旧密失效(置于末尾避免退避影响)
    mvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"alice\",\"password\":\"brand-new-1\"}"))
        .andExpect(status().isOk());
    mvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"alice\",\"password\":\"old-pass\"}"))
        .andExpect(status().isUnauthorized());
  }

  /** 当前密不符 → 401 + 记 access.denied,口令不变(原密仍可登录)。 */
  @Test
  void selfChangePassword_wrongCurrent_401_andAuditsDenied() throws Exception {
    pinOperatorPassword("alice", "right-pass", false);
    mvc.perform(post("/api/v1/auth/change-password").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"currentPassword\":\"wrong-pass\",\"newPassword\":\"brand-new-1\"}"))
        .andExpect(status().isUnauthorized());
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_audit WHERE action='access.denied' AND operator='alice'", Long.class));
    // 口令未被改动(change-password 验密失败不累计 login 退避,故此处可直接登录验证)
    mvc.perform(post("/api/v1/auth/login").contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"alice\",\"password\":\"right-pass\"}"))
        .andExpect(status().isOk());
  }

  /** 新密过短 → 400(at least 8);无会话 → 401。 */
  @Test
  void selfChangePassword_shortOrAnonymous_400_401() throws Exception {
    pinOperatorPassword("alice", "right-pass", false);
    mvc.perform(post("/api/v1/auth/change-password").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"currentPassword\":\"right-pass\",\"newPassword\":\"short\"}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value(
            org.hamcrest.Matchers.containsString("at least 8")));
    // 匿名(mallory 无会话)→ 401,即使携带口令
    mvc.perform(post("/api/v1/auth/change-password").cookie(session(MALLORY_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"currentPassword\":\"right-pass\",\"newPassword\":\"brand-new-1\"}"))
        .andExpect(status().isUnauthorized());
  }

  /** ADMIN 设密(人类选定口令)→ 清 must_change 标(强制首登改密解除)。 */
  @Test
  void adminSetPassword_clearsMustChangeFlag() throws Exception {
    jdbc.update("UPDATE app_operator SET must_change_password=true WHERE name='bob'");
    mvc.perform(post("/api/v1/operators/bob/password").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"password\":\"new-secret-1\"}"))
        .andExpect(status().isOk());
    assertEquals(Boolean.FALSE, jdbc.queryForObject(
        "SELECT must_change_password FROM app_operator WHERE name='bob'", Boolean.class),
        "ADMIN 设密后应清除必须改密标");
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
