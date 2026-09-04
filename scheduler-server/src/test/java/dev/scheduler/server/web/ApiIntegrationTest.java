package dev.scheduler.server.web;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.server.execute.ExecutorWorker;
import dev.scheduler.server.trigger.TriggerEngine;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
  @Autowired TriggerEngine triggerEngine;
  @Autowired ExecutorWorker executorWorker;

  /** 上下文启动(绑定时刻)前就存在的任务 id,用于断言 per-task 指标 series。 */
  private static long METRICS_TASK_ID;

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
  }

  @BeforeEach
  void resetDb() {
    jdbc.execute("TRUNCATE execution, execution_outcome, app_task RESTART IDENTITY CASCADE");
    CLOCK.now = BASE;
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

    // 持有领导权(装配的 advisory-lock Bean)时,同步扫描一次 → 命中 tick,登记一条 DUE
    triggerEngine.scanOnce();
    mvc.perform(get("/api/v1/executions"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[*].status", hasItem("DUE")));

    // 执行器工作一步:认领并运行已注册的 DemoHandler → 回写 SUCCESS
    boolean processed = executorWorker.workOne();
    assertTrue(processed, "expected the seeded handling loop to claim and run one work item");

    mvc.perform(get("/api/v1/executions"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[*].status", hasItem("SUCCESS")));
    mvc.perform(get("/api/v1/executions").param("taskId", String.valueOf(id)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].status").value("SUCCESS"));
  }

  @Test
  void prometheus_exposesSchedulerMetrics() throws Exception {
    long id = postTask("metrics-task");
    triggerEngine.scanOnce(); // 产生一条 DUE → DUE 队列最深年龄 > 0(真实数据,非空集合)

    long dueCount = jdbc.queryForObject(
        "SELECT count(*) FROM execution WHERE status='DUE' AND task_id=?", Long.class, id);
    assertTrue(dueCount > 0, "scanOnce under leadership should have left a DUE row");

    String prom = mvc.perform(get("/actuator/prometheus"))
        .andExpect(status().isOk())
        .andReturn().getResponse().getContentAsString();
    // 断言绑定时刻存活任务的 tagged series(task_id 标签存在),而非仅裸指标名。
    assertTrue(prom.contains("scheduler_active_runs{task_id=\"" + METRICS_TASK_ID + "\","),
        "missing per-task scheduler_active_runs{task_id=...} series in prometheus:\n" + prom);
    assertTrue(prom.contains("scheduler_due_queue_max_age_seconds{task_id=\"" + METRICS_TASK_ID + "\","),
        "missing per-task scheduler_due_queue_max_age_seconds{task_id=...} series in prometheus:\n" + prom);
  }

  @Test
  void cancelDue_setsCanceled_runningIs409() throws Exception {
    long id = postTask("cancel-task");
    MvcResult r = mvc.perform(post("/api/v1/tasks/" + id + "/trigger"))
        .andExpect(status().isCreated()).andReturn();
    long execId = objectMapper.readTree(r.getResponse().getContentAsString()).get("id").asLong();

    mvc.perform(post("/api/v1/executions/" + execId + "/cancel"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("CANCELED"));
    assertEquals("CANCELED", jdbc.queryForObject(
        "SELECT status FROM execution WHERE id=?", String.class, execId));
  }

  private long postTask(String name) throws Exception {
    String body = "{\"name\":\"" + name + "\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
        + "\"cron\":\"" + CRON + "\",\"maxActiveConcurrent\":1}";
    MvcResult r = mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isCreated())
        .andReturn();
    return objectMapper.readTree(r.getResponse().getContentAsString()).get("id").asLong();
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