package dev.scheduler.server.reconcile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.JdbcExecutionRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.retry.FailureResolver;
import dev.scheduler.server.retry.RetryPolicy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** 真 PG:确定性校验 Reconciler 回收过期租约孤儿 RUNNING 的四种情形(不可重试/可重试/批量/新鲜不动)。 */
@Testcontainers
class ReconcilerTest {
  @Container static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>("postgres:16-alpine");
  private static JdbcTemplate jdbc;

  /** 固定时钟:断言 next_retry_at 由注入时钟 + 退避算出,而非实时/DB 时钟(租约豁免只用 DB now())。 */
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T10:00:00Z"), ZoneOffset.UTC);

  private TaskRepository tasks;
  private ExecutionRepository executions;

  @BeforeAll static void setUp() {
    Flyway.configure()
        .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
        .load().migrate();
    jdbc = new JdbcTemplate(new DriverManagerDataSource(
        PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()));
  }

  @BeforeEach void clearTables() {
    jdbc.execute("TRUNCATE execution, execution_outcome, app_task RESTART IDENTITY CASCADE");
    tasks = new JdbcTaskRepository(jdbc);
    executions = new JdbcExecutionRepository(jdbc);
  }

  private long createTask(int maxRetries, long backoffMs, String pattern) {
    return tasks.create(new Task(null, "t", "cron", "demo", "0 */5 * * * *",
        1, 300, maxRetries, backoffMs, pattern, 4, true, false)).id();
  }

  /** 种子一条孤儿 RUNNING(attempt 已 valid 过),lease_until 过期由调用方按场景给。 */
  private long seedRunning(long taskId, String key, int attempt, String leaseExpr) {
    return jdbc.queryForObject(
        "INSERT INTO execution (task_id, status, idempotency_key, shard_count, worker_id,"
            + " attempt, lease_until, next_retry_at)"
            + " VALUES (?, 'RUNNING', ?, 1, 'orphan-worker', ?, " + leaseExpr + ", NULL) RETURNING id",
        Long.class, taskId, key, attempt);
  }

  private Reconciler reconciler() {
    return new Reconciler(tasks, executions,
        new FailureResolver(executions, new RetryPolicy(), CLOCK), "reconciler");
  }

  private Execution byId(long id) {
    return executions.findById(id).orElseThrow();
  }

  private long outcomeCount(long execId, String status) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_outcome WHERE execution_id=? AND status=?",
        Long.class, execId, status);
  }

  @Test void expiredRunningOnNonRetryableTask_isReclaimedAsFailedWithDlq() {
    long taskId = createTask(0, 1000, null); // maxRetries=0 → 不可重试
    long execId = seedRunning(taskId, "k-orphan-1", 1, "now() - interval '1 hour'");

    int n = reconciler().scanOnce();

    assertEquals(1, n);
    Execution e = byId(execId);
    assertEquals(ExecutionStatus.FAILED, e.status(), "孤儿回收 → 转 FAILED");
    assertEquals("reconciler", e.workerId(), "FAILED 由 reconciler 标记");
    assertEquals(1L, outcomeCount(execId, "FAILED"), "落一条 FAILED outcome");
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution WHERE id=?", Boolean.class, execId),
        "不可重试 → FailureResolver DLQ 分支置 dead_letter=true");
    assertEquals(0L, outcomeCount(execId, "DUE"), "耗尽:无 DUE outcome");
  }

  @Test void expiredRunningOnRetryableTask_isRequeuedWithClockBasedNextRetryAt() {
    long taskId = createTask(2, 1000, null); // maxRetries=2, 无 pattern → 可重试
    long execId = seedRunning(taskId, "k-orphan-2", 1, "now() - interval '1 hour'");

    int n = reconciler().scanOnce();

    assertEquals(1, n);
    Execution e = byId(execId);
    assertEquals(ExecutionStatus.DUE, e.status(), "可重试 → 回 DUE 待再次认领");
    assertNotNull(e.nextRetryAt(), "重试必须写入 next_retry_at");
    assertEquals(CLOCK.instant().plusMillis(1000), e.nextRetryAt(),
        "next_retry_at = 注入时钟 + attempt1 退避(backoff=1000ms),而非实时时钟");
    assertEquals(1L, outcomeCount(execId, "DUE"), "重试落 DUE outcome");
  }

  @Test void multipleExpiredRuns_allReclaimedAtOnce() {
    long taskId = createTask(0, 1000, null);
    long a = seedRunning(taskId, "k-orphan-a", 1, "now() - interval '1 hour'");
    long b = seedRunning(taskId, "k-orphan-b", 2, "now() - interval '30 minutes'");

    int n = reconciler().scanOnce();

    assertEquals(2, n, "两条过期 RUNNING 都回收");
    assertEquals(ExecutionStatus.FAILED, byId(a).status());
    assertEquals(ExecutionStatus.FAILED, byId(b).status());
  }

  @Test void freshLeaseRunning_isLeftUntouched() {
    long taskId = createTask(0, 1000, null);
    long execId = seedRunning(taskId, "k-fresh", 1, "now() + interval '60 seconds'");

    int n = reconciler().scanOnce();

    assertEquals(0, n, "租约仍有效 → 不回收");
    Execution e = byId(execId);
    assertEquals(ExecutionStatus.RUNNING, e.status(), "新鲜 RUNNING 原样不动");
    assertEquals(0L, outcomeCount(execId, "FAILED"), "未回收 → 无失败 outcome");
  }
}