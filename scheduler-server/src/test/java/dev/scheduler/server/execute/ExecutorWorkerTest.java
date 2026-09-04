package dev.scheduler.server.execute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.JdbcExecutionRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.execute.ExecutorWorker;
import dev.scheduler.server.handler.ExecutionHandler;
import dev.scheduler.server.handler.HandlerContext;
import dev.scheduler.server.handler.HandlerRegistry;
import dev.scheduler.server.handler.MapHandlerRegistry;
import dev.scheduler.server.retry.FailureResolver;
import dev.scheduler.server.retry.RetryPolicy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 用真 PG,确定性校验 ExecutorWorker 的认领-执行-回写环路与所有权规则。 */
class ExecutorWorkerTest extends AbstractExecutorWorkerTest {

  private TaskRepository tasks;
  private ExecutionRepository executions;

  /** 记录调用的测试 handler;fail=true 时 handle 抛异常,用于验证失败回写。 */
  static class RecordingHandler implements ExecutionHandler {
    final boolean fail;
    final List<HandlerContext> calls = new ArrayList<>();
    RecordingHandler(boolean fail) { this.fail = fail; }
    @Override public String ref() { return "rec"; }
    @Override public void handle(HandlerContext ctx) { calls.add(ctx); if (fail) throw new RuntimeException("boom"); }
  }

  /** 固定时钟:重试测试据此断言 next_retry_at,避免依赖实时时钟。 */
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T10:00:00Z"), ZoneOffset.UTC);

  @BeforeEach void clearTables() {
    jdbc.execute("TRUNCATE execution, execution_outcome, app_task RESTART IDENTITY CASCADE");
    tasks = new JdbcTaskRepository(jdbc);
    executions = new JdbcExecutionRepository(jdbc);
  }

  /** 旧 2 参签名委托给新默认:maxRetries=0、backoff=1000ms、pattern=null(即不再重试)。 */
  private long createTask(String handlerRef, int maxActiveConcurrent) {
    return createTask(handlerRef, 0, 1000, null, maxActiveConcurrent);
  }

  private long createTask(String handlerRef, int maxRetries, long backoffMs, String pattern,
                          int maxActiveConcurrent) {
    return tasks.create(new Task(null, "t", "cron", handlerRef, "0 */5 * * * *",
        1, 300, maxRetries, backoffMs, pattern, maxActiveConcurrent, true, false)).id();
  }

  private long outcomeCount(long executionId, String status) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_outcome WHERE execution_id=? AND status=?",
        Long.class, executionId, status);
  }

  private ExecutorWorker worker(HandlerRegistry registry) {
    return new ExecutorWorker(tasks, executions, registry, "worker-a",
        new FailureResolver(executions, new RetryPolicy(), CLOCK), CLOCK);
  }

  @Test void happyPath_claimsRunsAndWritesSuccess() {
    var rec = new RecordingHandler(false);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 1);
    long execId = executions.createDue(Execution.ofDue(taskId, "k-happy", 1));

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    assertEquals("rec", registry.get("rec").ref()); // demo/rec ref 可派发
    assertEquals(1, rec.calls.size()); // handler 真实被调用
    assertEquals(execId, rec.calls.get(0).executionId());

    Execution e = executions.findById(execId).orElseThrow();
    assertEquals(ExecutionStatus.SUCCESS, e.status());
    assertEquals("worker-a", e.workerId());
    assertEquals(1L, outcomeCount(execId, "SUCCESS")); // outcome 写入 SUCCESS
  }

  @Test void missingHandler_failsExecution_andWritesOutcome() {
    var rec = new RecordingHandler(false);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("nope", 1); // handlerRef 未注册
    long execId = executions.createDue(Execution.ofDue(taskId, "k-nope", 1));

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Execution e = executions.findById(execId).orElseThrow();
    assertEquals(ExecutionStatus.FAILED, e.status()); // RUNNING→FAILED 合法回写
    assertEquals(1L, outcomeCount(execId, "FAILED"));
    assertEquals(0, rec.calls.size()); // 未命中任何 handler
  }

  @Test void handlerThrows_failsExecution_withoutEscapingThrowable() {
    var rec = new RecordingHandler(true);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 1);
    long execId = executions.createDue(Execution.ofDue(taskId, "k-throw", 1));

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Execution e = executions.findById(execId).orElseThrow();
    assertEquals(ExecutionStatus.FAILED, e.status());
    assertEquals(1, rec.calls.size());
    assertEquals(1L, outcomeCount(execId, "FAILED"));
  }

  @Test void handlerThrowsRetryable_movesToDue_withClockBasedNextRetryAt() {
    var rec = new RecordingHandler(true);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 2, 1000, null, 1); // maxRetries=2, 无 pattern, 可重试
    long execId = executions.createDue(Execution.ofDue(taskId, "k-retry", 1));

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Execution e = executions.findById(execId).orElseThrow();
    assertEquals(ExecutionStatus.DUE, e.status(), "可重试 → 回到 DUE 待再次认领");
    assertNotNull(e.nextRetryAt(), "重试必须写入 next_retry_at");
    assertEquals(CLOCK.instant().plusMillis(1000), e.nextRetryAt(),
        "next_retry_at 由注入时钟 + 退避(attempt1 backoff=1000ms)算出,而非实时时钟");
    assertEquals(1L, outcomeCount(execId, "DUE"), "重试落 DUE outcome");
    assertEquals(1, e.attempt(), "claim 已累加到 1,重试不再改增 attempt");
    // markStatus(FAILED) 亦落 FAILED outcome;此处仅需验证重试落 DUE 且 next_retry_at 按注入时钟。
  }

  @Test void handlerThrowsExhausted_failsAndDeadLetters_noDueOutcome() {
    var rec = new RecordingHandler(true);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 0, 1000, null, 1); // maxRetries=0 → 耗尽
    long execId = executions.createDue(Execution.ofDue(taskId, "k-exhaust", 1));

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Execution e = executions.findById(execId).orElseThrow();
    assertEquals(ExecutionStatus.FAILED, e.status());
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution WHERE id=?", Boolean.class, execId),
        "重试耗尽 → 置 dead_letter=true");
    assertEquals(0L, outcomeCount(execId, "DUE"), "耗尽:无 DUE outcome");
  }

  @Test void handlerThrowsPatternMismatch_failsAndDeadLetters() {
    var rec = new RecordingHandler(true);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 2, 1000, "timeout", 1); // pattern 不匹配抛出的 "boom"
    long execId = executions.createDue(Execution.ofDue(taskId, "k-pattern", 1));

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Execution e = executions.findById(execId).orElseThrow();
    assertEquals(ExecutionStatus.FAILED, e.status());
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution WHERE id=?", Boolean.class, execId),
        "pattern 不匹配 → 视为不可重试,置 dead_letter=true");
    assertEquals(0L, outcomeCount(execId, "DUE"));
  }

  @Test void unclaimedExecution_neverMutatedByThisWorker() {
    var rec = new RecordingHandler(false);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", /* maxActiveConcurrent */ 1);
    // 另一 worker 已认领并 RUNNING(占满配额),有效租约
    jdbc.update("""
      INSERT INTO execution (task_id, status, idempotency_key, shard_count, worker_id, lease_until)
      VALUES (?, 'RUNNING', ?, 1, 'other-worker', now() + interval '60 seconds')""",
        taskId, "running-other");
    long dueId = executions.createDue(Execution.ofDue(taskId, "k-due", 1));

    boolean processed = worker(registry).workOne();

    assertFalse(processed); // 配额占满 → 未认领到任何 execution,本次不处理
    Execution running = jdbc.query(
        "SELECT * FROM execution WHERE idempotency_key='running-other'",
        (rs, i) -> new Execution(rs.getLong("id"), rs.getLong("task_id"),
            ExecutionStatus.valueOf(rs.getString("status")), rs.getString("idempotency_key"),
            rs.getString("args"), rs.getInt("shard_index"), rs.getInt("shard_count"),
            rs.getInt("attempt"), rs.getString("worker_id"),
            rs.getTimestamp("lease_until") != null ? rs.getTimestamp("lease_until").toInstant() : null,
            rs.getTimestamp("next_retry_at") != null ? rs.getTimestamp("next_retry_at").toInstant() : null,
            rs.getTimestamp("started_at") != null ? rs.getTimestamp("started_at").toInstant() : null,
            rs.getTimestamp("finished_at") != null ? rs.getTimestamp("finished_at").toInstant() : null,
            rs.getString("result_payload")))
        .get(0);
    assertEquals(ExecutionStatus.RUNNING, running.status()); // 他人拥有的 execution 未被本 worker 改动
    Execution due = executions.findById(dueId).orElseThrow();
    assertEquals(ExecutionStatus.DUE, due.status()); // 未认领 → 仍 DUE,绝不回写
  }

  @Test void noWork_emptyTables_returnsFalse() {
    assertFalse(worker(new MapHandlerRegistry(List.of(() -> new RecordingHandler(false)))).workOne());
  }
}
