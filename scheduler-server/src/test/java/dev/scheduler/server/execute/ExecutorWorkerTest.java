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
import java.util.concurrent.CancellationException;
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

  /** 始终抛取消信号的 handler,验证协作取消走 CANCELED 而非常规失败判定。 */
  static class CancellingHandler implements ExecutionHandler {
    final List<HandlerContext> calls = new ArrayList<>();
    @Override public String ref() { return "cancel"; }
    @Override public void handle(HandlerContext ctx) {
      calls.add(ctx);
      throw new CancellationException("cancel me");
    }
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

  @Test void staleOwnerCannotClobberReownedRow_guardSuppressesWritebackAndRetry() {
    // handler 运行期间模拟真实竞态:reconciler 回收过期租约(FAILED + 重试→DUE),随后 worker-b 重新
    // 认领该行(→ RUNNING owned by worker-b)。worker-a 的迟回收写 markStatusOwned(FAILED) 必须被
    // ownership 守卫拦截:不覆盖 worker-b 的 RUNNING,也不调度任何重试(零 DUE outcome)。
    long taskId = createTask("reclaim", 2, 1000, null, 1); // maxRetries=2:若无守卫,迟回收写会落 DUE
    long execId = executions.createDue(Execution.ofDue(taskId, "k-stale-owner", 1));
    Instant lease = Instant.now().plusSeconds(60);
    var reclaiming = new ExecutionHandler() {
      @Override public String ref() { return "reclaim"; }
      @Override public void handle(HandlerContext ctx) {
        // 模拟 reconciler 回收:落 FAILED 后经重试决策回 DUE(此处直接 raw 复位,不落重试 outcome,从而让
        // "零 DUE outcome" 断言只反映 worker-a 的迟回收写是否被抑制),再被 worker-b 重新认领。
        executions.markStatus(execId, ExecutionStatus.FAILED, "reconciler", "lease expired");
        jdbc.update("UPDATE execution SET status='DUE', next_retry_at=NULL WHERE id=?", execId);
        executions.claim(execId, taskId, "worker-b", lease, 1);
        throw new RuntimeException("boom");
      }
    };
    var registry = new MapHandlerRegistry(List.of(() -> reclaiming));

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Execution e = executions.findById(execId).orElseThrow();
    assertEquals(ExecutionStatus.RUNNING, e.status(), "stale worker 的回写不得覆盖 worker-b 的 RUNNING");
    assertEquals("worker-b", e.workerId(), "行仍归重新认领的 worker-b 所有,未被 worker-a 改动");
    assertEquals(0L, outcomeCount(execId, "DUE"), "被守卫拦截的回写 → 不调度重试,零 DUE outcome");
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

  @Test void cancelRequestedBeforeRun_marksCanceled_handlerNeverCalled() {
    var rec = new RecordingHandler(false);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 1);
    // 预置 DUE 且 cancel_requested=true:worker 认领后运行前检查即取消,不调度 handler。
    jdbc.update("""
      INSERT INTO execution (task_id, status, idempotency_key, shard_count, cancel_requested)
      VALUES (?, 'DUE', ?, 1, true)""",
        taskId, "k-cancel-before");
    long execId = jdbc.queryForObject(
        "SELECT id FROM execution WHERE idempotency_key='k-cancel-before'", Long.class);

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Execution e = executions.findById(execId).orElseThrow();
    assertEquals(ExecutionStatus.CANCELED, e.status(), "运行前已请求取消 → CANCELED");
    assertEquals(0, rec.calls.size(), "已取消的候选不应被调度 handler");
    assertEquals(1L, outcomeCount(execId, "CANCELED"));
  }

  @Test void handlerCancellation_marksCanceled_noRetry() {
    var rec = new CancellingHandler();
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("cancel", 2, 1000, null, 1); // 即使任务可重试,取消也不走重试
    long execId = executions.createDue(Execution.ofDue(taskId, "k-cancel-run", 1));

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Execution e = executions.findById(execId).orElseThrow();
    assertEquals(ExecutionStatus.CANCELED, e.status(), "取消信号映射 CANCELED 而非 FAILED");
    assertEquals(1, rec.calls.size());
    assertEquals(0L, outcomeCount(execId, "DUE"), "取消不调度重试,无 DUE outcome");
    assertEquals(0L, outcomeCount(execId, "FAILED"), "取消不落 FAILED");
  }

  @Test void handlerRunsNormally_butCancelRequestedMidRun_marksCanceled() {
    long taskId = createTask("scripted", 1);
    long execId = executions.createDue(Execution.ofDue(taskId, "k-midrun", 1));
    var mid = new RecordingHandler(false);
    var scripted = new ExecutionHandler() {
      @Override public String ref() { return "scripted"; }
      @Override public void handle(HandlerContext ctx) {
        executions.requestCancel(execId); // 运行期间经仓库置位
        mid.handle(ctx); // 然后正常返回
      }
    };
    var registry = new MapHandlerRegistry(List.of(() -> scripted));

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Execution e = executions.findById(execId).orElseThrow();
    assertEquals(ExecutionStatus.CANCELED, e.status(), "运行期间被请求取消 → 正常返回后复查仍落 CANCELED");
    assertEquals(1, mid.calls.size(), "handler 确已正常运行(非运行前取消)");
    assertEquals(1L, outcomeCount(execId, "CANCELED"));
    assertEquals(0L, outcomeCount(execId, "SUCCESS"), "不会同时落 SUCCESS");
  }

  @Test void noWork_emptyTables_returnsFalse() {
    assertFalse(worker(new MapHandlerRegistry(List.of(() -> new RecordingHandler(false)))).workOne());
  }
}
