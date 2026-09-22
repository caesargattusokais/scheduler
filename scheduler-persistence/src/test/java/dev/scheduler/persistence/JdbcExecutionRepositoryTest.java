package dev.scheduler.persistence;
import static org.junit.jupiter.api.Assertions.*;
import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Task;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcExecutionRepositoryTest extends AbstractPostgresTest {
  private final JdbcTaskRepository taskRepo = new JdbcTaskRepository(jdbc);
  private final JdbcExecutionRepository execRepo = new JdbcExecutionRepository(jdbc);

  @BeforeEach void clean() {
    jdbc.update("TRUNCATE app_task, execution, execution_outcome RESTART IDENTITY CASCADE");
  }

  private long newTask(int maxActiveConcurrent) {
    Task t = taskRepo.create(new Task(null, "t"+System.nanoTime(), "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, maxActiveConcurrent, true, false));
    return t.id();
  }

  private Execution ofDue(long taskId, String key) {
    return Execution.ofDue(taskId, key, 1);
  }

  private int outcomes(long executionId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_outcome WHERE execution_id=?", Integer.class, executionId);
  }

  private boolean deadLetter(long executionId) {
    return Boolean.TRUE.equals(jdbc.queryForObject(
        "SELECT dead_letter FROM execution WHERE id=?", Boolean.class, executionId));
  }

  private boolean cancelRequested(long executionId) {
    return Boolean.TRUE.equals(jdbc.queryForObject(
        "SELECT cancel_requested FROM execution WHERE id=?", Boolean.class, executionId));
  }

  private long failedExecution(int maxActiveConcurrent, String key) {
    long taskId = newTask(maxActiveConcurrent);
    long id = execRepo.createDue(ofDue(taskId, key));
    execRepo.claim(id, taskId, "w1", Instant.now().plusSeconds(60), maxActiveConcurrent);
    execRepo.markStatus(id, ExecutionStatus.FAILED, "w1", "boom");
    return id;
  }

  @Test void createDueIsIdempotent() {
    long taskId = newTask(8);
    String key = "t1:k1";
    long first = execRepo.createDue(ofDue(taskId, key));
    long second = execRepo.createDue(ofDue(taskId, key));

    assertTrue(first > 0);
    assertEquals(first, second, "same idempotency key must yield the same execution id");
  }

  @Test void findCandidateReturnsDue() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t2:k1"));

    var cand = execRepo.findCandidate(taskId);
    assertTrue(cand.isPresent());
    assertEquals(id, cand.get().id());
    assertEquals(ExecutionStatus.DUE, cand.get().status());
  }

  @Test void findCandidateSkipsDueRowWhoseRetryAtIsInFuture() {
    long taskId = newTask(8);
    // 一条 next_retry_at 在未来(now()+1h)的 DUE:未到重试时间,不应被 findCandidate 返回
    jdbc.update(
        "INSERT INTO execution (task_id, status, idempotency_key, shard_count, next_retry_at) "
            + "VALUES (?, 'DUE', ?, 1, now() + interval '1 hour')",
        taskId, "t8:futureretry");

    assertTrue(execRepo.findCandidate(taskId).isEmpty(),
        "a DUE row with next_retry_at in the future must not be returned by findCandidate");
  }

  @Test void findCandidateReturnsDueRowWithNullNextRetryAt() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t8:nullretry")); // next_retry_at 默认 NULL

    var cand = execRepo.findCandidate(taskId);
    assertTrue(cand.isPresent());
    assertEquals(id, cand.get().id());
  }

  @Test void claimSucceedsThenSecondFailsAndWritesOutcome() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t3:k1"));
    Instant lease = Instant.now().plusSeconds(60);

    assertTrue(execRepo.claim(id, taskId, "w1", lease, 8));
    assertEquals(1, outcomes(id), "Ruling 1: claim must append a RUNNING outcome row");

    assertFalse(execRepo.claim(id, taskId, "w2", lease, 8), "already RUNNING, second claim must fail");
    assertEquals(1, outcomes(id), "no extra outcome after failed claim");
  }

  @Test void countActiveIsOneAfterClaim() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t4:k1"));
    execRepo.claim(id, taskId, "w1", Instant.now().plusSeconds(60), 8);

    assertEquals(1, execRepo.countActive(taskId));
  }

  @Test void excessiveConcurrencyRejectsSecondClaim() {
    long taskId = newTask(1);
    long running = execRepo.createDue(ofDue(taskId, "t5:k1"));
    long due = execRepo.createDue(ofDue(taskId, "t5:k2"));
    Instant lease = Instant.now().plusSeconds(60);

    assertTrue(execRepo.claim(running, taskId, "w1", lease, 1));
    // quota saturated (countActive==1): second DUE execution cannot be claimed under maxConcurrent=1
    assertFalse(execRepo.claim(due, taskId, "w2", lease, 1));
    assertEquals(ExecutionStatus.DUE, execRepo.findById(due).get().status());
  }

  @Test void markStatusWritesSuccessAndOutcome() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t6:k1"));
    execRepo.claim(id, taskId, "w1", Instant.now().plusSeconds(60), 8);

    execRepo.markStatus(id, ExecutionStatus.SUCCESS, "w1", "done");

    assertEquals(ExecutionStatus.SUCCESS, execRepo.findById(id).get().status());
    assertEquals(2, outcomes(id), "claim outcome + SUCCESS outcome");
    assertTrue(execRepo.findById(id).get().finishedAt() != null);
  }

  @Test void markStatusOwnedWrongOwner_isSuppressedNoOutcome_noClobber() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t6a:k1"));
    execRepo.claim(id, taskId, "w1", Instant.now().plusSeconds(60), 8); // -> RUNNING, worker_id=w1

    boolean ok = execRepo.markStatusOwned(id, ExecutionStatus.SUCCESS, "w2", "stale done");

    assertFalse(ok, "owner mismatch → 守卫必须拦截回写");
    Execution e = execRepo.findById(id).get();
    assertEquals(ExecutionStatus.RUNNING, e.status(), "非持有者的回写不得改动行状态");
    assertEquals("w1", e.workerId(), "非持有者的回写不得改动 owner");
    assertEquals(0, successOutcomes(id), "被守卫拦截 → 不落 SUCCESS outcome");
  }

  @Test void markStatusOwnedCorrectOwner_transitionsWithSingleOutcome() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t6b:k1"));
    execRepo.claim(id, taskId, "w1", Instant.now().plusSeconds(60), 8); // -> RUNNING, worker_id=w1

    boolean ok = execRepo.markStatusOwned(id, ExecutionStatus.SUCCESS, "w1", "done");

    assertTrue(ok, "持有者自身回写应成功");
    Execution e = execRepo.findById(id).get();
    assertEquals(ExecutionStatus.SUCCESS, e.status());
    assertEquals(1, successOutcomes(id), "恰一条 SUCCESS outcome");
    assertNotNull(e.finishedAt(), "终态必须写 finished_at");
  }

  @Test void markStatusOwnedOnAlreadyTerminal_isSuppressedNoSecondOutcome() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t6c:k1"));
    execRepo.claim(id, taskId, "w1", Instant.now().plusSeconds(60), 8);
    execRepo.markStatus(id, ExecutionStatus.SUCCESS, "w1", "done"); // 已 SUCCESS 终态

    boolean ok = execRepo.markStatusOwned(id, ExecutionStatus.SUCCESS, "w1", "again");

    assertFalse(ok, "非可迁移(SUCCESS→SUCCESS)→ 守卫返回 false");
    assertEquals(1, successOutcomes(id), "不得再落第二条 outcome");
  }

  private int successOutcomes(long executionId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_outcome WHERE execution_id=? AND status='SUCCESS'",
        Integer.class, executionId);
  }

  @Test void illegalTransitionThrows() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t7:k1"));

    // DUE -> SUCCESS skips RUNNING: ExecutionTransitions rejects it
    assertThrows(IllegalStateException.class,
        () -> execRepo.markStatus(id, ExecutionStatus.SUCCESS, "w1", "skip"));
    // DUE -> NOT_CREATED also illegal
    assertThrows(IllegalStateException.class,
        () -> execRepo.markStatus(id, ExecutionStatus.NOT_CREATED, "w1", "nope"));
  }

  @Test void scheduleRetryMovesFailedToDueWithDelayAndOutcome() {
    long id = failedExecution(8, "t8:k1");
    Instant retryAt = Instant.now().plusSeconds(10);

    execRepo.scheduleRetry(id, retryAt, "boom");

    Execution e = execRepo.findById(id).get();
    assertEquals(ExecutionStatus.DUE, e.status());
    assertNotNull(e.nextRetryAt(), "next_retry_at must be written");
    assertTrue(e.nextRetryAt().isAfter(Instant.now().minusSeconds(1)));
    assertTrue(e.nextRetryAt().isBefore(Instant.now().plusSeconds(20)), "delay ~10s");
    assertEquals(1, e.attempt(), "attempt must not change on retry");
    assertEquals("w1", e.workerId(), "worker_id must stay with the failing worker");
    // claim RUNNING outcome + FAILED outcome + DUE retry outcome
    assertEquals(3, outcomes(id));
    Integer dueOutcomes = jdbc.queryForObject(
        "SELECT count(*) FROM execution_outcome WHERE execution_id=? AND status='DUE' AND detail='boom'",
        Integer.class, id);
    assertEquals(1, dueOutcomes, "retry must append a DUE outcome carrying the original failure detail");
  }

  @Test void scheduleRetryOnNonFailedIsSilentNoOutcome() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t9:k1"));
    execRepo.claim(id, taskId, "w1", Instant.now().plusSeconds(60), 8); // -> RUNNING, not FAILED

    execRepo.scheduleRetry(id, Instant.now().plusSeconds(10), "boom");

    assertEquals(ExecutionStatus.RUNNING, execRepo.findById(id).get().status());
    assertEquals(1, outcomes(id), "CAS 0 rows: no extra outcome");
  }

  @Test void markDeadLetterSetsFlagOnFailed() {
    long id = failedExecution(8, "t10:k1");
    int before = outcomes(id);

    execRepo.markDeadLetter(id, "unrecoverable");

    assertTrue(deadLetter(id));
    assertEquals(before, outcomes(id), "dead_letter is not a status transition: no outcome row");
  }

  @Test void markDeadLetterOnNonFailedDoesNothing() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t11:k1"));
    execRepo.claim(id, taskId, "w1", Instant.now().plusSeconds(60), 8); // RUNNING, not FAILED

    execRepo.markDeadLetter(id, "nope");

    assertFalse(deadLetter(id));
  }

  @Test void requestCancelSetsFlagOnRunningOnly_noOutcome() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t12:k1"));
    execRepo.claim(id, taskId, "w1", Instant.now().plusSeconds(60), 8); // -> RUNNING

    assertTrue(execRepo.requestCancel(id));
    assertTrue(cancelRequested(id), "cancel_requested 标志必须置位");
    assertEquals(ExecutionStatus.RUNNING, execRepo.findById(id).get().status(),
        "取消请求非状态迁移,仍 RUNNING");
    assertEquals(1, outcomes(id), "取消请求不落 outcome(真实转 CANCELED 时才落)");
  }

  @Test void requestCancelOnDueReturnsFalse() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t13:k1")); // DUE,非 RUNNING

    assertFalse(execRepo.requestCancel(id), "DUE 不可请求协作取消");
    assertFalse(cancelRequested(id));
  }

  @Test void requestCancelOnTerminalReturnsFalse() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t14:k1"));
    execRepo.claim(id, taskId, "w1", Instant.now().plusSeconds(60), 8);
    execRepo.markStatus(id, ExecutionStatus.SUCCESS, "w1", "done"); // SUCCESS 终态

    assertFalse(execRepo.requestCancel(id), "终态不可请求协作取消");
  }

  @Test void isCancelRequestedDefaultsFalseAndReadsTrue() {
    long taskId = newTask(8);
    long id = execRepo.createDue(ofDue(taskId, "t15:k1"));
    assertFalse(cancelRequestedViaRepo(id), "新建行 cancel_requested 列默认 false");

    execRepo.claim(id, taskId, "w1", Instant.now().plusSeconds(60), 8);
    execRepo.requestCancel(id);
    assertTrue(cancelRequestedViaRepo(id));
  }

  private boolean cancelRequestedViaRepo(long executionId) {
    return execRepo.isCancelRequested(executionId);
  }

  @Test void findDeadLettersReturnsOnlyFailedAndDeadLetter_orderedById() {
    long taskId = newTask(8);
    // FAILED+dead_letter(应返回)
    long dead = failedExecution(8, "t16:k1");
    execRepo.markDeadLetter(dead, "unrecoverable");
    // FAILED 未置标记(不应返回)
    long plain = failedExecution(8, "t16:k2");
    // SUCCESS 终态(不应返回)
    long success = execRepo.createDue(ofDue(taskId, "t16:k3"));
    execRepo.claim(success, taskId, "w1", Instant.now().plusSeconds(60), 8);
    execRepo.markStatus(success, ExecutionStatus.SUCCESS, "w1", "done");

    var dlq = execRepo.findDeadLetters();

    assertEquals(1, dlq.size(), "only the FAILED+dead_letter row is a dead letter");
    assertEquals(dead, dlq.get(0).id());
  }

  @Test void requeueMovesFailedDeadLetterToDue_resetAndOutcome() {
    long id = failedExecution(8, "t17:k1"); // attempt=1 after claim
    execRepo.markDeadLetter(id, "unrecoverable");

    assertTrue(execRepo.requeue(id));

    Execution e = execRepo.findById(id).get();
    assertEquals(ExecutionStatus.DUE, e.status());
    assertEquals(0, e.attempt(), "requeue must reset attempt to 0");
    assertNull(e.nextRetryAt(), "requeue must clear next_retry_at");
    assertFalse(deadLetter(id), "requeue must clear dead_letter marker");
    Integer dueOutcomes = jdbc.queryForObject(
        "SELECT count(*) FROM execution_outcome WHERE execution_id=? AND status='DUE' AND detail='requeue'",
        Integer.class, id);
    assertEquals(1, dueOutcomes, "requeue must append a DUE outcome with detail='requeue'");
  }

  @Test void requeueOnNonFailedReturnsFalse_noChangeNoOutcome() {
    long id = failedExecution(8, "t18:k1");
    execRepo.markDeadLetter(id, "unrecoverable");
    execRepo.requeue(id); // 先成功入队到 DUE
    int outcomesBefore = outcomes(id);

    assertFalse(execRepo.requeue(id), "a DUE (non-FAILED) row must not be requeueable");

    assertEquals(ExecutionStatus.DUE, execRepo.findById(id).get().status(),
        "row must be unchanged after failed requeue");
    assertEquals(0, execRepo.findById(id).get().attempt());
    assertEquals(outcomesBefore, outcomes(id), "CAS 0 rows: no extra outcome");
  }

  @Test void sli_emptyTable_returnsZeros() {
    var sli = execRepo.sli();
    assertEquals(0, sli.completed1h());
    assertEquals(0, sli.failed1h());
    assertEquals(0.0, sli.p95LatencyMs(), 0.0, "无终态样本 → p95 兜 0");
  }

  /** SLI 吞吐按近 1h finished_at 切:终态(SUCCESS/CANCELED)计入 completed,FAILED 计入 failed;更早的终态不入窗。 */
  @Test void sli_countsCompletedAndFailedWithinOneHourWindow() {
    long taskId = newTask(8);
    // 1h 内 finished → 计入
    jdbc.update("INSERT INTO execution (task_id,status,idempotency_key,shard_count,started_at,finished_at)"
        + " VALUES (?, 'SUCCESS', ?, 1, now() - interval '1 hour', now())", taskId, "sli:s1");
    jdbc.update("INSERT INTO execution (task_id,status,idempotency_key,shard_count,started_at,finished_at)"
        + " VALUES (?, 'FAILED', ?, 1, now() - interval '30 minutes', now() - interval '10 minutes')",
        taskId, "sli:s2");
    // 2h 前 finished → 不入 1h 窗
    jdbc.update("INSERT INTO execution (task_id,status,idempotency_key,shard_count,started_at,finished_at)"
        + " VALUES (?, 'SUCCESS', ?, 1, now() - interval '3 hours', now() - interval '2 hours')",
        taskId, "sli:old-success");
    jdbc.update("INSERT INTO execution (task_id,status,idempotency_key,shard_count,started_at,finished_at)"
        + " VALUES (?, 'FAILED', ?, 1, now() - interval '3 hours', now() - interval '2 hours')",
        taskId, "sli:old-fail");
    // 非终态 → 不入窗
    long running = execRepo.createDue(ofDue(taskId, "sli:running"));
    execRepo.claim(running, taskId, "w1", Instant.now().plusSeconds(60), 8);

    var sli = execRepo.sli();
    assertEquals(1, sli.completed1h(), "1h 内恰 1 条 SUCCESS");
    assertEquals(1, sli.failed1h(), "1h 内恰 1 条 FAILED");
  }

  /** p95 完成延迟取近 24h finished 的 (finished_at - started_at),percentile_cont 线性插值。 */
  @Test void sli_computesP95LatencyOverRecent24h() {
    long taskId = newTask(8);
    // 两条终态,完成延迟 100ms 与 900ms(均在窗口)→ p95 = 100 + 0.95*(900-100) = 860ms
    jdbc.update("INSERT INTO execution (task_id,status,idempotency_key,shard_count,started_at,finished_at)"
        + " VALUES (?, 'SUCCESS', ?, 1, now() - interval '5 minutes', now() - interval '5 minutes' + interval '100 milliseconds')",
        taskId, "sli:p1");
    jdbc.update("INSERT INTO execution (task_id,status,idempotency_key,shard_count,started_at,finished_at)"
        + " VALUES (?, 'SUCCESS', ?, 1, now() - interval '5 minutes', now() - interval '5 minutes' + interval '900 milliseconds')",
        taskId, "sli:p2");

    var sli = execRepo.sli();
    assertEquals(860.0, sli.p95LatencyMs(), 1e-6, "两条延迟 [100,900] 的 p95 线性插值应为 860ms");
  }
}
