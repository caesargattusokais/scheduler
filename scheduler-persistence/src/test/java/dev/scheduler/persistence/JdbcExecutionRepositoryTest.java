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
}
