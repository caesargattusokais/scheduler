package dev.scheduler.persistence;
import static org.junit.jupiter.api.Assertions.*;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.core.Task;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcShardRepositoryTest extends AbstractPostgresTest {
  private final JdbcTaskRepository taskRepo = new JdbcTaskRepository(jdbc);
  private final JdbcShardRepository shardRepo = new JdbcShardRepository(jdbc);

  @BeforeEach void clean() {
    jdbc.update("TRUNCATE app_task, execution, execution_shard, execution_shard_outcome "
        + "RESTART IDENTITY CASCADE");
  }

  private long newTask(int shardCount, int maxActiveConcurrent) {
    Task t = taskRepo.create(new Task(null, "t"+System.nanoTime(), "cron", "demo", "*/5 * * * *",
        shardCount, 300, 0, 1000, null, maxActiveConcurrent, true, false));
    return t.id();
  }

  private long seedParentAndShards(long taskId, int shardCount) {
    return shardRepo.createParentWithShards(taskId, "p:"+System.nanoTime(), shardCount).id();
  }

  private Shard shard(long executionId, int idx) {
    return shardRepo.findShards(executionId).get(idx);
  }

  private boolean claimShard(long shardId, long taskId, String worker, int maxConcurrent) {
    return shardRepo.claim(shardId, taskId, worker, Instant.now().plusSeconds(60), maxConcurrent);
  }

  private int shardCount(long executionId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard WHERE execution_id=?", Integer.class, executionId);
  }

  private int outcomes(long shardId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=?", Integer.class, shardId);
  }

  private int successOutcomes(long shardId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='SUCCESS'",
        Integer.class, shardId);
  }

  private boolean deadLetter(long shardId) {
    return Boolean.TRUE.equals(jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=?", Boolean.class, shardId));
  }

  private boolean cancelRequested(long shardId) {
    return Boolean.TRUE.equals(jdbc.queryForObject(
        "SELECT cancel_requested FROM execution_shard WHERE id=?", Boolean.class, shardId));
  }

  // Task 1:父/N-shard 创建 + 读取
  @Test void createParentAndShards_oneParentWithNShards() {
    long taskId = newTask(3, 8);
    var parent = shardRepo.createParentWithShards(taskId, "p:k1", 3);

    assertNotNull(parent.id());
    assertEquals(3, parent.shardCount(), "父 shard_count=3");
    assertEquals(ExecutionStatus.DUE, parent.status());
    assertEquals(3, shardCount(parent.id()), "N=3 → 3 个 shard");

    var shards = shardRepo.findShards(parent.id());
    assertEquals(3, shards.size());
    for (int i = 0; i < 3; i++) {
      assertEquals(i, shards.get(i).shardIndex(), "shard_index 0..2 升序");
      assertEquals(parent.id(), shards.get(i).executionId());
      assertEquals(ExecutionStatus.DUE, shards.get(i).status());
    }
  }

  @Test void createParent_sameKey_idempotent_noDupShards() {
    long taskId = newTask(3, 8);
    String parentKey = "p:k2";

    var first = shardRepo.createParentWithShards(taskId, parentKey, 3);
    var second = shardRepo.createParentWithShards(taskId, parentKey, 3);

    assertEquals(first.id(), second.id(), "same parentKey → 同一父 id");
    assertEquals(3, shardCount(first.id()), "父重复创建不重复插 shard,总数仍 N");
  }

  @Test void findShards_orderedByIndex() {
    long taskId = newTask(4, 8);
    var parent = shardRepo.createParentWithShards(taskId, "p:k3", 4);

    var shards = shardRepo.findShards(parent.id());

    assertEquals(4, shards.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(i, shards.get(i).shardIndex(), "按 shard_index 升序");
    }
  }

  @Test void createParent_shardCountOne_stillParentPlusOneShard() {
    long taskId = newTask(1, 8);
    var parent = shardRepo.createParentWithShards(taskId, "p:k4", 1);

    assertEquals(1, parent.shardCount());
    assertEquals(1, shardCount(parent.id()), "N=1 → 1 父 + 1 shard(统一模型)");
    var shard = shardRepo.findShards(parent.id()).get(0);
    assertEquals(0, shard.shardIndex());
    // findShard 按 id 读单条
    var byId = shardRepo.findShard(shard.id());
    assertEquals(shard.id(), byId.get().id());
    assertEquals(parent.id(), byId.get().executionId());
  }

  // Task 2:worker 侧生命周期
  @Test void findCandidateReturnsDueShard() {
    long taskId = newTask(3, 8);
    long parentId = seedParentAndShards(taskId, 3);

    var cand = shardRepo.findCandidate(taskId);

    assertTrue(cand.isPresent());
    assertEquals(parentId, cand.get().executionId());
    assertEquals(0, cand.get().shardIndex(), "按 shard id 升序取首条");
    assertEquals(ExecutionStatus.DUE, cand.get().status());
  }

  @Test void findCandidateSkipsFutureRetryShard() {
    long taskId = newTask(1, 8);
    seedParentAndShards(taskId, 1); // 唯一 shard 0,默认 next_retry_at NULL
    // 把它标成未来才到期的重试闸:DUE 但未到重试时间 → 不应被 findCandidate 返回
    jdbc.update("UPDATE execution_shard SET next_retry_at = now() + interval '1 hour'");

    assertTrue(shardRepo.findCandidate(taskId).isEmpty(),
        "a DUE shard with next_retry_at in the future must not be returned by findCandidate");
  }

  @Test void findCandidateReturnsNullRetryShard() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1); // next_retry_at 默认 NULL

    var cand = shardRepo.findCandidate(taskId);
    assertTrue(cand.isPresent());
    assertEquals(0, cand.get().shardIndex());
    assertEquals(parentId, cand.get().executionId());
  }

  @Test void claimThenSecondFailsAndOutcome() {
    long taskId = newTask(2, 8);
    long parentId = seedParentAndShards(taskId, 2);
    long shard0 = shard(parentId, 0).id();

    assertTrue(claimShard(shard0, taskId, "w1", 8));
    assertEquals(1, outcomes(shard0), "claim 必须落 RUNNING outcome 行");
    Shard claimed = shardRepo.findShard(shard0).get();
    assertEquals(ExecutionStatus.RUNNING, claimed.status());
    assertEquals(1, claimed.attempt(), "claim 递增 attempt");
    assertEquals("w1", claimed.workerId());
    assertNotNull(claimed.leaseUntil());

    assertFalse(claimShard(shard0, taskId, "w2", 8), "already RUNNING, second claim must fail");
    assertEquals(1, outcomes(shard0), "no extra outcome after failed claim");
  }

  @Test void countActiveIsOneAfterClaim() {
    long taskId = newTask(2, 8);
    long parentId = seedParentAndShards(taskId, 2);

    claimShard(shard(parentId, 0).id(), taskId, "w1", 8);

    assertEquals(1, shardRepo.countActive(taskId));
  }

  @Test void excessiveConcurrencyRejectsSecondShard() {
    long taskId = newTask(2, 1); // maxActiveConcurrent=1
    long parentId = seedParentAndShards(taskId, 2);
    long shard0 = shard(parentId, 0).id();
    long shard1 = shard(parentId, 1).id();

    assertTrue(claimShard(shard0, taskId, "w1", 1));
    assertEquals(1, shardRepo.countActive(taskId));
    // 配额已饱和(countActive==1):第二 shard 无法在 maxConcurrent=1 下被领取
    assertFalse(claimShard(shard1, taskId, "w2", 1));
    assertEquals(ExecutionStatus.DUE, shardRepo.findShard(shard1).get().status());
  }

  @Test void markStatusWritesSuccessAndOutcome() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id();
    claimShard(shard0, taskId, "w1", 8);

    shardRepo.markStatus(shard0, ExecutionStatus.SUCCESS, "w1", "done");

    Shard s = shardRepo.findShard(shard0).get();
    assertEquals(ExecutionStatus.SUCCESS, s.status());
    assertTrue(s.finishedAt() != null, "终态必须写 finished_at");
    assertEquals(2, outcomes(shard0), "claim outcome + SUCCESS outcome");
  }

  @Test void markStatusOwnedWrongOwner_suppressedNoClobber() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id();
    claimShard(shard0, taskId, "w1", 8); // -> RUNNING, worker_id=w1

    boolean ok = shardRepo.markStatusOwned(shard0, ExecutionStatus.SUCCESS, "w2", "stale done");

    assertFalse(ok, "owner mismatch → 守卫必须拦截回写");
    Shard s = shardRepo.findShard(shard0).get();
    assertEquals(ExecutionStatus.RUNNING, s.status(), "非持有者的回写不得改动行状态");
    assertEquals("w1", s.workerId(), "非持有者的回写不得改动 owner");
    assertEquals(0, successOutcomes(shard0), "被守卫拦截 → 不落 SUCCESS outcome");
  }

  @Test void markStatusOwnedCorrectOwner_singleOutcome() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id();
    claimShard(shard0, taskId, "w1", 8); // -> RUNNING, worker_id=w1

    boolean ok = shardRepo.markStatusOwned(shard0, ExecutionStatus.SUCCESS, "w1", "done");

    assertTrue(ok, "持有者自身回写应成功");
    Shard s = shardRepo.findShard(shard0).get();
    assertEquals(ExecutionStatus.SUCCESS, s.status());
    assertNotNull(s.finishedAt(), "终态必须写 finished_at");
    assertEquals(1, successOutcomes(shard0), "恰一条 SUCCESS outcome");
  }

  @Test void markStatusOwnedOnAlreadyTerminal_false() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id();
    claimShard(shard0, taskId, "w1", 8);
    shardRepo.markStatus(shard0, ExecutionStatus.SUCCESS, "w1", "done"); // 已 SUCCESS 终态

    boolean ok = shardRepo.markStatusOwned(shard0, ExecutionStatus.SUCCESS, "w1", "again");

    assertFalse(ok, "非可迁移(SUCCESS→SUCCESS)→ 守卫返回 false");
    assertEquals(1, successOutcomes(shard0), "不得再落第二条 outcome");
  }

  @Test void illegalTransitionThrows() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id(); // DUE

    // DUE -> SUCCESS skips RUNNING: ExecutionTransitions rejects it
    assertThrows(IllegalStateException.class,
        () -> shardRepo.markStatus(shard0, ExecutionStatus.SUCCESS, "w1", "skip"));
    // DUE -> NOT_CREATED also illegal
    assertThrows(IllegalStateException.class,
        () -> shardRepo.markStatus(shard0, ExecutionStatus.NOT_CREATED, "w1", "nope"));
  }

  private long failedShard(long taskId, int shardCount) {
    long parentId = seedParentAndShards(taskId, shardCount);
    long shard0 = shard(parentId, 0).id();
    claimShard(shard0, taskId, "w1", 8); // attempt -> 1
    shardRepo.markStatus(shard0, ExecutionStatus.FAILED, "w1", "boom");
    return shard0;
  }

  @Test void scheduleRetryMovesFailedToDueWithOutcome() {
    long taskId = newTask(1, 8);
    long shard0 = failedShard(taskId, 1);
    Instant retryAt = Instant.now().plusSeconds(10);

    shardRepo.scheduleRetry(shard0, retryAt, "boom");

    Shard s = shardRepo.findShard(shard0).get();
    assertEquals(ExecutionStatus.DUE, s.status());
    assertNotNull(s.nextRetryAt(), "next_retry_at must be written");
    assertTrue(s.nextRetryAt().isAfter(Instant.now().minusSeconds(1)));
    assertTrue(s.nextRetryAt().isBefore(Instant.now().plusSeconds(20)), "delay ~10s");
    assertEquals(1, s.attempt(), "attempt must not change on retry");
    assertEquals("w1", s.workerId(), "worker_id must stay with the failing worker");
    // claim RUNNING outcome + FAILED outcome + DUE retry outcome
    assertEquals(3, outcomes(shard0));
    Integer dueOutcomes = jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='DUE' AND detail='boom'",
        Integer.class, shard0);
    assertEquals(1, dueOutcomes, "retry must append a DUE outcome carrying the original failure detail");
  }

  @Test void scheduleRetryOnNonFailedSilent() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id();
    claimShard(shard0, taskId, "w1", 8); // -> RUNNING, not FAILED

    shardRepo.scheduleRetry(shard0, Instant.now().plusSeconds(10), "boom");

    assertEquals(ExecutionStatus.RUNNING, shardRepo.findShard(shard0).get().status());
    assertEquals(1, outcomes(shard0), "CAS 0 rows: no extra outcome");
  }

  @Test void markDeadLetterSetFlagOnFailed() {
    long taskId = newTask(1, 8);
    long shard0 = failedShard(taskId, 1);
    int before = outcomes(shard0);

    shardRepo.markDeadLetter(shard0, "unrecoverable");

    assertTrue(deadLetter(shard0));
    assertEquals(before, outcomes(shard0), "dead_letter is not a status transition: no outcome row");
  }

  @Test void isCancelRequested_defaultsFalse_readsTrue() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id();
    assertFalse(shardRepo.isCancelRequested(shard0), "新建 shard cancel_requested 列默认 false");

    // 取消请求由控制台/回收器写入(非 worker 生命周期方法),此处经 SQL 置位后读取
    jdbc.update("UPDATE execution_shard SET cancel_requested=true WHERE id=?", shard0);
    assertTrue(shardRepo.isCancelRequested(shard0));
  }
}
