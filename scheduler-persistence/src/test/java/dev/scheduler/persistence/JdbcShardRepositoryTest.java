package dev.scheduler.persistence;
import static org.junit.jupiter.api.Assertions.*;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.core.Task;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
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

  /** 取带整轮预算 W 的任务。 */
  private long newTaskBudget(int shardCount, int maxActiveConcurrent, Long budgetMs) {
    Task t = taskRepo.create(new Task(null, "t" + System.nanoTime(), "cron", "demo", "*/5 * * * *",
        shardCount, 300, 0, 1000, null, maxActiveConcurrent, true, false, null, null, budgetMs));
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

  @Test void createParent_singleTransaction_exactlyOneParentWithNDueShards() {
    // 回归:M3 spec §2 要求 父 + N 条 execution_shard 单事务原子提交。
    long taskId = newTask(4, 8);

    var parent = shardRepo.createParentWithShards(taskId, "p:atomic", 4);

    // 恰一条父 execution(原子性守护:不得残留父->shard 间的部分提交)。
    Integer parentRows = jdbc.queryForObject(
        "SELECT count(*) FROM execution WHERE idempotency_key=?", Integer.class, "p:atomic");
    assertEquals(1, parentRows, "fresh key → 恰一条父 execution");
    // 恰 N 条 shard 且全部 DUE(父 + shard 在单事务内一起提交)。
    Integer dueShards = jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard WHERE execution_id=? AND status='DUE'",
        Integer.class, parent.id());
    assertEquals(4, dueShards, "单事务提交 N=4 条 DUE shard");
    assertEquals(4, shardCount(parent.id()), "总 shard 数 = N");
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

  /** M3 防御守卫:shardCount=0 会扇出 0 个 shard → 父恒 DUE 无法汇聚,必须拒绝。 */
  @Test void createParentWithShards_zeroShardCount_throwsIllegalArgumentException() {
    long taskId = newTask(1, 8);

    assertThrows(IllegalArgumentException.class,
        () -> shardRepo.createParentWithShards(taskId, "p:zero", 0),
        "shardCount < 1 必须抛 IllegalArgumentException,不得残留恒 DUE 的父");
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

  // M6.5 重跑溯源:5 参重载须落 args + rerun_of;3 参重载缺省为 NULL/NULL
  @Test void createParentWithShards_sixParams_storesArgsAndRerunOf() {
    long taskId = newTask(2, 8);
    long sourceId = shardRepo.createParentWithShards(taskId, "p:src", 2).id();

    var rerun = shardRepo.createParentWithShards(taskId, "p:rerun", 2, "{\"x\":1}", sourceId);

    assertEquals(2, rerun.shardCount());
    assertEquals(2, shardCount(rerun.id()), "重跑轮同样扇出 2 shard");
    assertEquals(sourceId, rerun.rerunOf(), "重跑轮 rerun_of 应指向源轮(溯源)");
    assertEquals("{\"x\":1}", rerun.args(), "重跑轮继承源轮 args");
    // 直查 DB 复核列确实落盘(非仅内存对象)
    assertEquals(sourceId, jdbc.queryForObject(
        "SELECT rerun_of FROM execution WHERE id=?", Long.class, rerun.id()));
    assertEquals("{\"x\":1}", jdbc.queryForObject(
        "SELECT args FROM execution WHERE id=?", String.class, rerun.id()));
    // 源轮 rerun_of 恒 NULL
    assertNull(jdbc.queryForObject(
        "SELECT rerun_of FROM execution WHERE id=?", Long.class, sourceId));
  }

  @Test void createParentWithShards_threeParams_storesNullArgsAndRerunOf() {
    long taskId = newTask(1, 8);
    var parent = shardRepo.createParentWithShards(taskId, "p:plain", 1);

    assertNull(parent.rerunOf(), "普通触发/cron 轮 rerun_of 恒 NULL");
    assertNull(parent.args(), "普通轮不复制 args");
  }

  // Task 2:worker 侧生命周期
  @Test void findCandidateReturnsDueShard() {
    long taskId = newTask(3, 8);
    long parentId = seedParentAndShards(taskId, 3);

    var cand = shardRepo.findCandidate(taskId, null); // workerId null → offset 0(旧行为)

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

    assertTrue(shardRepo.findCandidate(taskId, null).isEmpty(),
        "a DUE shard with next_retry_at in the future must not be returned by findCandidate");
  }

  @Test void findCandidateReturnsNullRetryShard() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1); // next_retry_at 默认 NULL

    var cand = shardRepo.findCandidate(taskId, null);
    assertTrue(cand.isPresent());
    assertEquals(0, cand.get().shardIndex());
    assertEquals(parentId, cand.get().executionId());
  }

  /** 4c 选片摊开:同 task 多 DUE 片,不同 worker 各认领不同 offset 的片(哈希 mod 可领数),避免全员抢同一最低 id。 */
  @Test void findCandidate_spreadsAcrossWorkersAndIsStablePerWorker() {
    long taskId = newTask(5, 8);
    seedParentAndShards(taskId, 5); // shard 0..4,单父按 shard id 升序与 index 对齐

    String a = "worker-alpha", b = "worker-beta";
    int total = 5;
    int offA = (int) ((a.hashCode() & 0x7fffffff) % total);
    int offB = (int) ((b.hashCode() & 0x7fffffff) % total);

    var ca1 = shardRepo.findCandidate(taskId, a).orElseThrow();
    var ca2 = shardRepo.findCandidate(taskId, a).orElseThrow();
    var cb = shardRepo.findCandidate(taskId, b).orElseThrow();
    assertEquals(offA, ca1.shardIndex(), "worker hash 稳定落到可领集内的该 offset");
    assertEquals(offA, ca2.shardIndex(), "同 worker 集合未变时两次取同片");
    assertEquals(offB, cb.shardIndex());
    assertNotEquals(offA, offB, "两 worker 偏移错开,认领不同片");
    assertEquals(ExecutionStatus.DUE, ca1.status());
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

  @Test void renewLease_extendsWhileOwned_ignoresWhenLostOrFinal() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id();
    claimShard(shard0, taskId, "w1", 8);

    Instant renewed = Instant.now().plusSeconds(300).truncatedTo(ChronoUnit.MICROS);
    assertTrue(shardRepo.renewLease(shard0, "w1", renewed), "持有者续约应成功");
    assertEquals(renewed, shardRepo.findShard(shard0).get().leaseUntil());

    // 非持有者(worker 变更)续约 → 0 行静默 false
    assertFalse(shardRepo.renewLease(shard0, "w2", renewed.plusSeconds(10)));
    assertEquals(renewed, shardRepo.findShard(shard0).get().leaseUntil(), "非持有者不得改变租约");

    // 终态(如写回 SUCCESS)后不再续约
    assertTrue(shardRepo.markStatusOwned(shard0, ExecutionStatus.SUCCESS, "w1", "ok"));
    assertFalse(shardRepo.renewLease(shard0, "w1", renewed.plusSeconds(20)));
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

  /** MED-1 硬配额:同一任务的两个不同 shard 在 maxConcurrent=1 下并发认领,行级锁串行化后恰有一个成功。
   *  并发突发不再超配(读后写竞态由 app_task 行锁消除);该断言是硬不变量,锁缺失时可能同时成功,锁在则必恰一。 */
  @Test void concurrentClaims_respectHardQuota() throws Exception {
    long taskId = newTask(2, 1); // maxConcurrent=1,两个 DUE shard
    long parentId = seedParentAndShards(taskId, 2);
    long shard0 = shard(parentId, 0).id();
    long shard1 = shard(parentId, 1).id();

    int threads = 2;
    var pool = Executors.newFixedThreadPool(threads);
    var start = new CountDownLatch(1);
    AtomicInteger succeeded = new AtomicInteger();
    try {
      Runnable claim0 = () -> { awaitO(start); if (claimShard(shard0, taskId, "w1", 1)) succeeded.incrementAndGet(); };
      Runnable claim1 = () -> { awaitO(start); if (claimShard(shard1, taskId, "w2", 1)) succeeded.incrementAndGet(); };
      pool.submit(claim0);
      pool.submit(claim1);
      start.countDown(); // 同时放行,制造并发认领窗口
      Thread.sleep(1500);
    } finally {
      pool.shutdown();
    }
    assertEquals(1, succeeded.get(), "maxConcurrent=1 下并发认领两个 shard,行级锁应保证恰有一个成功");
    assertEquals(1, shardRepo.countActive(taskId), "超配额 shard 不得进入 RUNNING");
  }

  private static void awaitO(CountDownLatch l) {
    try { l.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
  }

  /** 全局口径:跨全部任务数 RUNNING 且租约有效的 shard(指标卡;不受任务创建时机影响)。 */
  @Test void countActive_globalAcrossAllTasks() {
    long t1 = newTask(2, 8);
    long t2 = newTask(1, 8);
    claimShard(shard(seedParentAndShards(t1, 2), 0).id(), t1, "w1", 8);
    claimShard(shard(seedParentAndShards(t2, 1), 0).id(), t2, "w2", 8);

    assertEquals(2, shardRepo.countActive(), "两任务各 1 个 RUNNING → 全局 2");
    assertEquals(1, shardRepo.countActive(t1), "per-task 口径仍按任务过滤");
  }

  /** 队龄按 queued_at 量(而非父 created_at);FAILD→DUE requeue 重置 queued_at,重排后不虚高。 */
  @Test void maxDueQueueAge_fromQueuedAt_resetOnRequeue() {
    long taskId = newTask(2, 8);
    long parentId = seedParentAndShards(taskId, 2);
    long shard0 = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='DUE', queued_at=now() - interval '120 seconds' WHERE id=?", shard0);

    assertTrue(shardRepo.maxDueQueueAgeSeconds() >= 120, "队龄按 queued_at 量,应 ≥120s");

    // requeue(FAILED→DUE)把 queued_at 重置为 now,队龄应回落到 ~0(另一分片同为新建 ≈0)
    jdbc.update("UPDATE execution_shard SET status='FAILED' WHERE id=?", shard0);
    assertTrue(shardRepo.requeueShard(shard0));
    assertEquals(0L, shardRepo.maxDueQueueAgeSeconds(), "requeue 重置 queued_at,队龄应回 0");
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

    shardRepo.scheduleRetry(shard0, retryAt, null, "boom");

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

  /** 重试(FAILED→DUE,最常见的再入队路径)重置 queued_at → 队龄从"重新入队"起算,不再虚高。 */
  @Test void scheduleRetry_resetsQueuedAt() {
    long taskId = newTask(1, 8);
    long shard0 = failedShard(taskId, 1);
    // 制造"很久前入队"假象:若不重置 queued_at,重试后队龄会虚高到 ~120s
    jdbc.update("UPDATE execution_shard SET queued_at=now() - interval '120 seconds' WHERE id=?", shard0);

    shardRepo.scheduleRetry(shard0, Instant.now().plusSeconds(10), null, "boom");

    assertEquals(ExecutionStatus.DUE, shardRepo.findShard(shard0).get().status());
    assertEquals(0L, shardRepo.maxDueQueueAgeSeconds(), "重试重置 queued_at,队龄应回 0,而非沿用 120s 前的入队时间");
  }

  @Test void scheduleRetryOnNonFailedSilent() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id();
    claimShard(shard0, taskId, "w1", 8); // -> RUNNING, not FAILED

    shardRepo.scheduleRetry(shard0, Instant.now().plusSeconds(10), null, "boom");

    assertEquals(ExecutionStatus.RUNNING, shardRepo.findShard(shard0).get().status());
    assertEquals(1, outcomes(shard0), "CAS 0 rows: no extra outcome");
  }

  // ---- Task 4:整轮重试预算 W + claim 每轮重置 started_at ----

  @Test void scheduleRetry_setsRetryBudgetWindowOnce_doesNotExtendOnRetries() {
    long taskId = newTaskBudget(1, 8, 300_000L);
    long shard0 = failedShard(taskId, 1);
    shardRepo.scheduleRetry(shard0, Instant.now().plusSeconds(10), 300_000L, "boom");
    java.sql.Timestamp first = jdbc.queryForObject(
        "SELECT retry_budget_until FROM execution_shard WHERE id=?", java.sql.Timestamp.class, shard0);
    assertNotNull(first, "首次进重试置 retry_budget_until=now()+W");

    // 再失败一次(CAS 无关,直接落地 FAILED),改更大预算重试:窗口不得续后,COALESCE 保持首置。
    jdbc.update("UPDATE execution_shard SET status='FAILED' WHERE id=?", shard0);
    shardRepo.scheduleRetry(shard0, Instant.now().plusSeconds(10), 600_000L, "boom2");
    java.sql.Timestamp second = jdbc.queryForObject(
        "SELECT retry_budget_until FROM execution_shard WHERE id=?", java.sql.Timestamp.class, shard0);
    assertEquals(first, second, "预算窗口自首次失败起算,重试间不续");
  }

  @Test void scheduleRetry_noBudget_leavesWindowNull() {
    long taskId = newTask(1, 8); // 无预算
    long shard0 = failedShard(taskId, 1);
    shardRepo.scheduleRetry(shard0, Instant.now().plusSeconds(10), null, "boom");
    assertNull(jdbc.queryForObject(
        "SELECT retry_budget_until FROM execution_shard WHERE id=?", java.sql.Timestamp.class, shard0),
        "未设 W → 不置预算窗(行为=现状按次数)");
  }

  @Test void retryBudgetExhausted_reflectsDeadlineAgainstDbNow() {
    long taskId = newTaskBudget(1, 8, 300_000L);
    long shard0 = failedShard(taskId, 1);
    shardRepo.scheduleRetry(shard0, Instant.now().plusSeconds(10), 300_000L, "boom");
    assertFalse(shardRepo.retryBudgetExhausted(shard0), "窗口未到 → 未耗尽");

    jdbc.update("UPDATE execution_shard SET retry_budget_until=now() - interval '1 second' WHERE id=?", shard0);
    assertTrue(shardRepo.retryBudgetExhausted(shard0), "预算到点(DB now)> → 耗尽");
  }

  @Test void retryBudgetExhausted_noBudget_isFalse() {
    long taskId = newTask(1, 8);
    long shard0 = failedShard(taskId, 1);
    assertFalse(shardRepo.retryBudgetExhausted(shard0), "未设 W → 恒 false(=现状仅按次数)");
  }

  @Test void claim_resetsStartedAt_eachClaim() {
    long taskId = newTask(1, 8);
    long shard0 = shard(seedParentAndShards(taskId, 1), 0).id();
    claimShard(shard0, taskId, "w1", 8);
    // 回 FAILED 再重试认领:started_at 须重置为认领时刻,而非沿用首次认领时间(超时从本轮起算)。
    jdbc.update("UPDATE execution_shard SET started_at=now() - interval '10 minutes' WHERE id=?", shard0);
    jdbc.update("UPDATE execution_shard SET status='FAILED' WHERE id=?", shard0);
    jdbc.update("UPDATE execution_shard SET status='DUE', next_retry_at=NULL WHERE id=?", shard0);
    claimShard(shard0, taskId, "w1", 8);
    java.sql.Timestamp started = jdbc.queryForObject(
        "SELECT started_at FROM execution_shard WHERE id=?", java.sql.Timestamp.class, shard0);
    assertNotNull(started);
    assertTrue(started.toInstant().isAfter(Instant.now().minusSeconds(5)),
        "重试重新认领 → started_at 重置为 now(),非 10 分钟前的旧值");
  }

  @Test void markDeadLetterSetFlagOnFailed() {
    long taskId = newTask(1, 8);
    long shard0 = failedShard(taskId, 1);
    int before = outcomes(shard0);

    shardRepo.markDeadLetter(shard0, "unrecoverable");

    assertTrue(deadLetter(shard0));
    assertEquals(before, outcomes(shard0), "dead_letter is not a status transition: no outcome row");
  }

  /** DLQ 诊断带出:各分片取最近一次 FAILED detail(DISTINCT ON);不同任务名的 handlerRef 一并解析。 */
  @Test void failureDetailsByShardIds_andTaskRefsByShardIds() {
    long t1 = newTask(2, 8);
    long parentId = seedParentAndShards(t1, 2);
    long s0 = shard(parentId, 0).id();
    long s1 = shard(parentId, 1).id();
    // 该父仍 DUE 未认领,task 名直接查库对齐
    String taskName = jdbc.queryForObject("SELECT name FROM app_task WHERE id=?", String.class, t1);
    String handlerRef = jdbc.queryForObject("SELECT handler_ref FROM app_task WHERE id=?", String.class, t1);

    claimShard(s0, t1, "w1", 8);
    shardRepo.markStatus(s0, ExecutionStatus.FAILED, "w1", "boom"); // FAILED detail=boom
    // 再插一条更旧的 FAILED detail(created_at 早 1h),验证 DISTINCT ON 取最新而非任意
    jdbc.update("INSERT INTO execution_shard_outcome (shard_id, status, created_at, detail) "
        + "VALUES (?, 'FAILED', now()-interval '1 hour', 'older-failed')", s0);
    // s1 无 FAILED outcome
    Map<Long, String> failed = shardRepo.findFailureDetailsByShardIds(List.of(s0, s1));
    assertEquals(1, failed.size(), "仅带 FAILED outcome 的分片有 detail");
    assertEquals("boom", failed.get(s0), "DISTINCT ON 取最近一条 FAILED detail");

    Map<Long, TaskRef> refs = shardRepo.findTaskRefsByShardIds(List.of(s0, s1, 999999L));
    assertEquals(2, refs.size(), "两个分片各解析出所属任务;不存在 shard 无条目");
    assertEquals(taskName, refs.get(s0).name());
    assertEquals(handlerRef, refs.get(s0).handlerRef());
    assertEquals(taskName, refs.get(s1).name());
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

  // ---- Task 3:父汇聚 + FAIL_FAST + 父取消 + DLQ/requeue ----

  private String parentStatus(long parentId) {
    return jdbc.queryForObject("SELECT status FROM execution WHERE id=?", String.class, parentId);
  }

  private int parentOutcomes(long parentId, String status) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_outcome WHERE execution_id=? AND status=?",
        Integer.class, parentId, status);
  }

  private boolean parentCancelRequested(long parentId) {
    return Boolean.TRUE.equals(jdbc.queryForObject(
        "SELECT cancel_requested FROM execution WHERE id=?", Boolean.class, parentId));
  }

  /** 构建 3-shard 父:shard0 RUNNING 并回写终态(SUCCESS 或 FAILED),shard1 RUNNING,shard2 DUE。 */
  private long seededTerminalParent(long taskId) {
    long parentId = seedParentAndShards(taskId, 3);
    claimShard(shard(parentId, 0).id(), taskId, "w1", 8);
    claimShard(shard(parentId, 1).id(), taskId, "w2", 8);
    return parentId;
  }

  @Test void aggregateParent_allShardsSuccess_parentSuccess() {
    long taskId = newTask(3, 8);
    long parentId = seedParentAndShards(taskId, 3);
    for (int i = 0; i < 3; i++) {
      long sid = shard(parentId, i).id();
      claimShard(sid, taskId, "w"+i, 8);
      shardRepo.markStatus(sid, ExecutionStatus.SUCCESS, "w"+i, "done");
    }

    // 全部终态已就位、父仍 DUE → 列在待汇聚
    assertTrue(shardRepo.parentsNeedingAggregation().stream().anyMatch(p -> p.executionId() == parentId),
        "parent 仍 DUE 且 ≥1 终态 shard → 待汇聚");
    assertEquals(ExecutionStatus.DUE, ExecutionStatus.valueOf(parentStatus(parentId)));

    assertTrue(shardRepo.finalizeParent(parentId, ExecutionStatus.SUCCESS, "all done"));
    assertEquals(ExecutionStatus.SUCCESS, ExecutionStatus.valueOf(parentStatus(parentId)));
    assertNotNull(shardRepo.findParent(parentId).get().finishedAt(), "父终态必须写 finished_at");
    assertEquals(1, parentOutcomes(parentId, "SUCCESS"), "恰一条父 SUCCESS outcome");
    assertFalse(shardRepo.parentsNeedingAggregation().stream().anyMatch(p -> p.executionId() == parentId),
        "父已终态 → 不再待汇聚");
    assertTrue(shardRepo.parentsNeedingAggregation().isEmpty());
  }

  @Test void aggregateParent_oneFailed_parentFailed() {
    long taskId = newTask(3, 8);
    long parentId = seedParentAndShards(taskId, 3);
    claimShard(shard(parentId, 0).id(), taskId, "w1", 8);
    shardRepo.markStatus(shard(parentId, 0).id(), ExecutionStatus.FAILED, "w1", "boom");
    for (int i = 1; i < 3; i++) {
      long sid = shard(parentId, i).id();
      claimShard(sid, taskId, "w"+i, 8);
      shardRepo.markStatus(sid, ExecutionStatus.SUCCESS, "w"+i, "done");
    }

    assertTrue(shardRepo.parentsNeedingAggregation().stream().anyMatch(p -> p.executionId() == parentId));
    assertTrue(shardRepo.finalizeParent(parentId, ExecutionStatus.FAILED, "boom"));
    assertEquals(ExecutionStatus.FAILED, ExecutionStatus.valueOf(parentStatus(parentId)));
    assertEquals(1, parentOutcomes(parentId, "FAILED"));
  }

  @Test void finalizeParent_idempotent_CAS0Skips() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    assertTrue(shardRepo.finalizeParent(parentId, ExecutionStatus.SUCCESS, "done"));

    boolean again = shardRepo.finalizeParent(parentId, ExecutionStatus.SUCCESS, "again");

    assertFalse(again, "父已终态(非 DUE)→ CAS 0 行 → 幂等返回 false");
    assertEquals(1, parentOutcomes(parentId, "SUCCESS"), "不得重复落第二条父 outcome");
  }

  @Test void failFast_cancelsRunningAndDueSiblings() {
    long taskId = newTask(3, 8);
    long parentId = seededTerminalParent(taskId); // shard0, shard1 RUNNING; shard2 DUE
    long shard0 = shard(parentId, 0).id();
    long shard1 = shard(parentId, 1).id();
    long shard2 = shard(parentId, 2).id();
    shardRepo.markStatus(shard0, ExecutionStatus.FAILED, "w1", "boom"); // 触发失败片(已 FAILED)

    shardRepo.cancelSiblings(shard0, "failfast");

    assertFalse(cancelRequested(shard0), "触发失败片自身不置取消信号");
    assertTrue(cancelRequested(shard1), "RUNNING 兄弟置协作取消信号");
    assertEquals(ExecutionStatus.RUNNING, shardRepo.findShard(shard1).get().status(),
        "RUNNING 兄弟仅置信号,状态仍 RUNNING");
    assertEquals(ExecutionStatus.CANCELED, shardRepo.findShard(shard2).get().status(),
        "DUE 兄弟直编 CANCELED");
    Integer canceledOutcome = jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='CANCELED' AND detail='failfast'",
        Integer.class, shard2);
    assertEquals(1, canceledOutcome, "DUE 兄弟落 CANCELED('failfast') outcome");
    assertTrue(shardRepo.hasRunningShard(parentId), "shard1 仍 RUNNING → hasRunningShard true");
  }

  @Test void requestCancelParent_flagsRunningAndCancelsDue() {
    long taskId = newTask(3, 8);
    long parentId = seededTerminalParent(taskId); // shard0, shard1 RUNNING; shard2 DUE
    long shard0 = shard(parentId, 0).id();
    long shard2 = shard(parentId, 2).id();

    boolean hasRunning = shardRepo.requestCancelParent(parentId);

    assertTrue(hasRunning, "仍有 RUNNING shard 被置信号 → 返回 true");
    assertTrue(cancelRequested(shard0), "RUNNING shard 置协作取消信号");
    assertEquals(ExecutionStatus.CANCELED, shardRepo.findShard(shard2).get().status(), "DUE shard 直编 CANCELED");
    Integer cascade = jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='CANCELED' AND detail='cascade cancel'",
        Integer.class, shard2);
    assertEquals(1, cascade, "DUE shard 落 'cascade cancel' outcome");
    assertTrue(parentCancelRequested(parentId), "父置 cancel_requested");
    assertTrue(shardRepo.hasRunningShard(parentId), "RUNNING shard 未立刻终态 → hasRunningShard true");
  }

  @Test void cancelParentImmediate_cancelsParentAndShards() {
    long taskId = newTask(3, 8);
    long parentId = seededTerminalParent(taskId); // shard0, shard1 RUNNING; shard2 DUE
    long shard1 = shard(parentId, 1).id();
    long shard2 = shard(parentId, 2).id();

    shardRepo.cancelParentImmediate(parentId);

    assertEquals(ExecutionStatus.CANCELED, ExecutionStatus.valueOf(parentStatus(parentId)), "父直编 CANCELED");
    assertNotNull(shardRepo.findParent(parentId).get().finishedAt(), "父终态写 finished_at");
    assertEquals(1, parentOutcomes(parentId, "CANCELED"), "父落 'cancel parent' outcome");
    assertEquals(ExecutionStatus.CANCELED, shardRepo.findShard(shard1).get().status(), "RUNNING shard 硬取消");
    assertEquals(ExecutionStatus.CANCELED, shardRepo.findShard(shard2).get().status(), "DUE shard 硬取消");
    assertFalse(shardRepo.hasRunningShard(parentId), "无可取消的 RUNNING shard → false");
  }

  @Test void findDeathLetterShards_listsOnlyFailedDeadLetter() {
    long taskId = newTask(2, 8);
    long parentId = seedParentAndShards(taskId, 2);
    // shard0 FAILED+dead_letter(进 DLQ);shard1 仅 FAILED 不标(不进 DLQ)
    claimShard(shard(parentId, 0).id(), taskId, "w1", 8);
    shardRepo.markStatus(shard(parentId, 0).id(), ExecutionStatus.FAILED, "w1", "boom");
    shardRepo.markDeadLetter(shard(parentId, 0).id(), "unrecoverable");
    claimShard(shard(parentId, 1).id(), taskId, "w2", 8);
    shardRepo.markStatus(shard(parentId, 1).id(), ExecutionStatus.FAILED, "w2", "transient");

    var dlq = shardRepo.findDeathLetterShards();

    assertEquals(1, dlq.size(), "仅 FAILED 且 dead_letter 的 shard 进 DLQ");
    assertEquals(shard(parentId, 0).id(), dlq.get(0).id());
    assertEquals(ExecutionStatus.FAILED, dlq.get(0).status());
  }

  @Test void requeueShard_movesFailedToDue_reset() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id();
    claimShard(shard0, taskId, "w1", 8); // attempt -> 1
    shardRepo.markStatus(shard0, ExecutionStatus.FAILED, "w1", "boom");
    jdbc.update("UPDATE execution_shard SET dead_letter=true, next_retry_at=now()+interval '10 minutes'"
        + " WHERE id=?", shard0); // 模拟 DLQ 终末行

    assertTrue(shardRepo.requeueShard(shard0));

    Shard s = shardRepo.findShard(shard0).get();
    assertEquals(ExecutionStatus.DUE, s.status(), "FAILED → DUE");
    assertEquals(0, s.attempt(), "attempt 重置为 0");
    assertNull(s.nextRetryAt(), "next_retry_at 清空");
    assertFalse(s.deadLetter(), "dead_letter 复位");
    Integer requeue = jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='DUE' AND detail='requeue'",
        Integer.class, shard0);
    assertEquals(1, requeue, "落 DUE('requeue') outcome");
  }

  /** MED-2 回归:先前被协作取消标记(cancel_requested=true)、后以真实错误失败(FAILED)的片,
   *   requeue 必须清掉 cancel_requested,否则重排后立即又被判协作取消,死锁在 DUE↔CANCEL 循环。 */
  @Test void requeueClearsCancelRequested() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id();
    claimShard(shard0, taskId, "w1", 8); // RUNNING, attempt -> 1
    jdbc.update("UPDATE execution_shard SET status='FAILED', cancel_requested=true WHERE id=?", shard0);

    assertTrue(cancelRequested(shard0), "前置:片已被协作取消标记");
    assertTrue(shardRepo.requeueShard(shard0), "FAILED → requeue 成功");
    assertFalse(cancelRequested(shard0), "requeue 应清 cancel_requested 标记");
  }

  @Test void requeueShardOnNonFailed_false() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long shard0 = shard(parentId, 0).id();
    claimShard(shard0, taskId, "w1", 8); // RUNNING, not FAILED

    assertFalse(shardRepo.requeueShard(shard0), "非 FAILED → CAS 0 行 → false");
    assertEquals(ExecutionStatus.RUNNING, shardRepo.findShard(shard0).get().status());
  }

  // ---- v2 可靠性纵深 §1:findExpiredRunning 双信号 ----

  private void seedWorker(String id, int lastSeenSecondsAgo) {
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES (?, '',"
        + " now() - make_interval(secs => ?), 'ALIVE')", id, (double) lastSeenSecondsAgo);
  }

  /** 活性优先:owner worker 心跳失联(last_seen 距今>30s)但租约未过期 → 认定孤儿。 */
  @Test void findExpiredRunning_ownerHeartbeatStale_isReturned() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    claimShard(sid, taskId, "w-stale", 8);   // RUNNING, worker_id='w-stale'
    seedWorker("w-stale", 120);              // last_seen 120s 前 > stale(30)

    boolean hit = shardRepo.findExpiredRunning(taskId, 30).stream().anyMatch(e -> e.id() == sid);
    assertTrue(hit, "owner 心跳失联 → 该行列入孤儿(活性优先)");
  }

  /** 活性负例:owner 心跳新鲜(距今<30s)且租约未过期 → 不回收。 */
  @Test void findExpiredRunning_ownerHeartbeatFresh_isSkipped() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    claimShard(sid, taskId, "w-fresh", 8);   // RUNNING, worker_id='w-fresh'
    seedWorker("w-fresh", 5);                // last_seen 5s 前 < stale(30)

    assertTrue(shardRepo.findExpiredRunning(taskId, 30).isEmpty(), "owner 心跳新鲜 → 不回收");
  }

  /** 兜底路径保留:租约已过期即使 owner 心跳新鲜 → 仍回收(lease 兜底)。 */
  @Test void findExpiredRunning_leaseExpiredButOwnerFresh_stillReturned() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    claimShard(sid, taskId, "w-keep", 8);    // RUNNING, worker_id='w-keep'
    seedWorker("w-keep", 5);
    jdbc.update("UPDATE execution_shard SET lease_until = now() - interval '1 hour' WHERE id=?", sid);

    boolean hit = shardRepo.findExpiredRunning(taskId, 30).stream().anyMatch(e -> e.id() == sid);
    assertTrue(hit, "租约过期即使 owner 新鲜 → lease 兜底仍回收");
  }

  /** 兜底:worker_id IS NULL 的 RUNNING(异常数据/测试播种)只走租约;租约新鲜 → 不回收。 */
  @Test void findExpiredRunning_withoutOwnerId_onlyLeaseSignalApplies() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=NULL,"
        + " lease_until=now()+interval '60 seconds' WHERE id=?", sid);

    assertTrue(shardRepo.findExpiredRunning(taskId, 30).isEmpty(), "worker_id 空 + 租约新鲜 → 不回收");
  }

  // ---- Task 5:分布式执行超时 findOverRuntime ----

  @Test void findOverRuntime_returnsOnlyClaimedLongRunningShards() {
    long taskId = newTask(4, 8);
    long parentId = seedParentAndShards(taskId, 3);
    long overId = shard(parentId, 0).id();
    long freshId = shard(parentId, 1).id();
    long unclaimedId = shard(parentId, 2).id();
    // 超时:已认领(started_at 拨旧 10 分钟)
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w1',"
        + " started_at=now() - interval '10 minutes' WHERE id=?", overId);
    // 未超时:已认领但 started_at 新鲜
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w1',"
        + " started_at=now() WHERE id=?", freshId);
    // 未认领(worker_id null)的异常 RUNNING 行不参与超时
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=NULL,"
        + " started_at=now() - interval '10 minutes' WHERE id=?", unclaimedId);

    var expired = shardRepo.findOverRuntime(taskId, 60);

    assertEquals(1, expired.size(), "仅已认领且超时的分片进入超时判定");
    assertEquals(overId, expired.get(0).id());
  }
}
