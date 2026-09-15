package dev.scheduler.worker.execute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.JdbcWorkerRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.reconcile.Reconciler;
import dev.scheduler.worker.handler.ExecutionHandler;
import dev.scheduler.worker.handler.HandlerContext;
import dev.scheduler.worker.handler.HandlerRegistry;
import dev.scheduler.worker.handler.MapHandlerRegistry;
import dev.scheduler.worker.registration.WorkerRegistrar;
import dev.scheduler.persistence.retry.FailureResolver;
import dev.scheduler.persistence.retry.RetryPolicy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 用真 PG,确定性校验 ExecutorWorker 的 shard 认领-执行-回写环路与所有权规则(M3)。 */
class ExecutorWorkerTest extends AbstractExecutorWorkerTest {

  private TaskRepository tasks;
  private ShardRepository shards;

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
    jdbc.execute("TRUNCATE app_task, execution, execution_shard, execution_shard_outcome "
        + "RESTART IDENTITY CASCADE");
    tasks = new JdbcTaskRepository(jdbc);
    shards = new JdbcShardRepository(jdbc);
  }

  /** maxRetries=0、backoff=1000ms、pattern=null(即不再重试)。 */
  private long createTask(String handlerRef, int shardCount, int maxActiveConcurrent) {
    return createTask(handlerRef, 0, 1000, null, shardCount, maxActiveConcurrent);
  }

  private long createTask(String handlerRef, int maxRetries, long backoffMs, String pattern,
                          int shardCount, int maxActiveConcurrent) {
    return tasks.create(new Task(null, "t", "cron", handlerRef, "0 */5 * * * *",
        shardCount, 300, maxRetries, backoffMs, pattern, maxActiveConcurrent, true, false)).id();
  }

  /** 建父 execution + N 个 DUE shard(单事务),返回父 id。 */
  private long seedParentAndShards(long taskId, int shardCount) {
    return shards.createParentWithShards(taskId, "p:" + System.nanoTime(), shardCount).id();
  }

  private Shard shard(long executionId, int idx) {
    return shards.findShards(executionId).get(idx);
  }

  private long shardOutcomeCount(long shardId, String status) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status=?",
        Long.class, shardId, status);
  }

  private ExecutorWorker worker(HandlerRegistry registry) { return worker(registry, "worker-a"); }

  private ExecutorWorker worker(HandlerRegistry registry, String workerId) {
    return new ExecutorWorker(tasks, shards, registry, workerId,
        new FailureResolver(shards, new RetryPolicy(), CLOCK), CLOCK, 120, 30);
  }

  @Test void happyPath_claimsShard_runsWithShardContext_writesSuccess() {
    var rec = new RecordingHandler(false);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 1, 1);
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    assertEquals("rec", registry.get("rec").ref());
    assertEquals(1, rec.calls.size(), "handler 真实被调用");
    HandlerContext ctx = rec.calls.get(0);
    assertEquals(parentId, ctx.executionId(), "HandlerContext.executionId 应为父 id");
    assertEquals(shardId, ctx.shardId());
    assertEquals(0, ctx.shardIndex(), "单 shard 任务收 shardIndex=0");
    assertEquals(1, ctx.shardCount(), "单 shard 任务收 shardCount=1");

    Shard s = shards.findShard(shardId).orElseThrow();
    assertEquals(ExecutionStatus.SUCCESS, s.status(), "shard 回写成 SUCCESS");
    assertEquals("worker-a", s.workerId());
    assertEquals(1L, shardOutcomeCount(shardId, "SUCCESS"), "shard 落 SUCCESS outcome");
    // worker 绝不写父 execution 行:父仍 DUE,由 reconcile(finalizeParent)汇聚为 SUCCESS。
    assertEquals(ExecutionStatus.DUE, shards.findParent(parentId).orElseThrow().status(),
        "worker 不得写父 execution 行");
    assertTrue(shards.finalizeParent(parentId, ExecutionStatus.SUCCESS, "all done"), "模拟 reconcile");
    assertEquals(ExecutionStatus.SUCCESS, shards.findParent(parentId).orElseThrow().status(),
        "父经汇聚终 SUCCESS");
  }

  @Test void handlerResultPayload_writtenToShard_onSuccess() {
    long taskId = createTask("rec", 3, 8);
    long exec = seedParentAndShards(taskId, 3);
    var payloadHandler = new ExecutionHandler() {
      @Override public String ref() { return "rec"; }
      @Override public void handle(HandlerContext ctx) { }
      @Override public String resultPayload(HandlerContext ctx) {
        return "{\"workerId\":\"" + ctx.workerId() + "\",\"shardIndex\":" + ctx.shardIndex() + "}";
      }
    };
    var registry = new MapHandlerRegistry(List.of(() -> payloadHandler));
    assertTrue(worker(registry).workOne());
    var s = shard(exec, 0);
    assertNotNull(s.resultPayload(), "SUCCESS 后 result_payload 应被写回");
    assertTrue(s.resultPayload().contains("\"workerId\":\"worker-a\""),
        "payload 应含调用方 workerId(经 HandlerContext.workerId)");
    assertTrue(s.resultPayload().contains("\"shardIndex\":0"),
        "payload 应含分片下标");
    assertTrue(shards.findShards(exec).stream()
        .noneMatch(x -> x.shardIndex() != 0 && x.resultPayload() != null),
        "仅被认领执行的 shard 写 payload,其余保持空");
  }

  @Test void missingHandler_failsShard_andWritesOutcome() {
    var rec = new RecordingHandler(false);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("nope", 1, 1); // handlerRef 未注册
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    assertEquals(ExecutionStatus.FAILED, shards.findShard(shardId).orElseThrow().status(),
        "RUNNING→FAILED 合法回写");
    assertEquals(1L, shardOutcomeCount(shardId, "FAILED"));
    assertEquals(0, rec.calls.size(), "未命中任何 handler");
  }

  @Test void handlerThrows_failsShard_withoutEscapingThrowable() {
    var rec = new RecordingHandler(true);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 1, 1);
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    assertEquals(ExecutionStatus.FAILED, shards.findShard(shardId).orElseThrow().status());
    assertEquals(1, rec.calls.size());
    assertEquals(1L, shardOutcomeCount(shardId, "FAILED"));
  }

  @Test void handlerThrowsRetryable_schedulesShardRetry() {
    var rec = new RecordingHandler(true);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 2, 1000, null, 1, 1); // maxRetries=2, 无 pattern → 可重试
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Shard s = shards.findShard(shardId).orElseThrow();
    assertEquals(ExecutionStatus.DUE, s.status(), "可重试 → 回到 DUE 待再次认领");
    assertNotNull(s.nextRetryAt(), "重试必须写入 next_retry_at");
    assertEquals(CLOCK.instant().plusMillis(1000), s.nextRetryAt(),
        "next_retry_at 由注入时钟 + 退避(attempt1 backoff=1000ms)算出,而非实时时钟");
    assertEquals(1L, shardOutcomeCount(shardId, "DUE"), "重试落 DUE outcome");
    assertEquals(1, s.attempt(), "claim 已累加到 1,重试不再改增 attempt");
  }

  @Test void handlerThrowsExhausted_failsAndDeadLetters_shard_noDueOutcome() {
    var rec = new RecordingHandler(true);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 0, 1000, null, 1, 1); // maxRetries=0 → 耗尽
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Shard s = shards.findShard(shardId).orElseThrow();
    assertEquals(ExecutionStatus.FAILED, s.status());
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=?", Boolean.class, shardId),
        "重试耗尽 → 置 dead_letter=true");
    assertEquals(0L, shardOutcomeCount(shardId, "DUE"), "耗尽:无 DUE outcome");
  }

  @Test void handlerThrowsPatternMismatch_failsShard_noDueOutcome() {
    var rec = new RecordingHandler(true);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 2, 1000, "timeout", 1, 1); // pattern 不匹配抛出的 "boom"
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Shard s = shards.findShard(shardId).orElseThrow();
    assertEquals(ExecutionStatus.FAILED, s.status());
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=?", Boolean.class, shardId),
        "pattern 不匹配 → 视为不可重试,置 dead_letter=true");
    assertEquals(0L, shardOutcomeCount(shardId, "DUE"));
  }

  /** M6.3 relocate:3-shard、首个被认领的 shard 重试耗尽(FAILED+死信)时,FailureResolver.cancelSiblings
   *  把其余 DUE 兄弟直编 CANCELED —— 原 server ApiIntegrationTest.failFast_endToEnd_... 的执行语义搬至此。 */
  @Test void exhaustedShard_cancelSiblingDues() {
    var rec = new RecordingHandler(true);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 0, 1000, null, 3, 3); // shardCount=3, maxRetries=0 → 耗尽
    long parentId = seedParentAndShards(taskId, 3);

    boolean processed = worker(registry).workOne(); // 认领 id 最小的一枚 → 耗尽 FAILED + DLQ → FAIL_FAST

    assertTrue(processed);
    var ss = shards.findShards(parentId);
    assertEquals(1, ss.stream().filter(s -> s.status() == ExecutionStatus.FAILED).count(), "恰好一条 FAILED");
    assertEquals(2, ss.stream().filter(s -> s.status() == ExecutionStatus.CANCELED).count(),
        "FAIL_FAST:其余 DUE 兄弟被直编 CANCELED");
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=? AND status='FAILED'",
        Boolean.class, ss.get(0).id()), "耗尽 shard 置死信");
  }

  /** M6.3 relocate:重试调度后 next_retry_at 落未来闸,findCandidate 不得再认领;拨回过去闸才放行。
   *  原 server ApiIntegrationTest.retryAfterFailure_thenBackoffGate_thenRerunReachesSuccess 的闸门语义。
   *  用一次性失败 handler(镜像 server 的 FlakyHandler:首调抛错、次调成功)——RecordingHandler(fail=true)
   *  恒抛,放行后再调度会耗尽而非成功,故不用它。 */
  @Test void futureBackoffGate_blocksReclaim_thenPastGate_reruns() {
    final int[] calls = {0};
    var oneShot = new ExecutionHandler() {
      boolean failNext = true;
      @Override public String ref() { return "rec"; }
      @Override public void handle(HandlerContext ctx) {
        calls[0]++;
        if (failNext) { failNext = false; throw new RuntimeException("gate fail"); } // 仅首调抛错
      }
    };
    var registry = new MapHandlerRegistry(List.of(() -> oneShot));
    long taskId = createTask("rec", 1, 1000, null, 1, 1); // maxRetries=1 → 可重试一次
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();

    assertTrue(worker(registry).workOne(), "首调认领并重试调度(落到 next_retry_at)");
    Shard s = shards.findShard(shardId).orElseThrow();
    assertEquals(ExecutionStatus.DUE, s.status(), "可重试 → 回 DUE");
    assertNotNull(s.nextRetryAt(), "重试必须写 next_retry_at");
    assertTrue(s.nextRetryAt().isAfter(CLOCK.instant()), "next_retry_at 在注入时钟未来");

    jdbc.update("UPDATE execution_shard SET next_retry_at = now() + interval '1 hour' WHERE id=?",
        shardId); // 拨到更远的未来(闸以 DB now() 判读 → 排除候选)
    assertFalse(worker(registry).workOne(), "未来闸 → 候选被排除,不认领");
    assertEquals(ExecutionStatus.DUE, shards.findShard(shardId).orElseThrow().status(), "仍 DUE,未被重认领");

    jdbc.update("UPDATE execution_shard SET next_retry_at = now() - interval '1 second' WHERE id=?",
        shardId); // 调回过去闸
    assertTrue(worker(registry).workOne(), "过去闸 → 放行,次调成功");
    assertEquals(ExecutionStatus.SUCCESS, shards.findShard(shardId).orElseThrow().status(),
        "calls=2:handler 次调成功 → SUCCESS");
    assertEquals(2, calls[0], "handler 恰被调度两次(首败重试 + 放行后成功)");
  }

  @Test void handlerReceivesShardIndexAndCount() {
    var rec = new RecordingHandler(false);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 2, 2); // shardCount=2, maxActive=2
    long parentId = seedParentAndShards(taskId, 2);

    worker(registry).workOne(); // 认领 shard index 0
    worker(registry).workOne(); // 认领 shard index 1

    assertEquals(2, rec.calls.size(), "两次 workOne 各驱动一次 handler");
    assertEquals(0, rec.calls.get(0).shardIndex(), "第一次运行收 index 0");
    assertEquals(1, rec.calls.get(1).shardIndex(), "第二次运行收 index 1");
    assertEquals(2, rec.calls.get(0).shardCount(), "两 shard 任务皆收 shardCount=2");
    assertEquals(2, rec.calls.get(1).shardCount());
    assertEquals(parentId, rec.calls.get(0).executionId(), "两个 shard 同属同一父 execution");
    assertEquals(parentId, rec.calls.get(1).executionId());
  }

  @Test void staleOwnerShard_cannotClobberReownedShard_guardSuppressesWritebackAndRetry() {
    // shard 运行期间模拟真实竞态:reconciler 回收过期租约(FAILED + 重试→DUE),随后 worker-b 重新
    // 认领该分片(→ RUNNING owned by worker-b)。worker-a 的迟回收写 markStatusOwned(FAILED) 必须被
    // ownership 守卫拦截:不覆盖 worker-b 的 RUNNING,也不调度任何重试(零 DUE outcome)。
    long taskId = createTask("reclaim", 2, 1000, null, 1, 1); // maxRetries=2:若无守卫,迟回收写会落 DUE
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();
    Instant lease = Instant.now().plusSeconds(60);
    var reclaiming = new ExecutionHandler() {
      @Override public String ref() { return "reclaim"; }
      @Override public void handle(HandlerContext ctx) {
        // 模拟 reconciler 回收:落 FAILED 后经重试决策回 DUE(此处直接 raw 复位,不落重试 outcome,从而让
        // "零 DUE outcome" 断言只反映 worker-a 的迟回收写是否被抑制),再被 worker-b 重新认领。
        shards.markStatus(shardId, ExecutionStatus.FAILED, "reconciler", "lease expired");
        jdbc.update("UPDATE execution_shard SET status='DUE', next_retry_at=NULL WHERE id=?", shardId);
        shards.claim(shardId, taskId, "worker-b", lease, 1);
        throw new RuntimeException("boom");
      }
    };
    var registry = new MapHandlerRegistry(List.of(() -> reclaiming));

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    Shard s = shards.findShard(shardId).orElseThrow();
    assertEquals(ExecutionStatus.RUNNING, s.status(), "stale worker 的回写不得覆盖 worker-b 的 RUNNING");
    assertEquals("worker-b", s.workerId(), "分片仍归重新认领的 worker-b 所有,未被 worker-a 改动");
    assertEquals(0L, shardOutcomeCount(shardId, "DUE"), "被守卫拦截的回写 → 不调度重试,零 DUE outcome");
  }

  @Test void unclaimedShard_neverMutatedByThisWorker() {
    var rec = new RecordingHandler(false);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 2, 1); // shardCount=2, maxActiveConcurrent=1
    long parentId = seedParentAndShards(taskId, 2);
    long shardA = shard(parentId, 0).id();
    long shardB = shard(parentId, 1).id();

    // 另一 worker 已认领 shardA 并 RUNNING(占满配额),有效租约
    assertTrue(shards.claim(shardA, taskId, "other-worker", Instant.now().plusSeconds(60), 1));

    boolean processed = worker(registry).workOne();

    assertFalse(processed, "配额占满 → 未认领到任何分片,本次不处理");
    assertEquals(ExecutionStatus.RUNNING, shards.findShard(shardA).orElseThrow().status(),
        "他人拥有的分片未被本 worker 改动");
    assertEquals("other-worker", shards.findShard(shardA).orElseThrow().workerId());
    assertEquals(ExecutionStatus.DUE, shards.findShard(shardB).orElseThrow().status(),
        "未认领 → 仍 DUE,绝不回写");
  }

  @Test void cancelRequestedBeforeRun_marksShardCanceled_handlerNeverCalled() {
    var rec = new RecordingHandler(false);
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("rec", 1, 1);
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();
    // 预置 DUE 且 cancel_requested=true:worker 认领后运行前检查即取消,不调度 handler。
    jdbc.update("UPDATE execution_shard SET cancel_requested=true WHERE id=?", shardId);

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    assertEquals(ExecutionStatus.CANCELED, shards.findShard(shardId).orElseThrow().status(),
        "运行前已请求取消 → CANCELED");
    assertEquals(0, rec.calls.size(), "已取消的候选不应被调度 handler");
    assertEquals(1L, shardOutcomeCount(shardId, "CANCELED"));
  }

  @Test void handlerCancellation_marksShardCanceled_noRetry() {
    var rec = new CancellingHandler();
    var registry = new MapHandlerRegistry(List.of(() -> rec));
    long taskId = createTask("cancel", 2, 1000, null, 1, 1); // 即使任务可重试,取消也不走重试
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    assertEquals(ExecutionStatus.CANCELED, shards.findShard(shardId).orElseThrow().status(),
        "取消信号映射 CANCELED 而非 FAILED");
    assertEquals(1, rec.calls.size());
    assertEquals(0L, shardOutcomeCount(shardId, "DUE"), "取消不调度重试,无 DUE outcome");
    assertEquals(0L, shardOutcomeCount(shardId, "FAILED"), "取消不落 FAILED");
  }

  @Test void handlerRunsNormally_butCancelRequestedMidRun_marksShardCanceled() {
    long taskId = createTask("scripted", 1, 1);
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();
    var mid = new RecordingHandler(false);
    var scripted = new ExecutionHandler() {
      @Override public String ref() { return "scripted"; }
      @Override public void handle(HandlerContext ctx) {
        jdbc.update("UPDATE execution_shard SET cancel_requested=true WHERE id=?", shardId); // 运行期间置位
        mid.handle(ctx); // 然后正常返回
      }
    };
    var registry = new MapHandlerRegistry(List.of(() -> scripted));

    boolean processed = worker(registry).workOne();

    assertTrue(processed);
    assertEquals(ExecutionStatus.CANCELED, shards.findShard(shardId).orElseThrow().status(),
        "运行期间被请求取消 → 正常返回后复查仍落 CANCELED");
    assertEquals(1, mid.calls.size(), "handler 确已正常运行(非运行前取消)");
    assertEquals(1L, shardOutcomeCount(shardId, "CANCELED"));
    assertEquals(0L, shardOutcomeCount(shardId, "SUCCESS"), "不会同时落 SUCCESS");
  }

  @Test void noWork_emptyTables_returnsFalse() {
    assertFalse(worker(new MapHandlerRegistry(List.of(() -> new RecordingHandler(false)))).workOne());
  }

  @Test void twoWorkers_distributeShards_eachClaimsDisjointSubset() {
    long taskId = createTask("rec", 4, 8);
    long exec = seedParentAndShards(taskId, 4);
    var recA = new RecordingHandler(false);
    var recB = new RecordingHandler(false);
    var registryA = new MapHandlerRegistry(List.of(() -> recA));
    var registryB = new MapHandlerRegistry(List.of(() -> recB));
    // 交替驱动,直到无可认领(DUE 清空);DB 原子认领保证每片仅一个 worker 真正执行。
    for (int i = 0; i < 4; i++) { worker(registryA, "worker-a").workOne(); worker(registryB, "worker-b").workOne(); }
    List<Shard> all = shards.findShards(exec);
    assertEquals(Set.of(0, 1, 2, 3),
        all.stream().map(Shard::shardIndex).collect(Collectors.toSet()), "全部分片都被执行");
    assertTrue(all.stream().allMatch(s -> s.status() == ExecutionStatus.SUCCESS));
    var byWorker = all.stream().collect(Collectors.groupingBy(Shard::workerId,
        Collectors.mapping(Shard::shardIndex, Collectors.toSet())));
    assertEquals(2, byWorker.size(), "两个 worker 都分摊到分片");
    assertTrue(byWorker.containsKey("worker-a"));
    assertTrue(byWorker.containsKey("worker-b"));
    long claimed = byWorker.values().stream().mapToLong(Set::size).sum();
    assertEquals(4, claimed, "每个 shard 归属恰一个 worker,无重复认领 (disjoint + covering)");
    assertEquals(2, recA.calls.size(), "worker-a 跑了 2 片");
    assertEquals(2, recB.calls.size(), "worker-b 跑了 2 片");
  }

  /**
   * §活性回归:长运行 handler(阻塞超过 stale+reconcile 窗口)期间,只要 owner 的独立心跳仍在推进(last_seen
   * 新鲜),server Reconciler 的活性优先回收就绝不能误夺该健康 worker 的在途分片;handler 正常返回后落 SUCCESS,
   * 不 FAILED / 不重试。这正是 worker 心跳环从 Spring 默认单调度线程解耦到自持守护线程(WorkerConfig.HeartbeatLoop)
   * 所保障的回归。修复前心跳与 WorkLoop 同线程,handler 阻塞冻结 last_seen,本测试会 RED。
   */
  @Test void longRunningHandler_blockedBeyondReconcile_withFreshHeartbeat_isNotReclaimed() throws Exception {
    final long taskId = createTask("block", 1, 1);
    final long parentId = seedParentAndShards(taskId, 1);
    final long shardId = shard(parentId, 0).id();

    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var blockHandler = new ExecutionHandler() {
      @Override public String ref() { return "block"; }
      @Override public void handle(HandlerContext ctx) {
        entered.countDown();
        try { release.await(15, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      }
    };
    var registry = new MapHandlerRegistry(List.of(() -> blockHandler));
    ExecutorWorker w = worker(registry, "worker-a");

    // 线程 1:驱动 workOne()(认领 DUE 分片 → 运行长阻塞 handler)。
    Thread runner = new Thread(() -> w.workOne(), "test-longrunner");
    runner.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "handler 应已进入(分片已认领 RUNNING)");
    Shard underRun = shards.findShard(shardId).orElseThrow();
    assertEquals(ExecutionStatus.RUNNING, underRun.status(), "claim 后为 RUNNING");
    assertEquals("worker-a", underRun.workerId(), "归 worker-a 所有");

    // 租约拨到 DB 真实未来(worker claim 用固定时钟 CLOCK,早于 DB now(),故显式置未来租约),从而只留活性判别。
    jdbc.update("UPDATE execution_shard SET lease_until = now() + interval '300 seconds' WHERE id = ?", shardId);

    // 线程 2:模拟生产 HeartbeatLoop 的自持守护线程,在 handler 阻塞期间持续推进 owner 活性(last_seen=真实 now)。
    WorkerRegistrar registrar =
        new WorkerRegistrar(new JdbcWorkerRepository(jdbc), registry, "worker-a", Clock.systemUTC());
    // 先同步落一次心跳担保 worker 行 + 新鲜 last_seen 在 scanOnce 前已提交,消除首心跳与 `reconciler.stale=1`
    // 判活查询的写读竞争(否则扫描可能跑赢首心跳、误判失联而偶发失败,见 SDD ledger)。
    registrar.heartbeat();
    AtomicBoolean stop = new AtomicBoolean(false);
    Thread heartbeat = new Thread(() -> {
      while (!stop.get()) {
        registrar.heartbeat();
        try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      }
    }, "test-heartbeat");
    heartbeat.start();

    try {
      // 主线程(线程 3):在 handler 仍阻塞、owner 心跳新鲜时跑 server Reconciler,stale 窗口极小(1s)。
      Reconciler rc = new Reconciler(tasks, shards,
          new FailureResolver(shards, new RetryPolicy(), CLOCK), "reconciler", 1);
      int n = rc.scanOnce();

      assertEquals(0, n, "心跳新鲜的健康长运行分片不得被活性优先回收");
      assertEquals(ExecutionStatus.RUNNING, shards.findShard(shardId).orElseThrow().status(),
          "仍 RUNNING,未被夺/未回写 FAILED");
      assertEquals(0L, shardOutcomeCount(shardId, "FAILED"), "无 FAILED outcome");
      assertEquals(0L, shardOutcomeCount(shardId, "DUE"), "无重试 DUE outcome");
    } finally {
      stop.set(true);
      heartbeat.join(5000);
      release.countDown(); // 放行 handler
    }

    runner.join(10000);
    assertFalse(runner.isAlive(), "workOne 应已正常返回");
    assertEquals(ExecutionStatus.SUCCESS, shards.findShard(shardId).orElseThrow().status(),
        "长 handler 正常返回 → SUCCESS,未被活性误夺/重试");
    assertEquals(1L, shardOutcomeCount(shardId, "SUCCESS"), "恰好落一条 SUCCESS");
  }

  /** §活性正控:owner 心跳已停止(last_seen 陈旧)即使租约仍有效,活性优先回收也要立即接管(零永久 RUNNING)。 */
  @Test void deadWorker_heartbeatStale_reclaimedBeforeLeaseExpiry() throws Exception {
    long taskId = createTask("block", 1, 1);
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();
    String owner = "w-gone";
    // 真实 claim 语义:worker_id=owner、RUNNING、租约仍有效(+60s),但心跳已失联 > stale=1。
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=?,"
        + " lease_until=now()+interval '300 seconds', attempt=1 WHERE id=?", owner, shardId);
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status)"
        + " VALUES (?, '', now() - interval '2 minutes', 'ALIVE')", owner);

    Reconciler rc = new Reconciler(tasks, shards,
        new FailureResolver(shards, new RetryPolicy(), CLOCK), "reconciler", 1);
    int n = rc.scanOnce();

    assertEquals(1, n, "心跳失联 owner → 租约未到期前即被活性优先回收");
    Shard s = shards.findShard(shardId).orElseThrow();
    assertEquals(ExecutionStatus.FAILED, s.status(), "活性优先回收 → FAILED");
    assertEquals(1L, shardOutcomeCount(shardId, "FAILED"), "回收落 FAILED outcome");
  }
}
