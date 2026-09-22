package dev.scheduler.server.trigger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.IdempotencyKeys;
import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.JdbcExecutionRepository;
import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.leader.AdvisoryLockLeaderElection;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 用真 PG + 固定时钟,确定性校验 TriggerEngine 的幂等/pause/配额/leader 守卫。 */
class TriggerEngineTest extends AbstractTriggerEngineTest {

  private static final String CRON_EVERY_5 = "0 */5 * * * *";
  private static final Instant FIRED = Instant.parse("2026-01-01T10:05:00Z");

  private TaskRepository tasks;
  private ExecutionRepository executions;
  private ShardRepository shards;

  @BeforeEach void clearTables() {
    jdbc.execute("TRUNCATE execution, execution_shard, execution_shard_outcome, execution_outcome, app_task RESTART IDENTITY CASCADE");
    tasks = new JdbcTaskRepository(jdbc);
    executions = new JdbcExecutionRepository(jdbc);
    shards = new JdbcShardRepository(jdbc);
  }

  private long createTask(String cron, int shardCount, int maxActiveConcurrent) {
    return tasks.create(new Task(null, "t", "cron", "demo", cron,
        shardCount, 300, 0, 1000, null, maxActiveConcurrent, true, false)).id();
  }

  private long countExecutions(long taskId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution WHERE task_id=?", Long.class, taskId);
  }

  @Test void cronTickCreatesOneParentAndNDueShards() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long taskId = createTask(CRON_EVERY_5, /* shardCount */ 3, 1);
      var engine = new TriggerEngine(tasks, executions, shards, leader, FIXED_CLOCK, 200);
      engine.scanOnce();

      // 恰 1 父 execution(不是 N 条平铺 execution)
      assertEquals(1, countExecutions(taskId));
      long parentId = executions.findCandidate(taskId).orElseThrow().id();
      assertEquals(IdempotencyKeys.forTrigger(taskId, FIRED),
          shards.findParent(parentId).orElseThrow().idempotencyKey());
      // t.shardCount() 个 DUE shard
      var shardList = shards.findShards(parentId);
      assertEquals(3, shardList.size());
      assertTrue(shardList.stream().allMatch(s -> s.status() == ExecutionStatus.DUE));
    } finally { leader.close(); }
  }

  @Test void rescanIsIdempotent_noDuplicateParentOrShard() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long taskId = createTask(CRON_EVERY_5, /* shardCount */ 3, 1);
      var engine = new TriggerEngine(tasks, executions, shards, leader, FIXED_CLOCK, 200);
      engine.scanOnce();
      long parentId = executions.findCandidate(taskId).orElseThrow().id();
      int shardCountAfterFirst = shards.findShards(parentId).size();

      engine.scanOnce(); // 同一固定时钟再扫一次

      assertEquals(1, countExecutions(taskId)); // 父不再新增
      assertEquals(parentId, executions.findCandidate(taskId).orElseThrow().id()); // 仍是同一父
      assertEquals(shardCountAfterFirst, shards.findShards(parentId).size()); // shard 不增
    } finally { leader.close(); }
  }

  @Test void pausedTask_isNotFired() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long taskId = createTask(CRON_EVERY_5, 1, 1);
      var engine = new TriggerEngine(tasks, executions, shards, leader, FIXED_CLOCK, 200);
      engine.scanOnce();
      long before = countExecutions(taskId);
      assertTrue(before >= 1);

      tasks.setPaused(taskId, true); // 离开 findCronEnabledPage 结果集
      engine.scanOnce();
      assertEquals(before, countExecutions(taskId)); // pause 守卫:不再新增
    } finally { leader.close(); }
  }

  @Test void saturatedTask_stillBacklogsDue_butUnclaimable() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long taskId = createTask(CRON_EVERY_5, /* shardCount */ 1, /* maxActiveConcurrent */ 1);
      // 预先放入一条 RUNNING(用与本次 tick 不同的 key,使仅凭幂等键不足以屏蔽新建)
      jdbc.update("""
        INSERT INTO execution (task_id, status, idempotency_key, shard_count, lease_until)
        VALUES (?, 'RUNNING', ?, 1, now() + interval '60 seconds')""",
        taskId, "quota-occupying-key");
      assertEquals(1L, executions.countActive(taskId));

      var engine = new TriggerEngine(tasks, executions, shards, leader, FIXED_CLOCK, 200);
      engine.scanOnce();
      // §2.4 背压:配额占满也仍下发一条 DUE(积压),绝不静默丢 tick
      assertEquals(2, countExecutions(taskId));
      Execution backlog = executions.findCandidate(taskId).orElseThrow();
      assertEquals("DUE", backlog.status().name());
      // 该积压 DUE 因配额已满,claim 侧拒绝提升 → 仍是 DUE
      assertFalse(executions.claim(backlog.id(), taskId, "w", Instant.now().plusSeconds(60), 1));
      assertEquals("DUE", executions.findById(backlog.id()).get().status().name());
    } finally { leader.close(); }
  }

  @Test void nonLeaderCreatesNothing() {
    var holder = new AdvisoryLockLeaderElection(jdbc); // 占用唯一 session advisory lock
    try {
      assertTrue(holder.isLeader());
      var nonLeader = new AdvisoryLockLeaderElection(jdbc); // 当非 leader
      try {
        assertFalse(nonLeader.isLeader());
        long taskId = createTask(CRON_EVERY_5, 1, 1);
        var engine = new TriggerEngine(tasks, executions, shards, nonLeader, FIXED_CLOCK, 200);
        engine.scanOnce();
        assertEquals(0, countExecutions(taskId)); // leader 守卫:非 leader 不产生任何执行
      } finally { nonLeader.close(); }
    } finally { holder.close(); }
  }

  /** 3a interval 触发:任务 cron=null、intervalSeconds=60。FIXED now=10:05:00Z 恰为 60s 边界的整数秒,
   *  首次 scan 在边界即下发一条 DUE;再扫同一边界幂等不新增(键 = 边界 instant)。 */
  @Test void intervalTask_firesAtAlignedBoundary_idempotentOnRescan() {
    long interval = 60;
    long boundary = Math.floorDiv(FIXED_CLOCK.instant().getEpochSecond(), interval) * interval;
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long taskId = tasks.create(new Task(null, "t", "interval", "demo", null,
          1, 300, 0, 1000, null, 8, true, false, null, null, null, "UTC", (int) interval)).id();
      var engine = new TriggerEngine(tasks, executions, shards, leader, FIXED_CLOCK, 200);
      engine.scanOnce();
      assertEquals(1, countExecutions(taskId), "边界命中间隔 → 恰 1 父 execution");
      long parentId = executions.findCandidate(taskId).orElseThrow().id();
      assertEquals(IdempotencyKeys.forTrigger(taskId, Instant.ofEpochSecond(boundary)),
          shards.findParent(parentId).orElseThrow().idempotencyKey(), "幂等键 = 对齐边界 instant");
      assertEquals(1, shards.findShards(parentId).size(), "单 shard 任务 → 恰 1 条 DUE shard");

      engine.scanOnce(); // 同一边界再扫 → 幂等不新增
      assertEquals(1, countExecutions(taskId));
      assertEquals(parentId, executions.findCandidate(taskId).orElseThrow().id());
    } finally { leader.close(); }
  }

  /** 3a 时区感知 cron:同一 cron「0 5 10 * * *」(每日 10:05)在 UTC 解释恰落在本分钟窗(现 now=10:05:00Z)→ 触发;
   *  在 Asia/Shanghai(UTC+8)解释则本地 18:05 是本日最后一个 10:05 之后的下一天,落在 61s 窗外 → 不触发。
   *  证明时区改变 cron 匹配窗口(同一绝对时刻,不同本地日)。FIXED now=2026-01-01T10:05:00Z。 */
  @Test void cronTask_timezoneShiftsMatchingWindow() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long utc = tasks.create(new Task(null, "tz-utc", "cron", "demo", "0 5 10 * * *",
          1, 300, 0, 1000, null, 8, true, false, null, null, null, "UTC", null)).id();
      long sh = tasks.create(new Task(null, "tz-sh", "cron", "demo", "0 5 10 * * *",
          1, 300, 0, 1000, null, 8, true, false, null, null, null, "Asia/Shanghai", null)).id();
      var engine = new TriggerEngine(tasks, executions, shards, leader, FIXED_CLOCK, 200);
      engine.scanOnce();

      assertEquals(1, countExecutions(utc), "UTC 解释:10:05 tick 落在本分钟窗 → 触发");
      assertEquals(0, countExecutions(sh), "Asia/Shanghai 解释:本地 18:05 距下个 10:05 一整天 → 窗外不触发");
      long parentId = executions.findCandidate(utc).orElseThrow().id();
      assertEquals(IdempotencyKeys.forTrigger(utc, Instant.parse("2026-01-01T10:05:00Z")),
          shards.findParent(parentId).orElseThrow().idempotencyKey(), "fired 用绝对 instant,不随时区漂移");
    } finally { leader.close(); }
  }

  /** §4 游标分批:两个候选任务(minute 内各命中一次)在 batch=1 时跨 tick 覆盖,不重不漏。 */
  @Test void scanOnce_batchesAcrossTicks_coversAllTasksOnce() throws Exception {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long tA = createTask(CRON_EVERY_5, 1, 1);
      long tB = createTask(CRON_EVERY_5, 1, 1);
      var engine = new TriggerEngine(tasks, executions, shards, leader, FIXED_CLOCK, 1);
      engine.scanOnce(); // tick 1:第一页(仅最小 id 任务)
      assertEquals(1, countExecutions(tA) + countExecutions(tB),
          "batch=1 时首 tick 只处理一个任务 → 恰 1 条 execution");
      engine.scanOnce(); // tick 2:第二页(游标推进)
      assertEquals(2, countExecutions(tA) + countExecutions(tB),
          "batch=1 时两 tick 应覆盖两任务且不重复");
      assertEquals(1, countExecutions(tA), "任务 A 恰好一条(无重复 DUE)");
      assertEquals(1, countExecutions(tB), "任务 B 恰好一条(无重复 DUE)");
    } finally { leader.close(); }
  }
}
