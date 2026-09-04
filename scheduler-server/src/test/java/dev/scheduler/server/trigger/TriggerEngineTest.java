package dev.scheduler.server.trigger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.IdempotencyKeys;
import dev.scheduler.core.Execution;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.JdbcExecutionRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
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

  @BeforeEach void clearTables() {
    jdbc.execute("TRUNCATE execution, execution_outcome, app_task RESTART IDENTITY CASCADE");
    tasks = new JdbcTaskRepository(jdbc);
    executions = new JdbcExecutionRepository(jdbc);
  }

  private long createTask(String cron, int maxActiveConcurrent) {
    return tasks.create(new Task(null, "t", "cron", "demo", cron,
        1, 300, 0, 1000, null, maxActiveConcurrent, true, false)).id();
  }

  private long countExecutions(long taskId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution WHERE task_id=?", Long.class, taskId);
  }

  @Test void cronTickCreatesExactlyOneDueExecution() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long taskId = createTask(CRON_EVERY_5, 1);
      var engine = new TriggerEngine(tasks, executions, leader, FIXED_CLOCK);
      engine.scanOnce();

      assertEquals(1, countExecutions(taskId));
      Execution due = executions.findCandidate(taskId).orElseThrow();
      assertEquals("DUE", due.status().name());
      assertEquals(IdempotencyKeys.forTrigger(taskId, FIRED, 0), due.idempotencyKey());
    } finally { leader.close(); }
  }

  @Test void rescanIsIdempotent_noDuplicateDue() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long taskId = createTask(CRON_EVERY_5, 1);
      var engine = new TriggerEngine(tasks, executions, leader, FIXED_CLOCK);
      engine.scanOnce();
      engine.scanOnce(); // 同一固定时钟再扫一次
      assertEquals(1, countExecutions(taskId));
    } finally { leader.close(); }
  }

  @Test void pausedTask_isNotFired() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long taskId = createTask(CRON_EVERY_5, 1);
      var engine = new TriggerEngine(tasks, executions, leader, FIXED_CLOCK);
      engine.scanOnce();
      long before = countExecutions(taskId);
      assertTrue(before >= 1);

      tasks.setPaused(taskId, true); // 离开 findCronEnabled 结果集
      engine.scanOnce();
      assertEquals(before, countExecutions(taskId)); // pause 守卫:不再新增
    } finally { leader.close(); }
  }

  @Test void saturatedTask_stillBacklogsDue_butUnclaimable() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long taskId = createTask(CRON_EVERY_5, /* maxActiveConcurrent */ 1);
      // 预先放入一条 RUNNING(用与本次 tick 不同的 key,使仅凭幂等键不足以屏蔽新建)
      jdbc.update("""
        INSERT INTO execution (task_id, status, idempotency_key, shard_count, lease_until)
        VALUES (?, 'RUNNING', ?, 1, now() + interval '60 seconds')""",
        taskId, "quota-occupying-key");
      assertEquals(1L, executions.countActive(taskId));

      var engine = new TriggerEngine(tasks, executions, leader, FIXED_CLOCK);
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
        long taskId = createTask(CRON_EVERY_5, 1);
        var engine = new TriggerEngine(tasks, executions, nonLeader, FIXED_CLOCK);
        engine.scanOnce();
        assertEquals(0, countExecutions(taskId)); // leader 守卫:非 leader 不产生任何执行
      } finally { nonLeader.close(); }
    } finally { holder.close(); }
  }
}