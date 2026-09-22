package dev.scheduler.server.trigger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.IdempotencyKeys;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.EventRepository;
import dev.scheduler.persistence.InboundEvent;
import dev.scheduler.persistence.JdbcEventRepository;
import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.leader.AdvisoryLockLeaderElection;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 用真 PG 确定性校验 3b 事件触发的路由订阅、幂等、无订阅者消费与 leader 守卫。 */
class EventEngineTest extends AbstractTriggerEngineTest {

  private TaskRepository tasks;
  private ShardRepository shards;
  private EventRepository events;

  @BeforeEach void clearTables() {
    jdbc.execute("TRUNCATE app_task_event, execution, execution_shard, execution_shard_outcome,"
        + " execution_outcome, app_task RESTART IDENTITY CASCADE");
    tasks = new JdbcTaskRepository(jdbc);
    shards = new JdbcShardRepository(jdbc);
    events = new JdbcEventRepository(jdbc);
  }

  private long createEventTask(String... routes) {
    return tasks.create(new Task(null, "evt", "event", "demo", null,
        1, 300, 0, 1000, null, 8, true, false, null, null, null, "UTC", null, List.of(routes), "NONE", null)).id();
  }

  private long countExecutions(long taskId) {
    return jdbc.queryForObject("SELECT count(*) FROM execution WHERE task_id=?", Long.class, taskId);
  }

  /** 订阅路由的事件 → 恰一条父 execution;事件行回填并置 DISPATCHED。 */
  @Test void dispatch_createsOneRunForSubscribedRoute() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long taskId = createEventTask("order.created");
      long evtId = events.enqueue("order.created", "{\"oid\":7}", "evt-1");
      new EventEngine(events, tasks, shards, leader, 100).scanOnce();

      assertEquals(1, countExecutions(taskId), "一条事件 → 一次 run");
      long parentId = jdbc.queryForObject(
          "SELECT execution_id FROM app_task_event WHERE id=?", Long.class, evtId);
      assertEquals(IdempotencyKeys.forEvent("evt-1"), shards.findParent(parentId).orElseThrow().idempotencyKey(),
          "父 execution 幂等键 = event:{dedupeKey}");
      InboundEvent after = events.findById(evtId).orElseThrow();
      assertEquals(InboundEvent.STATUS_DISPATCHED, after.status());
      assertEquals(taskId, after.taskId(), "回填所触发的任务");
    } finally { leader.close(); }
  }

  /** 重放同一事件(dedupe 键)再扫 → 父不重复建轮(upsert 幂等),行仍 DISPATCHED。 */
  @Test void dispatch_isIdempotentOnReplay() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long taskId = createEventTask("order.created");
      long evtId = events.enqueue("order.created", "{}", "evt-dup");
      new EventEngine(events, tasks, shards, leader, 100).scanOnce();

      new EventEngine(events, tasks, shards, leader, 100).scanOnce(); // 再扫已 DISPATCHED 行
      assertEquals(1, countExecutions(taskId), "重扫不因 DISPATCHED 行再触;至多一轮");
    } finally { leader.close(); }
  }

  /** 事件无订阅路由 → 仍被消费(标记 DISPATCHED、无回填),不触发任何轮、不残留 PENDING。 */
  @Test void dispatch_consumesEventWithoutSubscriber() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long evtId = events.enqueue("no.subscriber", "{}", "evt-orphan");
      new EventEngine(events, tasks, shards, leader, 100).scanOnce();

      InboundEvent e = events.findById(evtId).orElseThrow();
      assertEquals(InboundEvent.STATUS_DISPATCHED, e.status(), "无订阅者事件仍被消费,避免永驻 PENDING");
      assertTrue(e.taskId() == null, "无回填任务");
      assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM execution", Long.class),
          "不触发任何轮");
      assertTrue(events.pending(10).isEmpty(), "无残留 PENDING");
    } finally { leader.close(); }
  }

  /** 多任务订阅同一路由 → 只触发 id 最小者(一条事件 → 一次 run)。 */
  @Test void dispatch_onlySubscribed() {
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      long subscribed = createEventTask("order.created");
      createEventTask("other.route"); // 未订阅该路由
      events.enqueue("order.created", "{}", "evt-1");
      new EventEngine(events, tasks, shards, leader, 100).scanOnce();

      assertEquals(1, countExecutions(subscribed), "订阅路由的任务被执行");
      assertEquals(0L, jdbc.queryForObject(
          "SELECT count(*) FROM execution WHERE task_id <> ?", Long.class, subscribed), "非订阅任务不被触发");
    } finally { leader.close(); }
  }

  /** 非 leader 不消费任何事件、不触发轮。 */
  @Test void nonLeader_createsNothing() {
    var holder = new AdvisoryLockLeaderElection(jdbc);
    try {
      assertTrue(holder.isLeader());
      long taskId = createEventTask("order.created");
      events.enqueue("order.created", "{}", "evt-nl");
      var nonLeader = new AdvisoryLockLeaderElection(jdbc);
      try {
        assertFalse(nonLeader.isLeader());
        new EventEngine(events, tasks, shards, nonLeader, 100).scanOnce();
        assertEquals(0, countExecutions(taskId), "leader 守卫:非 leader 不触发执行");
        assertTrue(events.pending(10).size() == 1, "事件仍 PENDING 待 leader 处理");
      } finally { nonLeader.close(); }
    } finally { holder.close(); }
  }
}