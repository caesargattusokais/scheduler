package dev.scheduler.persistence;
import static org.junit.jupiter.api.Assertions.*;
import dev.scheduler.core.Task;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcTaskRepositoryTest extends AbstractPostgresTest {
  /** 不依赖其它测试类的 TRUNCATE 顺序:共享静态 PG 容器会累积 app_task 行,本类断言 findScheduleEnabledPage 数量需隔离。 */
  @BeforeEach void clean() {
    jdbc.update("TRUNCATE app_task, execution, execution_outcome RESTART IDENTITY CASCADE");
  }

  @Test void roundTrip() {
    var repo = new JdbcTaskRepository(jdbc);
    var created = repo.create(new Task(null, "t1", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    assertTrue(created.id() > 0);
    assertTrue(repo.findById(created.id()).isPresent());
    assertEquals(1, repo.findScheduleEnabledPage(0L, 10).size());
    repo.setPaused(created.id(), true);
    assertTrue(repo.findScheduleEnabledPage(0L, 10).isEmpty());
  }

  /** 3a:建任务可配时区(仅 cron 解释)与间隔触发(interval_seconds,cron 可空)。间隔任务 cron=null 仍被
   *  findScheduleEnabledPage 选中(3a 请求谓词放宽);timezone 落库回读;update 覆写两列。 */
  @Test void create_persistsTimezoneAndIntervalTrigger() {
    var repo = new JdbcTaskRepository(jdbc);
    Task created = repo.create(new Task(null, "interval-task", "interval", "demo", null,
        1, 300, 0, 1000, null, 8, true, false, null, null, null, "Asia/Shanghai", 60));
    Task cur = repo.findById(created.id()).orElseThrow();
    assertEquals("Asia/Shanghai", cur.timezone(), "timezone 落库回读");
    assertEquals(60, cur.intervalSeconds(), "interval_seconds 落库回读");
    assertNull(cur.cron());
    assertEquals(1, repo.findScheduleEnabledPage(0L, 10).size(),
        "cron=null 的间隔任务被调度扫描选中");
    // 时区化 cron 任务:缺省时区回读为 UTC
    Task cronTz = repo.create(new Task(null, "cron-tz", "cron", "demo", "0 */5 * * * *",
        1, 300, 0, 1000, null, 8, true, false, null, null, null, "America/New_York", null));
    assertEquals("America/New_York", repo.findById(cronTz.id()).orElseThrow().timezone());
    assertNull(repo.findById(cronTz.id()).orElseThrow().intervalSeconds());
    // update 覆写 timezone/interval
    Task updated = new Task(cronTz.id(), "cron-tz", "cron", "demo", null,
        1, 300, 0, 1000, null, 8, true, false, null, null, null, "UTC", 120);
    assertTrue(repo.update(updated.id(), updated));
    Task after = repo.findById(cronTz.id()).orElseThrow();
    assertEquals("UTC", after.timezone());
    assertEquals(120, after.intervalSeconds());
    assertNull(after.cron());
  }

  @Test void findScheduleEnabledPage_resumesPastCursor_withoutSkipping() {
    var repo = new JdbcTaskRepository(jdbc);
    repo.create(new Task(null, "t1", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    repo.create(new Task(null, "t2", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    var all = repo.findScheduleEnabledPage(0L, 1);
    assertEquals(1, all.size());
    assertEquals(repo.findAll().get(0).id(), all.get(0).id());
    var page2 = repo.findScheduleEnabledPage(all.get(0).id(), 1);
    assertEquals(1, page2.size());
    assertFalse(page2.get(0).id() == all.get(0).id()); // 第二页不重复第一页
    var tail = repo.findScheduleEnabledPage(page2.get(0).id(), 10);
    assertTrue(tail.isEmpty()); // 扫尽
  }

  @Test void update() {
    var repo = new JdbcTaskRepository(jdbc);
    var c = repo.create(new Task(null, "t1", "cron", "demo", "0 */5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    boolean ok = repo.update(c.id(), new Task(c.id(), "t2", "cron", "demo",
        "0 */6 * * * *", 3, 600, 2, 2000, ".*err.*", 4, true, true));
    assertTrue(ok);
    var cur = repo.findById(c.id()).orElseThrow();
    assertEquals("t2", cur.name());
    assertEquals("0 */6 * * * *", cur.cron());
    assertEquals(3, cur.shardCount());
    assertEquals(600, cur.timeoutSeconds());
    assertEquals(2, cur.maxRetries());
    assertEquals(2000L, cur.backoffMs());
    assertEquals(".*err.*", cur.retryableFailurePattern());
    assertEquals(4, cur.maxActiveConcurrent());
    assertTrue(cur.paused());
    assertFalse(repo.update(99999L, c)); // 不存在 → false
  }

  @Test void delete_unreferenced_removesRow() {
    var repo = new JdbcTaskRepository(jdbc);
    var c = repo.create(new Task(null, "del1", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    assertEquals(0, repo.executionCount(c.id()));
    assertEquals(0, repo.dagReferenceCount(c.id()));
    assertTrue(repo.delete(c.id()));
    assertTrue(repo.findById(c.id()).isEmpty(), "删除后查无此任务");
  }

  @Test void delete_blockedByExecution_returnsFalseKeepsRow() {
    var repo = new JdbcTaskRepository(jdbc);
    var c = repo.create(new Task(null, "del2", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    jdbc.update("INSERT INTO execution (task_id, status, idempotency_key) "
        + "VALUES (?, 'SUCCESS', 'del-ik')", c.id());
    assertEquals(1, repo.executionCount(c.id()));

    assertFalse(repo.delete(c.id()), "有执行记录的任务不得被删");
    assertTrue(repo.findById(c.id()).isPresent(), "被阻塞时任务必须留存");
  }

  @Test void delete_blockedByDagNode_returnsFalseKeepsRow() {
    var repo = new JdbcTaskRepository(jdbc);
    var c = repo.create(new Task(null, "del3", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    long dagId = jdbc.queryForObject(
        "INSERT INTO app_dag (name) VALUES ('d') RETURNING id", Long.class);
    jdbc.update("INSERT INTO app_dag_node (dag_id, node_key, task_id) "
        + "VALUES (?, 'n1', ?)", dagId, c.id());
    assertEquals(1, repo.dagReferenceCount(c.id()));

    assertFalse(repo.delete(c.id()), "被 DAG 节点引用的任务不得被删");
    assertTrue(repo.findById(c.id()).isPresent());
  }

  @Test void createAndUpdate_persistRetryStrategyFields() {
    var repo = new JdbcTaskRepository(jdbc);
    var created = repo.create(new Task(null, "t1", "cron", "demo", "*/5 * * * *",
        1, 60, 3, 500, null, 8, true, false, "linear", 60_000L, 300_000L));
    Task fromDb = repo.findById(created.id()).orElseThrow();
    assertEquals("linear", fromDb.retryMode());
    assertEquals(60_000L, fromDb.retryCapMs());
    assertEquals(300_000L, fromDb.retryBudgetMs());

    Task updated = new Task(created.id(), "t2", "cron", "demo", "0 */5 * * * *",
        2, 120, 5, 1000, null, 8, true, false, "fixed", null, null);
    assertTrue(repo.update(updated.id(), updated));
    Task after = repo.findById(updated.id()).orElseThrow();
    assertEquals("fixed", after.retryMode());
    assertNull(after.retryCapMs(), "update 置 null → 落库 NULL 而非 0");
    assertNull(after.retryBudgetMs());
  }

  @Test void convenienceConstructor_createsWithExponentialMode() {
    var repo = new JdbcTaskRepository(jdbc);
    var created = repo.create(new Task(null, "t1", "cron", "demo", "*/5 * * * *",
        1, 60, 0, 1000, null, 8, true, false));
    assertEquals("exponential", repo.findById(created.id()).orElseThrow().retryMode(),
        "13 参便捷构造 retryMode=null → INSERT COALESCE 落 'exponential'");
  }

  /** 3b:事件触发任务(event_routes 非空,cron/interval 皆空)落库回读;不进定时扫描;仅匹配订阅的路由。 */
  @Test void create_persistsEventRoutes_andFindEnabledByRouteMatches() {
    var repo = new JdbcTaskRepository(jdbc);
    Task evt = repo.create(new Task(null, "evt-task", "event", "demo", null,
        1, 300, 0, 1000, null, 8, true, false, null, null, null, "UTC", null,
        List.of("order.created", "order.updated"), "NONE", null));
    Task cur = repo.findById(evt.id()).orElseThrow();
    assertEquals(List.of("order.created", "order.updated"), cur.eventRoutes(), "event_routes 落库回读");
    assertEquals("event", cur.kind(), "kind 落库回读");
    assertTrue(repo.findScheduleEnabledPage(0L, 10).isEmpty(), "纯事件任务(cron/interval 皆空)不出现在定时扫描");
    assertEquals(List.of(evt.id()), repo.findEnabledByRoute("order.created").stream().map(Task::id).toList());
    assertEquals(List.of(evt.id()), repo.findEnabledByRoute("order.updated").stream().map(Task::id).toList());
    assertTrue(repo.findEnabledByRoute("order.deleted").isEmpty(), "未订阅路由不匹配");
    assertTrue(cur.eventRoutes() != null && !cur.eventRoutes().isEmpty(), "hasEventRoutes 语义成立");
  }

  /** 3b:暂停/禁用的任务不参与路由匹配;update 覆写 event_routes。 */
  @Test void findEnabledByRoute_excludesPausedAnd_updateOverwritesRoutes() {
    var repo = new JdbcTaskRepository(jdbc);
    Task t = repo.create(new Task(null, "evt-t", "event", "demo", null,
        1, 300, 0, 1000, null, 8, true, false, null, null, null, "UTC", null, List.of("r"), "NONE", null));
    repo.setPaused(t.id(), true);
    assertTrue(repo.findEnabledByRoute("r").isEmpty(), "暂停的事件任务不匹配路由");

    Task updated = new Task(t.id(), "evt-t", "event", "demo", null,
        1, 300, 0, 1000, null, 8, true, false, null, null, null, "UTC", null, List.of("new.r"), "NONE", null);
    assertTrue(repo.update(updated.id(), updated));
    assertEquals(List.of("new.r"), repo.findById(t.id()).orElseThrow().eventRoutes(), "update 覆写 event_routes");
    assertTrue(repo.findEnabledByRoute("r").isEmpty());
    assertEquals(List.of(t.id()), repo.findEnabledByRoute("new.r").stream().map(Task::id).toList());
  }
}
