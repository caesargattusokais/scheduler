package dev.scheduler.persistence;
import static org.junit.jupiter.api.Assertions.*;
import dev.scheduler.core.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcTaskRepositoryTest extends AbstractPostgresTest {
  /** 不依赖其它测试类的 TRUNCATE 顺序:共享静态 PG 容器会累积 app_task 行,本类断言 findCronEnabledPage 数量需隔离。 */
  @BeforeEach void clean() {
    jdbc.update("TRUNCATE app_task, execution, execution_outcome RESTART IDENTITY CASCADE");
  }

  @Test void roundTrip() {
    var repo = new JdbcTaskRepository(jdbc);
    var created = repo.create(new Task(null, "t1", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    assertTrue(created.id() > 0);
    assertTrue(repo.findById(created.id()).isPresent());
    assertEquals(1, repo.findCronEnabledPage(0L, 10).size());
    repo.setPaused(created.id(), true);
    assertTrue(repo.findCronEnabledPage(0L, 10).isEmpty());
  }

  @Test void findCronEnabledPage_resumesPastCursor_withoutSkipping() {
    var repo = new JdbcTaskRepository(jdbc);
    repo.create(new Task(null, "t1", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    repo.create(new Task(null, "t2", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    var all = repo.findCronEnabledPage(0L, 1);
    assertEquals(1, all.size());
    assertEquals(repo.findAll().get(0).id(), all.get(0).id());
    var page2 = repo.findCronEnabledPage(all.get(0).id(), 1);
    assertEquals(1, page2.size());
    assertFalse(page2.get(0).id() == all.get(0).id()); // 第二页不重复第一页
    var tail = repo.findCronEnabledPage(page2.get(0).id(), 10);
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
}
