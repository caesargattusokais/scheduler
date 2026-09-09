package dev.scheduler.persistence;
import static org.junit.jupiter.api.Assertions.*;
import dev.scheduler.core.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcTaskRepositoryTest extends AbstractPostgresTest {
  /** 不依赖其它测试类的 TRUNCATE 顺序:共享静态 PG 容器会累积 app_task 行,本类断言 findCronEnabled 数量需隔离。 */
  @BeforeEach void clean() {
    jdbc.update("TRUNCATE app_task, execution, execution_outcome RESTART IDENTITY CASCADE");
  }

  @Test void roundTrip() {
    var repo = new JdbcTaskRepository(jdbc);
    var created = repo.create(new Task(null, "t1", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    assertTrue(created.id() > 0);
    assertTrue(repo.findById(created.id()).isPresent());
    assertEquals(1, repo.findCronEnabled().size());
    repo.setPaused(created.id(), true);
    assertTrue(repo.findCronEnabled().isEmpty());
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
}
