package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** app_webhook 订阅仓储单测:CRUD + kinds 数组往返(enabled/max_attempts/backoff_ms 落库)。 */
class JdbcWebhookRepositoryTest extends AbstractPostgresTest {
  private JdbcWebhookRepository repo;

  @BeforeEach
  void beforeEach() {
    jdbc.execute("TRUNCATE app_webhook RESTART IDENTITY CASCADE");
    repo = new JdbcWebhookRepository(jdbc);
  }

  @Test
  void create_persistsAllFieldsAndRoundTripsKinds() {
    long id = repo.create("https://hooks.example.com/x", "s3cret",
        List.of("execution.completed", "execution.failed"), true, 7, 2500);
    Webhook w = repo.list().get(0);
    assertEquals(id, w.id());
    assertEquals("https://hooks.example.com/x", w.url());
    assertEquals("s3cret", w.secret());
    assertEquals(List.of("execution.completed", "execution.failed"), w.kinds(), "kinds 数组往返");
    assertTrue(w.enabled());
    assertEquals(7, w.maxAttempts());
    assertEquals(2500, w.backoffMs());
    assertTrue(w.createdAt() != null);
  }

  @Test
  void create_emptyKindsRoundTripsAsEmpty() {
    repo.create("https://h/x", null, List.of(), true, 5, 1000);
    List<String> kinds = repo.list().get(0).kinds();
    assertTrue(kinds.isEmpty(), "kinds 缺省空数组");
  }

  @Test
  void update_overwritesFieldsAndReturnsTrue() {
    long id = repo.create("https://h/a", "s1", List.of("a"), true, 5, 1000);
    boolean ok = repo.update(id, "https://h/b", "s2", List.of("b", "c"), false, 3, 500);
    assertTrue(ok);
    Webhook w = repo.list().get(0);
    assertEquals("https://h/b", w.url());
    assertEquals("s2", w.secret());
    assertEquals(List.of("b", "c"), w.kinds());
    assertFalse(w.enabled());
    assertEquals(3, w.maxAttempts());
    assertEquals(500, w.backoffMs());
    assertEquals(1, repo.list().size(), "update 不新增行");
  }

  @Test
  void update_missingId_returnsFalse() {
    assertFalse(repo.update(9999, "https://h/x", null, List.of(), true, 5, 1000));
  }

  @Test
  void delete_removesRowAndReturnsTrue() {
    long id = repo.create("https://h/x", null, List.of(), true, 5, 1000);
    assertTrue(repo.delete(id));
    assertTrue(repo.list().isEmpty(), "删后列表空");
  }

  @Test
  void delete_missingId_returnsFalse() {
    assertFalse(repo.delete(9999));
  }

  @Test
  void list_ordersByIdAscending() {
    long a = repo.create("https://h/a", null, List.of(), true, 5, 1000);
    long b = repo.create("https://h/b", null, List.of(), true, 5, 1000);
    long c = repo.create("https://h/c", null, List.of(), true, 5, 1000);
    assertEquals(List.of(a, b, c), repo.list().stream().map(Webhook::id).toList());
  }
}