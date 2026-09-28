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

  /** 便捷建行:scope 缺省 ALL + 空选中集。 */
  private long defaultCreate(String url, String secret, List<String> kinds, boolean enabled,
                             int maxAttempts, long backoffMs) {
    return repo.create(url, secret, kinds, enabled, maxAttempts, backoffMs, "ALL", List.of(), List.of());
  }

  @Test
  void create_persistsAllFieldsAndRoundTripsKinds() {
    long id = repo.create("https://hooks.example.com/x", "s3cret",
        List.of("execution.completed", "execution.failed"), true, 7, 2500,
        "INCLUDE", List.of(11L, 22L), List.of(3L));
    Webhook w = repo.list().get(0);
    assertEquals(id, w.id());
    assertEquals("https://hooks.example.com/x", w.url());
    assertEquals("s3cret", w.secret());
    assertEquals(List.of("execution.completed", "execution.failed"), w.kinds(), "kinds 数组往返");
    assertTrue(w.enabled());
    assertEquals(7, w.maxAttempts());
    assertEquals(2500, w.backoffMs());
    assertEquals("INCLUDE", w.scopeMode(), "scope_mode 落库");
    assertEquals(List.of(11L, 22L), w.selectedTaskIds(), "selected_task_ids 数组往返");
    assertEquals(List.of(3L), w.selectedDagIds(), "selected_dag_ids 数组往返");
    assertTrue(w.createdAt() != null);
  }

  @Test
  void create_emptyKindsRoundTripsAsEmpty() {
    repo.create("https://h/x", null, List.of(), true, 5, 1000, "ALL", List.of(), List.of());
    Webhook w = repo.list().get(0);
    assertTrue(w.kinds().isEmpty(), "kinds 缺省空数组");
    assertEquals(List.of(), w.selectedTaskIds(), "选中集缺省空");
  }

  @Test
  void update_overwritesFieldsAndReturnsTrue() {
    long id = defaultCreate("https://h/a", "s1", List.of("a"), true, 5, 1000);
    boolean ok = repo.update(id, "https://h/b", "s2", List.of("b", "c"), false, 3, 500,
        "EXCLUDE", List.of(9L), List.of());
    assertTrue(ok);
    Webhook w = repo.list().get(0);
    assertEquals("https://h/b", w.url());
    assertEquals("s2", w.secret());
    assertEquals(List.of("b", "c"), w.kinds());
    assertFalse(w.enabled());
    assertEquals(3, w.maxAttempts());
    assertEquals(500, w.backoffMs());
    assertEquals("EXCLUDE", w.scopeMode(), "scope_mode 可更新");
    assertEquals(List.of(9L), w.selectedTaskIds(), "任务选中集可更新");
    assertEquals(1, repo.list().size(), "update 不新增行");
  }

  @Test
  void update_missingId_returnsFalse() {
    assertFalse(repo.update(9999, "https://h/x", null, List.of(), true, 5, 1000, "ALL", List.of(), List.of()));
  }

  @Test
  void delete_removesRowAndReturnsTrue() {
    long id = defaultCreate("https://h/x", null, List.of(), true, 5, 1000);
    assertTrue(repo.delete(id));
    assertTrue(repo.list().isEmpty(), "删后列表空");
  }

  @Test
  void delete_missingId_returnsFalse() {
    assertFalse(repo.delete(9999));
  }

  @Test
  void list_ordersByIdAscending() {
    long a = defaultCreate("https://h/a", null, List.of(), true, 5, 1000);
    long b = defaultCreate("https://h/b", null, List.of(), true, 5, 1000);
    long c = defaultCreate("https://h/c", null, List.of(), true, 5, 1000);
    assertEquals(List.of(a, b, c), repo.list().stream().map(Webhook::id).toList());
  }
}