package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** app_task_event 入站事件 outbox 持久层单测:幂等入队、PENDING 拉取、分派回填 + CAS、列表分页。 */
class JdbcEventRepositoryTest extends AbstractPostgresTest {
  private JdbcEventRepository repo;

  @BeforeEach
  void beforeEach() {
    jdbc.execute("TRUNCATE app_task_event RESTART IDENTITY CASCADE");
    repo = new JdbcEventRepository(jdbc);
  }

  /** 入队返回自增 id 且按入队字段落库(payload 以 JSON 文本回读,status PENDING)。 */
  @Test
  void enqueue_persistsRowAndReturnsId() throws Exception {
    long id = repo.enqueue("order.created", "{\"oid\":7}", "evt-1");
    assertTrue(id > 0);

    InboundEvent e = repo.pending(10).get(0);
    assertEquals(id, e.id());
    assertEquals("order.created", e.routeKey());
    assertEquals(new ObjectMapper().readTree("{\"oid\":7}"),
        new ObjectMapper().readTree(e.payload()), "payload 内容一致");
    assertEquals("evt-1", e.dedupeKey());
    assertEquals(InboundEvent.STATUS_PENDING, e.status());
    assertNull(e.taskId());
    assertNull(e.executionId());
    assertNotNull(e.createdAt());
    assertNull(e.dispatchedAt());
  }

  /** 相同 dedupe_key 重复提交 → 不插新行,返回既有行 id。 */
  @Test
  void enqueue_sameDedupeKey_returnsExistingIdNoDuplicate() {
    long id1 = repo.enqueue("order.created", "{}", "dup-key");
    long id2 = repo.enqueue("order.created", "{\"diff\":1}", "dup-key");
    assertEquals(id1, id2, "幂等命中应返回同一行 id");
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_task_event", Long.class),
        "不得重复插入");
    assertEquals("{}", repo.pending(10).get(0).payload(), "重放不改既有 payload");
  }

  /** pending 仅返回 PENDING 行;DISPATCHED 不可见。 */
  @Test
  void pending_filtersByStatus() {
    long p1 = repo.enqueue("a.b", "{}", "k1");
    long p2 = repo.enqueue("a.b", "{}", "k2");
    repo.markDispatched(p1, 7L, 11L);

    List<InboundEvent> pending = repo.pending(10);
    assertEquals(1, pending.size(), "仅 PENDING 行");
    assertEquals(p2, pending.get(0).id());
  }

  /** markDispatched:回填 task_id/execution_id、置 DISPATCHED、dispatched_at 落库;CAS 幂等(重复调用 0 行)。 */
  @Test
  void markDispatched_backfillsAndIsIdempotent() {
    long id = repo.enqueue("a.b", "{}", "d1");
    repo.markDispatched(id, 42L, 99L);

    InboundEvent e = repo.findById(id).orElseThrow();
    assertEquals(InboundEvent.STATUS_DISPATCHED, e.status());
    assertEquals(42L, e.taskId());
    assertEquals(99L, e.executionId());
    assertNotNull(e.dispatchedAt());

    repo.markDispatched(id, 1L, 2L); // 已 DISPATCHED → CAS 0 行,不改回填
    InboundEvent after = repo.findById(id).orElseThrow();
    assertEquals(42L, after.taskId(), "CAS 防覆盖");
    assertEquals(99L, after.executionId());
  }

  /** markDispatched 可传 null 回填(事件无订阅者,仅标记消费)。 */
  @Test
  void markDispatched_allowsNullBackfill() {
    long id = repo.enqueue("a.b", "{}", "null1");
    repo.markDispatched(id, null, null);
    InboundEvent e = repo.findById(id).orElseThrow();
    assertEquals(InboundEvent.STATUS_DISPATCHED, e.status());
    assertNull(e.taskId());
    assertNull(e.executionId());
  }

  /** findPage 按 id 降序分页,count 为全量。 */
  @Test
  void findPage_ordersDescAndCounts() {
    long e1 = repo.enqueue("a.b", "{}", "p1");
    long e2 = repo.enqueue("a.b", "{}", "p2");
    long e3 = repo.enqueue("a.b", "{}", "p3");

    assertEquals(3L, repo.count());
    List<InboundEvent> page = repo.findPage(10, 0);
    assertEquals(e3, page.get(0).id(), "降序:最新在前");
    assertEquals(e1, page.get(2).id());
    // 分页走 offset
    List<InboundEvent> second = repo.findPage(2, 2);
    assertEquals(1, second.size());
    assertEquals(e1, second.get(0).id());
  }

  @Test
  void findById_missing_returnsEmpty() {
    assertFalse(repo.findById(999999L).isPresent());
  }
}