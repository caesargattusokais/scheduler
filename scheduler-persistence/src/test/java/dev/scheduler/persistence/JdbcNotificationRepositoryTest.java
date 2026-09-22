package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * app_notification outbox 持久层单测:幂等入队、due 到期过滤、sent/retry/failed 状态转移与 attempts 自增。
 */
class JdbcNotificationRepositoryTest extends AbstractPostgresTest {
  private JdbcNotificationRepository repo;

  @BeforeEach
  void beforeEach() {
    jdbc.execute("TRUNCATE app_notification RESTART IDENTITY CASCADE");
    repo = new JdbcNotificationRepository(jdbc);
  }

  /** 入队返回自增 id 且行按入队字段落库(payload_json 以 JSON 文本回读)。 */
  @Test
  void enqueue_persistsRowAndReturnsId() throws Exception {
    long id = repo.enqueue("execution.failed", "alice", "execution", 42L,
        "{\"task\":\"t1\"}", "evt-1");
    assertTrue(id > 0, "应返回自增 id");

    OutboundNotification n = repo.due(10).get(0);
    assertEquals(id, n.id());
    assertEquals("execution.failed", n.kind());
    assertEquals("alice", n.operator());
    assertEquals("execution", n.targetType());
    assertEquals(42L, n.targetId());
    // jsonb 会归一化空白(键值冒号后补空格),故按解析后的树比较,而非字节级比对。
    assertEquals(new ObjectMapper().readTree("{\"task\":\"t1\"}"),
        new ObjectMapper().readTree(n.payload()), "payload 内容一致");
    assertEquals(OutboundNotification.STATUS_PENDING, n.status());
    assertEquals(0, n.attempts(), "入队未投递 attempts=0");
    assertNull(n.nextRetryAt());
    assertNotNull(n.createdAt());
    assertNull(n.sentAt());
  }

  /** 相同 idempotency_key 重复 fire → 不插新行,返回既有行 id。 */
  @Test
  void enqueue_sameIdempotencyKey_returnsExistingIdNoDuplicate() {
    long id1 = repo.enqueue("execution.failed", null, "execution", 1L, "{}", "dup-key");
    long id2 = repo.enqueue("execution.failed", null, "execution", 1L, "{}", "dup-key");
    assertEquals(id1, id2, "幂等命中应返回同一行 id");
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_notification", Long.class),
        "不得重复插入");
  }

  /** due 仅返回 PENDING 且到期(next_retry_at 为空或 ≤ now)的行;未到期/已终态不可见。 */
  @Test
  void due_filtersByStatusAndDueTime() {
    long due1 = repo.enqueue("a", null, null, null, "{}", "k1"); // 空 next_retry_at → 立即可投
    long future = repo.enqueue("a", null, null, null, "{}", "k2");
    jdbc.update("UPDATE app_notification SET next_retry_at = now() + interval '1 hour' WHERE id = ?", future);
    long failed = repo.enqueue("a", null, null, null, "{}", "k3");
    repo.markFailed(failed, "boom");
    long sent = repo.enqueue("a", null, null, null, "{}", "k4");
    repo.markSent(sent);

    List<OutboundNotification> due = repo.due(10);
    assertEquals(1, due.size(), "仅可投的 PENDING 行");
    assertEquals(due1, due.get(0).id(), "到期的 k1 才可见");
  }

  /** markSent → SENT、attempts+1、sent_at=now、清 last_error。 */
  @Test
  void markSent_setsStateAndIncrementsAttempts() {
    long id = repo.enqueue("a", null, null, null, "{}", "s1");
    repo.markSent(id);
    OutboundNotification n = byId(id);
    assertEquals(OutboundNotification.STATUS_SENT, n.status());
    assertEquals(1, n.attempts(), "markSent 应自增 attempts");
    assertNull(n.lastError(), "last_error 置空");
    assertNotNull(n.sentAt(), "sent_at 落库");
    assertTrue(repo.due(10).stream().noneMatch(x -> x.id() == id), "SENT 终态不入 due");
  }

  /** markRetry → PENDING、attempts+1、next_retry_at=给定时刻、记错误。 */
  @Test
  void markRetry_setsBackoffTimeAndIncrementsAttempts() {
    long id = repo.enqueue("a", null, null, null, "{}", "r1");
    Instant next = Instant.now().plusSeconds(10);
    repo.markRetry(id, next, "http 500");
    OutboundNotification n = byId(id);
    assertEquals(OutboundNotification.STATUS_PENDING, n.status(), "可重试维持在 PENDING");
    assertEquals(1, n.attempts(), "markRetry 应自增 attempts");
    assertEquals(next.toEpochMilli(), n.nextRetryAt().toEpochMilli(), "next_retry_at 写为给定时刻");
    assertEquals("http 500", n.lastError());
  }

  /** markFailed → FAILED、attempts+1、记错误。 */
  @Test
  void markFailed_setsTerminalState() {
    long id = repo.enqueue("a", null, null, null, "{}", "f1");
    repo.markFailed(id, "permanent");
    OutboundNotification n = byId(id);
    assertEquals(OutboundNotification.STATUS_FAILED, n.status());
    assertEquals(1, n.attempts(), "markFailed 应自增 attempts");
    assertEquals("permanent", n.lastError());
  }

  /** 连续多次重试后 attempts 累积可见,退避到点后再投。 */
  @Test
  void retryLoop_backoffThenDueAgain() {
    long id = repo.enqueue("a", null, null, null, "{}", "rl1");
    repo.markRetry(id, Instant.now().minusSeconds(1), "try1"); // 退避到点(过去)
    List<OutboundNotification> due = repo.due(10);
    assertEquals(1, due.size(), "退避已到 → 重新可投");
    assertEquals(1, due.get(0).attempts(), "携带已重试次数供调度决定是否继续退避");
    assertEquals(id, due.get(0).id());
  }

  /** 重试耗尽 markFailed 后不再进入 due(终态)。 */
  @Test
  void retryLoop_exhaustedStaysOutOfDue() {
    long id = repo.enqueue("a", null, null, null, "{}", "rl2");
    for (int i = 0; i < 4; i++) repo.markRetry(id, Instant.now().minusSeconds(1), "try");
    repo.markFailed(id, "exhausted");
    assertTrue(repo.due(10).isEmpty(), "FAILED 终态不入 due");
    assertEquals(5, byId(id).attempts(), "4 次重试 + 1 次失败 = attempts 5");
  }

  /** 直查整行(兼容终态行,due() 会过滤)。 */
  private OutboundNotification byId(long id) {
    return jdbc.queryForObject(
        "SELECT id, kind, operator, target_type, target_id, payload_json, status, attempts,"
            + " next_retry_at, last_error, created_at, sent_at FROM app_notification WHERE id = ?",
        (rs, i) -> new OutboundNotification(
            rs.getLong("id"), rs.getString("kind"), rs.getString("operator"),
            rs.getString("target_type"), rs.getObject("target_id") == null ? null : rs.getLong("target_id"),
            rs.getString("payload_json"), rs.getString("status"), rs.getInt("attempts"),
            ts(rs.getTimestamp("next_retry_at")), rs.getString("last_error"),
            ts(rs.getTimestamp("created_at")), ts(rs.getTimestamp("sent_at"))),
        id);
  }

  private static Instant ts(Timestamp t) {
    return t == null ? null : t.toInstant();
  }
}