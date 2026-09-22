package dev.scheduler.persistence;

import java.sql.ResultSet;
import java.time.Instant;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcNotificationRepository implements NotificationRepository {
  private final JdbcTemplate jdbc;

  public JdbcNotificationRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  private static final String COLS = "id, kind, operator, target_type, target_id, payload_json,"
      + " status, attempts, next_retry_at, last_error, created_at, sent_at";

  @Override
  public long enqueue(String kind, String operator, String targetType, Long targetId,
                      String payloadJson, String idempotencyKey) {
    String sql = "INSERT INTO app_notification"
        + " (kind, operator, target_type, target_id, payload_json, idempotency_key)"
        + " VALUES (?, ?, ?, ?, ?::jsonb, ?) ON CONFLICT (idempotency_key) DO NOTHING RETURNING id";
    List<Long> newId = jdbc.query(sql, (rs, i) -> rs.getLong(1),
        kind, operator, targetType, targetId, payloadJson, idempotencyKey);
    if (!newId.isEmpty()) return newId.get(0);
    // 幂等命中既有行 → 返回其 id。
    return jdbc.queryForObject("SELECT id FROM app_notification WHERE idempotency_key = ?",
        Long.class, idempotencyKey);
  }

  @Override
  public List<OutboundNotification> due(int limit) {
    return jdbc.query("SELECT " + COLS + " FROM app_notification"
            + " WHERE status = ? AND (next_retry_at IS NULL OR next_retry_at <= now())"
            + " ORDER BY id LIMIT ?",
        (rs, row) -> row(rs), OutboundNotification.STATUS_PENDING, limit);
  }

  @Override
  public void markSent(long id) {
    jdbc.update("UPDATE app_notification SET status = ?, attempts = attempts + 1,"
        + " sent_at = now(), last_error = NULL WHERE id = ?", OutboundNotification.STATUS_SENT, id);
  }

  @Override
  public void markRetry(long id, Instant nextRetryAt, String lastError) {
    jdbc.update("UPDATE app_notification SET status = ?, attempts = attempts + 1,"
            + " next_retry_at = ?, last_error = ? WHERE id = ?",
        OutboundNotification.STATUS_PENDING, java.sql.Timestamp.from(nextRetryAt), lastError, id);
  }

  @Override
  public void markFailed(long id, String lastError) {
    jdbc.update("UPDATE app_notification SET status = ?, attempts = attempts + 1,"
        + " last_error = ?, sent_at = NULL WHERE id = ?", OutboundNotification.STATUS_FAILED, lastError, id);
  }

  private static OutboundNotification row(ResultSet rs) throws java.sql.SQLException {
    return new OutboundNotification(
        rs.getLong("id"),
        rs.getString("kind"),
        rs.getString("operator"),
        rs.getString("target_type"),
        rs.getObject("target_id") == null ? null : rs.getLong("target_id"),
        rs.getString("payload_json"),          // jsonb 经 rs.getString 返回 JSON 文本
        rs.getString("status"),
        rs.getInt("attempts"),
        rs.getTimestamp("next_retry_at") == null ? null : rs.getTimestamp("next_retry_at").toInstant(),
        rs.getString("last_error"),
        rs.getTimestamp("created_at").toInstant(),
        rs.getTimestamp("sent_at") == null ? null : rs.getTimestamp("sent_at").toInstant());
  }
}