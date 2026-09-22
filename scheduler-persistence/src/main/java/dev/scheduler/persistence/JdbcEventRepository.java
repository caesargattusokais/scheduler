package dev.scheduler.persistence;

import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcEventRepository implements EventRepository {
  private final JdbcTemplate jdbc;

  public JdbcEventRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  private static final String COLS = "id, route_key, payload_json, dedupe_key, status,"
      + " task_id, execution_id, created_at, dispatched_at";

  @Override
  public long enqueue(String routeKey, String payloadJson, String dedupeKey) {
    String sql = "INSERT INTO app_task_event (route_key, payload_json, dedupe_key)"
        + " VALUES (?, ?::jsonb, ?) ON CONFLICT (dedupe_key) DO NOTHING RETURNING id";
    List<Long> newId = jdbc.query(sql, (rs, i) -> rs.getLong(1), routeKey, payloadJson, dedupeKey);
    if (!newId.isEmpty()) return newId.get(0);
    // 幂等命中既有行 → 返回其 id(重放不新建)。
    return jdbc.queryForObject("SELECT id FROM app_task_event WHERE dedupe_key = ?",
        Long.class, dedupeKey);
  }

  @Override
  public Optional<InboundEvent> findById(long id) {
    return jdbc.query("SELECT " + COLS + " FROM app_task_event WHERE id = ?",
        (rs, row) -> row(rs), id).stream().findFirst();
  }

  @Override
  public List<InboundEvent> pending(int limit) {
    return jdbc.query("SELECT " + COLS + " FROM app_task_event"
            + " WHERE status = ? ORDER BY id LIMIT ?",
        (rs, row) -> row(rs), InboundEvent.STATUS_PENDING, limit);
  }

  @Override
  public void markDispatched(long id, Long taskId, Long executionId) {
    jdbc.update("UPDATE app_task_event SET status = ?, task_id = ?, execution_id = ?,"
            + " dispatched_at = now() WHERE id = ? AND status = ?",
        InboundEvent.STATUS_DISPATCHED, taskId, executionId, id, InboundEvent.STATUS_PENDING);
  }

  @Override
  public List<InboundEvent> findPage(int limit, int offset) {
    return jdbc.query("SELECT " + COLS + " FROM app_task_event"
            + " ORDER BY id DESC LIMIT ? OFFSET ?",
        (rs, row) -> row(rs), limit, offset);
  }

  @Override
  public long count() {
    Long c = jdbc.queryForObject("SELECT count(*) FROM app_task_event", Long.class);
    return c == null ? 0 : c;
  }

  private static InboundEvent row(ResultSet rs) throws java.sql.SQLException {
    return new InboundEvent(
        rs.getLong("id"),
        rs.getString("route_key"),
        rs.getString("payload_json"),        // jsonb 经 rs.getString 返回 JSON 文本
        rs.getString("dedupe_key"),
        rs.getString("status"),
        (Long) rs.getObject("task_id"),
        (Long) rs.getObject("execution_id"),
        rs.getTimestamp("created_at").toInstant(),
        rs.getTimestamp("dispatched_at") == null ? null : rs.getTimestamp("dispatched_at").toInstant());
  }
}