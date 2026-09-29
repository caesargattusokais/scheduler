package dev.scheduler.persistence;

import dev.scheduler.core.AlertEpisode;
import dev.scheduler.core.AlertEpisodeView;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;

/** alert_episode 的 JDBC 读/写实现。视图查询统一 LEFT JOIN app_task/app_dag 出 target_name。 */
public class JdbcAlertRepository implements AlertRepository {
  private final JdbcTemplate jdbc;

  public JdbcAlertRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /** 视图查询公共 SELECT 段:join target_name(task/dag → name,其他 NULL)。 */
  private static final String VIEW_SELECT = """
      SELECT e.id, e.rule, e.target_type, e.target_id, e.severity, e.status, e.value,
             e.opened_at, e.opened_value, e.resolved_at, e.resolved_value,
             CASE e.target_type WHEN 'task' THEN t.name WHEN 'dag' THEN d.name ELSE NULL END AS target_name
        FROM alert_episode e
        LEFT JOIN app_task t ON e.target_type='task' AND e.target_id=t.id
        LEFT JOIN app_dag  d ON e.target_type='dag'  AND e.target_id=d.id
        WHERE e.status = ?
        ORDER BY e.opened_at DESC""";

  @Override
  public Optional<AlertEpisode> findActiveByKey(String key) {
    return jdbc.query(
        "SELECT id, key, rule, target_type, target_id, severity, status, sample_count, value,"
            + " opened_at, opened_value, resolved_at, resolved_value FROM alert_episode"
            + " WHERE key = ? AND status <> 'RESOLVED' LIMIT 1",
        rs -> rs.next() ? Optional.of(map(rs)) : Optional.empty(),
        key);
  }

  @Override
  public AlertEpisode insert(AlertEpisode e) {
    // RETURNING id 回填自增主键注入返回记录。同一 key 已存在活跃行时 partial unique(alert_episode_active_key_uq)
    // 会抛 DuplicateKeyException——交由调用方捕获决定回落/丢弃,这里原样抛出。
    Long id = jdbc.query(
        "INSERT INTO alert_episode (key, rule, target_type, target_id, severity, status, sample_count, value)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
        rs -> rs.next() ? rs.getLong(1) : null,
        e.key(), e.rule(), e.targetType(), e.targetId(), e.severity(), e.status(), e.sampleCount(), e.value());
    return new AlertEpisode(id, e.key(), e.rule(), e.targetType(), e.targetId(), e.severity(),
        e.status(), e.sampleCount(), e.value(), e.openedAt(), e.openedValue(), e.resolvedAt(), e.resolvedValue());
  }

  @Override
  public void update(AlertEpisode e) {
    jdbc.update("UPDATE alert_episode SET status=?, sample_count=?, value=?,"
            + " opened_at=?, opened_value=?, resolved_at=?, resolved_value=? WHERE id=?",
        e.status(), e.sampleCount(), e.value(),
        ts(e.openedAt()), e.openedValue(), ts(e.resolvedAt()), e.resolvedValue(), e.id());
  }

  @Override
  public void deleteByKey(String key) {
    // 仅删活跃(PENDING/OPEN)行;RESOLVED 历史保留。
    jdbc.update("DELETE FROM alert_episode WHERE key = ? AND status <> 'RESOLVED'", key);
  }

  @Override
  public long pruneResolved(Instant cutoff) {
    return jdbc.update("DELETE FROM alert_episode WHERE status='RESOLVED' AND resolved_at < ?",
        ts(cutoff));
  }

  @Override
  public List<AlertEpisodeView> findActive() {
    return jdbc.query(VIEW_SELECT, (rs, i) -> mapView(rs), "OPEN");
  }

  @Override
  public List<AlertEpisodeView> findResolved(int limit, int offset) {
    return jdbc.query(VIEW_SELECT + " LIMIT ? OFFSET ?", (rs, i) -> mapView(rs),
        "RESOLVED", limit, offset);
  }

  @Override
  public long countResolved() {
    Long c = jdbc.queryForObject(
        "SELECT count(*) FROM alert_episode e WHERE e.status='RESOLVED'", Long.class);
    return c == null ? 0 : c;
  }

  /** 行映射(id/key 全字段,供 findActiveByKey/insert 用)。target_id/时间列可空用 getObject。 */
  private AlertEpisode map(java.sql.ResultSet rs) throws java.sql.SQLException {
    return new AlertEpisode(rs.getLong("id"), rs.getString("key"), rs.getString("rule"),
        rs.getString("target_type"), rs.getObject("target_id", Long.class), rs.getString("severity"),
        rs.getString("status"), rs.getInt("sample_count"), rs.getString("value"),
        tsInstant(rs, "opened_at"), rs.getString("opened_value"),
        tsInstant(rs, "resolved_at"), rs.getString("resolved_value"));
  }

  /** 视图映射(join target_name,无 key/updated_at)。 */
  private AlertEpisodeView mapView(java.sql.ResultSet rs) throws java.sql.SQLException {
    return new AlertEpisodeView(rs.getLong("id"), rs.getString("rule"), rs.getString("target_type"),
        rs.getObject("target_id", Long.class), rs.getString("target_name"), rs.getString("severity"),
        rs.getString("status"), rs.getString("value"),
        tsInstant(rs, "opened_at"), rs.getString("opened_value"),
        tsInstant(rs, "resolved_at"), rs.getString("resolved_value"));
  }

  private static Timestamp ts(Instant i) {
    return i == null ? null : Timestamp.from(i);
  }

  private static Instant tsInstant(java.sql.ResultSet rs, String col) throws java.sql.SQLException {
    Timestamp t = rs.getObject(col, Timestamp.class);
    return t == null ? null : t.toInstant();
  }
}