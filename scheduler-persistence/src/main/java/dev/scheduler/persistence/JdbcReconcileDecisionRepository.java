package dev.scheduler.persistence;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

public class JdbcReconcileDecisionRepository implements ReconcileDecisionRepository {
  private final JdbcTemplate jdbc;

  public JdbcReconcileDecisionRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  private static final RowMapper<ReconcileDecision> MAP = (rs, i) -> new ReconcileDecision(
      rs.getLong("id"),
      rs.getTimestamp("created_at") != null ? rs.getTimestamp("created_at").toInstant() : null,
      rs.getString("phase"),
      rs.getString("target_type"),
      rs.getLong("target_id"),
      rs.getObject("task_id", Long.class),
      rs.getString("reason"),
      rs.getString("outcome"),
      rs.getString("from_status"),
      rs.getString("to_status"),
      rs.getObject("attempt", Integer.class),
      rs.getString("detail"));

  @Override public void record(ReconcileDecision d) {
    jdbc.update("""
        INSERT INTO reconcile_decision
          (phase, target_type, target_id, task_id, reason, outcome, from_status, to_status, attempt, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        d.phase(), d.targetType(), d.targetId(), d.taskId(), d.reason(),
        d.outcome(), d.fromStatus(), d.toStatus(), d.attempt(), d.detail());
  }

  @Override public Map<String, Long> countPhasesLastHour() {
    Map<String, Long> out = new HashMap<>();
    jdbc.query("""
        SELECT phase, count(*) AS c FROM reconcile_decision
        WHERE created_at >= now() - interval '1 hour'
        GROUP BY phase""",
        (RowCallbackHandler) rs -> out.put(rs.getString("phase"), rs.getLong("c")));
    return out;
  }

  @Override public List<ReconcileDecision> findRecent(int limit, int offset) {
    return jdbc.query(
        "SELECT * FROM reconcile_decision ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
        MAP, limit, offset);
  }

  @Override public long count() {
    Long c = jdbc.queryForObject("SELECT count(*) FROM reconcile_decision", Long.class);
    return c == null ? 0 : c;
  }
}