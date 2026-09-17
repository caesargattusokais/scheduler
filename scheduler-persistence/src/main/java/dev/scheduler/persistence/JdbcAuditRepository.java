package dev.scheduler.persistence;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.core.TargetType;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcAuditRepository implements AuditRepository {
  private final JdbcTemplate jdbc;
  public JdbcAuditRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

  @Override
  public void record(String operator, String action, TargetType targetType,
                     long targetId, String metaJson, String source) {
    // ?::json 把文本转 JSONB;metaJson 为 null 时 NULL::json = NULL。append-only 单 INSERT,不需幂等键。
    jdbc.update("""
        INSERT INTO app_audit (operator, action, target_type, target_id, meta, source)
        VALUES (?, ?, ?, ?, ?::json, ?)""",
        operator, action, targetType.db(), targetId, metaJson, source);
  }

  /** WHERE 片段(与 JdbcTaskRepository)配套 {@link #filterArgs}。operator 子串 ILIKE,其余等值。 */
  private String where(String operator, String action, String targetType,
                       Long targetId, Instant from, Instant to) {
    StringBuilder w = new StringBuilder();
    if (operator != null && !operator.isBlank()) w.append(" AND operator ILIKE ?");
    if (action != null && !action.isBlank()) w.append(" AND action = ?");
    if (targetType != null && !targetType.isBlank()) w.append(" AND target_type = ?");
    if (targetId != null) w.append(" AND target_id = ?");
    if (from != null) w.append(" AND occurred_at >= ?");
    if (to != null) w.append(" AND occurred_at <= ?");
    return w.toString();
  }

  private List<Object> filterArgs(String operator, String action, String targetType,
                                  Long targetId, Instant from, Instant to) {
    List<Object> a = new ArrayList<>();
    if (operator != null && !operator.isBlank()) a.add("%" + operator.trim() + "%");
    if (action != null && !action.isBlank()) a.add(action.trim());
    if (targetType != null && !targetType.isBlank()) a.add(targetType.trim());
    if (targetId != null) a.add(targetId);
    if (from != null) a.add(Timestamp.from(from));
    if (to != null) a.add(Timestamp.from(to));
    return a;
  }

  @Override
  public List<AuditEntry> findPage(String operator, String action, String targetType,
                                   Long targetId, Instant from, Instant to,
                                   int limit, int offset) {
    List<Object> a = filterArgs(operator, action, targetType, targetId, from, to);
    a.add(limit); a.add(offset);
    return jdbc.query("SELECT id, occurred_at, operator, action, target_type, target_id, meta, source"
            + " FROM app_audit WHERE 1=1" + where(operator, action, targetType, targetId, from, to)
            + " ORDER BY occurred_at DESC, id DESC LIMIT ? OFFSET ?",
        (rs, i) -> new AuditEntry(
            rs.getLong("id"), rs.getTimestamp("occurred_at").toInstant(),
            rs.getString("operator"), rs.getString("action"), rs.getString("target_type"),
            rs.getLong("target_id"), rs.getString("meta"), rs.getString("source")),
        a.toArray());
  }

  @Override
  public long count(String operator, String action, String targetType,
                    Long targetId, Instant from, Instant to) {
    Long c = jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE 1=1"
            + where(operator, action, targetType, targetId, from, to),
        Long.class, filterArgs(operator, action, targetType, targetId, from, to).toArray());
    return c == null ? 0 : c;
  }
}