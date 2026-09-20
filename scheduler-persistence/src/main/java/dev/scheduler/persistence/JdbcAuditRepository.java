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
                     long targetId, String metaJson, String diffJson, String beforeJson, String source) {
    // ?::json 把文本转 JSONB;metaJson/diffJson/beforeJson 为 null 时 NULL::json = NULL。
    jdbc.update("""
        INSERT INTO app_audit (operator, action, target_type, target_id, meta, diff, before_meta, source)
        VALUES (?, ?, ?, ?, ?::json, ?::json, ?::json, ?)""",
        operator, action, targetType.db(), targetId, metaJson, diffJson, beforeJson, source);
  }

  /** WHERE 片段(与 JdbcTaskRepository)配套 {@link #filterArgs}。operator 子串 ILIKE,其余等值。
   *  diff/before/meta 三列 JSON 顶层键过滤可跨列 AND(diffField→diff、beforeField→before_meta、metaField→meta)。 */
  private String where(String operator, String action, String targetType,
                       Long targetId, Instant from, Instant to, Boolean hasDiff,
                       String diffField, String beforeField, String metaField) {
    StringBuilder w = new StringBuilder();
    if (operator != null && !operator.isBlank()) w.append(" AND operator ILIKE ?");
    if (action != null && !action.isBlank()) w.append(" AND action = ?");
    if (targetType != null && !targetType.isBlank()) w.append(" AND target_type = ?");
    if (targetId != null) w.append(" AND target_id = ?");
    if (from != null) w.append(" AND occurred_at >= ?");
    if (to != null) w.append(" AND occurred_at <= ?");
    if (Boolean.TRUE.equals(hasDiff)) w.append(" AND diff IS NOT NULL AND diff <> '{}'::jsonb");
    // 顶层键存在用 ->(单占位符)而非 JSONB ?运算符(双 ?? 会被 JDBC 误读为两个占位符)。
    // diff 值为 [before,after] 数组,绝不为 JSON null,故 (diff -> ?) IS NOT NULL 等价键存在;before/meta 同。
    if (diffField != null && !diffField.isBlank()) w.append(" AND (diff -> ?) IS NOT NULL");
    if (beforeField != null && !beforeField.isBlank()) w.append(" AND (before_meta -> ?) IS NOT NULL");
    if (metaField != null && !metaField.isBlank()) w.append(" AND (meta -> ?) IS NOT NULL");
    return w.toString();
  }

  private List<Object> filterArgs(String operator, String action, String targetType,
                                  Long targetId, Instant from, Instant to,
                                  Boolean hasDiff, String diffField, String beforeField, String metaField) {
    List<Object> a = new ArrayList<>();
    if (operator != null && !operator.isBlank()) a.add("%" + operator.trim() + "%");
    if (action != null && !action.isBlank()) a.add(action.trim());
    if (targetType != null && !targetType.isBlank()) a.add(targetType.trim());
    if (targetId != null) a.add(targetId);
    if (from != null) a.add(Timestamp.from(from));
    if (to != null) a.add(Timestamp.from(to));
    if (diffField != null && !diffField.isBlank()) a.add(diffField.trim());
    if (beforeField != null && !beforeField.isBlank()) a.add(beforeField.trim());
    if (metaField != null && !metaField.isBlank()) a.add(metaField.trim());
    return a;
  }

  @Override
  public List<AuditEntry> findPage(String operator, String action, String targetType,
                                   Long targetId, Instant from, Instant to, Boolean hasDiff,
                                   String diffField, String beforeField, String metaField,
                                   int limit, int offset) {
    List<Object> a = filterArgs(operator, action, targetType, targetId, from, to,
        hasDiff, diffField, beforeField, metaField);
    a.add(limit); a.add(offset);
    return jdbc.query("SELECT id, occurred_at, operator, action, target_type, target_id, meta, source, diff, before_meta"
            + " FROM app_audit WHERE 1=1"
            + where(operator, action, targetType, targetId, from, to, hasDiff, diffField, beforeField, metaField)
            + " ORDER BY occurred_at DESC, id DESC LIMIT ? OFFSET ?",
        (rs, i) -> new AuditEntry(
            rs.getLong("id"), rs.getTimestamp("occurred_at").toInstant(),
            rs.getString("operator"), rs.getString("action"), rs.getString("target_type"),
            rs.getLong("target_id"), rs.getString("meta"), rs.getString("source"),
            rs.getString("diff"), rs.getString("before_meta")),
        a.toArray());
  }

  @Override
  public long count(String operator, String action, String targetType,
                    Long targetId, Instant from, Instant to, Boolean hasDiff,
                    String diffField, String beforeField, String metaField) {
    Long c = jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE 1=1"
            + where(operator, action, targetType, targetId, from, to, hasDiff, diffField, beforeField, metaField),
        Long.class, filterArgs(operator, action, targetType, targetId, from, to,
            hasDiff, diffField, beforeField, metaField).toArray());
    return c == null ? 0 : c;
  }
}