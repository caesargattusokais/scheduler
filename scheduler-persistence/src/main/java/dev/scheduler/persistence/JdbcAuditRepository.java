package dev.scheduler.persistence;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.core.AuditIntegrity;
import dev.scheduler.core.TargetType;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class JdbcAuditRepository implements AuditRepository {
  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;

  public JdbcAuditRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
    this.tx = new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
  }

  /** 审计链 append 的 DB 咨询锁键(固定常量):串行化"取前驱→插入",避免并发读到同一前驱产生 fork 导致假篡改告警。 */
  private static final long CHAIN_LOCK = 0x4155444F_4E4C59L; // 'AUDONLY'

  @Override
  public void record(String operator, String action, TargetType targetType,
                     long targetId, String metaJson, String diffJson, String beforeJson, String source) {
    // ?::jsonb 把文本转 JSONB;metaJson/diffJson/beforeJson 为 null 时 NULL::jsonb = NULL。
    // 取证链:同事务内先取咨询锁串行化,再读末行 chunk 作 prev,插入行并以 scheduler_audit_chunk() 计算自身 chunk
    // (哈希函数与 V13 迁移/校验端同源,杜绝字节漂移)。occurred_at 由 now() 生成(chunk 内用同一 now())。
    tx.executeWithoutResult(status -> {
      jdbc.queryForObject("SELECT pg_advisory_xact_lock(?)", Object.class, CHAIN_LOCK);
      String prev = jdbc.query("SELECT chunk_hash FROM app_audit ORDER BY id DESC LIMIT 1",
          rs -> rs.next() ? rs.getString(1) : null);
      jdbc.update("""
          INSERT INTO app_audit (operator, action, target_type, target_id, meta, diff, before_meta, source, occurred_at, prev_hash, chunk_hash)
          VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?, now(), ?,
                  scheduler_audit_chunk(?::text, ?, ?, ?, ?, now(), ?::jsonb, ?, ?::jsonb, ?::jsonb))""",
          operator, action, targetType.db(), targetId, metaJson, diffJson, beforeJson, source, prev,
          prev, operator, action, targetType.db(), targetId, metaJson, source, diffJson, beforeJson);
    });
  }

  @Override
  public AuditIntegrity integrity() {
    // total = 表全量;chained = 已入链行数;firstTamperedId = 自哈希异常 UNION 链衔接异常 的最靠前行 id。
    Long total = jdbc.queryForObject("SELECT count(*) FROM app_audit", Long.class);
    Long chained = jdbc.queryForObject(
        "SELECT count(*) FROM app_audit WHERE chunk_hash IS NOT NULL", Long.class);
    Long tampered = jdbc.query(
        """
        SELECT MIN(x.id) FROM (
          SELECT id FROM app_audit WHERE chunk_hash IS NOT NULL
            AND chunk_hash <> scheduler_audit_chunk(prev_hash, operator, action, target_type, target_id,
                                                    occurred_at, meta, source, diff, before_meta)
          UNION ALL
          SELECT cur.id FROM app_audit cur JOIN app_audit prev ON prev.id = cur.id - 1
            WHERE cur.prev_hash IS DISTINCT FROM prev.chunk_hash
        ) x""",
        rs -> rs.next() ? (rs.getObject(1) == null ? null : rs.getLong(1)) : null);
    return new AuditIntegrity(total == null ? 0 : total, chained == null ? 0 : chained, tampered);
  }

  @Override
  public long archiveOlderThan(Instant cutoff, int limit) {
    // 归档即删除+重链:同事务内 INSERT→DELETE→重链。以本事务 now() 写入 archived_at,并只删该批次
    // (archived_at = 当前最大),避免并发归档误删彼此批次;随后 scheduler_audit_rechain() 重算剩余行,
    // 使删除不破坏取证链(integrity() 仍 verified)。LIMIT 按 id 升序截断单次删除行数,供预算控制。
    return tx.execute(status -> {
      long copied = jdbc.update("""
          INSERT INTO app_audit_archive (id, operator, action, target_type, target_id, meta, diff, before_meta,
                                         source, occurred_at, prev_hash, chunk_hash, archived_at)
          SELECT id, operator, action, target_type, target_id, meta, diff, before_meta,
                 source, occurred_at, prev_hash, chunk_hash, now()
            FROM app_audit
           WHERE id IN (SELECT id FROM app_audit WHERE occurred_at < ? ORDER BY id LIMIT ?)""",
          Timestamp.from(cutoff), limit);
      if (copied > 0) {
        jdbc.update("""
            DELETE FROM app_audit
             WHERE id IN (SELECT id FROM app_audit_archive
                           WHERE archived_at = (SELECT max(archived_at) FROM app_audit_archive))""");
        jdbc.queryForObject("SELECT scheduler_audit_rechain()", Long.class);
      }
      return copied;
    });
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