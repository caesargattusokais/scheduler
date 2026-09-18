package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.core.TargetType;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcAuditRepositoryTest extends AbstractPostgresTest {
  private AuditRepository audits;

  @BeforeEach
  void clean() {
    jdbc.execute("TRUNCATE app_audit RESTART IDENTITY CASCADE");
    audits = new JdbcAuditRepository(jdbc);
  }

  @Test
  void record_appendsSingleRow_occurredAtFromDbNowMetaJson() {
    audits.record("ops", "task.create", TargetType.TASK, 7L, "{\"name\":\"x\"}", "cli");
    List<Map<String, Object>> rows = jdbc.queryForList(
        "SELECT operator, action, target_type, target_id, meta, source, occurred_at FROM app_audit");
    assertEquals(1, rows.size());
    Map<String, Object> r = rows.get(0);
    assertEquals("ops", r.get("operator"));
    assertEquals("task.create", r.get("action"));
    assertEquals("task", r.get("target_type"));
    assertEquals(7L, r.get("target_id"));
    assertTrue(r.get("meta") != null, "meta 应落库:" + r.get("meta"));
    assertEquals("cli", r.get("source"));
    assertTrue(r.get("occurred_at") instanceof Timestamp, "occurred_at 应由 DB now() 回填");
  }

  @Test
  void record_nullMetaAndSource_storeSqlNull() {
    audits.record("a", "task.pause", TargetType.TASK, 1L, null, null);
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE operator='a'", Long.class));
    assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE meta IS NOT NULL", Long.class));
    assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE source IS NOT NULL", Long.class));
  }

  @Test
  void findPage_filtersAndPagination_descByOccurredAt() {
    // 同 tick 插入:occurred_at=now() 相同,以 id DESC 稳定平局(后插者在先)。
    audits.record("alice", "task.create", TargetType.TASK, 1L, "{\"name\":\"a\"}", null);
    audits.record("alice", "task.update", TargetType.TASK, 1L, null, null);
    audits.record("bob",   "task.create", TargetType.TASK, 2L, null, null);
    audits.record("alice", "dag.create",  TargetType.DAG,  9L, null, null); // id 最大 → 倒序首位

    assertEquals(4, audits.count(null, null, null, null, null, null));
    assertEquals(4, audits.findPage(null, null, null, null, null, null, 100, 0).size());

    // operator 子串(ILIKE)
    assertEquals(3, audits.count("ali", null, null, null, null, null));
    // action 等值
    assertEquals(2, audits.count(null, "task.create", null, null, null, null));
    // targetType 等值
    assertEquals(1, audits.count(null, null, "dag", null, null, null));
    // targetId 等值
    assertEquals(1, audits.count(null, null, null, 9L, null, null));
    // 组合精确定位 (task, 1)
    assertEquals(2, audits.count(null, null, "task", 1L, null, null));
    // 时间窗(occurred_at >= from AND <= to)
    assertEquals(4, audits.count(null, null, null, null,
        Instant.parse("2000-01-01T00:00:00Z"), Instant.parse("2200-01-01T00:00:00Z")));
    assertEquals(0, audits.count(null, null, null, null,
        Instant.parse("2200-01-01T00:00:00Z"), Instant.parse("2300-01-01T00:00:00Z")));

    // 分页 + 倒序:limit=2 → 最近 2 条(id DESC 平局 → dag.create, 再 bob 的 task.create)
    List<AuditEntry> p2 = audits.findPage(null, null, null, null, null, null, 2, 0);
    assertEquals(2, p2.size());
    assertEquals("dag.create", p2.get(0).action());
    assertEquals(9L, p2.get(0).targetId());
    assertEquals("task.create", p2.get(1).action());
    assertEquals(2L, p2.get(1).targetId());
    // offset=2 → 剩余 2 条仍按 id DESC:后插者 task.update(id2) 在前,task.create(id1) 在后
    List<AuditEntry> pOff = audits.findPage(null, null, null, null, null, null, 100, 2);
    assertEquals(2, pOff.size());
    assertEquals("task.update", pOff.get(0).action());
    assertEquals(1L, pOff.get(0).targetId());
    assertEquals("task.create", pOff.get(1).action());
  }
}