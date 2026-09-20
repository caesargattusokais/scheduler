package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.core.AuditIntegrity;
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

    assertEquals(4, audits.count(null, null, null, null, null, null, null, null));
    assertEquals(4, audits.findPage(null, null, null, null, null, null, null, null, 100, 0).size());

    // operator 子串(ILIKE)
    assertEquals(3, audits.count("ali", null, null, null, null, null, null, null));
    // action 等值
    assertEquals(2, audits.count(null, "task.create", null, null, null, null, null, null));
    // targetType 等值
    assertEquals(1, audits.count(null, null, "dag", null, null, null, null, null));
    // targetId 等值
    assertEquals(1, audits.count(null, null, null, 9L, null, null, null, null));
    // 组合精确定位 (task, 1)
    assertEquals(2, audits.count(null, null, "task", 1L, null, null, null, null));
    // 时间窗(occurred_at >= from AND <= to)
    assertEquals(4, audits.count(null, null, null, null,
        Instant.parse("2000-01-01T00:00:00Z"), Instant.parse("2200-01-01T00:00:00Z"), null, null));
    assertEquals(0, audits.count(null, null, null, null,
        Instant.parse("2200-01-01T00:00:00Z"), Instant.parse("2300-01-01T00:00:00Z"), null, null));

    // 分页 + 倒序:limit=2 → 最近 2 条(id DESC 平局 → dag.create, 再 bob 的 task.create)
    List<AuditEntry> p2 = audits.findPage(null, null, null, null, null, null, null, null, 2, 0);
    assertEquals(2, p2.size());
    assertEquals("dag.create", p2.get(0).action());
    assertEquals(9L, p2.get(0).targetId());
    assertEquals("task.create", p2.get(1).action());
    assertEquals(2L, p2.get(1).targetId());
    // offset=2 → 剩余 2 条仍按 id DESC:后插者 task.update(id2) 在前,task.create(id1) 在后
    List<AuditEntry> pOff = audits.findPage(null, null, null, null, null, null, null, null, 100, 2);
    assertEquals(2, pOff.size());
    assertEquals("task.update", pOff.get(0).action());
    assertEquals(1L, pOff.get(0).targetId());
    assertEquals("task.create", pOff.get(1).action());
  }

  @Test
  void record_withDiffAndBefore_storesColumns_andReadBackViaFindPage() {
    // 8-arg record 带 meta + diff + before(均 JSON 文本;diff 形如 {field:[before,after]},before 为全量前态)
    audits.record("alice", "task.update", TargetType.TASK, 5L,
        "{\"name\":\"after\",\"shardCount\":1}",
        "{\"cron\":[\"0 * * * * ?\",\"0 */5 * * * ?\"],\"retryCapMs\":[null,10000]}",
        "{\"name\":\"before\",\"shardCount\":1,\"paused\":false}", "cli");
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_audit WHERE id=? AND diff IS NOT NULL AND before_meta IS NOT NULL",
        Long.class, jdbc.queryForObject("SELECT id FROM app_audit WHERE action='task.update'", Long.class)));
    List<AuditEntry> rows = audits.findPage(null, "task.update", null, null, null, null, null, null, 10, 0);
    assertEquals(1, rows.size());
    AuditEntry e = rows.get(0);
    assertEquals("{\"cron\":[\"0 * * * * ?\",\"0 */5 * * * ?\"],\"retryCapMs\":[null,10000]}".replaceAll("\\s+", ""),
                 e.diff().replaceAll("\\s+", ""));
    // jsonb 不保键序(canonical 按 key 长度排序)且::text 在冒号后补空格,无法整串等值;
    // 先去空白再逐键 contains 校验完整回读(见 brief 陷阱)。
    String b = e.before().replaceAll("\\s+", "");
    assertTrue(b.contains("\"name\":\"before\"") && b.contains("\"shardCount\":1")
        && b.contains("\"paused\":false"));
    assertEquals("{\"name\":\"after\",\"shardCount\":1}".replaceAll("\\s+", ""), e.meta().replaceAll("\\s+", ""));
    assertEquals("cli", e.source());
  }

  @Test
  void record_withoutBefore_beforeColumnIsSqlNull() {
    audits.record("bob", "task.create", TargetType.TASK, 3L, null, null);
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE before_meta IS NULL AND diff IS NULL", Long.class));
  }

  @Test
  void record_withoutDiff_diffColumnIsSqlNull() {
    // 既有 6-arg 走 default 委托 → diff 落 NULL(未启用 diff 的动作/旧行语义)
    audits.record("bob", "task.create", TargetType.TASK, 3L, null, null);
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE diff IS NULL", Long.class));
    assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE diff IS NOT NULL", Long.class));
  }

  @Test
  void findPage_hasDiffFiltersToRowsWithActualChanges_only() {
    // 三态:非空 diff(有实际变更)、{} (no-op update)、NULL (未启用 diff 的动作)
    audits.record("alice", "task.update", TargetType.TASK, 1L, "{\"name\":\"a\"}",
        "{\"cron\":[\"x\",\"y\"]}", null, null);                       // 非空 → 命中
    audits.record("alice", "task.update", TargetType.TASK, 2L, "{\"name\":\"b\"}",
        "{}", null, null);                                             // {} → 排除
    audits.record("bob", "task.create", TargetType.TASK, 3L, null, null); // NULL → 排除

    assertEquals(3, audits.count(null, null, null, null, null, null, null, null));
    // hasDiff=true → 仅真正改了字段的非空 diff 行
    assertEquals(1, audits.count(null, null, null, null, null, null, Boolean.TRUE, null));
    assertEquals(1, audits.findPage(null, null, null, null, null, null, Boolean.TRUE, null, 10, 0).size());
    // hasDiff=null 不过滤
    assertEquals(3, audits.count(null, null, null, null, null, null, null, null));
    // diffField 顶层键存在 → 仅含该键的行;{} / NULL / 别字段行排除
    assertEquals(1, audits.count(null, null, null, null, null, null, null, "cron"));
    assertEquals("{\"cron\":[\"x\",\"y\"]}".replaceAll("\\s+", ""),
        audits.findPage(null, null, null, null, null, null, null, "cron", 10, 0).get(0).diff()
            .replaceAll("\\s+", ""));
    assertEquals(0, audits.count(null, null, null, null, null, null, null, "shardCount"));
    // 组合:hasDiff=true 且改过某字段
    assertEquals(1, audits.count(null, null, null, null, null, null, Boolean.TRUE, "cron"));
    // 未命中字段 + hasDiff 组合 → 0
    assertEquals(0, audits.count(null, null, null, null, null, null, Boolean.TRUE, "shardCount"));
  }

  @Test
  void findPage_beforeAndMetaFieldFilterAcrossColumns_jointAnd() {
    // 三行 before/meta/diff 各异,验证 beforeField/metaField 单列过滤 + 与 diffField 跨列 AND 联合检索。
    // 行1:before 含 shardCount,meta 含 shardCount,diff 含 cron(有实际变更)
    audits.record("alice", "task.update", TargetType.TASK, 1L,
        "{\"name\":\"after\",\"shardCount\":1}",
        "{\"cron\":[\"x\",\"y\"]}",
        "{\"name\":\"before\",\"shardCount\":2,\"paused\":false}", "cli");
    // 行2:before 含 cron,meta 含 cron,无 diff(pause 类动作)
    audits.record("bob", "dag.pause", TargetType.DAG, 9L,
        "{\"name\":\"dagb\",\"cron\":\"0 *\"}",
        null,
        "{\"name\":\"dagbf\",\"cron\":\"0 *\"}", "cli");
    // 行3:仅 meta,null diff,null before(6-arg:meta + source,null)
    audits.record("carol", "execution.cancel", TargetType.EXECUTION, 3L, "{\"name\":\"c\"}", null);

    assertEquals(3, audits.count(null, null, null, null, null, null, null, null, null, null));
    // beforeField → 过滤 before_meta 顶层键存在(行3 无 before → 排除)
    assertEquals(1, audits.count(null, null, null, null, null, null, null, null, "shardCount", null));
    assertEquals(1, audits.count(null, null, null, null, null, null, null, null, "cron", null));
    assertEquals(2, audits.count(null, null, null, null, null, null, null, null, "name", null));
    // metaField → 过滤 meta 顶层键存在
    assertEquals(1, audits.count(null, null, null, null, null, null, null, null, null, "cron"));
    assertEquals(1, audits.count(null, null, null, null, null, null, null, null, null, "shardCount"));
    assertEquals(3, audits.count(null, null, null, null, null, null, null, null, null, "name"));
    // 跨列 AND:diff 含 cron 且 before 含 shardCount → 行1
    assertEquals(1, audits.count(null, null, null, null, null, null, null, "cron", "shardCount", null));
    // 跨列 AND:before 含 shardCount 且 meta 含 shardCount → 行1(行2 before 无 shardCount,行3 无 before)
    assertEquals(1, audits.count(null, null, null, null, null, null, null, null, "shardCount", "shardCount"));
    // 跨列 AND:未命中组合 → 0(before 含 shardCount 且 meta 含 cron:行1 meta 无 cron,行2 before 无 shardCount)
    assertEquals(0, audits.count(null, null, null, null, null, null, null, null, "shardCount", "cron"));
    // findPage 12-arg 读回被命中行,验证 diff/before/meta 出参合一(行1)
    AuditEntry hit = audits.findPage(null, null, null, null, null, null, null, "cron", "shardCount", null, 10, 0).get(0);
    assertEquals("{\"cron\":[\"x\",\"y\"]}".replaceAll("\\s+", ""), hit.diff().replaceAll("\\s+", ""));
    String b = hit.before().replaceAll("\\s+", "");
    assertTrue(b.contains("\"name\":\"before\"") && b.contains("\"shardCount\":2") && b.contains("\"paused\":false"));
    assertEquals("alice", hit.operator());
    assertEquals(1L, hit.targetId());
  }

  @Test
  void record_buildsTamperEvidenceChain_firstRowPrevNull() {
    // 三条写入(第2条带 diff/before,覆盖全列哈希)。
    audits.record("alice", "task.create", TargetType.TASK, 1L, "{\"name\":\"a\"}", "cli");
    audits.record("bob", "task.update", TargetType.TASK, 2L,
        "{\"name\":\"b\",\"shardCount\":1}", "{\"cron\":[\"x\",\"y\"]}",
        "{\"name\":\"beforeB\",\"paused\":true}", "cli");
    audits.record("carol", "dag.pause", TargetType.DAG, 9L, "{\"name\":\"c\"}", "scheduler");

    AuditIntegrity ok = audits.integrity();
    assertEquals(3, ok.totalRecords());
    assertEquals(3, ok.chainedRecords());
    assertEquals(null, ok.firstTamperedId());
    assertTrue(ok.isVerified(), "全量入链且无篡改 → verified");

    // 链衔接:id 升序下,行n+1.prev_hash == 行n.chunk_hash(首行 prev NULL)。
    List<Map<String, Object>> rows = jdbc.queryForList(
        "SELECT id, prev_hash, chunk_hash FROM app_audit ORDER BY id");
    assertEquals(3, rows.size());
    assertEquals(null, rows.get(0).get("prev_hash"), "首行链头 prev_hash 应为 NULL");
    assertEquals(rows.get(0).get("chunk_hash"), rows.get(1).get("prev_hash"));
    assertEquals(rows.get(1).get("chunk_hash"), rows.get(2).get("prev_hash"));
    // 自身哈希与 DB 同源函数重算一致(SQL helper 是写入/校验的单一实现源)。
    assertEquals(rows.get(1).get("chunk_hash"), jdbc.queryForObject(
        "SELECT scheduler_audit_chunk(prev_hash, operator, action, target_type, target_id,"
            + " occurred_at, meta, source, diff, before_meta) FROM app_audit WHERE id=?",
        String.class, rows.get(1).get("id")));
  }

  @Test
  void integrity_tamperingAMiddleRow_flagsItsId() {
    audits.record("alice", "task.create", TargetType.TASK, 1L, "{\"name\":\"a\"}", "cli");
    audits.record("bob", "task.update", TargetType.TASK, 2L, "{\"name\":\"b\"}", "cli");
    audits.record("carol", "dag.pause", TargetType.DAG, 9L, "{\"name\":\"c\"}", "cli");
    long midId = jdbc.queryForObject("SELECT id FROM app_audit WHERE action='task.update'", Long.class);

    jdbc.update("UPDATE app_audit SET meta='{\"name\":\"evil\"}'::jsonb WHERE id=?", midId);

    AuditIntegrity bad = audits.integrity();
    assertTrue(bad.totalRecords() == 3 && bad.chainedRecords() == 3);
    assertFalse(bad.isVerified(), "改过数据行 → 不通过");
    assertEquals(midId, bad.firstTamperedId().longValue(), "首个被篡改行应指向改动的 id");
  }

  @Test
  void integrity_untamperedRow_recomputeMatchesAfterIdempotentReinsert() {
    // 冒烟:改后把 meta 改回原值,self-hash 重新一致 → verified 恢复(篡改检测为"内容与哈希不符")。
    audits.record("alice", "task.create", TargetType.TASK, 1L, "{\"name\":\"a\"}", "cli");
    long id = jdbc.queryForObject("SELECT id FROM app_audit", Long.class);
    jdbc.update("UPDATE app_audit SET meta='{\"name\":\"evil\"}'::jsonb WHERE id=?", id);
    assertFalse(audits.integrity().isVerified());
    jdbc.update("UPDATE app_audit SET meta='{\"name\":\"a\"}'::jsonb WHERE id=?", id);
    assertTrue(audits.integrity().isVerified(), "改回原值后内容与哈希恢复一致");
  }
}