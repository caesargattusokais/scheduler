package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcReconcileDecisionRepositoryTest extends AbstractPostgresTest {
  private final JdbcReconcileDecisionRepository repo = new JdbcReconcileDecisionRepository(jdbc);

  @BeforeEach void clean() {
    jdbc.update("TRUNCATE reconcile_decision RESTART IDENTITY CASCADE");
  }

  @Test void record_persistsAllColumns_withDbClock() {
    repo.record(ReconcileDecision.of("timeout_reclaim", "shard", 42L, 7L, "runtime timeout",
        "REQUEUED", "RUNNING", "FAILED", 1, "exceeded 300s"));

    ReconcileDecision d = repo.findRecent(10, 0).get(0);

    assertEquals("timeout_reclaim", d.phase());
    assertEquals("shard", d.targetType());
    assertEquals(42L, d.targetId());
    assertEquals(7L, d.taskId());
    assertEquals("REQUEUED", d.outcome());
    assertEquals("RUNNING", d.fromStatus());
    assertEquals("FAILED", d.toStatus());
    assertEquals(1, d.attempt());
    assertEquals("exceeded 300s", d.detail());
    assertNotNull(d.createdAt(), "DB now() 盖章 created_at");
  }

  @Test void countPhasesLastHour_groupsByPhaseWithinWindow() {
    repo.record(ReconcileDecision.of("near_miss", "shard", 1L, null, "r", "SKIPPED",
        "RUNNING", "RUNNING", 1, null));
    repo.record(ReconcileDecision.of("near_miss", "shard", 2L, null, "r", "SKIPPED",
        "RUNNING", "RUNNING", 1, null));
    repo.record(ReconcileDecision.of("parent_aggregate", "execution", 3L, null, "all ok",
        "SUCCESS", "DUE", "SUCCESS", null, null));
    // 拨旧 2h 的 near_miss:落在 1h 窗外,不计入。
    jdbc.update("UPDATE reconcile_decision SET created_at = now() - interval '2 hours' WHERE target_id=1");

    var counts = repo.countPhasesLastHour();

    assertEquals(1L, counts.get("near_miss"), "窗内 1 条(窗外的 target_id=1 被拨旧)");
    assertEquals(1L, counts.get("parent_aggregate"));
    assertNull(counts.get("timeout_reclaim"), "无该 phase → 不出现在结果里(调用方按 0 处理)");
  }

  @Test void findRecent_ordersByCreatedAtDescThenIdDesc_andCounts() {
    repo.record(ReconcileDecision.of("near_miss", "shard", 1L, null, "a", "SKIPPED",
        "RUNNING", "RUNNING", 1, null));
    repo.record(ReconcileDecision.of("near_miss", "shard", 2L, null, "b", "SKIPPED",
        "RUNNING", "RUNNING", 1, null));
    repo.record(ReconcileDecision.of("near_miss", "shard", 3L, null, "c", "SKIPPED",
        "RUNNING", "RUNNING", 1, null));

    var page = repo.findRecent(2, 0);
    assertEquals(2, page.size());
    assertEquals(3L, page.get(0).targetId(), "created_at DESC, id DESC → 最新在前");
    assertEquals(3L, repo.count());
  }
}