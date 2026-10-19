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

  @Test void countPhaseLastHour_onlyCountsMatchingPhaseWithinWindow() {
    repo.record(ReconcileDecision.of("near_miss", "shard", 1L, null, "r", "SKIPPED",
        "RUNNING", "RUNNING", 1, null));
    repo.record(ReconcileDecision.of("parent_aggregate", "execution", 2L, null, "all ok",
        "SUCCESS", "DUE", "SUCCESS", null, null));
    // 拨旧 2h 的 near_miss:落在 1h 窗外,不计入。
    jdbc.update("UPDATE reconcile_decision SET created_at = now() - interval '2 hours' WHERE phase='near_miss'");

    assertEquals(0L, repo.countPhaseLastHour("near_miss"), "1h 窗外的行不计入");
    assertEquals(1L, repo.countPhaseLastHour("parent_aggregate"));
    assertEquals(0L, repo.countPhaseLastHour("timeout_reclaim"));
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