package dev.scheduler.persistence;

import java.time.Instant;

/** reconcile_decision 决策原子(append-only):对账器一次回收/汇聚/竞态裁决的留痕。
 *  phase: timeout_reclaim / orphan_reclaim / parent_aggregate / near_miss;
 *  targetType: shard / execution(父);outcome: REQUEUED/DEAD_LETTER(回收)或父终态(汇聚)或 SKIPPED(near-miss)。 */
public record ReconcileDecision(
    long id,
    Instant createdAt,
    String phase,
    String targetType,
    long targetId,
    Long taskId,
    String reason,
    String outcome,
    String fromStatus,
    String toStatus,
    Integer attempt,
    String detail) {

  /** 写入用工厂:id/createdAt 由 DB 生成,置 0/null。 */
  public static ReconcileDecision of(String phase, String targetType, long targetId, Long taskId,
                                     String reason, String outcome, String fromStatus, String toStatus,
                                     Integer attempt, String detail) {
    return new ReconcileDecision(0L, null, phase, targetType, targetId, taskId, reason,
        outcome, fromStatus, toStatus, attempt, detail);
  }
}