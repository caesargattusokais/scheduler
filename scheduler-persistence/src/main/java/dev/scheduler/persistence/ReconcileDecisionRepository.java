package dev.scheduler.persistence;

import java.util.List;

/** reconcile_decision 决策原子仓库:append-only 写入 + 只读查询(指标 gauge + 活动视图)。 */
public interface ReconcileDecisionRepository {
  /** 追加一条决策(id/created_at 由 DB 生成)。 */
  void record(ReconcileDecision d);

  /** 近 1h 各 phase 的决策计数(单条 GROUP BY;DB now() - interval '1 hour',单一时钟,勿传 JVM 时钟)。
   *  无该 phase 的行不出现在 map 里(调用方按 0 处理)。 */
  java.util.Map<String, Long> countPhasesLastHour();

  /** 活动视图:按 created_at DESC, id DESC 倒序分页。 */
  List<ReconcileDecision> findRecent(int limit, int offset);

  /** 活动全量计数(分页 total)。 */
  long count();
}