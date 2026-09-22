package dev.scheduler.core;

/**
 * 3c 分片聚合/部分成功:把任务声明的成功策略 + 分片终态计数 → 父执行终态。Reconciler(执行级)与
 * DagEngine(节点级)共用同一份推导,避免两处漂移。
 *
 * <p>策略三选一(successPolicyType,另配 successPolicyValue):
 * <ul>
 *   <li>NONE/NULL —— 严格 all-or-nothing:任一失败片 → 未达标;</li>
 *   <li>RATIO_PERCENT —— 成功占比 ≥ value%(value∈[1,99]);</li>
 *   <li>MIN_SUCCESS —— 成功片数 ≥ value;</li>
 *   <li>MAX_FAILURES —— 失败片数 ≤ value。</li>
 * </ul>
 * 终态推导:有取消片且无失败片 → CANCELED(人工意图优先);达标且全成功 → SUCCESS;达标且有失败片 →
 * PARTIAL_SUCCESS;未达标 → FAILED。策略缺省(NONE)时严格复现 old all-or-nothing 语义。 */
public final class SuccessPolicy {
  public static final String NONE = "NONE";
  public static final String RATIO_PERCENT = "RATIO_PERCENT";
  public static final String MIN_SUCCESS = "MIN_SUCCESS";
  public static final String MAX_FAILURES = "MAX_FAILURES";

  private SuccessPolicy() {}

  /** 策略是否「达标」:true 表示该批分片结果被任务接受(tolerated)。NONE/NULL/未知 → failed==0。 */
  public static boolean satisfied(String type, Integer value, long success, long failed, long total) {
    if (type == null || type.isBlank() || type.equals(NONE)) return failed == 0;
    int v = value == null ? 0 : value;
    return switch (type) {
      case RATIO_PERCENT -> success * 100L >= (long) v * total;        // success/total ≥ v/100
      case MIN_SUCCESS -> success >= v;
      case MAX_FAILURES -> failed <= v;
      default -> failed == 0;
    };
  }

  /** 依策略 + 分片终态计数推导父终态;anyCancelled 表示 ≥1 片被人工取消。 */
  public static ExecutionStatus aggregateTerminal(String type, Integer value,
      long success, long failed, long total, boolean anyCancelled) {
    if (anyCancelled && failed == 0) return ExecutionStatus.CANCELED;   // 无失败片的批量取消 → CANCELED
    if (!satisfied(type, value, success, failed, total)) return ExecutionStatus.FAILED;
    return failed == 0 ? ExecutionStatus.SUCCESS : ExecutionStatus.PARTIAL_SUCCESS;
  }
}