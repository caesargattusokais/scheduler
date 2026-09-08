package dev.scheduler.core;

public final class DagRunNodeTransitions {
  private DagRunNodeTransitions() {}

  /** 节点迁移:PENDING→RUNNING/SKIPPED/CANCELED;RUNNING→终态;终态不可逆。 */
  public static boolean canTransition(DagRunNodeStatus from, DagRunNodeStatus to) {
    return switch (from) {
      case PENDING  -> to == DagRunNodeStatus.RUNNING || to == DagRunNodeStatus.SKIPPED
                    || to == DagRunNodeStatus.CANCELED;
      case RUNNING  -> to == DagRunNodeStatus.SUCCESS || to == DagRunNodeStatus.FAILED
                    || to == DagRunNodeStatus.CANCELED;
      case SUCCESS, FAILED, CANCELED, SKIPPED -> false;
    };
  }

  /** run 迁移:仅 PENDING→终态;终态不可逆(取消是最终语义)。 */
  public static boolean canRunTransition(DagRunStatus from, DagRunStatus to) {
    return from == DagRunStatus.PENDING && to.isTerminal();
  }
}
