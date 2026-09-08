package dev.scheduler.core;

/** dag_run 状态:存储仅 PENDING(创建)→ 终态;RUNNING 绝不落库,是读侧派生(spec §1.3)。 */
public enum DagRunStatus {
  PENDING, SUCCESS, FAILED, CANCELED;

  public boolean isTerminal() {
    return this == SUCCESS || this == FAILED || this == CANCELED;
  }
}
