package dev.scheduler.core;

/** dag_run_node 状态:真实运行单元状态机。SKIPPED 仅从 PENDING 可达(上游失败,绝不 spawn)。 */
public enum DagRunNodeStatus {
  PENDING, RUNNING, SUCCESS, FAILED, CANCELED, SKIPPED;

  public boolean isTerminal() {
    return this == SUCCESS || this == FAILED || this == CANCELED || this == SKIPPED;
  }
}
