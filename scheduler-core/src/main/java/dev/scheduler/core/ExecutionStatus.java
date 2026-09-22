package dev.scheduler.core;

public enum ExecutionStatus {
  NOT_CREATED, DUE, RUNNING, SUCCESS, PARTIAL_SUCCESS, FAILED, ORPHANED, CANCELED;

  public boolean isTerminal() {
    return this == SUCCESS || this == PARTIAL_SUCCESS || this == FAILED || this == CANCELED;
  }
}
