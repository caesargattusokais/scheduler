package dev.scheduler.core;

public enum ExecutionStatus {
  NOT_CREATED, DUE, RUNNING, SUCCESS, FAILED, ORPHANED, CANCELED;

  public boolean isTerminal() {
    return this == SUCCESS || this == FAILED || this == CANCELED;
  }
}
