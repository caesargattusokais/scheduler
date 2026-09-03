package dev.scheduler.core;

public final class ExecutionTransitions {
  private ExecutionTransitions() {}

  public static boolean canTransition(ExecutionStatus from, ExecutionStatus to) {
    return switch (from) {
      case DUE       -> to == ExecutionStatus.RUNNING || to == ExecutionStatus.CANCELED;
      case RUNNING   -> to == ExecutionStatus.SUCCESS || to == ExecutionStatus.FAILED || to == ExecutionStatus.CANCELED;
      case FAILED    -> to == ExecutionStatus.DUE; // 重试/人工重跑排回
      case CANCELED  -> to == ExecutionStatus.DUE; // 取消后可重排
      case NOT_CREATED, SUCCESS, ORPHANED -> false;
    };
  }
}