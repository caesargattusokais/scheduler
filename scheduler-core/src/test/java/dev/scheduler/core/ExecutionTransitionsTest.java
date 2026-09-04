package dev.scheduler.core;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class ExecutionTransitionsTest {
  @Test void claimNotAllowedFromSuccess() {
    assertFalse(ExecutionTransitions.canTransition(ExecutionStatus.SUCCESS, ExecutionStatus.RUNNING));
  }
  @Test void claimAllowedFromDue() {
    assertTrue(ExecutionTransitions.canTransition(ExecutionStatus.DUE, ExecutionStatus.RUNNING));
  }
  @Test void successNotTerminalRenamed() {
    assertTrue(ExecutionStatus.SUCCESS.isTerminal());
    assertTrue(ExecutionStatus.FAILED.isTerminal());
    assertFalse(ExecutionStatus.RUNNING.isTerminal());
  }
}
