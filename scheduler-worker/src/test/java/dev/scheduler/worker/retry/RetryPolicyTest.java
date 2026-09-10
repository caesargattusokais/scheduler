package dev.scheduler.worker.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.Task;
import org.junit.jupiter.api.Test;

/**
 * {@link RetryPolicy} 纯决策逻辑测试:shouldRetry 各分支 + delayMs 指数退避。
 * 均不依赖 DB/时钟,纯函数单测。
 */
class RetryPolicyTest {
  private final RetryPolicy policy = new RetryPolicy();

  /** 构造最小合法 Task(仅关注 maxRetries / backoffMs / retryableFailurePattern)。 */
  private static Task task(int maxRetries, long backoffMs, String pattern) {
    return new Task(1L, "t", "kind", "handler", null, 1, 60,
        maxRetries, backoffMs, pattern, 1, true, false);
  }

  // ---- shouldRetry ----

  @Test
  void shouldRetry_whenAttemptLeMax() {
    Task t = task(3, 1000L, null);
    assertTrue(policy.shouldRetry(t, 1, "boom"));
    assertTrue(policy.shouldRetry(t, 3, "boom"));
  }

  @Test
  void shouldRetry_whenAttemptGtMax() {
    Task t = task(3, 1000L, null);
    assertFalse(policy.shouldRetry(t, 4, "boom"));
  }

  @Test
  void shouldRetry_whenPatternMatches() {
    Task t = task(3, 1000L, "timeout|conn.*reset");
    assertTrue(policy.shouldRetry(t, 1, "connection reset"));
  }

  @Test
  void shouldRetry_whenPatternDoesNotMatch() {
    Task t = task(3, 1000L, "timeout");
    assertFalse(policy.shouldRetry(t, 1, "permission denied"));
  }

  @Test
  void shouldRetry_whenPatternBlank() {
    Task blank = task(3, 1000L, "");
    assertTrue(policy.shouldRetry(blank, 1, null));
    assertTrue(policy.shouldRetry(blank, 3, "anything"));
  }

  @Test
  void shouldRetry_whenMaxZero() {
    Task t = task(0, 1000L, null);
    assertFalse(policy.shouldRetry(t, 1, "boom"));
  }

  @Test
  void shouldRetry_whenDetailNullWithPattern() {
    Task t = task(3, 1000L, "timeout");
    assertFalse(policy.shouldRetry(t, 1, null));
  }

  // ---- delayMs ----

  @Test
  void delayMs_attempt1() {
    assertEquals(200L, policy.delayMs(200L, 1));
  }

  @Test
  void delayMs_attempt2() {
    assertEquals(400L, policy.delayMs(200L, 2));
  }

  @Test
  void delayMs_attempt3() {
    assertEquals(800L, policy.delayMs(200L, 3));
  }

  @Test
  void delayMs_cappedAtOneHour() {
    // backoff 2^40 ms,attempt=40 → 移位溢出 long,应封顶 1 小时。
    assertEquals(3600_000L, policy.delayMs(1L << 40, 40));
    // backoff 2^30 ms → 2^69 超出 long,仍封顶。
    assertEquals(3600_000L, policy.delayMs(1L << 30, 40));
  }
}