package dev.scheduler.persistence.retry;

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

  private Task task(long backoffMs) { return task(backoffMs, null, null); }
  private Task task(long backoffMs, String mode, Long cap) {
    return new Task(1L, "t", "kind", "handler", null, 1, 60, 3, backoffMs, null, 8, true, false,
        mode, cap, null);
  }

  // ---- delayMs ----

  @Test void exponentialMode_isExistingDefaultBackoff() {
    assertEquals(200L, policy.delayMs(task(200L), 1));
    assertEquals(400L, policy.delayMs(task(200L), 2));
    assertEquals(800L, policy.delayMs(task(200L), 3));
    assertEquals(3600_000L, policy.delayMs(task(1L << 40, "exponential", null), 40), "溢出守卫 → 全局 1h");
    assertEquals(3600_000L, policy.delayMs(task(1L << 30, "exponential", null), 40), "超上限 → 封顶 1h");
  }

  @Test void fixedMode_constantBackoffCapped() {
    assertEquals(200L, policy.delayMs(task(200L, "fixed", null), 1));
    assertEquals(200L, policy.delayMs(task(200L, "fixed", null), 6), "fixed 不随 attempt 增长");
    assertEquals(1000L, policy.delayMs(task(2000L, "fixed", 1000L), 100), "任务级 cap 覆盖 fixed");
  }

  @Test void linearMode_growsWithAttemptCapped() {
    assertEquals(200L, policy.delayMs(task(200L, "linear", null), 1));
    assertEquals(400L, policy.delayMs(task(200L, "linear", null), 2));
    assertEquals(800L, policy.delayMs(task(200L, "linear", null), 4));
    assertEquals(3600_000L, policy.delayMs(task(1L << 50, "linear", null), 40), "linear 乘法溢出守卫 → 全局 1h");
  }

  @Test void perTaskCap_overridesGlobalDefault() {
    assertEquals(4000L, policy.delayMs(task(2000L, "exponential", 6000L), 2), "2^1*2000=4000 未超 cap");
    assertEquals(6000L, policy.delayMs(task(2000L, "exponential", 6000L), 3), "3 次 8000 超 cap 6000 → 取 cap");
    assertEquals(3600_000L, policy.delayMs(task(1L << 30, "exponential", null), 40), "任务未设 cap → 兜底全局 1h");
  }
}