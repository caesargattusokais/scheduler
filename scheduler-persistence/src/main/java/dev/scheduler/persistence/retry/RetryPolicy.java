package dev.scheduler.persistence.retry;

import dev.scheduler.core.Task;
import java.util.regex.Pattern;

/**
 * 重试决策纯类(无 DB、无时钟,只决策不执行)。
 * 职责:{@link #shouldRetry} 判定某次失败是否可重试;{@link #delayMs} 计算下次重试前等待时长。
 */
public class RetryPolicy {
  /** 退避封顶:1 小时(毫秒)。 */
  static final long CAP = 3600_000L;

  /**
   * 是否应重试:attempt 未超过 maxRetries,且(pattern 为 null/空白表示不过滤,或 pattern 在
   * failureDetail 上命中)。pattern 非空且 failureDetail 为 null 时视为不命中。
   */
  public boolean shouldRetry(Task task, int attempt, String failureDetail) {
    if (attempt > task.maxRetries()) {
        return false;
    }
    String pat = task.retryableFailurePattern();
    if (pat == null || pat.isBlank()) {
        return true;
    }
    if (failureDetail == null) {
        return false;
    }
    return Pattern.compile(pat).matcher(failureDetail).find();
  }

  /**
   * 指数退避等待时长:backoffMs * 2^(attempt-1),封顶 1 小时。
   * 移位溢出 long 或超过上限时返回封顶值。
   */
  public long delayMs(long backoffMs, int attempt) {
    if (attempt <= 1) {
        return Math.min(backoffMs, CAP);
    }
    long result = backoffMs;
    for (int i = 1; i < attempt; i++) {
      if (result > CAP / 2) {
          return CAP;
      }
      result <<= 1;
    }
    return Math.min(result, CAP);
  }
}
