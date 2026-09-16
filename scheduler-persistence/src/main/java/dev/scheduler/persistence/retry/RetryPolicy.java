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
   * 按任务配置的退避模式计算下次重试前等待时长(封顶 = 任务级 retryCapMs,未设则全局 1h)。
   * fixed: backoff;linear: backoff*attempt;exponential: backoff*2^(attempt-1)(缺省,含 mode 为 null/未知)。
   * 乘法溢出守卫:首次即到顶或必超封顶 → 直接返回 cap,防移位溢出 long。
   */
  public long delayMs(Task task, int attempt) {
    long cap = task.retryCapMs() != null ? task.retryCapMs() : CAP;
    long backoff = task.backoffMs();
    if ("fixed".equals(task.retryMode())) {
      return Math.min(backoff, cap);
    }
    if ("linear".equals(task.retryMode())) {
      if (backoff >= cap) return cap;                    // 首次即到顶
      if (attempt > 1 && backoff > cap / attempt) return cap; // 乘法必超封顶 → 取顶,防溢出
      return Math.min(backoff * attempt, cap);
    }
    // exponential(缺省)
    if (attempt <= 1) {
      return Math.min(backoff, cap);
    }
    long result = backoff;
    for (int i = 1; i < attempt; i++) {
      if (result > cap / 2) {
        return cap;
      }
      result <<= 1;
    }
    return Math.min(result, cap);
  }
}
