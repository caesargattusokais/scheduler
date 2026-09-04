package dev.scheduler.core;
import java.time.Instant;

public final class IdempotencyKeys {
  private IdempotencyKeys() {}

  /** task + 触发时刻(毫秒) → 父 execution 的全局幂等键。父为该次触发的唯一代表(去掉 shardIndex 段)。 */
  public static String forTrigger(long taskId, Instant triggerAt) {
    return taskId + ":" + triggerAt.toEpochMilli();
  }
}