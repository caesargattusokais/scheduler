package dev.scheduler.core;
import java.time.Instant;

public final class IdempotencyKeys {
  private IdempotencyKeys() {}

  /** task + 触发时刻(毫秒) + shardIndex → 全局幂等键。确保同一触发不可能出现两个 execution。 */
  public static String forTrigger(long taskId, Instant triggerAt, int shardIndex) {
    return taskId + ":" + triggerAt.toEpochMilli() + ":" + shardIndex;
  }
}
