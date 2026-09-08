package dev.scheduler.core;
import java.time.Instant;
import java.util.UUID;

public final class IdempotencyKeys {
  private IdempotencyKeys() {}

  /** task + 触发时刻(毫秒) → 父 execution 的全局幂等键。父为该次触发的唯一代表(去掉 shardIndex 段)。 */
  public static String forTrigger(long taskId, Instant triggerAt) {
    return taskId + ":" + triggerAt.toEpochMilli();
  }

  /** dag 调度触发 → dag_run 全局幂等键(镜像 forTrigger 的 task+epochMs 形态)。 */
  public static String forDagTrigger(long dagId, Instant triggerAt) {
    return "dag:" + dagId + ":" + triggerAt.toEpochMilli();
  }

  /** dag 手动触发 → dag_run 全局幂等键。 */
  public static String forManualDagRun(long dagId) {
    return "dag:" + dagId + ":manual:" + UUID.randomUUID();
  }
}
