package dev.scheduler.core;
import java.time.Instant;

/** 一次执行实例(高频写入)。字段对齐 spec §1.2。 */
public record Execution(
    Long id, Long taskId, ExecutionStatus status, String idempotencyKey,
    String args, int shardIndex, int shardCount,
    int attempt, String workerId, Instant leaseUntil, Instant nextRetryAt,
    Instant startedAt, Instant finishedAt, String resultPayload) {

  /** 便捷构造:一次排入 DUE 的新执行。供调度将其登记为待领取。 */
  public static Execution ofDue(long taskId, String idempotencyKey, int shardCount) {
    return new Execution(null, taskId, ExecutionStatus.DUE, idempotencyKey,
        null, 0, shardCount, 0, null, null, null, null, null, null);
  }
}