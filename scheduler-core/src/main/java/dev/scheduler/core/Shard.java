package dev.scheduler.core;
import java.time.Instant;
/** 一个分片执行实例(真实认领/运行单元)。字段对齐 M3 spec §1.2。 */
public record Shard(
    Long id, Long executionId, int shardIndex, String shardData, ExecutionStatus status,
    int attempt, String workerId, Instant leaseUntil, Instant nextRetryAt,
    boolean cancelRequested, boolean deadLetter, Instant startedAt, Instant finishedAt,
    String resultPayload) {
  public static Shard ofDue(long executionId, int shardIndex) {
    return new Shard(null, executionId, shardIndex, null, ExecutionStatus.DUE,
        0, null, null, null, false, false, null, null, null);
  }
}