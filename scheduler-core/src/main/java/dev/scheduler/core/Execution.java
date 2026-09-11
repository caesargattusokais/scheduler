package dev.scheduler.core;
import java.time.Instant;

/** 一次执行实例(高频写入)。字段对齐 spec §1.2;rerunOf 指向被本轮重跑的源轮执行(溯源,非重跑则 null)。 */
public record Execution(
    Long id, Long taskId, ExecutionStatus status, String idempotencyKey,
    String args, int shardIndex, int shardCount,
    int attempt, String workerId, Instant leaseUntil, Instant nextRetryAt,
    Instant startedAt, Instant finishedAt, String resultPayload, Long rerunOf) {

  /** 便捷构造:一次排入 DUE 的新执行。供调度将其登记为待领取(无源轮)。 */
  public static Execution ofDue(long taskId, String idempotencyKey, int shardCount) {
    return new Execution(null, taskId, ExecutionStatus.DUE, idempotencyKey,
        null, 0, shardCount, 0, null, null, null, null, null, null, null);
  }

  /** 便捷构造:重跑某源轮(复制其 args 并携溯源 rerunOf)的新 DUE 执行。 */
  public static Execution ofRerun(long taskId, String idempotencyKey, int shardCount,
                                  String args, long rerunOf) {
    return new Execution(null, taskId, ExecutionStatus.DUE, idempotencyKey,
        args, 0, shardCount, 0, null, null, null, null, null, null, rerunOf);
  }
}
