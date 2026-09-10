package dev.scheduler.persistence.retry;

import dev.scheduler.core.Task;
import dev.scheduler.persistence.ShardRepository;
import java.time.Clock;

/**
 * 分片执行失败后的统一判定:按重试策略决定失败后走「重试」还是「死信」。
 * 纯决策 + 一次判定的组合,供 worker 与 reconciler 复用同一套判定逻辑。
 * RUNNING→FAILED 由调用方落定(worker 经 markStatusOwned,reconciler 经 markStatus);此处只做重试/死信决策。
 * 因此 handle 假定行已是 FAILED:scheduleRetry 的 CAS 要求 status='FAILED',markDeadLetter 亦只作用于 FAILED 行。
 *
 * <p>M3(R1):作用对象由 execution 改为 execution_shard 分片 —— handle 的 id 参数现指向 shard 表。
 */
public class FailureResolver {
  private final ShardRepository shards;
  private final RetryPolicy retryPolicy;
  private final Clock clock;

  public FailureResolver(ShardRepository shards, RetryPolicy retryPolicy, Clock clock) {
    this.shards = shards;
    this.retryPolicy = retryPolicy;
    this.clock = clock;
  }

  /** 决定一条已 FAILED 分片的下场。attempt = 本次失败运行的尝试号(post-claim),用于 shouldRetry 越界与退避指数。
   *  @param task    失败所属的任务定义(携带 maxRetries/backoff/pattern)
   *  @param shardId 失败的分片 id(行须已 FAILED,由调用方保证)
   *  @param attempt 本次失败的尝试号:>=1
   *  @param detail  失败原因描述,兼作重试/死信 outcome 的 reason
   */
  public void handle(Task task, long shardId, int attempt, String detail) {
    if (retryPolicy.shouldRetry(task, attempt, detail)) {
      shards.scheduleRetry(shardId,
          clock.instant().plusMillis(retryPolicy.delayMs(task.backoffMs(), attempt)), detail);
    } else {
      shards.markDeadLetter(shardId, "dlq:" + detail);
      // FAIL_FAST(§5):分片进入 DLQ 即意味着该批已失败,协作取消同父仍在 RUNNING/DUE 的兄弟,
      // 避免整批空耗。仅处于 DLQ 分支时触发(仍可重试的分片不取消兄弟)。
      shards.cancelSiblings(shardId, "fail_fast");
    }
  }
}
