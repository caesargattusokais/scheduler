package dev.scheduler.server.retry;

import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import java.time.Clock;

/**
 * 执行失败后的统一判定:按重试策略决定失败后走「重试」还是「死信」。
 * 纯决策 + 一次判定的组合,供 worker 与 reconciler 复用同一套判定逻辑。
 * RUNNING→FAILED 由调用方落定(worker 经 markStatusOwned,reconciler 经 markStatus);此处只做重试/死信决策。
 * 因此 handle 假定行已是 FAILED:scheduleRetry 的 CAS 要求 status='FAILED',markDeadLetter 亦只作用于 FAILED 行。
 */
public class FailureResolver {
  private final ExecutionRepository execs;
  private final RetryPolicy retryPolicy;
  private final Clock clock;

  public FailureResolver(ExecutionRepository execs, RetryPolicy retryPolicy, Clock clock) {
    this.execs = execs;
    this.retryPolicy = retryPolicy;
    this.clock = clock;
  }

  /** 决定一次已 FAILED 的执行的下场。attempt = 本次失败运行的尝试号(post-claim),用于 shouldRetry 越界与退避指数。
   *  @param task   失败所属的任务定义(携带 maxRetries/backoff/pattern)
   *  @param execId 失败的执行 id(行须已 FAILED,由调用方保证)
   *  @param attempt 本次失败的尝试号:>=1
   *  @param detail  失败原因描述,兼作重试/死信 outcome 的 reason
   */
  public void handle(Task task, long execId, int attempt, String detail) {
    if (retryPolicy.shouldRetry(task, attempt, detail)) {
      execs.scheduleRetry(execId,
          clock.instant().plusMillis(retryPolicy.delayMs(task.backoffMs(), attempt)), detail);
    } else {
      execs.markDeadLetter(execId, "dlq:" + detail);
    }
  }
}
