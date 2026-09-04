package dev.scheduler.server.retry;

import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import java.time.Clock;

/**
 * 执行失败后的统一判定:把本次失败落库,再按重试策略决定走「重试」还是「死信」。
 * 纯决策 + 一次判定的组合,供 worker 与后续 reconciler 复用同一套判定逻辑。
 * 注意:内置先落 RUNNING→FAILED 再调度,因为 scheduleRetry 的 CAS 要求 status='FAILED',
 * 而刚失败的 execution 此刻是 RUNNING——若顺序反过来,FAILED CAS 会 0 行静默丢 job。
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

  /** 决定一次失败的执行的下场。attempt = 本次失败运行的尝试号(post-claim)。
   *  @param task   失败所属的任务定义(携带 maxRetries/backoff/pattern)
   *  @param execId 失败的执行 id
   *  @param attempt 本次失败的尝试号:>=1,用于 shouldRetry 越界判定与退避指数
   *  @param workerId 失败发生的执行者
   *  @param detail  失败原因描述,兼作重试/死信 outcome 的 reason
   */
  public void handle(Task task, long execId, int attempt, String workerId, String detail) {
    // RUNNING->FAILED,必须落在 scheduleRetry 之前(见类注释)。
    execs.markStatus(execId, ExecutionStatus.FAILED, workerId, detail);
    if (retryPolicy.shouldRetry(task, attempt, detail)) {
      execs.scheduleRetry(execId,
          clock.instant().plusMillis(retryPolicy.delayMs(task.backoffMs(), attempt)), detail);
    } else {
      execs.markDeadLetter(execId, "dlq:" + detail);
    }
  }
}