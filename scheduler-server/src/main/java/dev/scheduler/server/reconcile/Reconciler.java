package dev.scheduler.server.reconcile;

import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.ExecutionRepository.ExpiredRun;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.retry.FailureResolver;

/**
 * 对账器:回收租约过期的孤儿 RUNNING(id 已认领却迟迟未回写)。
 * scanOnce 逐任务查出 lease_until <= DB now() 的 RUNNING,再委托共享 FailureResolver 做
 * 「落 FAILED + 重试/死信」的统一判定——与 worker 共用同一套重试决策,避免两套漂移。
 * leader 门控不在此处(由 ReconcileLoop.tick 在 scan 前把关),这里只做回收。
 */
public class Reconciler {
  private final TaskRepository tasks;
  private final ExecutionRepository execs;
  private final FailureResolver failureResolver;
  private final String workerId;

  public Reconciler(TaskRepository tasks, ExecutionRepository execs,
                    FailureResolver failureResolver, String workerId) {
    this.tasks = tasks;
    this.execs = execs;
    this.failureResolver = failureResolver;
    this.workerId = workerId;
  }

  /**
   * 对账扫描一次:为每个任务捞出所有租约已过期的 RUNNING(孤儿),逐条按重试策略回收为失败并返回过期行数。
   * 租约判定用 DB now()(服务器时钟一致,与本机墙钟无关)。
   */
  public int scanOnce() {
    int total = 0;
    for (Task task : tasks.findAll()) {
      for (ExpiredRun run : execs.findExpiredRunning(task.id())) {
        // 孤儿 RUNNING 已 claim(status=attempt+1 过),故本行 attempt 即本次运行号,原样传入(不加 1)。
        failureResolver.handle(task, run.id(), run.attempt(), workerId, "lease expired");
        total++;
      }
    }
    return total;
  }
}