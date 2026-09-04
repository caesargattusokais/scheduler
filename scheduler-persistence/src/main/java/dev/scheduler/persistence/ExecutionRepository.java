package dev.scheduler.persistence;
import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface ExecutionRepository {

  /** 对账器扫描到的孤儿 RUNNING:租约已过期仍 RUNNING。只投影对账所需的 id 与 attempt。 */
  record ExpiredRun(long id, int attempt) {}

  long createDue(Execution e);
  Optional<Execution> findCandidate(long taskId);
  boolean claim(long executionId, long taskId, String workerId,
                Instant leaseUntil, int maxConcurrent);
  Optional<Execution> findById(long id);
  long countActive(long taskId);
  /** 显式状态迁移(非持有者专属):对账回收/控制台取消用。CAS 0 行=行已被他方改走 → 静默返回 false,不落误导性 outcome。 */
  boolean markStatus(long id, ExecutionStatus to, String workerId, String detail);
  /**
   * 持有着专属的终态回写:worker 用它写回自己的成功/失败/取消。除要求状态可迁移外,还要求行仍归
   * {@code ownerWorkerId} 所有(worker_id 匹配)。若行已不在可迁移状态或已被他方(如 reconciler 回收后
   * 另一 worker 重新认领)接管 → 返回 false 且不落 outcome,静默丢弃该次回写。
   */
  boolean markStatusOwned(long id, ExecutionStatus to, String ownerWorkerId, String detail);
  void scheduleRetry(long id, Instant retryAt, String detail);
  void markDeadLetter(long id, String detail);

  /** 协作取消请求:仅置 cancel_requested 标志(不动状态),仅 RUNNING 生效;非 RUNNING 返回 false。不落 outcome。 */
  boolean requestCancel(long id);

  /** worker 轮询该行是否已被请求取消;列默认 false。 */
  boolean isCancelRequested(long id);

  /** DLQ 读取:只返回 FAILED 且已标记 dead_letter 的执行,按 id 升序。 */
  List<Execution> findDeadLetters();

  /** DLQ 手动重新入队:FAILD → DUE 复位 attempt=0/next_retry_at=NULL/dead_letter=false,同事务落
   *  DUE(detail='requeue') outcome。CAS on status='FAILED':0 行=已非 FAILED(竞态/终态)时静默返回
   *  false,不落误导性 outcome。 */
  boolean requeue(long id);

  /** 某任务下租约已过期(lease_until <= DB now)且仍 RUNNING 的孤儿执行;由 Reconciler 逐任务回收。 */
  List<ExpiredRun> findExpiredRunning(long taskId);
}
