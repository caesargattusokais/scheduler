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
  void markStatus(long id, ExecutionStatus to, String workerId, String detail);
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
