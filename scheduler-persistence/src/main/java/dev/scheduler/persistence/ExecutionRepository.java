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

  /** 某任务下租约已过期(lease_until <= DB now)且仍 RUNNING 的孤儿执行;由 Reconciler 逐任务回收。 */
  List<ExpiredRun> findExpiredRunning(long taskId);
}
