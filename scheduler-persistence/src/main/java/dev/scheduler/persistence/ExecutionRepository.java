package dev.scheduler.persistence;
import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import java.time.Instant;
import java.util.Optional;

public interface ExecutionRepository {
  long createDue(Execution e);
  Optional<Execution> findCandidate(long taskId);
  boolean claim(long executionId, long taskId, String workerId,
                Instant leaseUntil, int maxConcurrent);
  Optional<Execution> findById(long id);
  long countActive(long taskId);
  void markStatus(long id, ExecutionStatus to, String workerId, String detail);
  void scheduleRetry(long id, Instant retryAt, String detail);
  void markDeadLetter(long id, String detail);
}
