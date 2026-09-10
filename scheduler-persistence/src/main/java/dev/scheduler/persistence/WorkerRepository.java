package dev.scheduler.persistence;
import java.time.Instant;
import java.util.List;
public interface WorkerRepository {
  void upsertHeartbeat(WorkerRegistration w);
  List<WorkerRegistration> findAllAlive(Instant lastSeenAtLeast);
}