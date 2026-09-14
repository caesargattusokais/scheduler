package dev.scheduler.persistence;
import java.time.Instant;
import java.util.List;
public interface WorkerRepository {
  void upsertHeartbeat(WorkerRegistration w);
  List<WorkerRegistration> findAllAlive(Instant lastSeenAtLeast);

  /** 删除 last_seen 早于给定阈值的存量行(心跳方在写入后顺手自清死行)。 */
  void purgeStale(Instant olderThan);
}