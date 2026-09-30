package dev.scheduler.persistence;
import java.time.Instant;
import java.util.List;
public interface WorkerRepository {
  void upsertHeartbeat(WorkerRegistration w);
  List<WorkerRegistration> findAllAlive(Instant lastSeenAtLeast);

  /** E2-B abandon 安全前门:该 worker 是否当前 ALIVE(按 DB 时钟 now()-窗口判定,与 stuck 判定同源;
   *  不用 JVM 时钟,避免服务器与 DB 时钟偏斜导致误判活体为死而解除属权 → 双跑)。 */
  boolean isAlive(String workerId, int staleAfterSeconds);

  /** 停机/退位:把 last_seen 置极早,使本 worker 立即离开 findAllAlive(now-窗口)视界,在途分片立被他方认领。 */
  void markOffline(String workerId, Instant at);

  /** 删除 last_seen 早于给定阈值的存量行(心跳方在写入后顺手自清死行)。 */
  void purgeStale(Instant olderThan);
}