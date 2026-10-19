package dev.scheduler.worker.registration;

import dev.scheduler.persistence.WorkerRegistration;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.worker.handler.HandlerRegistry;
import java.time.Clock;
import java.time.Instant;

/** 向共享 DB 注册本 worker 并周期心跳:上报服务的 handler refs + 活性。 */
public class WorkerRegistrar {
  private final WorkerRepository repo;
  private final HandlerRegistry registry;
  private final String workerId;
  private final Clock clock;

  public WorkerRegistrar(WorkerRepository repo, HandlerRegistry registry, String workerId, Clock clock) {
    this.repo = repo;
    this.registry = registry;
    this.workerId = workerId;
    this.clock = clock;
  }

  public void heartbeat() {
    Instant now = clock.instant();
    repo.upsertHeartbeat(new WorkerRegistration(workerId, registry.refs(), now, "ALIVE"));
    // 自清死行:存活判断全靠读时窗口(findAllAlive),故 last_seen 早于 60s 的必定非活,删之零误伤。
    // 60s = 2× 读侧 LIVE_WINDOW(AvailableHandlerRefs) + 余量,活行(心跳 ≤10s)永不误删。
    // ⚠ 已知边界:这个硬编码 60s 实际上封顶了活性窗——回收侧 findExpiredRunning 把「worker 行缺失」判为死,
    //   故把 scheduler.worker.stale-after-seconds 调到 >60s 不会真的延长回收窗(行先被本清理删掉,孤儿提前被回收)。
    //   调窗上限按 60s 计;要真正放开需让清理阈值随该属性放大。
    repo.purgeStale(now.minusSeconds(60));
  }

  /** 停机排空完成后调用:把 last_seen 置极早,本 worker 立即离开存活视界,在途分片立可他 worker 认领。幂等。 */
  public void deregister() {
    repo.markOffline(workerId, clock.instant().minusSeconds(600));
  }
}