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
    // 自清死行:存活判断全靠读时 30s 窗口(findAllAlive),故 last_seen 早于 60s 的必定非活，
    // 删之零误伤。60s = 2× 读侧 LIVE_WINDOW(AvailableHandlerRefs) + 余量,活行(心跳 ≤10s)永不误删。
    repo.purgeStale(now.minusSeconds(60));
  }
}