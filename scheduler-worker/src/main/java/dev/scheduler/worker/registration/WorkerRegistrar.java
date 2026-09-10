package dev.scheduler.worker.registration;

import dev.scheduler.persistence.WorkerRegistration;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.worker.handler.HandlerRegistry;
import java.time.Clock;

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
    repo.upsertHeartbeat(new WorkerRegistration(workerId, registry.refs(), clock.instant(), "ALIVE"));
  }
}