package dev.scheduler.server.execute;

import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.handler.ExecutionHandler;
import dev.scheduler.server.handler.CancellationToken;
import dev.scheduler.server.handler.HandlerContext;
import dev.scheduler.server.handler.HandlerRegistry;
import dev.scheduler.server.retry.FailureResolver;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CancellationException;

/**
 * 执行器工作循环:为每个任务找一个 DUE 候选、原子认领、按 handlerRef 派发 handler、写回结果与
 * outcome。在每个节点上独立运行(不依赖选主)。{@link #workOne()} 至多处理一个 execution;
 * 认领失败(他人已认领/配额占满)则跳过,绝不对未认领的 execution 回写。
 */
public class ExecutorWorker {
  private final TaskRepository tasks;
  private final ExecutionRepository execs;
  private final HandlerRegistry handlers;
  private final String workerId;
  private final FailureResolver failureResolver;
  private final Clock clock;

  public ExecutorWorker(TaskRepository tasks, ExecutionRepository execs,
                        HandlerRegistry handlers, String workerId,
                        FailureResolver failureResolver, Clock clock) {
    this.tasks = tasks;
    this.execs = execs;
    this.handlers = handlers;
    this.workerId = workerId;
    this.failureResolver = failureResolver;
    this.clock = clock;
  }

  /** 处理一个 execution,返回是否处理了(找到了候选且认领成功)。 */
  public boolean workOne() {
    for (Task t : tasks.findAll()) {
      var cand = execs.findCandidate(t.id());
      if (cand.isEmpty()) continue;
      Instant lease = clock.instant().plusSeconds(60);
      if (!execs.claim(cand.get().id(), t.id(), workerId, lease, t.maxActiveConcurrent())) {
        continue; // 他人已认领或配额占满 → 未认领成功,绝不回写
      }
      ExecutionHandler h;
      try {
        h = handlers.get(t.handlerRef());
      } catch (IllegalArgumentException e) {
        // 未注册 handler 也是失败,走统一失败判定(cand 持 claim 前的尝试号,本次为 +1)。
        failureResolver.handle(t, cand.get().id(), cand.get().attempt() + 1, workerId, e.getMessage());
        return true;
      }
      // 运行前检查:已被请求取消则直接落 CANCELED,不再调度 handler(协作取消前置路径)。
      if (execs.isCancelRequested(cand.get().id())) {
        execs.markStatus(cand.get().id(), ExecutionStatus.CANCELED, workerId, "cancelled before run");
        return true;
      }
      CancellationToken token = new CancellationToken(execs.isCancelRequested(cand.get().id()));
      try {
        h.handle(new HandlerContext(cand.get().id(), cand.get().args(), token));
        // 正常返回后:若期间被请求取消,落 CANCELED;否则 SUCCESS。
        if (execs.isCancelRequested(cand.get().id())) {
          execs.markStatus(cand.get().id(), ExecutionStatus.CANCELED, workerId, "cancelled after run");
        } else {
          execs.markStatus(cand.get().id(), ExecutionStatus.SUCCESS, workerId, "ok");
        }
      } catch (CancellationException cex) {
        // 协作取消信号:distinct 路径,不路由到 failureResolver(不重试/不死信)。
        execs.markStatus(cand.get().id(), ExecutionStatus.CANCELED, workerId, "cancelled by handler");
      } catch (Throwable err) {
        // 普通失败走统一判定:应重试则调度重试,否则 FAILED + 死信。
        failureResolver.handle(t, cand.get().id(), cand.get().attempt() + 1, workerId,
            String.valueOf(err));
      }
      return true;
    }
    return false;
  }
}