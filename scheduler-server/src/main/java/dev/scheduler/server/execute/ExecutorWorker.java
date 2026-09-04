package dev.scheduler.server.execute;

import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ShardRepository;
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
 * 执行器工作循环:为每个任务找一个 DUE 候选分片、原子认领、按 handlerRef 派发 handler、回写
 * 结果与 outcome。在每个节点上独立运行(不依赖选主)。{@link #workOne()} 至多处理一个分片执行;
 * 认领失败(他人已认领/配额占满)则跳过,绝不对未认领的分片回写。M3(本任务):认领/运行/回写
 * 全作用于 execution_shard;worker 只读父 execution(取 args/shardCount),绝不写父行(父由 Reconciler 汇聚)。
 */
public class ExecutorWorker {
  private final TaskRepository tasks;
  private final ShardRepository shards;
  private final HandlerRegistry handlers;
  private final String workerId;
  private final FailureResolver failureResolver;
  private final Clock clock;

  public ExecutorWorker(TaskRepository tasks, ShardRepository shards,
                        HandlerRegistry handlers, String workerId,
                        FailureResolver failureResolver, Clock clock) {
    this.tasks = tasks;
    this.shards = shards;
    this.handlers = handlers;
    this.workerId = workerId;
    this.failureResolver = failureResolver;
    this.clock = clock;
  }

  /** 处理一个分片,返回是否处理了(找到了候选且认领成功)。 */
  public boolean workOne() {
    for (Task t : tasks.findAll()) {
      var cand = shards.findCandidate(t.id());
      if (cand.isEmpty()) continue;
      Shard shard = cand.get();
      Instant lease = clock.instant().plusSeconds(60);
      if (!shards.claim(shard.id(), t.id(), workerId, lease, t.maxActiveConcurrent())) {
        continue; // 他人已认领或配额占满 → 未认领成功,绝不回写
      }
      // 父 execution 只读(取 args/shardCount);worker 绝不对父行做任何写。
      Execution parent = shards.findParent(shard.executionId())
          .orElseThrow(() -> new IllegalStateException("no parent execution for shard " + shard.id()));
      ExecutionHandler h;
      try {
        h = handlers.get(t.handlerRef());
      } catch (IllegalArgumentException e) {
        // 未注册 handler 也是失败。仅当本 worker 仍是该分片持有者(markStatusOwned 回写成功)时才判重试/死信;
        // 若回写被 ownership 守卫拦截(分片已被他方接管),则静默放弃,不调度。
        if (shards.markStatusOwned(shard.id(), ExecutionStatus.FAILED, workerId, e.getMessage())) {
          failureResolver.handle(t, shard.id(), shard.attempt() + 1, e.getMessage());
        }
        return true;
      }
      // 运行前检查:已被请求取消则直接落 CANCELED,不再调度 handler(协作取消前置路径)。
      if (shards.isCancelRequested(shard.id())) {
        shards.markStatusOwned(shard.id(), ExecutionStatus.CANCELED, workerId, "cancelled before run");
        return true;
      }
      CancellationToken token = new CancellationToken(shards.isCancelRequested(shard.id()));
      HandlerContext ctx = new HandlerContext(shard.executionId(), shard.id(), shard.shardIndex(),
          parent.shardCount(), shard.shardData(), parent.args(), token);
      try {
        h.handle(ctx);
        // 正常返回后:若期间被请求取消,落 CANCELED;否则 SUCCESS。
        if (shards.isCancelRequested(shard.id())) {
          shards.markStatusOwned(shard.id(), ExecutionStatus.CANCELED, workerId, "cancelled after run");
        } else {
          shards.markStatusOwned(shard.id(), ExecutionStatus.SUCCESS, workerId, "ok");
        }
      } catch (CancellationException cex) {
        // 协作取消信号:distinct 路径,不路由到 failureResolver(不重试/不死信)。
        shards.markStatusOwned(shard.id(), ExecutionStatus.CANCELED, workerId, "cancelled by handler");
      } catch (Throwable err) {
        // 普通失败走统一判定:应重试则调度重试,否则 FAILED + 死信。ownership 守卫失败(stale)则整个重试/
        // 死信决策一并抑制,避免 double-completion / spurious-failure。
        String detail = String.valueOf(err);
        if (shards.markStatusOwned(shard.id(), ExecutionStatus.FAILED, workerId, detail)) {
          failureResolver.handle(t, shard.id(), shard.attempt() + 1, detail);
        }
      }
      return true;
    }
    return false;
  }
}
