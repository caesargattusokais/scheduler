package dev.scheduler.server.reconcile;

import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.core.SuccessPolicy;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.ShardRepository.ExpiredShard;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.persistence.retry.FailureResolver;
import dev.scheduler.server.service.NotificationFirer;
import java.util.List;

/**
 * 对账器:回收租约过期的孤儿 RUNNING shard(id 已认领却迟迟未回写),并对已收敛的父级做终态汇聚。
 * scanOnce 做两趟:
 *   (1) 孤儿回收——逐任务查出 lease_until <= DB now() 仍 RUNNING 的 shard,委托共享 FailureResolver 做
 *       「落 FAILED + 重试/死信/FAIL_FAST」的统一判定——与 worker 共用同一套判定,避免两套漂移。
 *   (2) 父级汇聚——对待汇聚的父级(parentsNeedingAggregation)按兄弟终态结果推导父终态(SUCCESS/FAILED/CANCELED)。
 * leader 门控不在此处(由 ReconcileLoop.tick 在 scan 前把关),这里只做回收/汇聚。
 */
public class Reconciler {
  private final TaskRepository tasks;
  private final ShardRepository shards;
  private final FailureResolver failureResolver;
  private final NotificationFirer firer;
  private final String workerId;
  private final int staleAfterSeconds;

  public Reconciler(TaskRepository tasks, ShardRepository shards,
                    FailureResolver failureResolver, NotificationFirer firer,
                    String workerId, int staleAfterSeconds) {
    this.tasks = tasks;
    this.shards = shards;
    this.failureResolver = failureResolver;
    this.firer = firer;
    this.workerId = workerId;
    this.staleAfterSeconds = staleAfterSeconds;
  }

  /**
   * 对账扫描一次,返回本趟回收的孤儿 shard 数。两趟固定顺序:先孤儿回收,再父级终态汇聚。
   * 租约判定用 DB now()(服务器时钟一致,与本机墙钟无关)。
   */
  public int scanOnce() {
    int reclaimed = 0;
    for (Task task : tasks.findAll()) {
      // 阶段 1 — §3 分布式执行超时:timeoutSeconds>0 的任务,已认领且运行超限的 RUNNING 分片走失败路径
      // (重试/死信/FAIL_FAST),发 execution.timeout。先于活性回收,故死属主 + 超时片保住 'runtime timeout'
      // detail 与超时通知;活体 worker 慢/超时由 B1 活性门排除,不在此收。worker 迟到写回由 owner 归属守卫拒
      // (worker_id 已被改写)→ 硬超时语义。
      if (task.timeoutSeconds() > 0) {
        for (ExpiredShard run : shards.findOverRuntime(task.id(), task.timeoutSeconds(), staleAfterSeconds)) {
          try {
            if (shards.markStatus(run.id(), ExecutionStatus.FAILED, workerId, "runtime timeout")) {
              failureResolver.handle(task, run.id(), run.attempt(), "runtime timeout");
              firer.shardTimedOut(run.id(), task.id(), run.attempt(), task.timeoutSeconds());
              reclaimed++;
            }
          } catch (IllegalStateException alreadyMovedOn) {
            // 快照后已终态/已迁移 → 跳过,不中止整批。
          }
        }
      }
      // 阶段 2 — 活性回收:对上阶段未收的孤儿(dead-owner 且未超时)走同一条失败路径,detail 'owner unresponsive'。
      // 阶段 1 已 FAILED 的片因 findExpiredRunning 要求 status='RUNNING' 而不再命中,两阶段互不重叠/不重复计数。
      for (ExpiredShard run : shards.findExpiredRunning(task.id(), staleAfterSeconds)) {
        try {
          if (shards.markStatus(run.id(), ExecutionStatus.FAILED, workerId,
                  "owner unresponsive")) {
            failureResolver.handle(task, run.id(), run.attempt(),
                "owner unresponsive");
            reclaimed++;
          }
        } catch (IllegalStateException alreadyMovedOn) {
          // 快照后该行已被 owner 抢先完成/被超时阶段回收(或已非法迁移)→ 已迁移,跳过这一行,不中止整批回收。
        }
      }
    }
    aggregateParents();
    return reclaimed;
  }

  /** 父级汇聚:对每个有待汇聚的父,全部兄弟终态后才推导父终态并落库。finalizeParent CAS-0 → 已推进,幂等跳过。
   *  3c 部分成功:父终态由任务成功策略(经 ParentAgg.taskId 取 Task)推导——失败片可被容忍(达标)→ PARTIAL_SUCCESS。 */
  private void aggregateParents() {
    for (var agg : shards.parentsNeedingAggregation()) {
      long pid = agg.executionId();
      List<Shard> parent = shards.findShards(pid);
      if (parent.isEmpty() || !parent.stream().allMatch(s -> s.status().isTerminal())) {
        continue; // 仍有兄弟未终态 → 父保持 DUE,待下趟。
      }
      Task task = tasks.findById(agg.taskId()).orElse(null);
      if (task == null) continue; // 任务已被物理删(Task 删除守卫不允许有执行,防御性分支)
      long success = parent.stream().filter(s -> s.status() == ExecutionStatus.SUCCESS).count();
      long failed = parent.stream().filter(s -> s.status() == ExecutionStatus.FAILED).count();
      boolean anyCancelled = parent.stream().anyMatch(s -> s.status() == ExecutionStatus.CANCELED);
      ExecutionStatus parentTerminal = SuccessPolicy.aggregateTerminal(
          task.successPolicyType(), task.successPolicyValue(), success, failed, parent.size(), anyCancelled);
      String detail = switch (parentTerminal) {
        case PARTIAL_SUCCESS -> success + "/" + parent.size() + " shards ok (" + failed + " partial failed)";
        case FAILED -> "shard failed";
        case CANCELED -> "shard cancelled";
        default -> "all shards ok";
      };
      shards.finalizeParent(pid, parentTerminal, detail);
      // 事件点火(次序:先死信、再父终态;均在父收敛落库后,幂等由 key 保证只发一次)。
      for (Shard s : parent) {
        if (s.deadLetter()) firer.shardDeadLettered(pid, s.id(), s.attempt());
      }
      firer.parentTerminal(pid, parentTerminal, parent);
    }
  }
}
