package dev.scheduler.persistence;
import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** 分片执行(execution_shard)。父/N-shard 创建 + 读取,以及 worker 侧的认领/写回/重试生命周期。
 *  语义逐条镜像 M2 {@link JdbcExecutionRepository} 对应方法,作用对象改为 shard(带 execution_id→task_id 的 JOIN)。 */
public interface ShardRepository {

  /** 创建父 execution(状态 DUE,幂等 by parentKey)+ 其 N 个 shard。单事务,父重复创建不重复插 shard。 */
  Execution createParentWithShards(long taskId, String parentKey, int shardCount);

  /** 按父 execution id 读父行。 */
  Optional<Execution> findParent(long executionId);

  /** 给定父 execution 下的全部分片,按 shard_index 升序。 */
  List<Shard> findShards(long executionId);

  /** 按 shard id 读单条分片。 */
  Optional<Shard> findShard(long shardId);

  /** 重试闸:某任务下待认领的 DUE shard(并 parent execution 确认 task_id;仅当 next_retry_at 为空或已到期待时才放行)。
   *  按 shard id 升序取首条。 */
  Optional<Shard> findCandidate(long taskId);

  /** 认领单个 DUE shard → RUNNING,并递增 attempt、记 started_at(首次)。受 task 级 maxConcurrent(CAS on active RUNNING count)
   *  闸门:配额已满或已非 DUE(竞态)时返回 false,不落误导性 outcome。成功时同事务落 RUNNING('claim') outcome。 */
  boolean claim(long shardId, long taskId, String workerId, Instant leaseUntil, int maxConcurrent);

  /** 某任务下仍持有有效租约(RUNNING 且 lease_until>now())的 shard 数,用于并发配额。 */
  long countActive(long taskId);

  /** 对账器扫描到的孤儿 RUNNING shard:租约已过期仍 RUNNING。只投影对账所需的 id 与 attempt。 */
  record ExpiredShard(long id, int attempt) {}

  /** 某任务下租约已过期(lease_until<=now())且仍 RUNNING 的孤儿 shard;由 Reconciler 逐任务回收。 */
  List<ExpiredShard> findExpiredRunning(long taskId);

  /** 显式状态迁移(非持有者专属):对账回收/控制台取消用。非法迁移抛 IllegalStateException;CAS 0 行=行已被他方改走
   *  → 静默返回 false,不落误导性 outcome。 */
  boolean markStatus(long shardId, ExecutionStatus to, String workerId, String detail);

  /**
   * 持有着专属的终态回写:worker 用它写回自己的成功/失败/取消。除要求状态可迁移外,还要求行仍归
   * {@code ownerWorkerId} 所有(worker_id 匹配)。若行已不在可迁移状态或已被他方接管(如 reconciler 回收后另一 worker
   * 重新认领)→ 返回 false 且不落 outcome,静默丢弃该次回写。 */
  boolean markStatusOwned(long shardId, ExecutionStatus to, String ownerWorkerId, String detail);

  /** 失败分片排回:FAILED → DUE 并写 next_retry_at。CAS on status='FAILED':0 行=竞态/非 FAILED,静默跳过不落 outcome。 */
  void scheduleRetry(long shardId, Instant retryAt, String detail);

  /** 仅置 dead_letter 标记(不改 status),仅 FAILED 生效;无 outcome 行。 */
  void markDeadLetter(long shardId, String detail);

  /** worker 轮询该 shard 是否已被请求取消;列默认 false。 */
  boolean isCancelRequested(long shardId);

  // ---- Task 3:父汇聚 + FAIL_FAST + 父取消 + DLQ/requeue ----

  /** 待汇聚的父 execution id 列表:父仍 DUE(未终态)且 ≥1 个 shard 已终态(SUCCESS/FAILED/CANCELED)。
   *  由对账器扫描,依兄弟终态结果决定父级终态。 */
  List<Long> parentsNeedingAggregation();

  /** 父级终态汇聚:父 CAS on status='DUE'(父只 DUE→终态,从不存 RUNNING),成功后同事务落父 execution_outcome。
   *  0 行 CAS=父已被他方终态/已推进 → 幂等返回 false,不落误导性 outcome。 */
  boolean finalizeParent(long parentId, ExecutionStatus terminal, String detail);

  /** FAIL_FAST:兄弟分片失败后,将同父的 RUNNING 兄弟置协作取消信号、DUE 兄弟直接编 CANCELED(+outcome)。
   *  单事务;以触发失败片 shardId 界定"兄弟"(排除自身)。 */
  void cancelSiblings(long shardId, String detail);

  /** DUE 父协作取消请求:置位 RUNNING shard 的 cancel_requested、DUE shard 直编 CANCELED(+outcome)、
   *  父置 cancel_requested。返回事务后是否仍有 RUNNING shard(即是否有 RUNNING 片被置信号)。 */
  boolean requestCancelParent(long parentId);

  /** DUE 父直取消:RUNNING/DUE 的 shard 全编 CANCELED(+outcome),父置 CANCELED(终态)+outcome。 */
  void cancelParentImmediate(long parentId);

  /** 该父下是否仍有 RUNNING shard。 */
  boolean hasRunningShard(long parentId);

  /** DLQ 读:FAILED 且已标 dead_letter 的分片,按 id 升序。 */
  List<Shard> findDeathLetterShards();

  /** 死信分片重排回队:FAILED → DUE 并重置 attempt/next_retry_at/dead_letter(+DUE outcome)。CAS on status='FAILED':
   *  0 行=竞态/非 FAILED → 返回 false。 */
  boolean requeueShard(long shardId);
}
