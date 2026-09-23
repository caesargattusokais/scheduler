package dev.scheduler.persistence;
import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 分片执行(execution_shard)。父/N-shard 创建 + 读取,以及 worker 侧的认领/写回/重试生命周期。
 *  语义逐条镜像 M2 {@link JdbcExecutionRepository} 对应方法,作用对象改为 shard(带 execution_id→task_id 的 JOIN)。 */
public interface ShardRepository {

  /** 创建父 execution(状态 DUE,幂等 by parentKey)+ 其 N 个 shard。单事务,父重复创建不重复插 shard。
   *  {@code shardCount < 1} 抛 IllegalArgumentException("shardCount must be >= 1")(0 扇出会产生恒 DUE 无法汇聚的父)。
   *  无源轮:args = null、rerunOf = null(普通触发/cron/dag)。 */
  Execution createParentWithShards(long taskId, String parentKey, int shardCount);

  /** 带源轮的重跑创建:同上,并复制源轮 args、落 rerun_of 溯源(引用的历史轮 id,新轮 READ 可见)。 */
  Execution createParentWithShards(long taskId, String parentKey, int shardCount,
                                   String args, Long rerunOf);

  /** 按父 execution id 读父行。 */
  Optional<Execution> findParent(long executionId);

  /** 给定父 execution 下的全部分片,按 shard_index 升序。 */
  List<Shard> findShards(long executionId);

  /** 按 shard id 读单条分片。 */
  Optional<Shard> findShard(long shardId);

  /** 重试闸:某任务下待认领的 DUE shard(并 parent execution 确认 task_id;仅当 next_retry_at 为空或已到期待时才放行)。
   *  4c 选片摊开:在该任务当前可领的 DUE 集合内取稳定 offset(= workerId 哈希对该集合大小的余数),使不同 worker
   *  认领不同片、避免全员抢同一最低 id 造成竞争;同 worker 两次调用(集合未变时)得同片。workerId 为 null → offset 0(旧行为)。 */
  Optional<Shard> findCandidate(long taskId, String workerId);

  /** 认领单个 DUE shard → RUNNING,并递增 attempt、started_at 无条件重置为 now()(每次认领=本轮运行起点,超时从本轮起算)。
   *  受 task 级 maxConcurrent(CAS on active RUNNING count)闸门:配额已满或已非 DUE(竞态)时返回 false,不落误导性
   *  outcome。成功时同事务落 RUNNING('claim') outcome。 */
  boolean claim(long shardId, long taskId, String workerId, Instant leaseUntil, int maxConcurrent);

  /** 某任务下仍持有有效租约(RUNNING 且 lease_until>now())的 shard 数,用于并发配额。 */
  long countActive(long taskId);

  /** 全局活跃 shard 数(RUNNING 且租约有效,跨全部任务)——指标全局 gauge,懒查,新建任务无需重启即计入。 */
  long countActive();

  /** 全局「DUE 最深队龄」(秒):最老的 DUE shard 自入队(queued_at)至今的秒数;无 DUE 则 0。
   *  queued_at 在每次进入 DUE 时置 now(),故重试/重排后队龄从重新入队算起,不会虚高。 */
  long maxDueQueueAgeSeconds();

  /** 运行中续约:仅当分片仍归 {@code ownerWorkerId}(worker_id 匹配)且仍 RUNNING 时延长 lease_until。
   *  长运行 handler(千万级批处理同步阻塞)期间 worker 周期调用,避免租约短于任务时长被 Reconciler 误回收;
   *  已终态(如写回 SUCCESS)或被接管(worker_id 变更)则 0 行 → 静默返回 false。无 outcome 行。 */
  boolean renewLease(long shardId, String ownerWorkerId, Instant leaseUntil);

  /** 对账器扫描到的孤儿 RUNNING shard:租约已过期仍 RUNNING,或 owner worker 心跳失联(stale)。只投影对账所需的 id 与 attempt。 */
  record ExpiredShard(long id, int attempt) {}

  /** 某任务下需回收的孤儿 RUNNING shard,由 Reconciler 逐任务回收。判定双信号:
   *  ① 租约已过期(lease_until<=now());② owner worker 心跳失联(worker.last_seen 距今 > staleAfterSeconds 秒或
   *  status 非 'ALIVE',仅当 worker_id 非空时判定——worker_id 为空的 RUNNING 异常行走租约兜底)。
   *  worker 判活口径与 server active 指标一致。 */
  List<ExpiredShard> findExpiredRunning(long taskId, int staleAfterSeconds);

  /** 超时的 RUNNING shard(生命周期网关):仅当分片已认领(worker_id 非空)且 started_at 距今超过 timeoutSeconds。
   *  返回对账所需的 id 与 attempt。 */
  List<ExpiredShard> findOverRuntime(long taskId, int timeoutSeconds);

  /** 显式状态迁移(非持有者专属):对账回收/控制台取消用。非法迁移抛 IllegalStateException;CAS 0 行=行已被他方改走
   *  → 静默返回 false,不落误导性 outcome。 */
  boolean markStatus(long shardId, ExecutionStatus to, String workerId, String detail);

  /**
   * 持有着专属的终态回写:worker 用它写回自己的成功/失败/取消。除要求状态可迁移外,还要求行仍归
   * {@code ownerWorkerId} 所有(worker_id 匹配)。若行已不在可迁移状态或已被他方接管(如 reconciler 回收后另一 worker
   * 重新认领)→ 返回 false 且不落 outcome,静默丢弃该次回写。 */
  boolean markStatusOwned(long shardId, ExecutionStatus to, String ownerWorkerId, String detail);

  /** 失败分片排回:FAILED → DUE 并写 next_retry_at。CAS on status='FAILED':0 行=竞态/非 FAILED,静默跳过不落 outcome。
   *  retryBudgetMs != null 且该 shard retry_budget_until 尚未置(首次进重试)时,置 retry_budget_until=now()+W;
   *  已置保持(窗口自首次失败起算,重试间不续)。 */
  void scheduleRetry(long shardId, Instant retryAt, Long retryBudgetMs, String detail);

  /** 整轮重试预算 W(W 时限的第二个终止条件)是否已耗尽:retry_budget_until 已置且 <= DB now()。未设 W 的对象恒 false。 */
  boolean retryBudgetExhausted(long shardId);

  /** 仅置 dead_letter 标记(不改 status),仅 FAILED 生效;无 outcome 行。 */
  void markDeadLetter(long shardId, String detail);

  /** worker 轮询该 shard 是否已被请求取消;列默认 false。 */
  boolean isCancelRequested(long shardId);

  // ---- Task 3:父汇聚 + FAIL_FAST + 父取消 + DLQ/requeue ----

  /** 待汇聚的父 execution id + 其 taskId:父仍 DUE(未终态)且 ≥1 个 shard 已终态(SUCCESS/FAILED/CANCELED)。
   *  由对账器扫描,依兄弟终态结果 + 任务成功策略(经 taskId 取 Task)决定父级终态。 */
  List<ParentAgg> parentsNeedingAggregation();

  /** 一个待汇聚的父及其所属任务:tid 供对账器取任务成功策略(部分成功语义的判定输入)。 */
  record ParentAgg(long executionId, long taskId) {}

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

  /** 某父执行下各分片最近一次 FAILED outcome 的 detail(shard_id → detail),即失败日志;无 FAILED 记录的分片无条目。 */
  Map<Long, String> findFailureDetails(long executionId);

  /** 给定分片集合内,各分片最近一次 FAILED outcome 的 detail(shard_id → 失败日志);无 FAILED 记录的分片无条目。 */
  Map<Long, String> findFailureDetailsByShardIds(java.util.Collection<Long> shardIds);

  /** 给定分片集合内,各分片所属任务的任务名/handlerRef(shard_id → TaskRef);经 execution→task 关联。 */
  Map<Long, TaskRef> findTaskRefsByShardIds(java.util.Collection<Long> shardIds);

  /** 该父下是否仍有 RUNNING shard。 */
  boolean hasRunningShard(long parentId);

  /** 全局死信 shard 计数(FAILED 且 dead_letter),指标 scheduler_dlq_depth 源。 */
  long countDeadLetter();

  /** DLQ 读:FAILED 且已标 dead_letter 的分片,按 id 升序。 */
  List<Shard> findDeathLetterShards();

  /** DLQ 分页:taskId 过滤(经 execution 关联)+ limit/offset,ORDER BY s.id。taskId 可为 null=全部。 */
  List<Shard> findDeathLetterShards(Long taskId, int limit, int offset);
  /** 与上同过滤条件的 DLQ 全量计数(不含分页)。 */
  long countDeathLetterShards(Long taskId);

  /** 死信分片重排回队:FAILED → DUE 并重置 attempt/next_retry_at/dead_letter(+DUE outcome),replay_count 自增。
   *  CAS on status='FAILED':0 行=竞态/非 FAILED → 返回 false。 */
  boolean requeueShard(long shardId);

  /** DLQ 自动重放候选:可自动重放的死信分片(FAILED 且 dead_letter,属 enabled AND NOT paused 且 dlq_max_replays>0 的任务),
   *  按 id 升序,供 leader 门控重放循环逐片决定 requeue(未超限)或弃(超限)。 */
  List<DlqReplayCandidate> findDlqReplayCandidates(int limit);

  /** 单个自动重放候选:shardId + 当前 replay_count + 所属任务的 dlq_max_replays(判定依据)。 */
  record DlqReplayCandidate(long shardId, int replayCount, int maxReplays) {}

  /** 永久弃掉一条死信分片(超限):物理删除,仅 FAILED 且 dead_letter 生效(CAS)。返回是否删除。 */
  boolean discardShard(long shardId);

  /** SUCCESS 后写回 result_payload,受 worker_id 归属守卫:行已不归该 owner(且非 SUCCESS)则静默返回 false。
   *  payload 不是 outcome,不落 outcome 行。返回是否写入。 */
  boolean recordResultPayload(long shardId, String ownerWorkerId, String payload);
}
