package dev.scheduler.core;
import java.time.Instant;
/** 一次 DAG 执行中的单个节点实例(spec §1.2);execution_id 惰性填(spawn 后)。attempt 为该节点已尝试次数
 *  (0 起,重试+1);nextRetryAt 非空=节点正在重试退避期(未到点前保持 PENDING,不重 spawn)。
 *  1c:runIf/nodeMaxRetries/nodeBackoffMs 为 createRun 时从当前定义封印进 run 的节点行为快照——run 完全自包含,
 *  DagEngine 推进只读这些封印值,之后定义怎么改都不影响本 run。 */
public record DagRunNode(Long id, Long dagRunId, String nodeKey, Long taskId, Long executionId,
    DagRunNodeStatus status, int sortOrder, String detail, Instant createdAt, Instant finishedAt,
    int attempt, Instant nextRetryAt, String runIf, int nodeMaxRetries, long nodeBackoffMs) {}