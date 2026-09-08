package dev.scheduler.core;
import java.time.Instant;
/** 一次 DAG 执行中的单个节点实例(spec §1.2);execution_id 惰性填(spawn 后。 */
public record DagRunNode(Long id, Long dagRunId, String nodeKey, Long taskId, Long executionId,
    DagRunNodeStatus status, int sortOrder, String detail, Instant createdAt, Instant finishedAt) {}
