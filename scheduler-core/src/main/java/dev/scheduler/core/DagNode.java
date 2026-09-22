package dev.scheduler.core;
/** DAG 节点:引用已注册任务,不做内联定义(spec §0)。nodeMaxRetries/nodeBackoffMs 为 1a 节点级重试预算
 *  (run 侧按 attempt 计数;nodeMaxRetries=0 不重试,保持旧行为)。 */
public record DagNode(Long id, Long dagId, String nodeKey, Long taskId, int sortOrder,
    int nodeMaxRetries, long nodeBackoffMs) {}
