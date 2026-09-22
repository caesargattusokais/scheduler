package dev.scheduler.core;
/** DAG 节点:引用已注册任务,不做内联定义(spec §0)。nodeMaxRetries/nodeBackoffMs 为 1a 节点级重试预算
 *  (run 侧按 attempt 计数;nodeMaxRetries=0 不重试,保持旧行为)。runIf 为 1b join 条件:
 *  'all_success'(默认)= 全部上游成功后执行/任一失败跳过;'any_success' = 任一上游成功即执行(OR-join)。 */
public record DagNode(Long id, Long dagId, String nodeKey, Long taskId, int sortOrder,
    int nodeMaxRetries, long nodeBackoffMs, String runIf) {}
