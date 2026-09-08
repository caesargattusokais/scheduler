package dev.scheduler.core;
/** DAG 节点:引用已注册任务,不做内联定义(spec §0)。 */
public record DagNode(Long id, Long dagId, String nodeKey, Long taskId, int sortOrder) {}
