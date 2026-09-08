package dev.scheduler.core;
/** DAG 有向边:from_parent → to_child(spec §1.1)。 */
public record DagEdge(Long id, Long dagId, Long fromNodeId, Long toNodeId) {}
