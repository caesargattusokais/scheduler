package dev.scheduler.core;
/** DAG 有向边:from_parent → to_child(spec §1.1)。
 *  1c 封印边(createRun 快照):from/to 引用该 run 内的 dag_run_node id,run 完全自包含;
 *  DagEngine 推进据此读上游拓扑,DagEngine 不再读当前定义 dag_edge。 */
public record DagRunEdge(Long id, Long dagRunId, Long fromNodeId, Long toNodeId) {}