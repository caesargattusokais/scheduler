package dev.scheduler.server.service;

import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.ShardRepository;
import java.util.List;
import java.util.Optional;

/** DAG 运行读取服务:GET /dags/runs 列表 与 run 详情(含每节点的 spawned execution 的 shards)。
 *  只读;dag_run 的 RUNNING 是派生值(存储只 PENDING→终态,spec §1.3)。 */
public class DagQueryService {

  private final DagRepository dags;
  private final ShardRepository shards;

  public DagQueryService(DagRepository dags, ShardRepository shards) {
    this.dags = dags;
    this.shards = shards;
  }

  public record NodeDetail(DagRunNode node, List<Shard> shards) {}
  public record RunDetail(DagRun run, String status, List<NodeDetail> nodes) {}

  public List<DagRun> listRuns(Long dagId) { return dags.findRuns(dagId); }

  public Optional<RunDetail> runDetail(long runId) {
    return dags.findRun(runId).map(run -> {
      List<DagRunNode> nodes = dags.findNodesOfRun(runId);
      return new RunDetail(run, deriveStatus(run, nodes),
          nodes.stream().map(n -> new NodeDetail(n,
              n.executionId() == null ? List.of() : shards.findShards(n.executionId()))).toList());
    });
  }

  /** 派生 dag_run 展示用 status:存储终态原样;否则任意节点非终态 → RUNNING(读侧只映射,不写库)。 */
  static String deriveStatus(DagRun run, List<DagRunNode> nodes) {
    if (run.status().isTerminal()) return run.status().name();
    return nodes.stream().anyMatch(n -> !n.status().isTerminal()) ? "RUNNING"
        : run.status().name();
  }
}
