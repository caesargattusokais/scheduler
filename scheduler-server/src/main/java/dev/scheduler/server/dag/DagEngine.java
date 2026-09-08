package dev.scheduler.server.dag;

import dev.scheduler.core.Dag;
import dev.scheduler.core.DagEdge;
import dev.scheduler.core.DagNode;
import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunNodeStatus;
import dev.scheduler.core.DagRunStatus;
import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.leader.LeaderElection;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.scheduling.support.CronExpression;

/** 工作流调度引擎(spec §2/§3):单 scan 两阶段——(1) 触发(leader 门控,cron tick → 幂等建 dag_run +
 *  PENDING 节点,镜像 TriggerEngine);(2) 传播(推进各活跃 run:惰性 spawn 就绪节点、由 spawned execution 的
 *  shards 只读派生节点终态、全节点终态后终结 dag_run,镜像 Reconciler 的父汇聚但作用对象是 dag 表)。
 *  DagEngine 是 dag 运行表唯一写者;worker/Reconciler 对 DAG 无感知,M4 不改执行运行时。 */
public class DagEngine {
  private final DagRepository dags;
  private final TaskRepository tasks;
  private final ShardRepository shards;
  private final LeaderElection leader;
  private final Clock clock;

  public DagEngine(DagRepository dags, TaskRepository tasks, ShardRepository shards,
                   LeaderElection leader, Clock clock) {
    this.dags = dags;
    this.tasks = tasks;
    this.shards = shards;
    this.leader = leader;
    this.clock = clock;
  }

  /** 扫描一次:仅 leader(镜像 TriggerEngine 的 leader 守卫)。 */
  public void scanOnce() {
    if (!leader.isLeader()) return;
    scanTriggers();
    scanPropagation();
  }

  /** 阶段一:对 enabled 且非 paused 的 cron DAG,分钟窗内命中 tick → 幂等建一条 dag_run(含全 PENDING 节点)。 */
  private void scanTriggers() {
    ZoneId zone = clock.getZone();
    ZonedDateTime now = clock.instant().atZone(zone);
    for (Dag d : dags.findCronEnabledDags()) {
      var cron = CronExpression.parse(d.cron());
      ZonedDateTime fired = cron.next(now.minusSeconds(61));
      if (fired != null && !fired.isAfter(now)) {
        dags.createScheduledRun(d.id(), fired.toInstant());
      }
    }
  }

  /** 阶段二:对每个活跃(未终态)run 应用 spec §3 的确定性传播规则。跨周期收敛:单 scan 只推进一层。 */
  private void scanPropagation() {
    for (DagRun run : dags.findActiveRuns()) {
      List<DagRunNode> nodes = dags.findNodesOfRun(run.id());
      if (nodes.isEmpty()) continue;
      propagateRun(run, nodes);
    }
  }

  /** 上游表:nodeKey → 直接上游 nodeKey 列表(读 dag 定义的边 + 节点 id↔key 映射)。 */
  private Map<String, List<String>> upstreamMap(long dagId, Map<Long, String> keyByNodeId) {
    Map<String, List<String>> up = new HashMap<>();
    for (DagEdge e : dags.findEdges(dagId)) {
      String to = keyByNodeId.get(e.toNodeId());
      String from = keyByNodeId.get(e.fromNodeId());
      if (to != null && from != null) up.computeIfAbsent(to, k -> new ArrayList<>()).add(from);
    }
    return up;
  }

  private void propagateRun(DagRun run, List<DagRunNode> nodes) {
    // nodeId → nodeKey(用 dag 定义节点映射边引用的 node id 到 run 内的 node_key)
    Map<Long, String> keyByNodeId = new HashMap<>();
    for (DagNode dn : dags.findNodes(run.dagId())) keyByNodeId.put(dn.id(), dn.nodeKey());
    Map<String, DagRunNode> byKey = new HashMap<>();
    for (DagRunNode n : nodes) byKey.put(n.nodeKey(), n);
    Map<String, List<String>> upstream = upstreamMap(run.dagId(), keyByNodeId);

    for (DagRunNode n : nodes) {
      if (n.status().isTerminal()) continue;
      List<DagRunNode> ups = upstream.getOrDefault(n.nodeKey(), List.of()).stream()
          .map(byKey::get).filter(Objects::nonNull).toList();
      if (n.status() == DagRunNodeStatus.PENDING) {
        stepPendingNode(run, n, ups);
      } else if (n.status() == DagRunNodeStatus.RUNNING) {
        stepRunningNode(n);
      }
    }

    List<DagRunNode> refreshed = dags.findNodesOfRun(run.id());
    if (refreshed.stream().allMatch(u -> u.status().isTerminal())) {
      boolean anyFailSkip = refreshed.stream()
          .anyMatch(u -> u.status() == DagRunNodeStatus.FAILED || u.status() == DagRunNodeStatus.SKIPPED);
      DagRunStatus terminal = anyFailSkip ? DagRunStatus.FAILED
          : refreshed.stream().anyMatch(u -> u.status() == DagRunNodeStatus.CANCELED)
              ? DagRunStatus.CANCELED : DagRunStatus.SUCCESS;
      String detail = anyFailSkip ? "node failed"
          : (terminal == DagRunStatus.CANCELED ? "node cancelled" : "all nodes ok");
      dags.finalizeRun(run.id(), terminal, detail);
    }
  }

  /** §3.1 PENDING 节点:上游失败→SKIPPED(绝不 spawn);上游取消→CANCELED;全 SUCCESS→惰性 spawn RUNNING;否则等。 */
  private void stepPendingNode(DagRun run, DagRunNode n, List<DagRunNode> ups) {
    boolean anyFailSkip = ups.stream()
        .anyMatch(u -> u.status() == DagRunNodeStatus.FAILED || u.status() == DagRunNodeStatus.SKIPPED);
    if (anyFailSkip) { dags.markNodeStatus(n.id(), DagRunNodeStatus.SKIPPED, "upstream failed"); return; }
    boolean anyCancelled = ups.stream().anyMatch(u -> u.status() == DagRunNodeStatus.CANCELED);
    if (anyCancelled) { dags.markNodeStatus(n.id(), DagRunNodeStatus.CANCELED, "upstream cancelled"); return; }
    boolean allTerminal = ups.stream().allMatch(u -> u.status().isTerminal());
    if (allTerminal) { // 无 FAILED/SKIPPED/CANCELED 且全终态 ⇒ 全 SUCCESS → 就绪
      spawnNode(run, n);
    }
    // 否则(有上游未终态)保持 PENDING,本周期不动作,等上游再扫描收敛。
  }

  private void spawnNode(DagRun run, DagRunNode n) {
    Task task = tasks.findById(n.taskId()).orElseThrow();
    String key = "dag:" + run.dagId() + ":run:" + run.id() + ":node:" + n.nodeKey();
    Execution parent = shards.createParentWithShards(n.taskId(), key, task.shardCount());
    dags.markNodeSpawned(n.id(), parent.id()); // CAS on PENDING;崩溃重放时幂等(同一 key 复用既有父+shard)
  }

  /** §3.2 RUNNING 节点:读其 execution 的全部 shard;全终态后派生节点终态(FAILED 先于 CANCELED 先于 SUCCESS)。 */
  private void stepRunningNode(DagRunNode n) {
    if (n.executionId() == null) return;
    List<Shard> ss = shards.findShards(n.executionId());
    if (!ss.stream().allMatch(s -> s.status().isTerminal())) return; // 未全终态 → 保持 RUNNING,等 worker/对账
    boolean anyFailed = ss.stream().anyMatch(s -> s.status() == ExecutionStatus.FAILED);
    DagRunNodeStatus t = anyFailed ? DagRunNodeStatus.FAILED
        : ss.stream().anyMatch(s -> s.status() == ExecutionStatus.CANCELED)
            ? DagRunNodeStatus.CANCELED : DagRunNodeStatus.SUCCESS;
    String detail = anyFailed ? "shards failed"
        : (t == DagRunNodeStatus.CANCELED ? "shards cancelled" : "all shards ok");
    dags.markNodeStatus(n.id(), t, detail);
  }
}
