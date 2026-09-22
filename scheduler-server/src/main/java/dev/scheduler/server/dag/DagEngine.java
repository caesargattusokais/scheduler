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
import dev.scheduler.core.IdempotencyKeys;
import dev.scheduler.core.Shard;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.leader.LeaderElection;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronExpression;

/** 工作流调度引擎(spec §2/§3):单 scan 两阶段——(1) 触发(leader 门控,cron tick → 幂等建 dag_run +
 *  PENDING 节点,镜像 TriggerEngine);(2) 传播(推进各活跃 run:惰性 spawn 就绪节点、由 spawned execution 的
 *  shards 只读派生节点终态、全节点终态后终结 dag_run,镜像 Reconciler 的父汇聚但作用对象是 dag 表)。
 *  DagEngine 是 dag 运行表唯一写者;worker/Reconciler 对 DAG 无感知,M4 不改执行运行时。 */
public class DagEngine {
  private static final Logger log = LoggerFactory.getLogger(DagEngine.class);
  private final DagRepository dags;
  private final TaskRepository tasks;
  private final ShardRepository shards;
  private final LeaderElection leader;
  private final Clock clock;
  private final int batchSize;   // 单 tick 每阶段最多处理的行数(§4 游标分批;页满推进、页未满回卷)
  private long lastDagId;        // 游标:阶段一上次扫到的最大 dag id;回卷=0 表示下轮从头
  private long lastRunId;        // 游标:阶段二上次扫到的最大 run id;回卷=0 表示下轮从头

  public DagEngine(DagRepository dags, TaskRepository tasks, ShardRepository shards,
                   LeaderElection leader, Clock clock, int scanBatchSize) {
    this.dags = dags;
    this.tasks = tasks;
    this.shards = shards;
    this.leader = leader;
    this.clock = clock;
    this.batchSize = scanBatchSize;
  }

  /** 扫描一次:仅 leader(镜像 TriggerEngine 的 leader 守卫)。 */
  public void scanOnce() {
    if (!leader.isLeader()) return;
    scanTriggers();
    scanPropagation();
  }

  /** 阶段一:每 tick 至多处理 batchSize 个 enabled 且非 paused 的 cron DAG,分钟窗内命中 tick → 幂等建一条 dag_run(含全 PENDING 节点)。 */
  private void scanTriggers() {
    ZoneId zone = clock.getZone();
    ZonedDateTime now = clock.instant().atZone(zone);
    List<Dag> page = dags.findCronEnabledDagsPage(lastDagId, batchSize);
    for (Dag d : page) {
      try {
        var cron = CronExpression.parse(d.cron());
        ZonedDateTime fired = cron.next(now.minusSeconds(61));
        if (fired != null && !fired.isAfter(now)) {
          dags.createScheduledRun(d.id(), fired.toInstant());
        }
      } catch (RuntimeException badCron) {
        // 单条坏 cron(历史存量)只跳过该 DAG,不拖垮本 tick 其余 DAG 触发/传播。新坏 cron 已在建/改时被预检拦下。
        log.warn("skipping cron trigger for dag {} due to bad cron '{}': {}",
            d.id(), d.cron(), badCron.toString());
      }
    }
    lastDagId = (page.size() == batchSize) ? page.get(page.size() - 1).id() : 0L;
  }

  /** 阶段二:每 tick 至多处理 batchSize 个活跃(未终态)run,应用 spec §3 的确定性传播规则。跨周期收敛:单 scan 只推进一步。 */
  private void scanPropagation() {
    List<DagRun> page = dags.findActiveRunsPage(lastRunId, batchSize);
    for (DagRun run : page) {
      List<DagRunNode> nodes = dags.findNodesOfRun(run.id());
      if (nodes.isEmpty()) continue;
      propagateRun(run, nodes);
    }
    lastRunId = (page.size() == batchSize) ? page.get(page.size() - 1).id() : 0L;
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

  /** M5.3 §1.4 节点单节点重跑(仅 operator):precondition 节点处终态(else IllegalStateException→409)。
   *  建新 execution(createParentWithShards,键 forNodeRerun)→ 经 dagRepository.rerunNodeToExecution 把节点
   *  回绕到新 execution + 重开 run → 返回回绕后节点现态。下游不自动复位(propagateRun 跳过已终态)。
   *  节点态写仅经本方法(引擎是 dag 运行表唯一写者)。
   */
  public DagRunNode rerunNode(long runId, long nodeId) {
    DagRunNode n = dags.findNode(nodeId).orElseThrow(
        () -> new IllegalStateException("no dag_run_node " + nodeId));
    if (!n.dagRunId().equals(runId)) throw new IllegalStateException("node " + nodeId + " not in run " + runId);
    if (!n.status().isTerminal()) throw new IllegalStateException(
        "node " + nodeId + " is " + n.status() + " and cannot be rerun");
    Task task = tasks.findById(n.taskId()).orElseThrow();
    Execution parent = shards.createParentWithShards(
        n.taskId(), IdempotencyKeys.forNodeRerun(runId, n.nodeKey()), task.shardCount());
    if (!dags.rerunNodeToExecution(runId, nodeId, parent.id())) {
      throw new IllegalStateException("node " + nodeId + " was not terminal; rerun cancelled");
    }
    return dags.findNode(nodeId).orElseThrow();
  }

  private void propagateRun(DagRun run, List<DagRunNode> nodes) {
    // nodeId → nodeKey(用 dag 定义节点映射边引用的 node id 到 run 内的 node_key)
    Map<Long, String> keyByNodeId = new HashMap<>();
    Map<String, DagNode> defByKey = new HashMap<>(); // nodeKey → 定义节点(读节点级重试预算)
    for (DagNode dn : dags.findNodes(run.dagId())) {
      keyByNodeId.put(dn.id(), dn.nodeKey());
      defByKey.put(dn.nodeKey(), dn);
    }
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
        stepRunningNode(n, defByKey.get(n.nodeKey()));
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

  /** §3.1 PENDING 节点:上游失败→SKIPPED(绝不 spawn);上游取消→CANCELED;全 SUCCESS→惰性 spawn RUNNING;否则等。
   *  1a:重试退避门——next_retry_at 在未来(节点刚被 scheduleNodeRetry 回绕待重试)→ 本周期保持 PENDING 不动作,
   *  到点才重 spawn(镜像 execution 层 DUE 的 next_retry_at 闸)。 */
  private void stepPendingNode(DagRun run, DagRunNode n, List<DagRunNode> ups) {
    if (n.nextRetryAt() != null && clock.instant().isBefore(n.nextRetryAt())) return;
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
    // key 带 attempt:重试重 spawn 必须换新 idempotency key,否则 createParentWithShards 幂等复用上一个
    // 已失败的父+shard(1a 修正:同一节点不同 attempt 是不同执行)。首 spawn attempt=0 → :a0。
    String key = "dag:" + run.dagId() + ":run:" + run.id() + ":node:" + n.nodeKey() + ":a" + n.attempt();
    Execution parent = shards.createParentWithShards(n.taskId(), key, task.shardCount());
    dags.markNodeSpawned(n.id(), parent.id()); // CAS on PENDING;崩溃重放时幂等(同一 key 复用既有父+shard)
  }

  /** §3.2 RUNNING 节点:读其 execution 的全部 shard;全终态后派生节点终态(FAILED 先于 CANCELED 先于 SUCCESS)。
   *  1a 重试劫持:若节点定义允许重试且 attempt 未超预算 → 失败时不标 FAILED,回绕 PENDING 待重试。 */
  private void stepRunningNode(DagRunNode n, DagNode def) {
    if (n.executionId() == null) return;
    List<Shard> ss = shards.findShards(n.executionId());
    if (!ss.stream().allMatch(s -> s.status().isTerminal())) return; // 未全终态 → 保持 RUNNING,等 worker/对账
    boolean anyFailed = ss.stream().anyMatch(s -> s.status() == ExecutionStatus.FAILED);
    if (anyFailed && def != null && n.attempt() < def.nodeMaxRetries()) {
      // 消费一次重试预算:回绕到 PENDING(execution_id 清空)+ attempt+1 + 退避到点;下游仍等;到点由
      // stepPendingNode 以新 idempotency key(:a{attempt})重 spawn 该节点(不复用已失败的父执行)。
      Instant retryAt = clock.instant().plusMillis(nodeRetryDelayMs(def, n.attempt()));
      dags.scheduleNodeRetry(n.id(), retryAt, n.attempt() + 1, "shards failed; will retry");
      return;
    }
    DagRunNodeStatus t = anyFailed ? DagRunNodeStatus.FAILED
        : ss.stream().anyMatch(s -> s.status() == ExecutionStatus.CANCELED)
            ? DagRunNodeStatus.CANCELED : DagRunNodeStatus.SUCCESS;
    String detail = anyFailed ? "shards failed"
        : (t == DagRunNodeStatus.CANCELED ? "shards cancelled" : "all shards ok");
    dags.markNodeStatus(n.id(), t, detail);
  }

  /** 节点级退避:指数(attempt 0 起)×backoff,封顶 1h;镜像 RetryPolicy 的 exponential 语义但用节点自持 backoff。 */
  private static long nodeRetryDelayMs(DagNode def, int attempt) {
    long backoff = Math.max(1L, def.nodeBackoffMs());
    return Math.min(3_600_000L, backoff * (1L << Math.min(attempt, 30)));
  }
}
