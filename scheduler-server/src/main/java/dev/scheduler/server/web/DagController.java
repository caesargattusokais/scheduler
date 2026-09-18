package dev.scheduler.server.web;

import dev.scheduler.core.Dag;
import dev.scheduler.core.DagEdge;
import dev.scheduler.core.DagNode;
import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunNodeStatus;
import dev.scheduler.core.DagRunStatus;
import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.DagRepository.EdgeInput;
import dev.scheduler.persistence.DagRepository.NodeInput;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.server.dag.DagEngine;
import dev.scheduler.server.service.AuditRecorder;
import dev.scheduler.server.service.DagQueryService;
import dev.scheduler.server.service.DagQueryService.RunDetail;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 工作流 DAG 控制面(spec §5):建/查/pause/resume/手动触发/run 列表详情/取消级联。 */
@RestController
@RequestMapping("/api/v1/dags")
public class DagController {

  private final DagRepository dags;
  private final ShardRepository shards;
  private final DagEngine dagEngine;
  private final DagQueryService query;
  private final AuditRecorder auditor;

  public DagController(DagRepository dags, ShardRepository shards, DagEngine dagEngine, AuditRecorder auditor) {
    this.dags = dags;
    this.shards = shards;
    this.dagEngine = dagEngine;
    this.query = new DagQueryService(dags, shards);
    this.auditor = auditor;
  }

  public record CreateDagRequest(String name, String description, String cron,
      List<NodeReq> nodes, List<EdgeReq> edges) {}
  public record NodeReq(String nodeKey, Long taskId, Integer sortOrder) {}
  public record EdgeReq(String from, String to) {}
  public record DagDetail(Dag dag, List<DagNode> nodes, List<DagEdge> edges) {}

  /** 建 DAG:name/cron 合法 + task 存在 + 无自环 + DFS 无环 + 键/边唯一(仓库校验,违规 → IllegalArgumentException → 400)。 */
  @PostMapping
  public ResponseEntity<Dag> create(
      @RequestHeader(value = "X-Operator", required = false) String operator,
      @RequestBody CreateDagRequest req) {
    if (req == null || req.name() == null || req.name().isBlank()) {
      throw new IllegalArgumentException("name is required");
    }
    if (req.cron() == null || req.cron().isBlank()) {
      throw new IllegalArgumentException("cron is required");
    }
    TaskController.validateCron(req.cron());
    if (req.nodes() == null || req.nodes().isEmpty()) {
      throw new IllegalArgumentException("nodes must not be empty");
    }
    List<NodeInput> nodes = req.nodes().stream()
        .map(n -> new NodeInput(n.nodeKey(), n.taskId(), n.sortOrder() == null ? 0 : n.sortOrder()))
        .toList();
    List<EdgeInput> edges = req.edges() == null ? List.of()
        : req.edges().stream().map(e -> new EdgeInput(e.from(), e.to())).toList();
    Dag created = dags.createDag(req.name(), req.description(), req.cron(), nodes, edges);
    auditor.record(operator, "dag.create", TargetType.DAG, created.id(), Map.of("name", created.name()));
    return ResponseEntity.status(HttpStatus.CREATED).body(created);
  }

  /** DAG 列表:name 子串过滤 + limit/offset 分页,返回 Page<Dag>。 */
  @GetMapping
  public Page<Dag> list(@RequestParam(required = false) String name,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    return new Page<>(dags.findDagsPage(name, p.limit(), p.offset()),
        dags.countDags(name), p.offset(), p.limit());
  }

  @GetMapping("/{id}")
  public DagDetail get(@PathVariable long id) {
    Dag dag = dags.findDag(id).orElseThrow(() -> notFound("dag " + id));
    return new DagDetail(dag, dags.findNodes(id), dags.findEdges(id));
  }

  @PostMapping("/{id}/pause")
  public Dag pause(@RequestHeader(value = "X-Operator", required = false) String operator, @PathVariable long id) {
    requireDag(id);
    dags.setPaused(id, true);
    auditor.record(operator, "dag.pause", TargetType.DAG, id, Map.of());
    return byId(id);
  }

  @PostMapping("/{id}/resume")
  public Dag resume(@RequestHeader(value = "X-Operator", required = false) String operator, @PathVariable long id) {
    requireDag(id);
    dags.setPaused(id, false);
    auditor.record(operator, "dag.resume", TargetType.DAG, id, Map.of());
    return byId(id);
  }

  /** 手动触发一次:建 dag_run + 全 PENDING 节点,交由 DagEngine 扫描推进。 */
  @PostMapping("/{id}/trigger")
  public ResponseEntity<DagRun> trigger(
      @RequestHeader(value = "X-Operator", required = false) String operator,
      @PathVariable long id) {
    requireDag(id);
    DagRun run = dags.createManualRun(id);
    auditor.record(operator, "dag.trigger", TargetType.DAG, id, Map.of());
    return ResponseEntity.status(HttpStatus.CREATED).body(run);
  }

  /** run 列表:dagId/status 过滤 + limit/offset 分页,返回 Page<DagRun>。 */
  @GetMapping("/runs")
  public Page<DagRun> runs(@RequestParam(required = false) Long dagId,
      @RequestParam(required = false) String status,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    return new Page<>(dags.findRunsPage(dagId, status, p.limit(), p.offset()),
        dags.countRuns(dagId, status), p.offset(), p.limit());
  }

  @GetMapping("/runs/{runId}")
  public RunDetail runDetail(@PathVariable long runId) {
    return query.runDetail(runId).orElseThrow(() -> notFound("dag run " + runId));
  }

  /** 取消级联(dag → node → execution → shard,spec §4):置 run 取消标志;每个非终态节点置 CANCELED +
   *  outcome;已 spawn 节点再由父取消级联到其 shards;全节点 CANCELED → run 立即终态 CANCELED。 */
  @PostMapping("/runs/{runId}/cancel")
  public ResponseEntity<RunDetail> cancel(
      @RequestHeader(value = "X-Operator", required = false) String operator,
      @PathVariable long runId) {
    DagRun run = dags.findRun(runId).orElseThrow(() -> notFound("dag run " + runId));
    if (run.status() == DagRunStatus.CANCELED) {
      return ResponseEntity.ok(query.runDetail(runId).orElseThrow()); // 幂等
    }
    if (run.status().isTerminal()) {
      throw new ResponseStatusException(HttpStatus.CONFLICT,
          "dag run " + runId + " is " + run.status() + " and cannot be cancelled");
    }
    dags.requestCancelRun(runId);
    for (DagRunNode n : dags.findNonTerminalNodes(runId)) {
      dags.markNodeStatus(n.id(), DagRunNodeStatus.CANCELED,
          n.executionId() == null ? "cascade cancel (not spawned)" : "cascade cancel");
      if (n.executionId() != null) {
        if (shards.hasRunningShard(n.executionId())) shards.requestCancelParent(n.executionId());
        else shards.cancelParentImmediate(n.executionId());
      }
    }
    // 操作者取消把全部非终态节点一次性置 CANCELED → 全节点已终态,立即固化终态(镜像 M3 cancelParentImmediate),
    // 不同于引擎 §3.3 的"下一 scan 收敛"。finalizeRun CAS-0 幂等,重放安全。
    dags.finalizeRun(runId, DagRunStatus.CANCELED, "cancelled by operator");
    auditor.record(operator, "dag_run.cancel", TargetType.DAG_RUN, runId, Map.of());
    return ResponseEntity.ok(query.runDetail(runId).orElseThrow());
  }

  /** M5.3 §1.4 单节点重跑:仅终态节点可用;404 缺失,非终态 → 409;成功 200 + 节点现态。节点态变更经 DagEngine。 */
  @PostMapping("/runs/{runId}/nodes/{nodeId}/rerun")
  public ResponseEntity<DagRunNode> rerunNode(
      @RequestHeader(value = "X-Operator", required = false) String operator,
      @PathVariable long runId, @PathVariable long nodeId) {
    dags.findRun(runId).orElseThrow(() -> notFound("dag run " + runId));
    DagRunNode node = dags.findNode(nodeId).orElseThrow(() -> notFound("dag run node " + nodeId));
    if (!node.dagRunId().equals(runId)) throw notFound("dag run node " + nodeId);
    DagRunNode restarted = dagEngine.rerunNode(runId, nodeId);
    auditor.record(operator, "dag_node.rerun", TargetType.DAG_RUN, runId, Map.of("nodeId", nodeId));
    return ResponseEntity.ok(restarted);
  }

  private Dag byId(long id) { return dags.findDag(id).orElseThrow(() -> notFound("dag " + id)); }
  private void requireDag(long id) { byId(id); }
  private static ResponseStatusException notFound(String what) {
    return new ResponseStatusException(HttpStatus.NOT_FOUND, what);
  }
}
