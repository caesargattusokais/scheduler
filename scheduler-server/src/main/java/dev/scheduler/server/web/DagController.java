package dev.scheduler.server.web;

import dev.scheduler.core.Dag;
import dev.scheduler.core.DagEdge;
import dev.scheduler.core.DagNode;
import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunNodeStatus;
import dev.scheduler.core.DagRunStatus;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.DagRepository.EdgeInput;
import dev.scheduler.persistence.DagRepository.NodeInput;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.server.service.DagQueryService;
import dev.scheduler.server.service.DagQueryService.RunDetail;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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
  private final DagQueryService query;

  public DagController(DagRepository dags, ShardRepository shards) {
    this.dags = dags;
    this.shards = shards;
    this.query = new DagQueryService(dags, shards);
  }

  public record CreateDagRequest(String name, String description, String cron,
      List<NodeReq> nodes, List<EdgeReq> edges) {}
  public record NodeReq(String nodeKey, Long taskId, Integer sortOrder) {}
  public record EdgeReq(String from, String to) {}
  public record DagDetail(Dag dag, List<DagNode> nodes, List<DagEdge> edges) {}

  /** 建 DAG:name/cron 合法 + task 存在 + 无自环 + DFS 无环 + 键/边唯一(仓库校验,违规 → IllegalArgumentException → 400)。 */
  @PostMapping
  public ResponseEntity<Dag> create(@RequestBody CreateDagRequest req) {
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
    return ResponseEntity.status(HttpStatus.CREATED).body(
        dags.createDag(req.name(), req.description(), req.cron(), nodes, edges));
  }

  @GetMapping public List<Dag> list() { return dags.findAllDags(); }

  @GetMapping("/{id}")
  public DagDetail get(@PathVariable long id) {
    Dag dag = dags.findDag(id).orElseThrow(() -> notFound("dag " + id));
    return new DagDetail(dag, dags.findNodes(id), dags.findEdges(id));
  }

  @PostMapping("/{id}/pause")  public Dag pause(@PathVariable long id)  { requireDag(id); dags.setPaused(id, true);  return byId(id); }
  @PostMapping("/{id}/resume") public Dag resume(@PathVariable long id) { requireDag(id); dags.setPaused(id, false); return byId(id); }

  /** 手动触发一次:建 dag_run + 全 PENDING 节点,交由 DagEngine 扫描推进。 */
  @PostMapping("/{id}/trigger")
  public ResponseEntity<DagRun> trigger(@PathVariable long id) {
    requireDag(id);
    return ResponseEntity.status(HttpStatus.CREATED).body(dags.createManualRun(id));
  }

  @GetMapping("/runs") public List<DagRun> runs(@RequestParam(required = false) Long dagId) {
    return query.listRuns(dagId);
  }

  @GetMapping("/runs/{runId}")
  public RunDetail runDetail(@PathVariable long runId) {
    return query.runDetail(runId).orElseThrow(() -> notFound("dag run " + runId));
  }

  /** 取消级联(dag → node → execution → shard,spec §4):置 run 取消标志;每个非终态节点置 CANCELED +
   *  outcome;已 spawn 节点再由父取消级联到其 shards;全节点 CANCELED → run 立即终态 CANCELED。 */
  @PostMapping("/runs/{runId}/cancel")
  public ResponseEntity<RunDetail> cancel(@PathVariable long runId) {
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
    dags.finalizeRun(runId, DagRunStatus.CANCELED, "cancelled by operator");
    return ResponseEntity.ok(query.runDetail(runId).orElseThrow());
  }

  private Dag byId(long id) { return dags.findDag(id).orElseThrow(() -> notFound("dag " + id)); }
  private void requireDag(long id) { byId(id); }
  private static ResponseStatusException notFound(String what) {
    return new ResponseStatusException(HttpStatus.NOT_FOUND, what);
  }
}
