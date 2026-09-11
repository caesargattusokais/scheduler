package dev.scheduler.server.web;

import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.server.service.ExecutionDetail;
import dev.scheduler.server.service.ExecutionQueryService;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 执行控制面(spec §5.1):列表/详情/取消/死信。
 *  M3 真正认领/运行单元是 execution_shard;父 execution 为 header(只存 DUE→终态,controller 从不写其 RUNNING)。
 *  取消:父级联(仍有 RUNNING shard → 协作取消 202,否则直取消 200);详情派生父 status(RUNNING);
 *  DLQ 列 shard;/shards/{id}/requeue 重排死信 shard(execution 级 requeue 已随 M3 移除)。 */
@RestController
@RequestMapping("/api/v1/executions")
public class ExecutionController {

  private final ExecutionRepository executions;
  private final ShardRepository shards;
  private final ExecutionQueryService queryService;

  public ExecutionController(ExecutionRepository executions, ShardRepository shards,
                             JdbcTemplate jdbc) {
    this.executions = executions;
    this.shards = shards;
    this.queryService = new ExecutionQueryService(jdbc, executions, shards);
  }

  @GetMapping
  public Page<Execution> list(
      @RequestParam(required = false) Long taskId,
      @RequestParam(required = false) String status,
      @RequestParam(required = false) String from,
      @RequestParam(required = false) String to,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    return queryService.list(taskId, status,
        parseInstant(from), parseInstant(to), limit, offset);
  }

  private static Instant parseInstant(String s) {
    if (s == null || s.isBlank()) return null;
    try {
      return Instant.parse(s); // ISO-8601 带偏移
    } catch (DateTimeParseException e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "invalid `from`/`to` (expected ISO-8601 with offset): " + s);
    }
  }

  /** 详情:父 header + 其分片;展示的父 status 为派生值(非终态父且含 RUNNING shard → 读作 RUNNING)。 */
  @GetMapping("/{id}")
  public ExecutionDetail get(@PathVariable long id) {
    return queryService.getDetail(id).orElseThrow(() -> notFound("execution " + id));
  }

  /** 可被重跑的源轮状态:终态(SUCCESS/FAILED/CANCELED)或 ORPHANED(已结束、不可复原地)轮。 */
  private static final Set<ExecutionStatus> RERUNNABLE =
      Set.of(ExecutionStatus.SUCCESS, ExecutionStatus.FAILED, ExecutionStatus.CANCELED,
          ExecutionStatus.ORPHANED);

  /**
   * 重跑指定一轮执行(M6.5 真重跑):引用该源轮,复制其 args 并落 rerun_of 溯源,新建一轮父 + 全部分片。
   * 源轮不存在 → 404;源轮非终态(DUE/RUNNING)→ 409。同一源轮可多次重跑,每次生成独立一轮(键 rerun:&lt;srcId&gt;:&lt;uuid&gt;)。
   */
  @PostMapping("/{id}/rerun")
  public ResponseEntity<Execution> rerun(@PathVariable long id) {
    Execution source = executions.findById(id).orElseThrow(() -> notFound("execution " + id));
    if (!RERUNNABLE.contains(source.status())) {
      throw new ResponseStatusException(HttpStatus.CONFLICT,
          "execution " + id + " is " + source.status() + " and cannot be rerun (only terminal/orphaned can)");
    }
    String key = "rerun:" + source.id() + ":" + UUID.randomUUID();
    Execution created = shards.createParentWithShards(
        source.taskId(), key, source.shardCount(), source.args(), source.id());
    return ResponseEntity.status(HttpStatus.CREATED).body(created);
  }

  /** DLQ 列表:FAILED 且已标 dead_letter 标记的分片,taskId 过滤 + limit/offset 分页,返回 Page<Shard>。 */
  @GetMapping("/dlq")
  public Page<Shard> dlq(
      @RequestParam(required = false) Long taskId,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    return new Page<>(shards.findDeathLetterShards(taskId, p.limit(), p.offset()),
        shards.countDeathLetterShards(taskId), p.offset(), p.limit());
  }

  /**
   * 死信分片手动重新入队:FAILED shard → DUE 复位 attempt=0/next_retry_at=NULL/dead_letter=false,返回 200 + 现态。
   * 分片不存在 → 404;存在但已非 FAILED → 409。
   */
  @PostMapping("/shards/{shardId}/requeue")
  public ResponseEntity<Shard> requeue(@PathVariable long shardId) {
    Shard s = shards.findShard(shardId).orElseThrow(() -> notFound("shard " + shardId));
    if (s.status() != ExecutionStatus.FAILED) {
      throw new ResponseStatusException(HttpStatus.CONFLICT,
          "shard " + shardId + " is " + s.status() + " and cannot be requeued (only FAILED can)");
    }
    shards.requeueShard(shardId);
    return ResponseEntity.ok(
        shards.findShard(shardId).orElseThrow(() -> notFound("shard " + shardId)));
  }

  /**
   * 父级级联取消(header 语义,父只在 DUE↔终态):
   * <ul>
   *   <li>已 CANCELED → 幂等 200。</li>
   *   <li>终态(SUCCESS/FAILED)→ 409,不可取消。</li>
   *   <li>父非终态(DUE):仍有 RUNNING shard → {@code requestCancelParent} 协作取消(置位 RUNNING 片、
   *       DUE 片编 CANCELED、父置 cancel_requested),返回 202 + 派生现态(父 DUE + RUNNING 片 → 读作 RUNNING);
   *       否则 → {@code cancelParentImmediate} 直取消(父 → CANCELED,全部非终态片 → CANCELED),
   *       返回 200 + 派生现态(CANCELED)。</li>
   * </ul>
   */
  @PostMapping("/{id}/cancel")
  public ResponseEntity<Execution> cancel(@PathVariable long id) {
    Execution e = executions.findById(id).orElseThrow(() -> notFound("execution " + id));
    if (e.status() == ExecutionStatus.CANCELED) {
      return ResponseEntity.ok(e); // 幂等:已取消
    }
    if (e.status() != ExecutionStatus.DUE) {
      throw new ResponseStatusException(HttpStatus.CONFLICT,
          "execution " + id + " is " + e.status() + " and cannot be cancelled (only DUE can)");
    }
    if (shards.hasRunningShard(id)) {
      shards.requestCancelParent(id); // 有 RUNNING 片 → 协作取消
      return ResponseEntity.accepted().body(derived(id));
    }
    shards.cancelParentImmediate(id); // 无 RUNNING 片 → 直取消(父 → CANCELED)
    return ResponseEntity.ok(derived(id));
  }

  /** 由当前父行 + 其分片派生控制面展示的父 status(读取只映射,不写库)。 */
  private Execution derived(long id) {
    Execution parent = executions.findById(id).orElseThrow(() -> notFound("execution " + id));
    List<Shard> ss = shards.findShards(id);
    return new Execution(parent.id(), parent.taskId(),
        ExecutionQueryService.deriveStatus(parent, ss),
        parent.idempotencyKey(), parent.args(), parent.shardIndex(), parent.shardCount(),
        parent.attempt(), parent.workerId(), parent.leaseUntil(), parent.nextRetryAt(),
        parent.startedAt(), parent.finishedAt(), parent.resultPayload(), parent.rerunOf());
  }

  private static ResponseStatusException notFound(String what) {
    return new ResponseStatusException(HttpStatus.NOT_FOUND, what);
  }
}
