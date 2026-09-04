package dev.scheduler.server.web;

import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.server.service.ExecutionQueryService;
import java.util.List;
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

/** 执行控制面(spec §5.1):列表/详情/取消。取消:DUE 直接转 CANCELED;RUNNING 置 cancel_requested
 *  协作取消(返回 202);终态(SUCCESS/FAILED)不可取消(409)。 */
@RestController
@RequestMapping("/api/v1/executions")
public class ExecutionController {

  private final ExecutionRepository executions;
  private final ExecutionQueryService queryService;
  private final String workerId;

  public ExecutionController(ExecutionRepository executions, JdbcTemplate jdbc, String schedulerWorkerId) {
    this.executions = executions;
    this.queryService = new ExecutionQueryService(jdbc, executions);
    this.workerId = schedulerWorkerId;
  }

  @GetMapping
  public List<Execution> list(@RequestParam(required = false) Long taskId,
                              @RequestParam(required = false) String status) {
    return queryService.list(taskId, status);
  }

  @GetMapping("/{id}")
  public Execution get(@PathVariable long id) {
    return queryService.get(id).orElseThrow(() -> notFound("execution " + id));
  }

  /** DLQ 列表:FAILED 且已置 dead_letter 标记的执行,按 id 升序;空 → 200 []。 */
  @GetMapping("/dlq")
  public List<Execution> dlq() {
    return executions.findDeadLetters();
  }

  /**
   * DLQ 手动重新入队:FAILED → DUE 复位 attempt=0/next_retry_at=NULL/dead_letter=false,返回 200 + 现态。
   * 行不存在 → 404;存在但已非 FAILED → 409。
   */
  @PostMapping("/{id}/requeue")
  public ResponseEntity<Execution> requeue(@PathVariable long id) {
    Execution e = executions.findById(id).orElseThrow(() -> notFound("execution " + id));
    if (e.status() != ExecutionStatus.FAILED) {
      throw new ResponseStatusException(HttpStatus.CONFLICT,
          "execution " + id + " is " + e.status() + " and cannot be requeued (only FAILED can)");
    }
    executions.requeue(id);
    return ResponseEntity.ok(executions.findById(id).orElseThrow(() -> notFound("execution " + id)));
  }

  /**
   * 取消 execution:
   * <ul>
   *   <li>DUE → 直接转 CANCELED,返回 200。</li>
   *   <li>RUNNING → 置 cancel_requested 协作取消(不动状态,由 worker 经 token 协作退出),返回 202 + 现态。</li>
   *   <li>已 CANCELED → 幂等 200。</li>
   *   <li>其他终态(SUCCESS/FAILED)→ 409,不可取消。</li>
   * </ul>
   */
  @PostMapping("/{id}/cancel")
  public ResponseEntity<Execution> cancel(@PathVariable long id) {
    Execution e = executions.findById(id).orElseThrow(() -> notFound("execution " + id));
    if (e.status() == ExecutionStatus.CANCELED) {
      return ResponseEntity.ok(e); // 幂等:已取消
    }
    if (e.status() == ExecutionStatus.DUE) {
      executions.markStatus(id, ExecutionStatus.CANCELED, workerId, "cancel via api");
      return ResponseEntity.ok(executions.findById(id).orElseThrow(() -> notFound("execution " + id)));
    }
    if (e.status() == ExecutionStatus.RUNNING) {
      // 协作取消:仅置 cancel_requested 标志(真实转 CANCELED 由 worker 落),返回 202 + 现态。
      executions.requestCancel(id);
      return ResponseEntity.accepted()
          .body(executions.findById(id).orElseThrow(() -> notFound("execution " + id)));
    }
    throw new ResponseStatusException(HttpStatus.CONFLICT,
        "execution " + id + " is " + e.status() + " and cannot be cancelled (only DUE or RUNNING can)");
  }

  private static ResponseStatusException notFound(String what) {
    return new ResponseStatusException(HttpStatus.NOT_FOUND, what);
  }
}
