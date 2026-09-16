package dev.scheduler.server.web;

import dev.scheduler.core.Execution;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.scheduling.support.CronExpression;

/** 任务控制面(spec §5.1):创建/列表/详情/暂停-恢复/手动触发。 */
@RestController
@RequestMapping("/api/v1/tasks")
public class TaskController {

  private final TaskRepository tasks;
  private final ShardRepository shards;
  private final AvailableHandlerRefs availableRefs;

  public TaskController(TaskRepository tasks, ShardRepository shards, AvailableHandlerRefs availableRefs) {
    this.tasks = tasks;
    this.shards = shards;
    this.availableRefs = availableRefs;
  }

  public record CreateTaskRequest(String name, String kind, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs,
      String retryableFailurePattern, Integer maxActiveConcurrent,
      String retryMode, Long retryCapMs, Long retryBudgetMs) {}

  public record UpdateTaskRequest(String name, String kind, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs,
      String retryableFailurePattern, Integer maxActiveConcurrent, Boolean paused,
      String retryMode, Long retryCapMs, Long retryBudgetMs) {}

  @PostMapping
  public ResponseEntity<Task> create(@RequestBody CreateTaskRequest req) {
    if (req == null || req.name() == null || req.name().isBlank()) {
      throw new IllegalArgumentException("name is required");
    }
    if (req.handlerRef() == null || req.handlerRef().isBlank()) {
      throw new IllegalArgumentException("handlerRef is required");
    }
    if (req.cron() == null || req.cron().isBlank()) {
      throw new IllegalArgumentException("cron is required");
    }
    requireAvailableHandlerRef(req.handlerRef());
    checkDefinition(req.name(), req.handlerRef(), req.cron(), req.shardCount(),
        req.timeoutSeconds(), req.maxRetries(), req.backoffMs(), req.maxActiveConcurrent(),
        req.retryMode(), req.retryCapMs(), req.retryBudgetMs());
    Task created = tasks.create(new Task(
        null, req.name(), req.kind() == null ? "cron" : req.kind(), req.handlerRef(), req.cron(),
        req.shardCount() == null ? 1 : req.shardCount(),
        req.timeoutSeconds() == null ? 300 : req.timeoutSeconds(),
        req.maxRetries() == null ? 0 : req.maxRetries(),
        req.backoffMs() == null ? 1000L : req.backoffMs(),
        req.retryableFailurePattern(), req.maxActiveConcurrent() == null ? 8 : req.maxActiveConcurrent(),
        true, false, req.retryMode(), req.retryCapMs(), req.retryBudgetMs()));
    return ResponseEntity.status(HttpStatus.CREATED).body(created);
  }

  @PutMapping("/{id}")
  public Task update(@PathVariable long id, @RequestBody UpdateTaskRequest req) {
    Task existing = requireTask(id);
    if (req == null || req.name() == null || req.name().isBlank()) {
      throw new IllegalArgumentException("name is required");
    }
    if (req.handlerRef() == null || req.handlerRef().isBlank()) {
      throw new IllegalArgumentException("handlerRef is required");
    }
    if (req.cron() == null || req.cron().isBlank()) {
      throw new IllegalArgumentException("cron is required");
    }
    requireAvailableHandlerRef(req.handlerRef());
    checkDefinition(req.name(), req.handlerRef(), req.cron(), req.shardCount(),
        req.timeoutSeconds(), req.maxRetries(), req.backoffMs(), req.maxActiveConcurrent(),
        req.retryMode(), req.retryCapMs(), req.retryBudgetMs());
    Task updated = new Task(id, req.name(), req.kind() == null ? existing.kind() : req.kind(),
        req.handlerRef(), req.cron(),
        req.shardCount() == null ? existing.shardCount() : req.shardCount(),
        req.timeoutSeconds() == null ? existing.timeoutSeconds() : req.timeoutSeconds(),
        req.maxRetries() == null ? existing.maxRetries() : req.maxRetries(),
        req.backoffMs() == null ? existing.backoffMs() : req.backoffMs(),
        req.retryableFailurePattern() == null ? existing.retryableFailurePattern() : req.retryableFailurePattern(),
        req.maxActiveConcurrent() == null ? existing.maxActiveConcurrent() : req.maxActiveConcurrent(),
        existing.enabled(),
        req.paused() == null ? existing.paused() : req.paused(),
        req.retryMode() == null ? existing.retryMode() : req.retryMode(),
        req.retryCapMs() == null ? existing.retryCapMs() : req.retryCapMs(),
        req.retryBudgetMs() == null ? existing.retryBudgetMs() : req.retryBudgetMs());
    if (!tasks.update(id, updated)) {
      throw notFound("task " + id);
    }
    return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  }

  /** 任务列表:name 子串、paused 过滤 + limit/offset 分页,返回 Page<Task>。 */
  @GetMapping
  public Page<Task> list(
      @RequestParam(required = false) String name,
      @RequestParam(required = false) Boolean paused,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    return new Page<>(tasks.findPage(name, paused, p.limit(), p.offset()),
        tasks.count(name, paused), p.offset(), p.limit());
  }

  @GetMapping("/{id}")
  public Task get(@PathVariable long id) {
    return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  }

  @PostMapping("/{id}/pause")
  public Task pause(@PathVariable long id) {
    requireTask(id);
    tasks.setPaused(id, true);
    return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  }

  @PostMapping("/{id}/resume")
  public Task resume(@PathVariable long id) {
    requireTask(id);
    tasks.setPaused(id, false);
    return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  }

  /**
   * 物理删除任务:仅当任务无任何 execution 且未被任何 DAG 节点/运行引用时允许(保留审计历史)。
   * 有子记录 → 409 并说明被什么阻塞;任务不存在 → 404;成功 → 204。
   */
  @DeleteMapping("/{id}")
  public ResponseEntity<Void> delete(@PathVariable long id) {
    requireTask(id);
    List<String> blockers = new ArrayList<>();
    long ec = tasks.executionCount(id);
    if (ec > 0) blockers.add(ec + " 条执行记录");
    long dc = tasks.dagReferenceCount(id);
    if (dc > 0) blockers.add("被 " + dc + " 处 DAG 节点/运行引用");
    if (!blockers.isEmpty()) {
      throw new ResponseStatusException(HttpStatus.CONFLICT,
          "task " + id + " cannot be deleted: " + String.join("、", blockers));
    }
    if (!tasks.delete(id)) {
      throw notFound("task " + id); // 竞态兜底:计数后并发插入的子记录
    }
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/{id}/trigger")
  public ResponseEntity<Execution> trigger(@PathVariable long id) {
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(manualRun(id, UUID.randomUUID().toString()));
  }

  /** 手动触发:建父 execution + 其 shard(重跑已 M6.5 移入 /executions/{id}/rerun)。 */
  private Execution manualRun(long taskId, String suffix) {
    Task t = requireTask(taskId);
    String key = "manual:" + t.id() + ":" + suffix;
    long pid = shards.createParentWithShards(t.id(), key, t.shardCount()).id();
    return shards.findParent(pid).orElseThrow(() -> notFound("execution " + pid));
  }

  private Task requireTask(long id) {
    return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  }

  /** M6.2:入口即拒绝孤儿 ref——handlerRef 必须为进程内 ∪ 存活 worker 并集中的某 ref(否则建/改 400)。 */
  private void requireAvailableHandlerRef(String handlerRef) {
    if (!availableRefs.refs().contains(handlerRef)) {
      throw new IllegalArgumentException("handler ref '" + handlerRef + "' served by no live worker");
    }
  }

  /** 定义域数值合法性:负值即 400;cron 必须 6/7 字段且可被 Spring 解析(字段值非法提前 400,防坏 cron 入库
   *  运行时级联拖垮触发扫描)。maxActiveConcurrent 为并发配额,必须 >= 1(0/负会让 claim 闸门恒 false → 任务
   *  永久卡 DUE)。 */
  static void checkDefinition(String name, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs,
      Integer maxActiveConcurrent, String retryMode, Long retryCapMs, Long retryBudgetMs) {
    if (retryMode != null && !retryMode.isBlank()
        && !(retryMode.equals("fixed") || retryMode.equals("linear") || retryMode.equals("exponential"))) {
      throw new IllegalArgumentException("retryMode must be one of fixed|linear|exponential");
    }
    if (retryCapMs != null && retryCapMs < 0) throw new IllegalArgumentException("retryCapMs must be >= 0");
    if (retryBudgetMs != null && retryBudgetMs < 0) throw new IllegalArgumentException("retryBudgetMs must be >= 0");
    if (timeoutSeconds != null && timeoutSeconds < 0) throw new IllegalArgumentException("timeoutSeconds must be >= 0");
    if (backoffMs != null && backoffMs < 0) throw new IllegalArgumentException("backoffMs must be >= 0");
    if (maxRetries != null && maxRetries < 0) throw new IllegalArgumentException("maxRetries must be >= 0");
    if (shardCount != null && shardCount < 1) throw new IllegalArgumentException("shardCount must be >= 1");
    if (maxActiveConcurrent != null && maxActiveConcurrent < 1) {
      throw new IllegalArgumentException("maxActiveConcurrent must be >= 1");
    }
    validateCron(cron);
  }

  /** 本项目 cron 一律 6 字段(或 7 字段带年);5 字段会被 Spring 6 解析器直接抛异常处决在前面。
   *  字段数合法后仍须 C 解析预检——cron 字段值(如秒 >= 60)非法时,解析要在触发扫描线程才炸并拖累整批,这里提前拦。 */
  static void validateCron(String cron) {
    int fields = cron.trim().split("\\s+").length;
    if (fields != 6 && fields != 7) {
      throw new IllegalArgumentException(
          "cron must be a 6-field (sec min hour dom mon dow) or 7-field (+year) expression, got "
              + fields + " fields: " + cron);
    }
    CronExpression.parse(cron); // cron 字段值非法 → 提前 400(而非运行时触发线程级联抛)
  }

  private static ResponseStatusException notFound(String what) {
    return new ResponseStatusException(HttpStatus.NOT_FOUND, what);
  }
}
