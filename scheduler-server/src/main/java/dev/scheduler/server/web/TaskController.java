package dev.scheduler.server.web;

import dev.scheduler.core.Execution;
import dev.scheduler.core.TargetType;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.security.CurrentOperator;
import dev.scheduler.server.service.AuditRecorder;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private final AuditRecorder auditor;
  private final CurrentOperator current;

  public TaskController(TaskRepository tasks, ShardRepository shards, AvailableHandlerRefs availableRefs,
                        AuditRecorder auditor, CurrentOperator current) {
    this.tasks = tasks;
    this.shards = shards;
    this.availableRefs = availableRefs;
    this.auditor = auditor;
    this.current = current;
  }

  public record CreateTaskRequest(String name, String kind, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs,
      String retryableFailurePattern, Integer maxActiveConcurrent,
      String retryMode, Long retryCapMs, Long retryBudgetMs,
      String timezone, Integer intervalSeconds, List<String> eventRoutes,
      String successPolicyType, Integer successPolicyValue) {}

  public record UpdateTaskRequest(String name, String kind, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs,
      String retryableFailurePattern, Integer maxActiveConcurrent, Boolean paused,
      String retryMode, Long retryCapMs, Long retryBudgetMs,
      String timezone, Integer intervalSeconds, List<String> eventRoutes,
      String successPolicyType, Integer successPolicyValue) {}

  @PostMapping
  public ResponseEntity<Task> create(
      @RequestBody CreateTaskRequest req) {
    if (req == null || req.name() == null || req.name().isBlank()) {
      throw new IllegalArgumentException("name is required");
    }
    if (req.handlerRef() == null || req.handlerRef().isBlank()) {
      throw new IllegalArgumentException("handlerRef is required");
    }
    requireAvailableHandlerRef(req.handlerRef());
    checkDefinition(req.name(), req.handlerRef(), req.cron(), req.shardCount(),
        req.timeoutSeconds(), req.maxRetries(), req.backoffMs(), req.maxActiveConcurrent(),
        req.retryMode(), req.retryCapMs(), req.retryBudgetMs(),
        req.timezone(), req.intervalSeconds(), req.eventRoutes(),
        req.successPolicyType(), req.successPolicyValue());
    Task created = tasks.create(new Task(
        null, req.name(), req.kind() == null ? (req.eventRoutes() == null || req.eventRoutes().isEmpty() ? "cron" : "event") : req.kind(),
        req.handlerRef(), req.cron(),
        req.shardCount() == null ? 1 : req.shardCount(),
        req.timeoutSeconds() == null ? 300 : req.timeoutSeconds(),
        req.maxRetries() == null ? 0 : req.maxRetries(),
        req.backoffMs() == null ? 1000L : req.backoffMs(),
        req.retryableFailurePattern(), req.maxActiveConcurrent() == null ? 8 : req.maxActiveConcurrent(),
        true, false, req.retryMode(), req.retryCapMs(), req.retryBudgetMs(),
        req.timezone() == null ? "UTC" : req.timezone(), req.intervalSeconds(),
        req.eventRoutes() == null ? List.of() : req.eventRoutes(),
        req.successPolicyType(), req.successPolicyValue()));
    auditor.record(current.get(), "task.create", TargetType.TASK, created.id(), taskMeta(created));
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
    requireAvailableHandlerRef(req.handlerRef());
    checkDefinition(req.name(), req.handlerRef(), req.cron(), req.shardCount(),
        req.timeoutSeconds(), req.maxRetries(), req.backoffMs(), req.maxActiveConcurrent(),
        req.retryMode(), req.retryCapMs(), req.retryBudgetMs(),
        req.timezone(), req.intervalSeconds(), req.eventRoutes(),
        req.successPolicyType(), req.successPolicyValue());
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
        req.retryBudgetMs() == null ? existing.retryBudgetMs() : req.retryBudgetMs(),
        req.timezone() == null ? existing.timezone() : req.timezone(),
        req.intervalSeconds() == null ? existing.intervalSeconds() : req.intervalSeconds(),
        req.eventRoutes() == null ? existing.eventRoutes() : req.eventRoutes(),
        req.successPolicyType() == null ? existing.successPolicyType() : req.successPolicyType(),
        req.successPolicyValue() == null ? existing.successPolicyValue() : req.successPolicyValue());
    if (!tasks.update(id, updated)) {
      throw notFound("task " + id);
    }
    Task saved = tasks.findById(id).orElseThrow(() -> notFound("task " + id));
    auditor.record(current.get(), "task.update", TargetType.TASK, saved.id(), taskMeta(saved), taskDiff(existing, saved), taskBefore(existing));
    return saved;
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
    Task before = requireTask(id);
    tasks.setPaused(id, true);
    auditor.record(current.get(), "task.pause", TargetType.TASK, id, Map.of(), null, taskBefore(before));
    return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  }

  @PostMapping("/{id}/resume")
  public Task resume(@PathVariable long id) {
    Task before = requireTask(id);
    tasks.setPaused(id, false);
    auditor.record(current.get(), "task.resume", TargetType.TASK, id, Map.of(), null, taskBefore(before));
    return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  }

  /**
   * 物理删除任务:仅当任务无任何 execution 且未被任何 DAG 节点/运行引用时允许(保留审计历史)。
   * 有子记录 → 409 并说明被什么阻塞;任务不存在 → 404;成功 → 204。
   */
  @DeleteMapping("/{id}")
  public ResponseEntity<Void> delete(@PathVariable long id) {
    Task before = requireTask(id);
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
    auditor.record(current.get(), "task.delete", TargetType.TASK, id, Map.of(), null, taskBefore(before));
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/{id}/trigger")
  public ResponseEntity<Execution> trigger(
      @PathVariable long id) {
    Task before = requireTask(id);
    Execution run = manualRun(id, UUID.randomUUID().toString());
    auditor.record(current.get(), "task.trigger", TargetType.TASK, id, Map.of(), null, taskBefore(before));
    return ResponseEntity.status(HttpStatus.CREATED).body(run);
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

  /** 审计 meta = 操作后已确认的 Task 后态紧凑字段(非 diff)。 */
  private Map<String, Object> taskMeta(Task t) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("name", t.name());
    m.put("kind", t.kind());
    m.put("handlerRef", t.handlerRef());
    m.put("cron", t.cron());
    m.put("retryableFailurePattern", t.retryableFailurePattern());
    m.put("maxActiveConcurrent", t.maxActiveConcurrent());
    m.put("shardCount", t.shardCount());
    m.put("timeoutSeconds", t.timeoutSeconds());
    m.put("maxRetries", t.maxRetries());
    m.put("backoffMs", t.backoffMs());
    m.put("retryMode", t.retryMode());
    m.put("retryCapMs", t.retryCapMs());
    m.put("retryBudgetMs", t.retryBudgetMs());
    m.put("successPolicyType", t.successPolicyType());
    m.put("successPolicyValue", t.successPolicyValue());
    m.put("enabled", t.enabled());
    m.put("paused", t.paused());
    return m;
  }

  /** 审计 before = 操作前全量 Task 定义(16 组件):供取证回溯 / delete 后还原;与 taskMeta 后态紧凑互补。 */
  private Map<String, Object> taskBefore(Task t) {
    Map<String, Object> b = new LinkedHashMap<>();
    b.put("id", t.id());
    b.put("name", t.name());
    b.put("kind", t.kind());
    b.put("handlerRef", t.handlerRef());
    b.put("cron", t.cron());
    b.put("shardCount", t.shardCount());
    b.put("timeoutSeconds", t.timeoutSeconds());
    b.put("maxRetries", t.maxRetries());
    b.put("backoffMs", t.backoffMs());
    b.put("retryableFailurePattern", t.retryableFailurePattern());
    b.put("maxActiveConcurrent", t.maxActiveConcurrent());
    b.put("enabled", t.enabled());
    b.put("paused", t.paused());
    b.put("retryMode", t.retryMode());
    b.put("retryCapMs", t.retryCapMs());
    b.put("retryBudgetMs", t.retryBudgetMs());
    b.put("successPolicyType", t.successPolicyType());
    b.put("successPolicyValue", t.successPolicyValue());
    return b;
  }

  /** task.update 的 before/after 字段级 diff:遍历 15 个可变字段,仅收录前后不同者 → {field:[before,after]}。 */
  private Map<String, Object> taskDiff(Task before, Task after) {
    Map<String, Object> d = new LinkedHashMap<>();
    putDiff(d, "name", before.name(), after.name());
    putDiff(d, "kind", before.kind(), after.kind());
    putDiff(d, "handlerRef", before.handlerRef(), after.handlerRef());
    putDiff(d, "cron", before.cron(), after.cron());
    putDiff(d, "shardCount", before.shardCount(), after.shardCount());
    putDiff(d, "timeoutSeconds", before.timeoutSeconds(), after.timeoutSeconds());
    putDiff(d, "maxRetries", before.maxRetries(), after.maxRetries());
    putDiff(d, "backoffMs", before.backoffMs(), after.backoffMs());
    putDiff(d, "retryableFailurePattern", before.retryableFailurePattern(), after.retryableFailurePattern());
    putDiff(d, "maxActiveConcurrent", before.maxActiveConcurrent(), after.maxActiveConcurrent());
    putDiff(d, "enabled", before.enabled(), after.enabled());
    putDiff(d, "paused", before.paused(), after.paused());
    putDiff(d, "retryMode", before.retryMode(), after.retryMode());
    putDiff(d, "retryCapMs", before.retryCapMs(), after.retryCapMs());
    putDiff(d, "retryBudgetMs", before.retryBudgetMs(), after.retryBudgetMs());
    putDiff(d, "timezone", before.timezone(), after.timezone());
    putDiff(d, "intervalSeconds", before.intervalSeconds(), after.intervalSeconds());
    putDiff(d, "eventRoutes", before.eventRoutes(), after.eventRoutes());
    putDiff(d, "successPolicyType", before.successPolicyType(), after.successPolicyType());
    putDiff(d, "successPolicyValue", before.successPolicyValue(), after.successPolicyValue());
    return d;
  }

  private void putDiff(Map<String, Object> d, String field, Object before, Object after) {
    if (!Objects.equals(before, after)) d.put(field, List.of(before, after));
  }

  /** M6.2:入口即拒绝孤儿 ref——handlerRef 必须为进程内 ∪ 存活 worker 并集中的某 ref(否则建/改 400)。 */
  private void requireAvailableHandlerRef(String handlerRef) {
    if (!availableRefs.refs().contains(handlerRef)) {
      throw new IllegalArgumentException("handler ref '" + handlerRef + "' served by no live worker");
    }
  }

  /** 定义域数值合法性:负值即 400;cron 必须 6/7 字段且可被 Spring 解析(字段值非法提前 400,防坏 cron 入库
   *  运行时级联拖垮触发扫描)。maxActiveConcurrent 为并发配额,必须 >= 1(0/负会让 claim 闸门恒 false → 任务
   *  永久卡 DUE)。
   *  3a/3b 触发时钟:任务必须恰具其一——cron、intervalSeconds、eventRoutes(非空),同时给两个 → 400(歧义)、
   *  全空 → 400(坏任务,否则该任务不触发任何轮);timezone 必须为合法 ZoneId(默认 UTC);intervalSeconds >= 1;
   *  event_routes 各路由 key 不得为空串。cron 为空时不校验 cron(此时走间隔或事件触发)。 */
  static void checkDefinition(String name, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs,
      Integer maxActiveConcurrent, String retryMode, Long retryCapMs, Long retryBudgetMs,
      String timezone, Integer intervalSeconds, List<String> eventRoutes,
      String successPolicyType, Integer successPolicyValue) {
    boolean hasCron = cron != null && !cron.isBlank();
    boolean hasInterval = intervalSeconds != null;
    boolean hasEvents = eventRoutes != null && !eventRoutes.isEmpty();
    int clockCount = (hasCron ? 1 : 0) + (hasInterval ? 1 : 0) + (hasEvents ? 1 : 0);
    if (clockCount > 1) {
      throw new IllegalArgumentException("provide exactly one of cron, intervalSeconds, eventRoutes");
    }
    if (clockCount == 0) {
      throw new IllegalArgumentException("cron, intervalSeconds or eventRoutes is required");
    }
    if (intervalSeconds != null && intervalSeconds < 1) {
      throw new IllegalArgumentException("intervalSeconds must be >= 1");
    }
    if (eventRoutes != null) {
      for (String r : eventRoutes) {
        if (r == null || r.isBlank()) throw new IllegalArgumentException("event route key must not be blank");
      }
    }
    if (timezone != null && !timezone.isBlank()) {
      try { ZoneId.of(timezone); }
      catch (java.time.DateTimeException badZone) {
        throw new IllegalArgumentException("bad timezone: " + timezone);
      }
    }
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
    // 3c 部分成功策略:类型白名单四选一;RATIO_PERCENT 值∈[1,99],MIN_SUCCESS ≥ 1,MAX_FAILURES ≥ 0。
    if (successPolicyType != null && !successPolicyType.isBlank()
        && !successPolicyType.equals("NONE") && !successPolicyType.equals("RATIO_PERCENT")
        && !successPolicyType.equals("MIN_SUCCESS") && !successPolicyType.equals("MAX_FAILURES")) {
      throw new IllegalArgumentException("successPolicyType must be one of NONE|RATIO_PERCENT|MIN_SUCCESS|MAX_FAILURES");
    }
    if ("RATIO_PERCENT".equals(successPolicyType)
        && (successPolicyValue == null || successPolicyValue < 1 || successPolicyValue > 99)) {
      throw new IllegalArgumentException("successPolicyValue for RATIO_PERCENT must be in [1,99]");
    }
    if ("MIN_SUCCESS".equals(successPolicyType) && (successPolicyValue == null || successPolicyValue < 1)) {
      throw new IllegalArgumentException("successPolicyValue for MIN_SUCCESS must be >= 1");
    }
    if ("MAX_FAILURES".equals(successPolicyType) && (successPolicyValue == null || successPolicyValue < 0)) {
      throw new IllegalArgumentException("successPolicyValue for MAX_FAILURES must be >= 0");
    }
    if (hasCron) validateCron(cron); // 间隔触发任务(cron 空)不校验 cron
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
