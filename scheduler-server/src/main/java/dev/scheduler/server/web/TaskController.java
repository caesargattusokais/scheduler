package dev.scheduler.server.web;

import dev.scheduler.core.Execution;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 任务控制面(spec §5.1):创建/列表/详情/暂停-恢复/手动触发。 */
@RestController
@RequestMapping("/api/v1/tasks")
public class TaskController {

  private final TaskRepository tasks;
  private final ShardRepository shards;

  public TaskController(TaskRepository tasks, ShardRepository shards) {
    this.tasks = tasks;
    this.shards = shards;
  }

  public record CreateTaskRequest(String name, String kind, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs,
      String retryableFailurePattern, Integer maxActiveConcurrent) {}

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
    validateCron(req.cron());
    if (req.timeoutSeconds() != null && req.timeoutSeconds() < 0) {
      throw new IllegalArgumentException("timeoutSeconds must be >= 0");
    }
    if (req.backoffMs() != null && req.backoffMs() < 0) {
      throw new IllegalArgumentException("backoffMs must be >= 0");
    }
    if (req.maxRetries() != null && req.maxRetries() < 0) {
      throw new IllegalArgumentException("maxRetries must be >= 0");
    }
    if (req.shardCount() != null && req.shardCount() < 1) {
      throw new IllegalArgumentException("shardCount must be >= 1");
    }
    Task created = tasks.create(new Task(
        null, req.name(), req.kind() == null ? "cron" : req.kind(), req.handlerRef(), req.cron(),
        req.shardCount() == null ? 1 : req.shardCount(),
        req.timeoutSeconds() == null ? 300 : req.timeoutSeconds(),
        req.maxRetries() == null ? 0 : req.maxRetries(),
        req.backoffMs() == null ? 1000L : req.backoffMs(),
        req.retryableFailurePattern(), req.maxActiveConcurrent() == null ? 8 : req.maxActiveConcurrent(),
        true, false));
    return ResponseEntity.status(HttpStatus.CREATED).body(created);
  }

  @GetMapping
  public List<Task> list() {
    return tasks.findAll();
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

  /** 手动触发一次:登记一条 DUE execution,交由执行器认领派发。 */
  @PostMapping("/{id}/trigger")
  public ResponseEntity<Execution> trigger(@PathVariable long id) {
    Task t = requireTask(id);
    String key = "manual:" + t.id() + ":" + UUID.randomUUID();
    long pid = shards.createParentWithShards(t.id(), key, t.shardCount()).id();
    Execution e = shards.findParent(pid).orElseThrow(() -> notFound("execution " + pid));
    return ResponseEntity.status(HttpStatus.CREATED).body(e);
  }

  private Task requireTask(long id) {
    return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  }

  /** 本项目 cron 一律 6 字段(或 7 字段带年);5 字段会被 Spring 6 解析器直接抛异常处决在前面。 */
  static void validateCron(String cron) {
    int fields = cron.trim().split("\\s+").length;
    if (fields != 6 && fields != 7) {
      throw new IllegalArgumentException(
          "cron must be a 6-field (sec min hour dom mon dow) or 7-field (+year) expression, got "
              + fields + " fields: " + cron);
    }
  }

  private static ResponseStatusException notFound(String what) {
    return new ResponseStatusException(HttpStatus.NOT_FOUND, what);
  }
}
