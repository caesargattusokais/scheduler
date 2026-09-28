package dev.scheduler.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.core.Execution;
import dev.scheduler.core.Shard;
import dev.scheduler.core.Task;
import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.NotificationRepository;
import dev.scheduler.persistence.OutboundNotification;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.persistence.Webhook;
import dev.scheduler.persistence.WebhookRepository;
import dev.scheduler.server.security.CurrentOperator;
import dev.scheduler.server.service.AuditRecorder;
import dev.scheduler.server.service.NotificationTaskMatcher;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.http.HttpStatus;
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

/** 通知告警闭环 API:
 * <ul>
 *   <li>{@code /api/v1/webhooks} 订阅端点 CRUD——写端点整体由 OperatorInterceptor 提权到 ADMIN;</li>
 *   <li>{@code /api/v1/notifications} 投递历史查询(读,开放)。</li>
 * </ul> */
@RestController
@RequestMapping("/api/v1")
public class NotificationsController {
  private final WebhookRepository webhooks;
  private final NotificationRepository notifications;
  private final ExecutionRepository executions;
  private final TaskRepository tasks;
  private final ShardRepository shards;
  private final ObjectMapper json;
  private final NotificationTaskMatcher taskMatcher;
  private final AuditRecorder auditor;
  private final CurrentOperator current;

  public NotificationsController(WebhookRepository webhooks, NotificationRepository notifications,
                                 ExecutionRepository executions, TaskRepository tasks,
                                 ShardRepository shards, ObjectMapper json,
                                 NotificationTaskMatcher taskMatcher,
                                 AuditRecorder auditor, CurrentOperator current) {
    this.webhooks = webhooks;
    this.notifications = notifications;
    this.executions = executions;
    this.tasks = tasks;
    this.shards = shards;
    this.json = json;
    this.taskMatcher = taskMatcher;
    this.auditor = auditor;
    this.current = current;
  }

  /** 订阅请求体:url 必填非空;maxAttempts 默认 5、backoffMs 默认 1000(与旧配置默认一致);kinds 空 = 订阅全部。
 *  scopeMode ALL/INCLUDE/EXCLUDE(缺省 ALL);selectedTaskIds/selectedDagIds 为任务维度选中集(缺省空)。 */
  public record WebhookRequest(String url, String secret, List<String> kinds,
                               Boolean enabled, Integer maxAttempts, Long backoffMs,
                               String scopeMode, List<Long> selectedTaskIds, List<Long> selectedDagIds) {}

  @GetMapping("/webhooks")
  public List<Webhook> listWebhooks() {
    return webhooks.list();
  }

  @PostMapping("/webhooks")
  public Webhook createWebhook(@RequestBody WebhookRequest req) {
    String url = requireUrl(req.url());
    boolean enabled = req.enabled() == null || req.enabled();
    int maxAttempts = req.maxAttempts() == null ? 5 : req.maxAttempts();
    long backoffMs = req.backoffMs() == null ? 1000 : req.backoffMs();
    validateBudget(maxAttempts, backoffMs);
    List<String> kinds = req.kinds() == null ? List.of() : req.kinds();
    String scopeMode = scope(req.scopeMode());
    List<Long> taskIds = orEmpty(req.selectedTaskIds());
    List<Long> dagIds = orEmpty(req.selectedDagIds());
    validateScope(scopeMode, taskIds, dagIds);
    long id = webhooks.create(url, req.secret(), kinds, enabled, maxAttempts, backoffMs,
        scopeMode, taskIds, dagIds);
    audit("webhook.create", id, url, kinds, enabled);
    return find(id);
  }

  @PutMapping("/webhooks/{id}")
  public Webhook updateWebhook(@PathVariable long id, @RequestBody WebhookRequest req) {
    String url = requireUrl(req.url());
    boolean enabled = req.enabled() == null || req.enabled();
    int maxAttempts = req.maxAttempts() == null ? 5 : req.maxAttempts();
    long backoffMs = req.backoffMs() == null ? 1000 : req.backoffMs();
    validateBudget(maxAttempts, backoffMs);
    List<String> kinds = req.kinds() == null ? List.of() : req.kinds();
    String scopeMode = scope(req.scopeMode());
    List<Long> taskIds = orEmpty(req.selectedTaskIds());
    List<Long> dagIds = orEmpty(req.selectedDagIds());
    validateScope(scopeMode, taskIds, dagIds);
    if (!webhooks.update(id, url, req.secret(), kinds, enabled, maxAttempts, backoffMs,
        scopeMode, taskIds, dagIds)) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "webhook " + id + " not found");
    }
    audit("webhook.update", id, url, kinds, enabled);
    return find(id);
  }

  @DeleteMapping("/webhooks/{id}")
  public Map<String, Object> deleteWebhook(@PathVariable long id) {
    if (!webhooks.delete(id)) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "webhook " + id + " not found");
    }
    auditor.record(current.get(), "webhook.delete", TargetType.NONE, id,
        Map.of("id", id));
    return Map.of("deleted", id);
  }

  /**
   * 投递历史每行回答「哪个执行完成 + 结果是什么」:通知行仅存 target_type/target_id 与 firer 落库的
   * 轻量 payload{parentId, terminal, ...},不带任务名/真实结果。此处对 execution 类通知 join 补全
   * taskName(经 execution→task)与 resultPayload(执行真实回写)。执行已删除/回收则两项为 null,UI 回落轻量 payload。
   */
  /**
   * deliveredTo:该通知投往的 webhook 端点列表。投递是「一条 outbox 行广播给所有 kinds 匹配的启用 webhook,
   * 全部收到 200 才标 SENT」,故 SENT 行此列表即实际收到者;PENDING/FAILED 行为投递目标(未达的端点由 lastError 指出)。
   * deliveredBody:投递时经 HTTP POST 实际发出的完整 JSON 信封(与 NotificationDispatcher.buildBody 同构,
   * 签 HMAC 的就是这份字节)——「通知的内容是什么」的直接答案。
   */
  public record NotificationView(long id, String kind, String status, String operator, int attempts,
                                 String lastError, Instant createdAt, Instant sentAt,
                                 String targetType, Long targetId, String payload,
                                 String taskName, String resultPayload,
                                 String deliveredBody, List<String> deliveredTo) {}

  @GetMapping("/notifications")
  public Page<NotificationView> listNotifications(
      @RequestParam(required = false) String kind,
      @RequestParam(required = false) String status,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    List<OutboundNotification> rows = notifications.findPage(kind, status, p.limit(), p.offset());
    List<Webhook> webhookTargets = webhooks.list();
    List<NotificationView> items = rows.stream()
        .map(n -> {
          String taskName = null;
          String resultPayload = null;
          if ("execution".equals(n.targetType()) && n.targetId() != null) {
            Execution ex = executions.findById(n.targetId()).orElse(null);
            if (ex != null) {
              Task t = ex.taskId() == null ? null : tasks.findById(ex.taskId()).orElse(null);
              taskName = t == null ? null : t.name();
              // 父 execution 仅作 header(结果写于分片),resultPayload 常为 null → 回落首个非空分片结果。
              resultPayload = ex.resultPayload();
              if (resultPayload == null) {
                resultPayload = shards.findShards(ex.id()).stream()
                    .map(Shard::resultPayload).filter(java.util.Objects::nonNull)
                    .findFirst().orElse(null);
              }
            }
          }
          return new NotificationView(n.id(), n.kind(), n.status(), n.operator(), n.attempts(),
              n.lastError(), n.createdAt(), n.sentAt(),
              n.targetType(), n.targetId(), n.payload(), taskName, resultPayload,
              deliveredBody(n), subscribedUrls(webhookTargets, n));
        }).toList();
    return new Page<>(items, notifications.count(kind, status), p.offset(), p.limit());
  }

  /** 该通知的投递目标:启用 + kinds 订阅 + 任务维度命中(与 dispatcher 过滤同源)的 webhook URL。
 *  SENT 行即实际收到者;PENDING/FAILED 行为投递目标(未达端点由 lastError 指出)。 */
  private List<String> subscribedUrls(List<Webhook> ws, OutboundNotification n) {
    return ws.stream()
        .filter(w -> w.enabled() && w.url() != null && !w.url().isBlank())
        .filter(w -> w.kinds().isEmpty() || w.kinds().contains(n.kind()))
        .filter(w -> taskMatcher.matches(w, n))
        .map(Webhook::url)
        .toList();
  }

  /** 重建投递出去的完整请求体(与 NotificationDispatcher.buildBody 字段/顺序一致);序列化失败回落 null。 */
  private String deliveredBody(OutboundNotification n) {
    try {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("id", n.id());
      m.put("kind", n.kind());
      m.put("operator", n.operator());
      m.put("targetType", n.targetType());
      m.put("targetId", n.targetId());
      m.put("payload", json.readTree(n.payload() == null ? "{}" : n.payload()));
      m.put("occurredAt", n.createdAt().toString());
      return json.writeValueAsString(m);
    } catch (Exception e) {
      return null;
    }
  }

  private Webhook find(long id) {
    return webhooks.list().stream()
        .filter(w -> w.id() == id).findFirst()
        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "webhook " + id + " not found"));
  }

  private void audit(String action, long id, String url, List<String> kinds, boolean enabled) {
    auditor.record(current.get(), action, TargetType.NONE, id,
        Map.of("url", url, "kinds", kinds, "enabled", enabled));
  }

  private static final Set<String> SCOPES = Set.of("ALL", "INCLUDE", "EXCLUDE");

  private static String scope(String s) {
    String v = (s == null || s.isBlank()) ? "ALL" : s.trim();
    if (!SCOPES.contains(v)) throw new IllegalArgumentException("invalid scopeMode: " + s);
    return v;
  }

  private static List<Long> orEmpty(List<Long> l) { return l == null ? List.of() : l; }

  /** INCLUDE 需至少一个选中项,否则退化为订阅全部与选择意图相悖;EXCLUDE/ALL 允许空。 */
  private static void validateScope(String scopeMode, List<Long> taskIds, List<Long> dagIds) {
    if ("INCLUDE".equals(scopeMode) && taskIds.isEmpty() && dagIds.isEmpty()) {
      throw new IllegalArgumentException("scopeMode INCLUDE requires selectedTaskIds or selectedDagIds");
    }
  }

  private static String requireUrl(String url) {
    if (url == null || url.isBlank()) throw new IllegalArgumentException("url is required");
    return url.trim();
  }

  private static void validateBudget(int maxAttempts, long backoffMs) {
    if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
    if (backoffMs < 0) throw new IllegalArgumentException("backoffMs must be >= 0");
  }
}