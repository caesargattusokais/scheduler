package dev.scheduler.server.web;

import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.NotificationRepository;
import dev.scheduler.persistence.OutboundNotification;
import dev.scheduler.persistence.Webhook;
import dev.scheduler.persistence.WebhookRepository;
import dev.scheduler.server.security.CurrentOperator;
import dev.scheduler.server.service.AuditRecorder;
import java.util.List;
import java.util.Map;
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
  private final AuditRecorder auditor;
  private final CurrentOperator current;

  public NotificationsController(WebhookRepository webhooks, NotificationRepository notifications,
                                 AuditRecorder auditor, CurrentOperator current) {
    this.webhooks = webhooks;
    this.notifications = notifications;
    this.auditor = auditor;
    this.current = current;
  }

  /** 订阅请求体:url 必填非空;maxAttempts 默认 5、backoffMs 默认 1000(与旧配置默认一致);kinds 空 = 订阅全部。 */
  public record WebhookRequest(String url, String secret, List<String> kinds,
                               Boolean enabled, Integer maxAttempts, Long backoffMs) {}

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
    long id = webhooks.create(url, req.secret(), kinds, enabled, maxAttempts, backoffMs);
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
    if (!webhooks.update(id, url, req.secret(), kinds, enabled, maxAttempts, backoffMs)) {
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

  @GetMapping("/notifications")
  public Page<OutboundNotification> listNotifications(
      @RequestParam(required = false) String kind,
      @RequestParam(required = false) String status,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    return new Page<>(notifications.findPage(kind, status, p.limit(), p.offset()),
        notifications.count(kind, status), p.offset(), p.limit());
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

  private static String requireUrl(String url) {
    if (url == null || url.isBlank()) throw new IllegalArgumentException("url is required");
    return url.trim();
  }

  private static void validateBudget(int maxAttempts, long backoffMs) {
    if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
    if (backoffMs < 0) throw new IllegalArgumentException("backoffMs must be >= 0");
  }
}