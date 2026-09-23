package dev.scheduler.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.persistence.NotificationRepository;
import dev.scheduler.persistence.OutboundNotification;
import dev.scheduler.persistence.Webhook;
import dev.scheduler.persistence.WebhookRepository;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

/**
 * 通知投递器:轮询 outbox 到期行,按 webhook 的 kinds 订阅过滤后经 HTTP POST 至少一次投递,
 * HMAC-SHA256 签名;可重试失败(网络/5xx/429)指数退避,永久失败(4xx 除外)或重试耗尽转 FAILED。
 * 一行的投递状态是原子单位:任一订阅 webhook 失败 → 整行转 PENDING(退避)重试整批,全部成功才 markSent。
 * 由 leader 门控的 {@link dev.scheduler.server.config.Beans.NotificationLoop} 周期驱动。
 */
@Component
public class NotificationDispatcher {
  private static final Logger log = LoggerFactory.getLogger(NotificationDispatcher.class);
  private static final int BATCH = 50;
  private static final long BACKOFF_CAP_MS = 3_600_000L; // 退避上限 1h

  private final NotificationRepository notifications;
  private final WebhookRepository webhooks;
  private final RestTemplate rest;
  private final ObjectMapper json;

  public NotificationDispatcher(NotificationRepository notifications, WebhookRepository webhooks,
                                RestTemplate rest, ObjectMapper json) {
    this.notifications = notifications;
    this.webhooks = webhooks;
    this.rest = rest;
    this.json = json;
  }

  /** 单次投递 pass:处理最多 BATCH 条到期行。返回成功 markSent 的行数(供指标/日志)。 */
  public int dispatchOnce() {
    int delivered = 0;
    for (OutboundNotification n : notifications.due(BATCH)) {
      Boolean outcome = deliverToAll(n); // null=全部成功,否则为终止 webhook 的描述
      if (outcome == null) {
        notifications.markSent(n.id());
        delivered++;
      }
    }
    return delivered;
  }

  /** 对 n 逐 webhook 投递:任一失败即终止整行并转移状态;全成功返回 null。 */
  private Boolean deliverToAll(OutboundNotification n) {
    for (Webhook w : webhooks.list()) {
      if (!w.enabled() || w.url() == null || w.url().isBlank()) continue;
      if (!subscribed(w, n.kind())) continue;
      DeliveryResult r = deliver(w, n);
      if (r.outcome == Outcome.OK) continue;
      if (r.outcome == Outcome.PERMANENT) {
        notifications.markFailed(n.id(), "permanent " + w.url() + ": " + r.error());
      } else if (remainingBudget(w, n)) {
        notifications.markRetry(n.id(), Instant.now().plusMillis(backoffMs(w, n.attempts())),
            "retry " + w.url() + ": " + r.error());
      } else {
        notifications.markFailed(n.id(), "retry-exhausted " + w.url() + ": " + r.error());
      }
      return Boolean.TRUE; // 已终止本轮,整行状态已转移
    }
    return null; // 无订阅 webhook 或全部成功 → 整体完成
  }

  private static boolean subscribed(Webhook w, String kind) {
    return w.kinds().isEmpty() || w.kinds().contains(kind);
  }

  /** 是否仍可在 maxAttempts 预算内安排下一次尝试(n.attempts 已完成的尝试次数)。 */
  private static boolean remainingBudget(Webhook w, OutboundNotification n) {
    return n.attempts() + 1 < w.maxAttempts();
  }

  private static long backoffMs(Webhook w, int attempts) {
    long v;
    try {
      v = w.backoffMs() * (1L << Math.min(attempts, 30));
    } catch (ArithmeticException e) {
      v = Long.MAX_VALUE;
    }
    return Math.min(v, BACKOFF_CAP_MS);
  }

  private DeliveryResult deliver(Webhook w, OutboundNotification n) {
    try {
      byte[] body = buildBody(n);
      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_JSON);
      headers.set("X-Schkid-Event", n.kind());
      headers.set("X-Schkid-Notification-Id", Long.toString(n.id()));
      if (w.secret() != null && !w.secret().isBlank()) {
        headers.set("X-Schkid-Signature", hmacSha256(body, w.secret()));
      }
      // RestTemplate 默认对 4xx/5xx 抛 HttpStatusCodeException(不会返回错误 ResponseEntity),2xx 则正常返回。
      ResponseEntity<byte[]> resp =
          rest.exchange(w.url(), HttpMethod.POST, new HttpEntity<>(body, headers), byte[].class);
      return new DeliveryResult(Outcome.OK, null);
    } catch (HttpStatusCodeException e) {
      int code = e.getStatusCode().value();
      String err = "http " + code + " " + w.url();
      log.warn("notification delivery failed: {} (kind={} id={})", err, n.kind(), n.id());
      // 429 与 5xx 可重试;其余 4xx 为永久(重试无望)。
      return new DeliveryResult(code == 429 || code >= 500 ? Outcome.RETRYABLE : Outcome.PERMANENT, err);
    } catch (ResourceAccessException e) {
      String err = "io " + w.url() + ": " + e.getMessage();
      log.warn("notification delivery io failure: {}", err);
      return new DeliveryResult(Outcome.RETRYABLE, err);
    } catch (Exception e) {
      String err = "bad " + w.url() + ": " + e;
      log.warn("notification delivery error: {}", err);
      return new DeliveryResult(Outcome.RETRYABLE, e.toString());
    }
  }

  private byte[] buildBody(OutboundNotification n) throws java.io.IOException {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("id", n.id());
    m.put("kind", n.kind());
    m.put("operator", n.operator());
    m.put("targetType", n.targetType());
    m.put("targetId", n.targetId());
    m.put("payload", json.readTree(n.payload() == null ? "{}" : n.payload()));
    m.put("occurredAt", n.createdAt().toString());
    return json.writeValueAsBytes(m);
  }

  private static String hmacSha256(byte[] body, String secret) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
      StringBuilder sb = new StringBuilder(64);
      for (byte b : mac.doFinal(body)) sb.append(String.format("%02x", b));
      return sb.toString();
    } catch (Exception e) {
      throw new IllegalStateException("HMAC failure", e);
    }
  }

  private enum Outcome { OK, RETRYABLE, PERMANENT }

  private record DeliveryResult(Outcome outcome, String error) { }
}