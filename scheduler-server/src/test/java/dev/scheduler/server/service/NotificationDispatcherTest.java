package dev.scheduler.server.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.persistence.NotificationRepository;
import dev.scheduler.persistence.OutboundNotification;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.RequestMatcher;
import org.springframework.test.web.client.response.MockRestResponseCreators;
import org.springframework.web.client.RestTemplate;

/**
 * NotificationDispatcher 纯单测:HMAC 签名、kinds 订阅过滤、退避/重试预算/终态分类。
 * 用 MockRestServiceServer 拦截真实 RestTemplate 的 HTTP,并抓取发出的请求体校验签名。
 */
class NotificationDispatcherTest {
  private static final String URL = "https://hooks.example.com/x";
  private static final String SECRET = "s3cret-key";

  private FakeNotifications fake;
  private NotificationProperties props;
  private RestTemplate rest;
  private MockRestServiceServer server;
  private ObjectMapper json;
  private NotificationDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    fake = new FakeNotifications();
    props = new NotificationProperties();
    rest = new RestTemplate();
    server = MockRestServiceServer.createServer(rest);
    json = new ObjectMapper();
    dispatcher = new NotificationDispatcher(fake, props, rest, json);
  }

  @AfterEach
  void verifyNoPending() {
    server.verify();
  }

  private NotificationProperties.Webhook webhook() {
    NotificationProperties.Webhook w = new NotificationProperties.Webhook();
    w.setUrl(URL);
    w.setSecret(SECRET);
    w.setEnabled(true);
    return w;
  }

  private OutboundNotification notif(long id, String kind, int attempts) {
    return new OutboundNotification(id, kind, null, "#" + kind, 7L, "{\"k\":\"v\"}",
        OutboundNotification.STATUS_PENDING, attempts, null, null, Instant.parse("2026-01-01T00:00:00Z"), null);
  }

  /** 成功:POST 携带 HMAC-SHA256 签名(kinds 匹配),整行 markSent,不退避不失败。 */
  @Test
  void dispatch_success_signsCorreclyAndMarksSent() {
    props.getWebhooks().add(webhook());
    fake.add(notif(1, "execution.failed", 0));

    AtomicReference<byte[]> sentBody = new AtomicReference<>();
    AtomicReference<HttpHeaders> sentHeaders = new AtomicReference<>();
    server.expect(request -> {
      MockClientHttpRequest mock = (MockClientHttpRequest) request;
      sentBody.set(mock.getBodyAsBytes());
      sentHeaders.set(new HttpHeaders(request.getHeaders()));
    }).andRespond(MockRestResponseCreators.withStatus(HttpStatus.OK));

    assertEquals(1, dispatcher.dispatchOnce(), "应投递成功 1 行");

    assertTrue(fake.sent.contains(1L), "成功应 markSent");
    assertTrue(fake.retried.isEmpty(), "成功不退避");
    assertTrue(fake.failed.isEmpty(), "成功不终态失败");
    assertEquals("execution.failed", sentHeaders.get().getFirst("X-Schkid-Event"));
    assertEquals("1", sentHeaders.get().getFirst("X-Schkid-Notification-Id"));
    assertEquals(hmac(sentBody.get(), SECRET), sentHeaders.get().getFirst("X-Schkid-Signature"));
  }

  /** kinds 不匹配 → 不投递该 webhook,但整行视作完成(无订阅者→已投递)。 */
  @Test
  void dispatch_kindNotSubscribed_skipsAndMarksSent() {
    NotificationProperties.Webhook w = webhook();
    w.setKinds(List.of("execution.completed"));
    props.getWebhooks().add(w);
    fake.add(notif(1, "execution.failed", 0)); // 未订阅 kind → 无 HTTP 调用

    assertEquals(1, dispatcher.dispatchOnce(), "无订阅 → 仍整体完成");
    assertTrue(fake.sent.contains(1L), "无订阅视作已投递");
    assertTrue(fake.failed.isEmpty());
    assertTrue(fake.retried.isEmpty());
  }

  /** 无 webhook(未配置)→ 整个投递为 no-op,绝不主动失败。 */
  @Test
  void dispatch_noWebhooks_isNoop() {
    fake.add(notif(1, "execution.failed", 0));
    assertEquals(1, dispatcher.dispatchOnce());
    assertTrue(fake.sent.contains(1L), "无订阅 → 完成(不留 PENDING 孤儿行)");
    assertTrue(fake.failed.isEmpty());
  }

  /** 4xx(非 429)→ 永久失败:markFailed,不重试。 */
  @Test
  void dispatch_clientError_marksPermanentFailed() {
    props.getWebhooks().add(webhook());
    fake.add(notif(1, "execution.failed", 0));
    server.expect(MockRestServiceServerHelpers.anyRequest)
        .andRespond(MockRestResponseCreators.withStatus(HttpStatus.BAD_REQUEST));

    assertEquals(0, dispatcher.dispatchOnce());
    assertTrue(fake.failed.contains(1L), "4xx 应终态失败");
    assertTrue(fake.failedErr.get(1L).contains("permanent"), "错误标注 permanent");
    assertTrue(fake.retried.isEmpty());
    assertTrue(fake.sent.isEmpty());
  }

  /** 5xx → 可重试:markRetry,next_retry_at ≈ now + backoff(attempts=0 → 基数),并携带错误。 */
  @Test
  void dispatch_serverError_schedulesExponentialBackoff() {
    NotificationProperties.Webhook w = webhook();
    w.setBackoffMs(1000);
    props.getWebhooks().add(w);
    fake.add(notif(1, "execution.failed", 0));
    server.expect(MockRestServiceServerHelpers.anyRequest)
        .andRespond(MockRestResponseCreators.withStatus(HttpStatus.INTERNAL_SERVER_ERROR));

    assertEquals(0, dispatcher.dispatchOnce());
    assertTrue(fake.retried.contains(1L), "5xx 应退避重试");
    assertTrue(fake.failed.isEmpty());
    assertTrue(fake.sent.isEmpty());
    long at = fake.retryAtOf(1L);
    long now = System.currentTimeMillis();
    assertTrue(at - now >= 500 && at - now <= 3000, "next_retry_at 应 ≈ now+1s,got Δ=" + (at - now));
    assertTrue(fake.retriedErr.get(1L).contains("retry"), "结合上下文标注重试");
  }

  /** 已重试 4 次(maxAttempts=5)→ 第 5 次失败不再退避,转 FAILED(retry-exhausted)。 */
  @Test
  void dispatch_retryBudgetExhausted_marksFailed() {
    NotificationProperties.Webhook w = webhook();
    w.setMaxAttempts(5);
    props.getWebhooks().add(w);
    fake.add(notif(1, "execution.failed", 4)); // 已 4 次尝试
    server.expect(MockRestServiceServerHelpers.anyRequest)
        .andRespond(MockRestResponseCreators.withStatus(HttpStatus.INTERNAL_SERVER_ERROR));

    assertEquals(0, dispatcher.dispatchOnce());
    assertTrue(fake.failed.contains(1L), "预算耗尽应终态失败");
    assertTrue(fake.retried.isEmpty());
    assertTrue(fake.failedErr.get(1L).contains("retry-exhausted"), "标注预算耗尽");
  }

  /** 429 → 可重试(与 4xx 区分)。 */
  @Test
  void dispatch_http429_retryable() {
    props.getWebhooks().add(webhook());
    fake.add(notif(1, "execution.failed", 0));
    server.expect(MockRestServiceServerHelpers.anyRequest)
        .andRespond(MockRestResponseCreators.withStatus(HttpStatus.TOO_MANY_REQUESTS));

    assertEquals(0, dispatcher.dispatchOnce());
    assertTrue(fake.retried.contains(1L), "429 视为可重试");
    assertTrue(fake.failed.isEmpty());
  }

  /** 多个订阅 webhook 全部成功 → 整行 markSent。 */
  @Test
  void dispatch_multipleWebhooks_allOk_marksSent() {
    NotificationProperties.Webhook w1 = webhook();
    NotificationProperties.Webhook w2 = webhook();
    w2.setUrl("https://hooks.example.com/y");
    props.getWebhooks().add(w1);
    props.getWebhooks().add(w2);
    fake.add(notif(1, "execution.failed", 0));
    server.expect(MockRestServiceServerHelpers.anyRequest)
        .andRespond(MockRestResponseCreators.withStatus(HttpStatus.OK));
    server.expect(MockRestServiceServerHelpers.anyRequest)
        .andRespond(MockRestResponseCreators.withStatus(HttpStatus.OK));

    assertEquals(1, dispatcher.dispatchOnce());
    assertTrue(fake.sent.contains(1L));
    assertTrue(fake.failed.isEmpty());
    assertTrue(fake.retried.isEmpty());
  }

  /** 任一订阅 webhook 失败 → 整行退避重试(下次整批重投)。 */
  @Test
  void dispatch_anyWebhookFails_retriesWholeRow() {
    NotificationProperties.Webhook w1 = webhook();
    NotificationProperties.Webhook w2 = webhook();
    w2.setUrl("https://hooks.example.com/y");
    props.getWebhooks().add(w1);
    props.getWebhooks().add(w2);
    fake.add(notif(1, "execution.failed", 0));
    server.expect(MockRestServiceServerHelpers.anyRequest)
        .andRespond(MockRestResponseCreators.withStatus(HttpStatus.OK));
    server.expect(MockRestServiceServerHelpers.anyRequest)
        .andRespond(MockRestResponseCreators.withStatus(HttpStatus.SERVICE_UNAVAILABLE));

    assertEquals(0, dispatcher.dispatchOnce());
    assertTrue(fake.retried.contains(1L), "任一 webhook 失败 → 整行重试");
    assertTrue(fake.sent.isEmpty());
  }

  // ---- fakes & helpers ----

  private static String hmac(byte[] body, String secret) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
      StringBuilder sb = new StringBuilder(64);
      for (byte b : mac.doFinal(body)) sb.append(String.format("%02x", b));
      return sb.toString();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  static final class MockRestServiceServerHelpers {
    static final RequestMatcher anyRequest = request -> { };
  }

  static final class FakeNotifications implements NotificationRepository {
    final List<OutboundNotification> queue = new ArrayList<>();
    final List<Long> sent = new ArrayList<>();
    final List<Long> retried = new ArrayList<>();
    final List<Long> failed = new ArrayList<>();
    final Map<Long, Instant> retryAt = new LinkedHashMap<>();
    final Map<Long, String> retriedErr = new LinkedHashMap<>();
    final Map<Long, String> failedErr = new LinkedHashMap<>();

    void add(OutboundNotification n) { queue.add(n); }

    long retryAtOf(long id) { return retryAt.get(id).toEpochMilli(); }

    @Override public long enqueue(String kind, String op, String tt, Long tid, String p, String key) { return 1; }

    @Override public List<OutboundNotification> due(int limit) {
      List<OutboundNotification> r = new ArrayList<>(queue);
      queue.clear();
      return r;
    }

    @Override public void markSent(long id) { sent.add(id); }

    @Override public void markRetry(long id, Instant at, String err) {
      retried.add(id);
      retryAt.put(id, at);
      retriedErr.put(id, err);
    }

    @Override public void markFailed(long id, String err) {
      failed.add(id);
      failedErr.put(id, err);
    }
  }
}