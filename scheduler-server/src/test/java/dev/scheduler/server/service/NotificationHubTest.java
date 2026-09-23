package dev.scheduler.server.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.persistence.NotificationRepository;
import dev.scheduler.persistence.OutboundNotification;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * NotificationHub 非阻断语义:fire 把结构化 payload 序列化入队;入队失败绝不抛给调用方,
 * 仅吞掉记日志(事件触发方不应因通知不可用而失败,镜像 AuditRecorder)。
 */
class NotificationHubTest {
  private final ObjectMapper json = new ObjectMapper();

  /** fire → 透传 kind/target/operator,payload 序列化为 JSON 文本入队。 */
  @Test
  void fire_serializesPayloadAndEnqueues() throws Exception {
    CapturingRepo repo = new CapturingRepo();
    NotificationHub hub = new NotificationHub(repo, json);

    hub.fire("execution.failed", "execution", 9L, "alice", Map.of("run", 3, "reason", "boom"), "evt-abc");

    assertEquals(1, repo.enqueued.size());
    Enqueued e = repo.enqueued.get(0);
    assertEquals("execution.failed", e.kind);
    assertEquals("execution", e.targetType);
    assertEquals(9L, e.targetId);
    assertEquals("alice", e.operator);
    assertEquals("evt-abc", e.idempotencyKey);
    assertEquals(Map.of("run", 3, "reason", "boom"),
        json.convertValue(json.readTree(e.payloadJson), Object.class), "payload 序列化后内容不变");
  }

  /** 入队抛错(如连不上 DB)→ fire 照常返回,不向调用方冒泡。 */
  @Test
  void fire_repoThrows_doesNotBreakCaller() {
    NotificationRepository boom = new NotificationRepository() {
      @Override public long enqueue(String k, String o, String tt, Long id, String p, String key) {
        throw new IllegalStateException("db down");
      }
      @Override public List<OutboundNotification> due(int limit) { return List.of(); }
      @Override public void markSent(long id) { }
      @Override public void markRetry(long id, Instant at, String err) { }
      @Override public void markFailed(long id, String err) { }
      @Override public List<OutboundNotification> findPage(String kind, String status, int limit, int offset) {
        return List.of();
      }
      @Override public long count(String kind, String status) { return 0; }
    };
    NotificationHub hub = new NotificationHub(boom, json);

    // 不抛异常即证明非阻断(否则 assertThrows 断言用内联坏掉的仓库名)。
    hub.fire("execution.failed", "execution", 1L, null, Map.of(), "evt-x");
  }

  private record Enqueued(String kind, String operator, String targetType, Long targetId,
                          String payloadJson, String idempotencyKey) { }

  static final class CapturingRepo implements NotificationRepository {
    final List<Enqueued> enqueued = new java.util.ArrayList<>();
    @Override public long enqueue(String kind, String op, String tt, Long tid, String p, String key) {
      enqueued.add(new Enqueued(kind, op, tt, tid, p, key));
      return enqueued.size();
    }
    @Override public List<OutboundNotification> due(int limit) { return List.of(); }
    @Override public void markSent(long id) { }
    @Override public void markRetry(long id, Instant at, String err) { }
    @Override public void markFailed(long id, String err) { }
    @Override public List<OutboundNotification> findPage(String kind, String status, int limit, int offset) {
      return List.of();
    }
    @Override public long count(String kind, String status) { return 0; }
  }
}