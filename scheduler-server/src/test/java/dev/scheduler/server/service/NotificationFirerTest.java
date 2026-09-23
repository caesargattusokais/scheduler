package dev.scheduler.server.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.persistence.NotificationRepository;
import dev.scheduler.persistence.OutboundNotification;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * NotificationFirer 纯单测:把终态事件映射成 kind + targetType/targetId + 结构化 payload + 幂等 idempotency_key。
 * 校验 kind 命名空间、CANCELED 不点火、幂等你形状(保证同事件重复调用去重)。
 */
class NotificationFirerTest {
  private final List<Fired> fired = new ArrayList<>();
  private NotificationFirer firer;
  private com.fasterxml.jackson.databind.ObjectMapper json;

  @BeforeEach
  void setUp() {
    json = new ObjectMapper();
    firer = new NotificationFirer(new NotificationHub(new CapturingRepo(fired), json));
  }

  /** 父 SUCCESS → execution.completed,target=父 id,payload 含 shardCount/failedShards=0。 */
  @Test
  void parentTerminal_success_firesCompleted() {
    List<Shard> shards = List.of(shard(1L, SUCCESS), shard(2L, SUCCESS));
    firer.parentTerminal(100L, ExecutionStatus.SUCCESS, shards);

    Fired e = only(fired);
    assertEquals("execution.completed", e.kind);
    assertEquals("execution", e.targetType);
    assertEquals(100L, e.targetId);
    assertEquals("parent:execution.completed:100", e.idempotencyKey);
    assertEquals(2, e.payload.get("shardCount"));
    assertEquals(0, e.payload.get("failedShards"));
    assertEquals("SUCCESS", e.payload.get("terminal"));
  }

  /** 父 FAILED → execution.failed,payload 统计失败分片数。 */
  @Test
  void parentTerminal_failed_firesFailedWithFailedCount() {
    List<Shard> shards = List.of(shard(1L, FAILED), shard(2L, SUCCESS), shard(3L, FAILED));
    firer.parentTerminal(100L, ExecutionStatus.FAILED, shards);

    Fired e = only(fired);
    assertEquals("execution.failed", e.kind);
    assertEquals("parent:execution.failed:100", e.idempotencyKey);
    assertEquals(2, e.payload.get("failedShards"), "两个失败分片计入");
    assertEquals(3, e.payload.get("shardCount"));
  }

  /** 父 CANCELED → 不点火(用户取消非失败告警)。 */
  @Test
  void parentTerminal_cancelled_firesNothing() {
    firer.parentTerminal(100L, ExecutionStatus.CANCELED, List.of(shard(1L, CANCELED)));
    assertTrue(fired.isEmpty(), "CANCELED 不点火");
  }

  /** 3c:父 PARTIAL_SUCCESS → execution.partial_completed,payload 统计失败分片数。 */
  @Test
  void parentTerminal_partialSuccess_firesPartialCompleted() {
    List<Shard> shards = List.of(shard(1L, SUCCESS), shard(2L, SUCCESS), shard(3L, FAILED));
    firer.parentTerminal(100L, ExecutionStatus.PARTIAL_SUCCESS, shards);

    Fired e = only(fired);
    assertEquals("execution.partial_completed", e.kind);
    assertEquals("execution", e.targetType);
    assertEquals(100L, e.targetId);
    assertEquals("parent:execution.partial_completed:100", e.idempotencyKey);
    assertEquals(3, e.payload.get("shardCount"));
    assertEquals(1, e.payload.get("failedShards"));
    assertEquals("PARTIAL_SUCCESS", e.payload.get("terminal"));
  }

  /** DLQ 分片 → execution.dead_letter,target=分片 id,幂等你 dlq:shardId:attempt。 */
  @Test
  void shardDeadLettered_firesWithShardTarget() {
    firer.shardDeadLettered(100L, 7L, 3);
    Fired e = only(fired);
    assertEquals("execution.dead_letter", e.kind);
    assertEquals("shard", e.targetType);
    assertEquals(7L, e.targetId);
    assertEquals("dlq:7:3", e.idempotencyKey);
    assertEquals(100, e.payload.get("executionId")); // JSON payload 反序列化回 Integer
    assertEquals(3, e.payload.get("attempt"));
  }

  /** DLQ 重发后重入(同 shard 再次尝试)→ attempt 变化换新幂等你,old/old-retry 不互相吞。 */
  @Test
  void shardDeadLettered_attemptDistinguishesRepetition() {
    firer.shardDeadLettered(100L, 7L, 3);
    firer.shardDeadLettered(100L, 7L, 4);
    assertEquals("dlq:7:3", fired.get(0).idempotencyKey);
    assertEquals("dlq:7:4", fired.get(1).idempotencyKey);
    assertEquals(2, fired.size());
  }

  /** 超时 → execution.timeout,target=分片,payload 带 taskId/attempt/timeoutSeconds。 */
  @Test
  void shardTimedOut_firesWithTimeoutInfo() {
    firer.shardTimedOut(7L, 9001L, 2, 300);
    Fired e = only(fired);
    assertEquals("execution.timeout", e.kind);
    assertEquals("shard", e.targetType);
    assertEquals(7L, e.targetId);
    assertEquals("timeout:7:2", e.idempotencyKey);
    assertEquals(9001, e.payload.get("taskId"));
    assertEquals(2, e.payload.get("attempt"));
    assertEquals(300, e.payload.get("timeoutSeconds"));
  }

  // ---- helpers ----

  private static final ExecutionStatus SUCCESS = ExecutionStatus.SUCCESS;
  private static final ExecutionStatus FAILED = ExecutionStatus.FAILED;
  private static final ExecutionStatus CANCELED = ExecutionStatus.CANCELED;

  private static Shard shard(long id, ExecutionStatus st) {
    return new Shard(id, null, 0, null, st, 0, null, null, null, false, false, null, null, null);
  }

  private Fired only(List<Fired> l) {
    assertEquals(1, l.size(), "应恰好发一个事件");
    return l.get(0);
  }

  private record Fired(String kind, String targetType, Long targetId, String idempotencyKey,
                       java.util.Map<String, Object> payload) { }

  private final class CapturingRepo implements NotificationRepository {
    private final List<Fired> out;
    CapturingRepo(List<Fired> out) { this.out = out; }
    @Override public long enqueue(String kind, String op, String tt, Long tid, String p, String key) {
      try {
        out.add(new Fired(kind, tt, tid, key, json.readValue(p, java.util.LinkedHashMap.class)));
      } catch (java.io.IOException e) {
        throw new IllegalStateException(e);
      }
      return out.size();
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