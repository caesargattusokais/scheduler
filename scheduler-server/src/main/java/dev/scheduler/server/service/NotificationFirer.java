package dev.scheduler.server.service;

import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;

/**
 * 通知底座在 kernel 事件侧的薄封装:把执行终态/超时/死信按结构化 payload + 幂等 idempotency_key 发进
 * {@link NotificationHub}(非阻断)。事件命名空间与 kind:
 *   execution.completed / execution.failed —— 父级终态(所有兄弟 shard 收敛后,由 Reconciler.aggregateParents),
 *                                        targetType=execution、targetId=父 id;幂等你:parent:kind:父id。
 *   execution.timeout                 —— RUNNING 分片超 timeout_seconds(Reconciler 扫描),targetType=shard。
 *   execution.dead_letter             —— FAILED 且 dead_letter=true 的分片,targetType=shard;幂等你:dlq:shardId:attempt。
 * CANCELED 不发(用户取消非失败告警)。调用方只负责在正确时点调用,payload/幂等你/kinds 归此处统一。
 */
@Service
public class NotificationFirer {
  private final NotificationHub hub;

  public NotificationFirer(NotificationHub hub) {
    this.hub = hub;
  }

  /** 父级终态:SUCCESS→execution.completed,FAILED→execution.failed,PARTIAL_SUCCESS→execution.partial_completed;CANCELED 不点火。 */
  public void parentTerminal(long executionId, ExecutionStatus terminal, List<Shard> shards) {
    String kind = switch (terminal) {
      case SUCCESS -> "execution.completed";
      case PARTIAL_SUCCESS -> "execution.partial_completed";
      case FAILED -> "execution.failed";
      default -> null;
    };
    if (kind == null) return;
    long failedShards = shards.stream().filter(s -> s.status() == ExecutionStatus.FAILED).count();
    hub.fire(kind, "execution", executionId, null, Map.of(
        "parentId", executionId,
        "shardCount", shards.size(),
        "failedShards", failedShards,
        "terminal", terminal.name()),
        "parent:" + kind + ":" + executionId);
  }

  /** 死信分片(FAILED 且 dead_letter=true):execution.dead_letter,以分片为靶、携带所属父。 */
  public void shardDeadLettered(long executionId, long shardId, int attempt) {
    hub.fire("execution.dead_letter", "shard", shardId, null, Map.of(
        "executionId", executionId,
        "shardId", shardId,
        "attempt", attempt),
        "dlq:" + shardId + ":" + attempt);
  }

  /** 运行超时(分片 RUNNING 超 timeout_seconds):execution.timeout,以分片为靶。 */
  public void shardTimedOut(long shardId, long taskId, int attempt, int timeoutSeconds) {
    hub.fire("execution.timeout", "shard", shardId, null, Map.of(
        "shardId", shardId,
        "taskId", taskId,
        "attempt", attempt,
        "timeoutSeconds", timeoutSeconds),
        "timeout:" + shardId + ":" + attempt);
  }
}