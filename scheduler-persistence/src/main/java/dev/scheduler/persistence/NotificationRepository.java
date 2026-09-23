package dev.scheduler.persistence;

import java.time.Instant;
import java.util.List;

public interface NotificationRepository {
  /** 幂等入队:idempotency_key 唯一(重复 fire → 命中既有行);返回实际落库行的 id。 */
  long enqueue(String kind, String operator, String targetType, Long targetId,
               String payloadJson, String idempotencyKey);

  /** 取「待投递且到期(next_retry_at 为空或已到)」的 PENDING 行,最多 limit 条、id 升序。 */
  List<OutboundNotification> due(int limit);

  /** 投递成功:status→SENT、attempts+1、sent_at=now、清 last_error。 */
  void markSent(long id);

  /** 可重试失败:status→PENDING、attempts+1、next_retry_at=给定时刻、记录错误。 */
  void markRetry(long id, Instant nextRetryAt, String lastError);

  /** 永久失败/重试耗尽:status→FAILED、attempts+1、记录错误。 */
  void markFailed(long id, String lastError);

  /** 投递历史分页:kind/status 精确过滤,created_at DESC;全 null = 不过滤。 */
  List<OutboundNotification> findPage(String kind, String status, int limit, int offset);

  /** findPage 同过滤条件的全量计数。 */
  long count(String kind, String status);
}