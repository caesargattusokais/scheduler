package dev.scheduler.persistence;

import java.time.Instant;

/**
 * app_task_event 入站事件 outbox 行映射。route_key 用于订阅匹配;payload 为 JSON 文本;
 * status 见 STATUS_* 常量;分派后回填所触发的 task_id 与所建父 execution_id。
 */
public record InboundEvent(
    long id,
    String routeKey,
    String payload,
    String dedupeKey,
    String status,
    Long taskId,
    Long executionId,
    Instant createdAt,
    Instant dispatchedAt) {

  public static final String STATUS_PENDING = "PENDING";
  public static final String STATUS_DISPATCHED = "DISPATCHED";
}