package dev.scheduler.persistence;

import java.time.Instant;

/**
 * app_notification 出站通知 outbox 行映射。payload 为 JSON 文本;status 见 STATUS_* 常量,
 * attempts 记录已发生的发送尝试次数(初始 0,每次 markSent/markRetry/markFailed 自增)。
 */
public record OutboundNotification(
    long id,
    String kind,
    String operator,
    String targetType,
    Long targetId,
    String payload,
    String status,
    int attempts,
    Instant nextRetryAt,
    String lastError,
    Instant createdAt,
    Instant sentAt) {

  public static final String STATUS_PENDING = "PENDING";
  public static final String STATUS_SENT = "SENT";
  public static final String STATUS_FAILED = "FAILED";
}