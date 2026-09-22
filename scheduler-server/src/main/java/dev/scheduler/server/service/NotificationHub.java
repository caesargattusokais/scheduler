package dev.scheduler.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.persistence.NotificationRepository;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 通知底座入点半:kernel 事件({@code execution.failed}/{@code execution.completed}/…)经 {@link #fire}
 * 落库为 outbox 行。镜像 {@link AuditRecorder} 的非阻断模式——入库失败只记日志,绝不抛回给调用方
 * (事件的触发方不该因通知不可用而失败)。idempotencyKey 由调用方给定(如「event:task-前缀:run-id」),
 * 重复 fire 由持久层唯一约束去重。
 */
@Service
public class NotificationHub {
  private static final Logger log = LoggerFactory.getLogger(NotificationHub.class);

  private final NotificationRepository notifications;
  private final ObjectMapper json;

  public NotificationHub(NotificationRepository notifications, ObjectMapper json) {
    this.notifications = notifications;
    this.json = json;
  }

  /** 非阻断入队一个通知。payload 为结构化数据,序列化成 JSON 存 outbox。 */
  public void fire(String kind, String targetType, Long targetId, String operator,
                   Map<String, Object> payload, String idempotencyKey) {
    try {
      String body = json.writeValueAsString(payload == null ? Map.of() : payload);
      notifications.enqueue(kind, operator, targetType, targetId, body, idempotencyKey);
    } catch (Exception e) {
      log.warn("notification enqueue failed(must not break caller); kind={} key={} err={}",
          kind, idempotencyKey, e.toString());
    }
  }
}