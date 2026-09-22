-- V19: 通用出站通知 outbox(通知底座)。kernel 事件经 NotificationHub.fire() 先落库成 PENDING 行
-- (idempotency_key 唯一去重),由 leader 门控的 NotificationDispatcher 周期轮询,至少一次、指数退避地
-- 投递给配置的 HTTP webhook 端点,HMAC-SHA256 签名。status: PENDING(待投/退避中)/ SENT / FAILED。
CREATE TABLE app_notification (
  id              bigserial PRIMARY KEY,
  kind            text NOT NULL,
  operator        text,
  target_type     text,
  target_id       bigint,
  payload_json    jsonb NOT NULL DEFAULT '{}'::jsonb,
  idempotency_key text NOT NULL UNIQUE,
  status          text NOT NULL DEFAULT 'PENDING',
  attempts        int  NOT NULL DEFAULT 0,
  next_retry_at   timestamptz,
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  sent_at         timestamptz
);
CREATE INDEX app_notification_due_idx ON app_notification (status, next_retry_at);