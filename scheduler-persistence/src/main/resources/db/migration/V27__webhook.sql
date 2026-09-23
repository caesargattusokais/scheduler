-- 4-1 通知告警闭环:webhook 订阅端点从配置文件迁移到 DB(app_webhook),由通知管理 API 增删改。
-- kinds 为订阅的 event kind 列表(空数组 = 订阅全部);attached attributes 与配置文件 V1 语义一致。
CREATE TABLE app_webhook (
  id           bigserial PRIMARY KEY,
  url          text NOT NULL,
  secret       text,
  kinds        text[] NOT NULL DEFAULT '{}',
  enabled      boolean NOT NULL DEFAULT TRUE,
  max_attempts int  NOT NULL DEFAULT 5,
  backoff_ms   bigint NOT NULL DEFAULT 1000,
  created_at   timestamptz NOT NULL DEFAULT now()
);