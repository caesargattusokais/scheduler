-- V31: alert episode(告警事件收敛)数据表。
-- key         告警去重键(rule target 维度);partial unique 保证同一 key 同时至多一条 PENDING/OPEN 活跃行,
--             RESOLVED 后释放——同 key 新爆发将插入新行,旧行保留 history。
-- status      PENDING(待确认) → OPEN(确认中) → RESOLVED(已回落)。
-- sample_count/value  当前聚合采样计数与代表值;opened_* 为 OPEN 确认时快照,resolved_* 为回落快照(冲突快照可空)。
CREATE TABLE alert_episode (
  id            bigserial PRIMARY KEY,
  key           text NOT NULL,
  rule          text NOT NULL,
  target_type   text NOT NULL DEFAULT 'none',
  target_id     bigint,
  severity      text NOT NULL,
  status        text NOT NULL DEFAULT 'PENDING',
  sample_count  int NOT NULL,
  value         text NOT NULL,
  opened_at     timestamptz,
  opened_value  text,
  resolved_at   timestamptz,
  resolved_value text,
  updated_at    timestamptz NOT NULL DEFAULT now()
);
-- 同一 key 存在活跃(PENDING/OPEN)行时禁止再插;RESOLVED 后不再受约束。
CREATE UNIQUE INDEX alert_episode_active_key_uq
  ON alert_episode (key) WHERE status IN ('PENDING','OPEN');
-- 活跃/历史按状态扫描 + 列表按 updated_at 排序的复合索引。
CREATE INDEX alert_episode_status_idx ON alert_episode (status, updated_at);
