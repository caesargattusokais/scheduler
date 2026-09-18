-- V10: 操作审计日志(仅 operator 主动动作;系统驱动的状态迁移由 execution_shard_outcome 覆盖)。
CREATE TABLE app_audit (
  id BIGSERIAL PRIMARY KEY,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- DB 权威时钟
  operator VARCHAR(64) NOT NULL,                  -- X-Operator,缺省 'anonymous'
  action VARCHAR(64) NOT NULL,                    -- 如 task.create
  target_type VARCHAR(16) NOT NULL,               -- task|execution|shard|dag|dag_run
  target_id BIGINT NOT NULL,
  meta JSONB NULL,                                -- 紧凑后态载荷(JSON 文本;null=无)
  source VARCHAR(255) NULL                        -- 请求方标识(保留列,当前写端传 null)
);

CREATE INDEX idx_audit_target   ON app_audit (target_type, target_id, occurred_at DESC);
CREATE INDEX idx_audit_operator ON app_audit (operator, occurred_at DESC);