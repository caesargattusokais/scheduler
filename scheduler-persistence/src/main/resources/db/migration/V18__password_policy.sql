-- 操作者强认证 · 口令策略:v1 口令历史仅存 bcrypt 哈希(明文不入库),供「防重」校验——
-- 改密/设密不得与当前活跃口令或最近 N 次历史口令相同。app_operator 仅停用不硬删,故不加外键;
-- created_at 由 DB 时钟写入,裁剪「最近 historySize 条」按 id 倒序(插入序即历史次序,时间单调)。
CREATE TABLE app_password_history (
  id bigserial PRIMARY KEY,
  operator_name text NOT NULL,
  password_hash text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX app_password_history_operator_idx ON app_password_history (operator_name, id DESC);