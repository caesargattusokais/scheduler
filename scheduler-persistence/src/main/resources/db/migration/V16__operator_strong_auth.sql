-- 操作者强认证:口令哈希 + 会话令牌 + 登录失败退避。
-- 口令:bcrypt 哈希文本(服务端校验);令牌:落库为 SHA-256(token) 明文 64-char hex 不入库。
-- 时间统一取 DB now()(V13 口径),避免应用/DB 时区漂移;绝对会话寿命由 resolve 的
-- `now() - created_at < interval '5 days'` 约束(令牌轮换驱动的绝对上限)。
ALTER TABLE app_operator ADD COLUMN password_hash text NULL;

CREATE TABLE app_auth_session (
  token_hash    text PRIMARY KEY,
  operator_name text NOT NULL REFERENCES app_operator(name),
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz NOT NULL,
  revoked_at    timestamptz
);

CREATE TABLE app_login_attempt (
  name         text PRIMARY KEY,
  failures     int    NOT NULL DEFAULT 0,
  locked_until timestamptz NOT NULL DEFAULT now()
);