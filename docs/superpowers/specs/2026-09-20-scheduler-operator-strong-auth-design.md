# 操作者强认证 (Operator Strong AuthN) — 设计

Date: 2026-09-20
Status: 批准待实施

## 背景与问题

调度器控制面 `/api/v1/**` 的操作者授权目前基于**自报身份**:客户端任意填 `X-Operator` 头,后端只按该名字查白名单+角色(OPERATOR/ADMIN)。这能阻止"误用任意名字乱写",但**不能阻止冒名**——任何人不持有任何秘密即可自称 `alice`。

本设计把信任模型从"声明身份"升级为"**持有秘密可验证**(密码 + 不可猜会话 token)",并针对主劫持向量(XSS 窃取 / 爆破 / 长期 token / 改密遗留)做硬化。

诚实边界:这是 IAM 的一段核心(密码认证 + 会话语义 + 端级双档角色),**不是完整 IAM**——无 MFA、无自助改密/找回、无细粒度授权、无告警消费端、生产仍须 TLS(cookie `Secure`)。

## 目标(non-goals 见边界)

1. 密码(BCrypt)登录签发会话;`app_operator` 加 `password_hash`,无密码不得登录。
2. 会话走 **HttpOnly + SameSite=Strict cookie**,JS 不可读 → 消解 XSS 窃取这条主链。
3. 短 TTL(8h)+ **滑动轮换**;会话绝对寿命 ≤ created_at + 5 天。
4. **登录退避限流**(DB 持久,指数上限 30s)防在线爆破。
5. **改密/停用 → 撤销该操作者全部活动会话**。
6. 拦截器从 cookie 解析身份(废 `X-Operator`);写门禁/角色判定/审计 operator 全改读解析出的 principal。
7. 前端登录页 + 身份展示/登出;移除侧栏"自报操作者名"。

## 数据模型(V16 迁移)

```sql
ALTER TABLE app_operator ADD COLUMN password_hash text NULL;

CREATE TABLE app_auth_session (
  token_hash    text PRIMARY KEY,          -- SHA-256(明文 token),非明文
  operator_name text NOT NULL REFERENCES app_operator(name),
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz NOT NULL,        -- 滑动续期
  revoked_at    timestamptz                  -- 非 NULL = 已撤销
);

CREATE TABLE app_login_attempt (
  name         text PRIMARY KEY,            -- 客户端提交的操作者名(含不存在者)
  failures     int    NOT NULL DEFAULT 0,
  locked_until timestamptz NOT NULL DEFAULT now()
);
```

- `app_operator.name` / `app_auth_session.operator_name` 用 FK 保留目录恒在性(重置播种 ON CONFLICT 时注意)。
- token 为下发的 32B 高熵随机;库内只存 SHA-256(高熵 → 库泄露不可逆推,故无需慢哈希);口令用 BCrypt(慢哈希)。
- 会话撤销/过期:resolve 时 `revoked_at IS NULL AND expires_at > now() AND now() - created_at < P5D`。

## 认证/会话端点(`AuthController`,`/api/v1/auth/**` 免写门禁)

- `POST /auth/login`:body `{name,password}` → 先查退避(`locked_until > now()` → 401 不再校验)→ BCrypt 验密 → 成功:重置该名 failures、签新 token,`Set-Cookie`;失败:`failures+1`,`locked_until = now + min(2^failures, 30s)`,记 `access.denied`(operator=提交名,meta{reason:"bad_credentials"/"locked"})。
  - 允许多个并发会话并存(各持各自 cookie);仅 logout/改密/deactivate 撤销对应或全部会话。
  - 响应 200 + `{operator: OperatorEntry, expiresAt}`;cookie `session=<token>; HttpOnly; SameSite=Strict; Path=/api; Secure(条件)`。
- `GET /auth/me`:回显 `{operator: OperatorEntry}`(解析当前 cookie;JS 用它判 401 → 跳登录;/旋转时服务端在此一并换 cookie)。
- `POST /auth/logout`:撤销当前 token session,`Set-Cookie` 清空。
- **滑动轮换**:任何已解析请求中剩余 TTL < 一半(`expires_at - now() < TTL/2`)时,服务端撤销旧 token、签新 cookie(新 token 前移 `expires_at`),但仅当绝对寿命 `now() - created_at < 5d` 才轮换;超 5d 直接拒 → 强制重登。使每 token ≤ 8h、会话最长 5d。

## 密码管理

- `POST /api/v1/operators/{name}/password`(ADMIN,`OperatorController`):body `{password:≥8}` → BCrypt upsert;随后 `UPDATE app_auth_session SET revoked_at=now() WHERE operator_name=? AND revoked_at IS NULL`(**撤销全部活动会话**)。
- `POST /api/v1/operators/{name}/deactivate`(已有)→ 追加撤销该操作者全部会话。
- 引导:`scheduler.operators.default-password` 属性,启动对 `password_hash IS NULL` 操作者幂等填 BCrypt(首个 ADMIN 进场通道;生产用端点设独立密码,属性仅首进)。

## 拦截器改造(`OperatorInterceptor`)

- 读 cookie `session` → SHA-256 → 查 `app_auth_session`(未过期/未撤销)→ 得操作者名 → `operators.roleOf` 得角色。
- 缺失/无效/过期/撤销 → 401;角色不足 → 403;两者记 `access.denied` 审计(operator=解析名或提交名)。
- 解析出的身份写入 `CurrentOperator`(ThreadLocal;preHandle 填 / afterCompletion 清),控制器/AuditRecorder 读它,**全链路移除 X-Operator 依赖**。
- `/api/v1/auth/login`(及 `/auth/**` 登入口)从写门禁豁免。

## 前端

- 登录页:name+password;成功后 JS 仅持 cookie(`credentials:'include'` 同源默认带),不再存 token。
- 挂载 `GET /auth/me`:401 → 登录门;成功 → 身份展示。侧栏「自报操作者名」改「当前登录身份 + 登出」。
- `req()` 移除 X-Operator 头(fetch 同源自动带 cookie);写操作沿用 cookie。
- OperatorsPage 增「设密码」;审计/归档沿 cookie。

## 测试

- 持久层:session 建/解/过期/撤销/轮换续期;退避累加与重置;改密撤销全部会话。
- 集成:
  - `MockMvcBuilderCustomizer` 由「默认 X-Operator=alice」改「login(alice) 取 cookie 作默认」→ 既有写用例零改动。
  - 新用例:未登录写 401;弱角色 403;登录失败退避锁定 401;轮换后旧 cookie 失效;改密后旧会话 401;login 成功 sets cookie + row;logout 撤销。

## 非目标 / 边界(诚实声明)

不做:MFA/TOTP、SSO/LDAP/OIDC 联合、自助改密与忘记密码、细粒度资源级授权、多租户、告警接收端、跨应用 token 同步。CSRF 仅靠 SameSite=Strict(同源 SPA);非浏览器客户端凭 cookie 受限(属说明项)。生产部署必须启用 TLS 使 `Secure` cookie 生效。