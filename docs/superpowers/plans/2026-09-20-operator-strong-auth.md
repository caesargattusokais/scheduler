# 操作者强认证 (Operator Strong AuthN) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把控制面身份从自报 `X-Operator` 头升级为密码验证 + HttpOnly cookie 会话(短 TTL 滑动轮换、登录退避、改密撤销),并加操作者设密码端点与前端登录门。

**Architecture:** BCrypt 存口令(SHA-256 存会话 token hash);会话走 HttpOnly+SameSite=Strict cookie;`OperatorInterceptor` 从 cookie 解析身份写入 `CurrentOperator`(ThreadLocal),全链路废 X-Operator;`AuthService` 编排登录/解析/轮换/登出;NEW `AuthRepository` + 扩展 `OperatorRepository`;前端登录页 + 身份展示。

**Tech Stack:** Spring Boot 3.2.5 / `spring-security-crypto`(BCrypt,离线 BOM 管 6.2.4)/ PostgreSQL 16 (Testcontainers) / React 前端。Maven 离线 `mvn -o`。

**Spec:** `docs/superpowers/specs/2026-09-20-scheduler-operator-strong-auth-design.md`(本 plan 的唯一权威,冲突以 spec 裁决)。

## Global Constraints

- TTL 常量:`TOKEN_TTL = Duration.ofHours(8)`,`ABSOLUTE_SESSION = Duration.ofDays(5)`,`BACKOFF_CAP_SECONDS = 30`,`MIN_PASSWORD = 8`;cookie 名 `session`,path `/api`,SameSite=Strict,HttpOnly 恒开,`Secure` 由 `scheduler.auth.secure-cookies`(默认 false,开发 HTTP)条件加。
- token = `SecureRandom` 32 字节 → 32 字节 hex(/32 小写);库内只存 `SHA-256(hex)`(43 字符 hex SHA-256)。
- 会话 token_hash 用 SHA-256(高熵可快哈希);口令一律 BCrypt(慢哈希)。
- 退避:`locked_until = now + min(2^failures, 30s)`,失败+1 成功清零;先查退避再验密。
- 写门禁语义不变(ADMIN_WRITES 同表);`/api/v1/auth/login` 豁免写门禁;读端点开放(有 cookie 则解析身份,无则匿名)。
- 改密/停用 → `revokeAllForOperator` 该操作者全部活动会话。
- 每 commit 带 `Co-Authored-By: Claude Code <noreply@anthropic.com>`。

---

### Task 1: 持久层 — V16 会话/口令/退避

**Files:**
- Create: `scheduler-persistence/src/main/resources/db/migration/V16__operator_strong_auth.sql`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/AuthRepository.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcAuthRepository.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/OperatorRepository.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcOperatorRepository.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcAuthRepositoryTest.java`
- Modify: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcOperatorRepositoryTest.java`

**Interfaces:**
- Consumes: 既有 `AbstractPostgresTest`(提供 `jdbc` JdbcTemplate,Flyway 已迁移)。
- Produces: `AuthRepository` 接口(下述),`OperatorRepository` 增 `activePasswordHash`/`setPassword`。

**V16 SQL:**
```sql
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
```

- [ ] **Step 1: 写 V16 迁移文件**(如上),并在 `JdbcAuthRepositoryTest` 断言列/表存在。

- [ ] **Step 2: `AuthRepository` 接口**
```java
public interface AuthRepository {
  /** 会话解析:token 明文(hex)→ 有效会话(未撤销、未过期、绝对寿命<5d);无效 → empty。 */
  record Session(String operator, Instant created, Instant expires) {}
  Optional<Session> resolve(String rawToken);
  /** 建会话:存 SHA-256(token),expires_at=now+ttl。 */
  void create(String rawToken, String operator, Duration ttl);
  /** 撤销指定 token 会话(登出/轮换旧).no-op 若不存在. */
  void revoke(String rawToken);
  /** 撤销该操作者全部活动会话(改密/deactivate)。 */
  void revokeAllForOperator(String operator);
  /** 退避:name 当前 locked_until(>now 才返回)。 */
  Optional<Instant> lockedUntil(String name);
  /** 记录一次失败:failures+1,locked_until=now+min(2^failures, maxLockSeconds)。 */
  void recordFailure(String name, int maxLockSeconds);
  /** 登录成功清零退避。 */
  void resetLockout(String name);
}
```
注意:`Optional<Session>` 是合法用法(record 内嵌于接口)。

- [ ] **Step 3: `JdbcAuthRepository` 实现**(经 `jdbc` JdbcTemplate;SHA-256 用 `MessageDigest`)。共用 SHA-256 工具放新文件 `dev.scheduler.persistence.AuthHashing`(`public static String sha256(String)` → hex;server 集成测试播种会话时亦用)。
```java
// resolve:SELECT operator_name, created_at, expires_at FROM app_auth_session
//  WHERE token_hash=? AND revoked_at IS NULL AND expires_at > now()
//  AND now() - created_at < interval '5 days'
// create:INSERT INTO app_auth_session(token_hash, operator_name, created_at, expires_at)
//   VALUES(?, ?, now(), now() + ?::interval)   // ttl → pg interval,或 Timestamp.from(now+ttl)
// revoke:UPDATE ... SET revoked_at=now() WHERE token_hash=?
// revokeAllForOperator:UPDATE ... SET revoked_at=now() WHERE operator_name=? AND revoked_at IS NULL
// lockedUntil:SELECT locked_until FROM app_login_attempt WHERE name=?
//   → 若存在且 locked_until>now 返回;refresh:记录失败时先 UPSERT 读当前 failures。
// recordFailure:一步 UPSERT:INSERT(name, failures, locked_until)
//   VALUES(?, 1, now()+least(power(2,1)::int,?) * interval '1 second')
//   ON CONFLICT (name) DO UPDATE SET failures=app_login_attempt.failures+1,
//     locked_until=now()+least(power(2, app_login_attempt.failures+1)::int, ?)*interval '1 second'
// resetLockout:DELETE FROM app_login_attempt WHERE name=?
```
(时间用 `Instant` 由服务端注入不现实——`recordFailure` 由 DB now() 兜底,同 V13 口径,避时区漂移。)

- [ ] **Step 4: `OperatorRepository` 增两法**
```java
/** 取 name 对应活跃操作者的口令哈希(未登记/停用/无密 → empty)。 */
Optional<String> activePasswordHash(String name);
/** 设置/重置口令哈希(upsert)。 */
void setPassword(String name, String bcryptHash);
```
Jdbc 实现:
```java
@Override public Optional<String> activePasswordHash(String name) {
  List<String> h = jdbc.query("SELECT password_hash FROM app_operator WHERE name=? AND active AND password_hash IS NOT NULL", (rs,i)->rs.getString(1), name);
  return h.isEmpty() ? Optional.empty() : Optional.of(h.get(0));
}
@Override public void setPassword(String name, String bcryptHash) {
  jdbc.update("UPDATE app_operator SET password_hash=? WHERE name=?", bcryptHash, name);
}
```

- [ ] **Step 5: 持久层测试**(`AbstractPostgresTest` 提供 `jdbc`;`@BeforeEach TRUNCATE app_auth_session, app_login_attempt; TRUNCATE app_operator ... 再播种 alice/bob`;auth repo 用 `new JdbcAuthRepository(jdbc)`),覆盖:
  - `create_thenResolve_ok`:create(token,"alice",8h) → resolve 返回 operator="alice"。
  - `resolve_wrongTokenAndRevoked_empty`:revoke 后 resolve empty。
  - `resolve_expired_empty`:create 后手动 `UPDATE expires_at=now()-1min` → empty。
  - `resolve_absoluteLifetime_exceeded_empty`:手动 `UPDATE created_at=now()-6 days` → empty。
  - `revokeAllForOperator_revokesAll`:alice 建 2 会话,bob 建 1;revokeAllForOperator("alice") → alice 两 empty、bob 仍 resolve。
  - `recordFailure_backoffEscalates_cappedAt30s`:记录 1 次 → lockedUntil>now;连续多次 → 递增;断言 locked_until-now ≤ 30s(上限)。
  - `resetLockout_clears`:fail 后 reset → lockedUntil empty。
  - `operator_setAndGetPassword`:jdbc 直接 `INSERT app_operator(name,role,active,password_hash)`→ setPassword → activePasswordHash 返回;`active` 停用/无密 → empty。
  - 断言 SHA-256 落库非明文(token 明文不入库)。

- [ ] **Step 6: 跑测试**
Run: `mvn -o -f D:/ai-project/scheduler/pom.xml -pl scheduler-persistence -am test`
Expected: 全绿。

- [ ] **Step 7: Commit**
```bash
cd /d/ai-project/scheduler && git add -A && git commit -m "feat(gov): V16 strong-auth persistence — session/backoff/password hash"
```

---

### Task 2: 服务端密码管理 + 引导

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/service/OperatorPasswordService.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/OperatorController.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/WebConfig.java`(如需豁免 login/跨域——通常不需要)
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: Task1 `AuthRepository.revokeAllForOperator`, `OperatorRepository.setPassword`。
- Produces: `POST /api/v1/operators/{name}/password` 端点;`scheduler.operators.default-password` 引导;改密/停用撤销会话。

- [ ] **Step 1: 写失败测试** `OperatorPasswordServiceTest`(纯单测,不连 DB——服务依赖接口,BCrypt 由 PasswordEncoder 注入):
  `setPassword_forcesBcryptHashAndRevokesSessions`(mock repo 验 setPassword 收到 `$2` 前缀 BCrypt + revokeAllForOperator 被调);`bootstrap_appliesDefaultToPasswordlessOnly`。

- [ ] **Step 2: 实现 `OperatorPasswordService`**
```java
@Service
public class OperatorPasswordService {
  public static final int MIN_PASSWORD = 8;
  private final OperatorRepository operators;
  private final AuthRepository auth;
  private final PasswordEncoder enc = new BCryptPasswordEncoder();
  public OperatorPasswordService(...) {...}
  /** 校验长度 → 编码 → setPassword → 撤销该操作者全部会话。抛 IllegalArgumentException(密码<8)。 */
  public void setPassword(String name, String raw) {
    if (raw == null || raw.length() < MIN_PASSWORD)
      throw new IllegalArgumentException("password must be at least " + MIN_PASSWORD + " chars");
    operators.setPassword(name, enc.encode(raw));
    auth.revokeAllForOperator(name);
  }
}
```

- [ ] **Step 3: `OperatorController` 加端点**(整体仍由拦截器收口 ADMIN):
```java
private final OperatorPasswordService passwords;
@PostMapping("/{name}/password")
public void setPassword(@PathVariable String name, @RequestBody Map<String,String> body) {
  passwords.setPassword(name, body.get("password"));
}
```
`deactivate` 追加 `auth.revokeAllForOperator(name)`(需注入 AuthRepository 或经 service)。选择:在控制器 deactivate 里调 `passwords.deactivate(name)`(Service 里加 `deactivate` → `operators.deactivate(name); auth.revokeAllForOperator(name);`),把副作用收口到 service。

- [ ] **Step 4: 引导 `default-password`**(Beans.java `seedOperators` 之后追加或并入):
```java
// 新增 bean,或扩展现有 bootstrap。对 password_hash IS NULL 的操作者应用 BCrypt(default-password 非空时)。
// 实现:读 @Value("${scheduler.operators.default-password:}")；
// 若非空,对 operators.list() 中需要(其 password_hash 为 NULL)者 setPassword。需主动查询 NULL——在 OperatorRepository 加
// List<String> namesWithoutPassword(); 或复用 list() + 单独查。计划选:加 OperatorRepository.namesWithoutPassword()。
```
- [ ] Step 4 修正: `OperatorRepository` 加 `List<String> namesWithoutPassword();`(`SELECT name FROM app_operator WHERE password_hash IS NULL`)。Bean:
```java
@Bean ApplicationRunner seedOperatorPasswords(OperatorRepository ops, OperatorPasswordService svc,
    @Value("${scheduler.operators.default-password:}") String defaultPwd) {
  return args -> { if (defaultPwd==null||defaultPwd.isBlank()) return;
    for (String n : ops.namesWithoutPassword()) svc.setPassword(n, defaultPwd); };
}
```

- [ ] **Step 5: 集成测试**(ApiIntegrationTest,ADMIN 设密码调 `POST /operators/{name}/password`):
  - alice(ADMIN)给 bob 设密 → 200;bob 犯旧会话/无密 → login 需新密。
  - 改密采样:设密前 bob 已有会话 → 设密后该会话 resolve empty(断言 DB count)。OPERATOR(bob)调用设密 → 403。
  - bootstrap:properties 设 `scheduler.operators.default-password=secret-pass` → 启动后 alice 可登录(见 Task3 login 后回补断言,或此处用 DB 断言 password_hash 非空)。
  - 注:ApiIntegrationTest 的 @SpringBootTest properties 需加 `scheduler.operators.default-password` 值以驱动 Task3 登录与既有守护(可设 `boot-pass`,Task3 用它登录 alice)。

- [ ] **Step 6: 跑测试**(server)
Run: `mvn -o -f D:/ai-project/scheduler/pom.xml -pl scheduler-server -am test`
Expected: 全绿(新增用例 + 既有 operator 管理用例按新语义微调)。

- [ ] **Step 7: Commit**
`feat(gov): strong-auth password mgmt + bootstrap default-password + revoke on change`

---

### Task 3: AuthService + AuthController + CurrentOperator + 拦截器 cookie 解析

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/security/CurrentOperator.java`
- Create: `scheduler-server/src/main/java/dev/scheduler/server/service/AuthService.java`
- Create: `scheduler-server/src/main/java/dev/scheduler/server/web/AuthController.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/security/OperatorInterceptor.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/WebConfig.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/service/AuthServiceTest.java`
- Modify: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: Task1 `AuthRepository`, Task1 `OperatorRepository.activePasswordHash`。
- Produces: `AuthController`(`/api/v1/auth/{login,logout,me}`);`CurrentOperator`(ThreadLocal<String>);拦截器 cookie 解析 + 轮换;退避。

- [ ] **Step 1: `CurrentOperator`**
```java
@Component
public class CurrentOperator {
  private final ThreadLocal<String> holder = new ThreadLocal<>();
  public void set(String operator) { holder.set(operator); }
  public String get() { return holder.get(); }   // null = 未认证
  public void clear() { holder.remove(); }
}
```

- [ ] **Step 2: `AuthService`(单测先行,注入接口):**
```java
@Service
public record/maybe class AuthService {
  static Duration TOKEN_TTL = Duration.ofHours(8);
  static Duration ABSOLUTE_SESSION = Duration.ofDays(5);
  static int BACKOFF_CAP = 30;
  record LoginResult(String operator, String token, Instant expiresAt) {}
  record ResolveResult(String operator, String rotatedToken) {} // rotatedToken null=不轮换
  public Optional<LoginResult> login(String name, String password) {
    // 退避:auth.lockedUntil(name) → present 且>now → 记 access.denied(locked) return empty
    // BCrypt:operators.activePasswordHash(name) → empty → 记 access.denied return empty
    // 验密失败 → auth.recordFailure(name, BACKOFF_CAP) + access.denied(bad_credentials) return empty
    // 成功 → auth.resetLockout(name); token=SHAHELPER.random(); auth.create(token,name,TOKEN_TTL); return LoginResult
  }
  public ResolveResult resolve(String rawToken) {
    // auth.resolve(rawToken) → empty return null;
    // 是否需要轮换:remaining = expires-now < TOKEN_TTL/2 → 若 now-created < ABSOLUTE_SESSION:
    //    新 token;auth.revoke(rawToken); auth.create(newTk, operator, TOKEN_TTL); rotate=newTk
    //  否则(超绝对寿命)→ auth.revoke; return null(强制重登)
    // 返回 ResolveResult(operator, rotatedTokenOrNull)
  }
  public void logout(String rawToken) { auth.revoke(rawToken); }
}
```
- token 生成/哈希工具:放 `AuthService` 内 static `SecureRandom` + `MessageDigest`;同时供拦截器用同一个对 token 的 SHA-256 查询函数。为避免重复,`AuthRepository` 内部已按明文查——resolve 接收明文即可,hash 在 repo 内算。token 生成仅服务端,放 `AuthService.randomToken()`。
- 退避在 login 的失败与 locked 分支都要记 access.denied 审计(operator=提交名)。auditor 注入。

- [ ] **Step 3: `AuthController`:**
```java
@RestController @RequestMapping("/api/v1/auth")
public class AuthController {
  @PostMapping("/login") ResponseEntity<?> login(@RequestBody Map<String,String> b, HttpServletResponse res)
    // 成功:setCookie(res, token); return Map.of("operator", entry, "expiresAt", ...)
    // 失败:throw ResponseStatusException(401, ...)
  @GetMapping("/me") ResponseEntity<?> me()  // curOperator.get() null→401;否则回 operator entry
  @PostMapping("/logout") void logout(HttpServletRequest req, HttpServletResponse res)
    // 读 cookie token → auth.logout → Set-Cookie 清空
  static void setCookie(HttpServletResponse res, String token) // HttpOnly; SameSite=Strict; Path=/api; Secure 按 prop
}
```
`operator entry` 取法:login 成功后需 OperatorEntry(供前端角色展示)。经 `OperatorService`/repo list 查,或 AuthService 返回 entry。简化:`POST login` 成功 → `operators.list()` 里找 name → OperatorEntry。
`Secure` 控制注入 `@Value("${scheduler.auth.secure-cookies:false}") boolean secureCookies`。

- [ ] **Step 4: 拦截器改写(cookie 解析 + 门禁),全依赖 CurrentOperator:**
```java
private static final String COOKIE = "session";
public boolean preHandle(req,res,handler) {
  if (!uri.startsWith(PREFIX)) return true;
  boolean isLogin = uri.equals("/api/v1/auth/login");
  // 解析 cookie → CurrentOperator + 轮换(若有)
  String who = resolveIdentity(req,res);   // cookie→authService.resolve;invalid→null
  if (isLogin) return true;
  boolean management = path.match(OPERATORS, uri);
  if (!management && !isWrite(method)) return true; // 读开放(who 可能已设)
  if (who == null) return deny(req,res,401,required,"authentication required");
  OperatorRole role = operators.roleOf(who).orElse(null);
  if (role==null || role.rank()<required.rank()) return deny(req,res,403,required,...);
  return true;
}
public void afterCompletion(...) { current.clear(); }
```
`resolveIdentity`:读 cookie `session`;空 → null;`authService.resolve` → null(无效/过期)→ 不记审计(读场景匿名正常);有效 → `current.set(operator)`,若 `rotatedToken != null` → `res.addCookie(encoded rotated cookie)`;返回 operator。
`deny` 里的 operator 用 `current.get()`(已解析)或提交值;审计 message 更新。

- [ ] **Step 5: WebConfig** 确保 `/api/v1/**` 拦截已涵盖 auth(已经是);`afterCompletion` 需配置(可选,ThreadLocal 用 finally 清理更稳——在 preHandle 返回 false 分支也 clear)。

- [ ] **Step 6: 集成测试替换默认认证载体**(关键改造):
`DefaultOperatorConfig` 从 `defaultRequest(get("/").header("X-Operator","alice"))` 改为:
```java
// 已知固定 token 明文 + 其 SHA-256 落库(在 resetDb 里为 alice 播种会话)
@Bean MockMvcBuilderCustomizer defaultOperatorCookie() {
  return builder -> builder.defaultRequest(get("/").cookie(new jakarta.servlet.http.Cookie("session", ALICE_TOKEN)));
}
```
`resetDb` 增:播种 alice/bob 会话:
```java
// ALICE_TOKEN = 固定 hex 字符串;DB 存 SHA-256
String aliceTkHash = AuthHashing.sha256(ALICE_TOKEN);
jdbc.update("INSERT INTO app_auth_session(token_hash, operator_name, created_at, expires_at) VALUES (?,?, now(), now()+ interval '8 hour')", aliceTkHash, "alice");
```
因此既有写用例默认带 alice 会话 cookie → 零改动通过;权限用例原先 `.header("X-Operator","bob")` 改为用 bob 会话 cookie(`cookie(new Cookie("session",BOB_TOKEN))`,`resetDb` 同时播种 BOB_TOKEN)。
新增用例:
  - login(bob, correct) → 200 Set-Cookie session + row;
  - login wrong pwd → 401 + access.denied;
  - 连续失败 → 退避 401(locked);
  - 带 alice cookie 写 → 200;
  - 轮换:把 alice 会话 `UPDATE expires_at=now()`(剩半内)→ 请求后旧 cookie 值失效 + 响应含新 Set-Cookie。设计上轮换由拦截器在请求响应实施;断言响应 header 有新的 Set-Cookie 且旧 token resolve empty、新 token resolve ok。
  - 改密后旧 alice 会话 → 401(见 Task2/此处联合)。
  - `/auth/me` 无 cookie → 401;带 alice cookie → 200.
  - `/auth/logout` → 会话撤销 → 之后写 401。
- 注意 `access.denied` 既有断言(operator=bob)自动转为 cookie 解析名 bob,无需改——但需确认 `resetDb` 给 bob 也播种 cookie。

- [ ] **Step 7: 跑测试**(server + persistence 已绿)
Run: `mvn -o -f D:/ai-project/scheduler/pom.xml -pl scheduler-server -am test`
Expected: 全绿。

- [ ] **Step 8: Commit**
`feat(gov): strong-auth session resolution + rotation + backoff + cookie interceptor`

---

### Task 4: 移除 X-Operator,控制器改读 CurrentOperator

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/DagController.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/ExecutionController.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/AuditController.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: Task3 `CurrentOperator`。
- Produces: 全链路无 `X-Operator`。

- [ ] **Step 1: 逐个控制器**把 `@RequestHeader(value="X-Operator", ...) String operator` 参数**删除**,auditor.record 调用第一参 `operator` → `currentOperator.get()`(注入 CurrentOperator bean)。涉及 TaskController(~6 处:create/update/pause/resume/trigger/delete)、DagController(~6)、ExecutionController(~3)、AuditController.archive。
- 审计 operator 语义:写端点已由拦截器保证已解析到 CurrentOperator,故 `currentOperator.get()` 非空;兜底 `== null ? "anonymous" : ...` 沿用 AuditRecorder 内部逻辑即可(record 首参传 `currentOperator.get()`)。

- [ ] **Step 2: 断言 server main 无残留 X-Operator**
Run: `grep -rn "X-Operator" scheduler-server/src/main/java` → 仅剩注释可留、代码清零。

- [ ] **Step 3: 回归**
Run: `mvn -o -f D:/ai-project/scheduler/pom.xml -pl scheduler-server -am test`
Expected: 全绿。

- [ ] **Step 4: Commit**
`refactor(gov): drop X-Operator — controllers read CurrentOperator from cookie session`

---

### Task 5: 前端登录门 + 身份展示

**Files:**
- Create: `web/src/pages/LoginPage.tsx`
- Modify: `web/src/api/client.ts`
- Modify: `web/src/api/types.ts`
- Modify: `web/src/App.tsx`
- Modify: `web/src/pages/OperatorsPage.tsx`
- Modify: `web/src/pages/TasksPage.tsx`(如含 X-Operator/operator 用法则改;通常只侧栏)

**Interfaces:**
- Consumes: `GET /auth/me`、`POST /auth/login`、`POST /auth/logout`、`POST /operators/{name}/password`。

- [ ] **Step 1: client.ts 增/改**
- 移除 `localStorage 'scheduler.operator'` 与 `X-Operator` 头;`req()` 用同源 fetch(浏览器自动带 cookie),保留 401 抛错。
- 增 `login(name,password)` → POST /auth/login(fetch 手动,需读 Set-Cookie——浏览器自动存);`me()` → GET /auth/me → OperatorEntry;`logout()` → POST /auth/logout;`setPassword(name,password)` → POST /operators/{name}/password。移除 `getOperatorName/setOperator`。
- types:复用 `OperatorEntry`。

- [ ] **Step 2: `LoginPage`**:name+password 表单,提交调 `login`;成功 → 跳 `/tasks`;失败 → 显示错误;挂载时若 `me()` 200 → 直接跳走。

- [ ] **Step 3: `App.tsx`**:用 `AuthGate` 包裹 routes:挂载 `me()`;401 → 渲染 LoginPage;成功 → 现主路由 + 侧栏显示 `{operator.name}`({role} 徽章)+「登出」按钮 → `logout()` → 回登录。移除原侧栏「操作者」输入框(self-report)。

- [ ] **Step 4: `OperatorsPage`**:加「设密码」列(ADMIN)/按钮 → prompt 或内联输入 → `setPassword`。展示当前登录者或基于 me()。

- [ ] **Step 5: 构建**
Run: `cd /d/ai-project/scheduler/web && npm run build`
Expected: `tsc -b` + vite build 通过。

- [ ] **Step 6: Commit**
`feat(gov): frontend login gate + identity display + password set`

---

### Task 6: 全量回归 + 收尾

- [ ] **Step 1: 全模块离线回归**
Run: `mvn -o -f D:/ai-project/scheduler/pom.xml test`
Run: `cd /d/ai-project/scheduler/web && npm run build`
Expected: 全绿;vite build 通过。

- [ ] **Step 2: 更新 Runbook/注释** 中 X-Operator 相关(dev-runbook 记忆文件更新由主导者处理;代码注释残留清理)。

- [ ] **Step 3: Commit**(若有额外修整)
`chore(gov): finalize strong-auth regression`

还在计划外但需在 Task6 补:CSS 登录页样式(复用现有 `btn/.card/.input`);若 `me()` 在 dev Vite 代理跨端口 cookie 丢失,配 vite `server.proxy` cookie(`secure:false`)——见记忆 dev-runbook。