# Scheduler 执行运行时深化:重试策略化 + 执行超时 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把执行失败「重试」升级为可配置策略(fixed/linear/exponential + 任务级退避封顶 + 整轮重试时限 W),并强制执行超时列 `timeout_seconds`(>0 时超过即分布式判定超时,走统一失败路径)。

**Architecture:** 重试决策仍在纯类 `RetryPolicy`(读 Task 携带的 mode/cap);整轮预算 W 用分片新列 `retry_budget_until`(首次进重试置 now()+W,到期转死信)做跨调用持久状态;执行超时复用既有 `timeout_seconds` 列、由 server Reconciler 同一 15s 对账循环对超过时限的 RUNNING 分片走既有 markStatus(FAILED)+`FailureResolver.handle` 回收。超时/预算判窗统一用 DB `now()`。

**Tech Stack:** Java 17、Spring Boot、PostgreSQL 16(Testcontainers)、Flyway 迁移。无新增依赖。

**Spec:** `docs/superpowers/specs/2026-09-16-scheduler-runtime-retry-timeout-design.md`

## Global Constraints

- **复用 `timeout_seconds`**:不新增执行超时列(否则两个超时列违背「不重复配置」);语义 0=不超时、>0=强制执行。V9 只加 `retry_mode`/`retry_cap_ms`/`retry_budget_ms`(app_task)与 `retry_budget_until`(execution_shard)。
- **React 表单不加这 4 字段**(spec non-goal);后端 DTO 可设即可。
- 超时/预算判窗均用 DB `now()`;RetryPolicy 退避仍用 JVM clock(注入 Clock)。
- 防双跑复用既有 worker_id 归属守卫 CAS;不新增并发机制。`FAILED→FAILED` 非法的 `IllegalStateException` 由 Reconciler 既有 catch 吞掉,超时/活性两分支对同一行的二次回收自然被跳过。
- 老任务默认兼容:V9 使现有行为不变(exponential + 1h 封顶 + 仅按次数);既有测试全绿。
- Task record 用 13 参便捷构造保留既有 `new Task(...)`(13 参)构造点编译,最小化 diff。
- 离线 `mvn -o`;模块测试 `-pl <mod> -am test -Dtest=... -Dsurefire.failIfNoSpecifiedTests=false`。

---

### Task 1: V9 迁移 + Task record 重试字段 + 持久层映射

**Files:**
- Create: `scheduler-persistence/src/main/resources/db/migration/V9__retry_timeout.sql`
- Modify: `scheduler-core/src/main/java/dev/scheduler/core/Task.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcTaskRepository.java:16-22,26-41,79-89`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcTaskRepositoryTest.java`

**Interfaces:**
- Produces: `Task` record 新增 3 组件(`String retryMode, Long retryCapMs, Long retryBudgetMs`,末3位) + 13 参便捷构造;`JdbcTaskRepository` 持久化这 3 列(MAP/create/update)。

- [ ] **Step 1: 写 V9 迁移**

```sql
-- 重试策略化 + 执行超时纵深(运行时深化子项目 1):
-- app_task 加退避模式/封顶/整轮预算三新列;execution_shard 加整轮重试预算到期点(首次进重试置 now()+W)。
-- timeout_seconds 为既有列,semantics 变更(0=不超时、>0=强制执行),不含在本迁移列变更内。
ALTER TABLE app_task
  ADD COLUMN retry_mode     VARCHAR(16) NOT NULL DEFAULT 'exponential',
  ADD COLUMN retry_cap_ms   BIGINT NULL,
  ADD COLUMN retry_budget_ms BIGINT NULL;
ALTER TABLE execution_shard
  ADD COLUMN retry_budget_until TIMESTAMPTZ NULL;
```

- [ ] **Step 2: 改 Task record(加 3 字段 + 13 参便捷构造)**

`Task.java`:

```java
public record Task(
    Long id, String name, String kind, String handlerRef,
    String cron, int shardCount, int timeoutSeconds,
    int maxRetries, long backoffMs, String retryableFailurePattern,
    int maxActiveConcurrent, boolean enabled, boolean paused,
    String retryMode, Long retryCapMs, Long retryBudgetMs) {
  public Task {
    if (shardCount < 1) throw new IllegalArgumentException("shardCount must be >= 1");
    if (maxActiveConcurrent < 1) throw new IllegalArgumentException("maxActiveConcurrent must be >= 1");
  }

  /** 仅含核心配置的便捷构造(既有 13 参调用点/测试 fixture 用);重试策略参数取缺省:mode null(=exponential)、无封顶覆盖、无整轮预算。 */
  public Task(Long id, String name, String kind, String handlerRef,
      String cron, int shardCount, int timeoutSeconds,
      int maxRetries, long backoffMs, String retryableFailurePattern,
      int maxActiveConcurrent, boolean enabled, boolean paused) {
    this(id, name, kind, handlerRef, cron, shardCount, timeoutSeconds,
        maxRetries, backoffMs, retryableFailurePattern, maxActiveConcurrent, enabled, paused,
        null, null, null);
  }
}
```

- [ ] **Step 3: 写失败的持久层测试**

在 `JdbcTaskRepositoryTest` 加(需新增 import `assertNull`、`assertTrue`):

```java
@Test void createAndUpdate_persistRetryStrategyFields() {
  var created = repo.create(new Task(null, "t1", "cron", "demo", "*/5 * * * *",
      1, 60, 3, 500, null, 8, true, false, "linear", 60_000L, 300_000L));
  Task fromDb = repo.findById(created.id()).orElseThrow();
  assertEquals("linear", fromDb.retryMode());
  assertEquals(60_000L, fromDb.retryCapMs());
  assertEquals(300_000L, fromDb.retryBudgetMs());

  Task updated = new Task(created.id(), "t2", "cron", "demo", "0 */5 * * * *",
      2, 120, 5, 1000, null, 8, true, false, "fixed", null, null);
  assertTrue(repo.update(updated.id(), updated));
  Task after = repo.findById(updated.id()).orElseThrow();
  assertEquals("fixed", after.retryMode());
  assertNull(after.retryCapMs(), "update 置 null → 落库 NULL 而非 0");
  assertNull(after.retryBudgetMs());
}
```

同时补一条:13 参便捷构造创建时 `retryMode` 经 COALESCE 落 `'exponential'`:

```java
@Test void convenienceConstructor_createsWithExponentialMode() {
  var created = repo.create(new Task(null, "t1", "cron", "demo", "*/5 * * * *",
      1, 60, 0, 1000, null, 8, true, false));
  assertEquals("exponential", repo.findById(created.id()).orElseThrow().retryMode(),
      "13 参便捷构造 retryMode=null → INSERT COALESCE 落 'exponential'");
}
```

- [ ] **Step 4: 运行验证失败**

Run: `mvn -o -pl scheduler-persistence -am test -Dtest=JdbcTaskRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `Task` 无 `retryMode`/`retryCapMs`/`retryBudgetMs` 组件;V9 未存在时 Flyway 迁移后表无新列。

- [ ] **Step 5: 实现 MAP / create / update**

`JdbcTaskRepository.java`:

MAP(第 16-22 行)加 3 列(nullable Long 用 `getObject` 判 null):

```java
  private static final RowMapper<Task> MAP = (rs, i) -> new Task(
      rs.getLong("id"), rs.getString("name"), rs.getString("kind"),
      rs.getString("handler_ref"), rs.getString("cron"),
      rs.getInt("shard_count"), rs.getInt("timeout_seconds"),
      rs.getInt("max_retries"), rs.getLong("backoff_ms"),
      rs.getString("retryable_failure_pattern"), rs.getInt("max_active_concurrent"),
      rs.getBoolean("enabled"), rs.getBoolean("paused"),
      rs.getString("retry_mode"), (Long) rs.getObject("retry_cap_ms"),
      (Long) rs.getObject("retry_budget_ms"));
```

create(第 24-41 行)INSERT 加列;`retry_mode` 用 COALESCE 兜缺省:

```java
  @Override public Task create(Task t) {
    KeyHolder kh = new GeneratedKeyHolder();
    jdbc.update(con -> {
      var ps = con.prepareStatement("""
        INSERT INTO app_task (name, kind, handler_ref, cron, shard_count, timeout_seconds,
                              max_retries, backoff_ms, retryable_failure_pattern, max_active_concurrent,
                              retry_mode, retry_cap_ms, retry_budget_ms)
        VALUES (?,?,?,?,?,?,?,?,?,?,COALESCE(?, 'exponential'),?,?)""",
          new String[]{"id"});
      ps.setString(1, t.name()); ps.setString(2, t.kind()); ps.setString(3, t.handlerRef());
      ps.setString(4, t.cron()); ps.setInt(5, t.shardCount()); ps.setInt(6, t.timeoutSeconds());
      ps.setInt(7, t.maxRetries()); ps.setLong(8, t.backoffMs());
      ps.setString(9, t.retryableFailurePattern()); ps.setInt(10, t.maxActiveConcurrent());
      ps.setString(11, t.retryMode());
      ps.setObject(12, t.retryCapMs(), java.sql.Types.BIGINT);
      ps.setObject(13, t.retryBudgetMs(), java.sql.Types.BIGINT);
      return ps;
    }, kh);
    return new Task(kh.getKey().longValue(), t.name(), t.kind(), t.handlerRef(), t.cron(),
        t.shardCount(), t.timeoutSeconds(), t.maxRetries(), t.backoffMs(),
        t.retryableFailurePattern(), t.maxActiveConcurrent(), true, false,
        t.retryMode(), t.retryCapMs(), t.retryBudgetMs());
  }
```

update(第 79-89 行)同样加 3 列:

```java
  @Override public boolean update(long id, Task t) {
    int rows = jdbc.update("""
        UPDATE app_task SET name=?, kind=?, handler_ref=?, cron=?, shard_count=?,
               timeout_seconds=?, max_retries=?, backoff_ms=?,
               retryable_failure_pattern=?, max_active_concurrent=?, paused=?,
               retry_mode=COALESCE(?, 'exponential'), retry_cap_ms=?, retry_budget_ms=?, updated_at=now()
        WHERE id=?""",
        t.name(), t.kind(), t.handlerRef(), t.cron(), t.shardCount(), t.timeoutSeconds(),
        t.maxRetries(), t.backoffMs(), t.retryableFailurePattern(), t.maxActiveConcurrent(),
        t.paused(), t.retryMode(), t.retryCapMs(), t.retryBudgetMs(), id);
    return rows > 0;
  }
```

> **Ruling(COALESCE 兜 mode):** `retry_mode` 列 `NOT NULL DEFAULT 'exponential'`;便捷构造 `retryMode=null` 若直接 INSERT NULL 会违约束。create/update 用 `COALESCE(?, 'exponential')` 使 null → 'exponential',与 DB 缺省一致,也保住「老任务行为不变」。

- [ ] **Step 6: 运行验证通过**

Run: `mvn -o -pl scheduler-persistence -am test -Dtest=JdbcTaskRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(含新增 2 测试 + 既有全量)。

- [ ] **Step 7: 提交**

```bash
git add scheduler-core/src/main/java/dev/scheduler/core/Task.java scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcTaskRepository.java scheduler-persistence/src/main/resources/db/migration/V9__retry_timeout.sql scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcTaskRepositoryTest.java
git commit -m "feat(runtime): V9 迁移 + Task 重试字段(retry_mode/cap/budget)持久化"
```

---

### Task 2: TaskController DTO + 校验

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java:39-45,59-68,85-96,184-195`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: 新 `Task` record 组件(Task 1)。
- Produces: `CreateTaskRequest`/`UpdateTaskRequest` 加 `retryMode/retryCapMs/retryBudgetMs`;`checkDefinition` 新校验(建/改可设,可空)。

- [ ] **Step 1: 写失败的集成测试**

在 `ApiIntegrationTest` 加:

```java
/** 重试策略化(§1):建任务可设 retry_mode/cap/budget,CREATE 响应复读;非法值 400。 */
@Test
void createTask_persistsRetryStrategyFields() throws Exception {
  String body = "{\"name\":\"strat-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
      + "\"cron\":\"" + CRON + "\",\"shardCount\":1,"
      + "\"timeoutSeconds\":0,\"retryMode\":\"linear\",\"retryCapMs\":60000,\"retryBudgetMs\":300000}";
  MvcResult r = mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
      .andExpect(status().isCreated()).andReturn();
  String got = r.getResponse().getContentAsString();
  assertTrue(got.contains("\"retryMode\":\"linear\""));
  assertTrue(got.contains("\"retryCapMs\":60000"));
  assertTrue(got.contains("\"retryBudgetMs\":300000"));

  String badMode = "{\"name\":\"bad-mode\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
      + "\"cron\":\"" + CRON + "\",\"shardCount\":1,\"retryMode\":\"quadratic\"}";
  mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(badMode))
      .andExpect(status().isBadRequest());
  String negCap = "{\"name\":\"neg-cap\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
      + "\"cron\":\"" + CRON + "\",\"shardCount\":1,\"retryCapMs\":-5}";
  mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(negCap))
      .andExpect(status().isBadRequest());
}
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn -o -pl scheduler-server -am test -Dtest=ApiIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `CreateTaskRequest` 无 `retryMode`/`retryCapMs`/`retryBudgetMs`;请求体非法 retry_mode 未被拒(返回 201 而非 400)。

- [ ] **Step 3: 实现 DTO + 校验 + 装配**

`TaskController.java`:

两个 request record(第 39-45 行)各加 3 字段:

```java
  public record CreateTaskRequest(String name, String kind, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs,
      String retryableFailurePattern, Integer maxActiveConcurrent,
      String retryMode, Long retryCapMs, Long retryBudgetMs) {}

  public record UpdateTaskRequest(String name, String kind, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs,
      String retryableFailurePattern, Integer maxActiveConcurrent, Boolean paused,
      String retryMode, Long retryCapMs, Long retryBudgetMs) {}
```

create 的 Task 构造(第 61-68 行)末 3 参直接透传:

```java
    Task created = tasks.create(new Task(
        null, req.name(), req.kind() == null ? "cron" : req.kind(), req.handlerRef(), req.cron(),
        req.shardCount() == null ? 1 : req.shardCount(),
        req.timeoutSeconds() == null ? 300 : req.timeoutSeconds(),
        req.maxRetries() == null ? 0 : req.maxRetries(),
        req.backoffMs() == null ? 1000L : req.backoffMs(),
        req.retryableFailurePattern(), req.maxActiveConcurrent() == null ? 8 : req.maxActiveConcurrent(),
        true, false, req.retryMode(), req.retryCapMs(), req.retryBudgetMs()));
```

update 的 Task 构造(第 87-96 行)末 3 参 null-fallback(省略不覆盖既有值):

```java
    Task updated = new Task(id, req.name(), req.kind() == null ? existing.kind() : req.kind(),
        req.handlerRef(), req.cron(),
        req.shardCount() == null ? existing.shardCount() : req.shardCount(),
        req.timeoutSeconds() == null ? existing.timeoutSeconds() : req.timeoutSeconds(),
        req.maxRetries() == null ? existing.maxRetries() : req.maxRetries(),
        req.backoffMs() == null ? existing.backoffMs() : req.backoffMs(),
        req.retryableFailurePattern() == null ? existing.retryableFailurePattern() : req.retryableFailurePattern(),
        req.maxActiveConcurrent() == null ? existing.maxActiveConcurrent() : req.maxActiveConcurrent(),
        existing.enabled(),
        req.paused() == null ? existing.paused() : req.paused(),
        req.retryMode() == null ? existing.retryMode() : req.retryMode(),
        req.retryCapMs() == null ? existing.retryCapMs() : req.retryCapMs(),
        req.retryBudgetMs() == null ? existing.retryBudgetMs() : req.retryBudgetMs());
```

`checkDefinition`(第 184 行)签名加 3 参,并加校验(第 187 行后):

```java
  static void checkDefinition(String name, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs,
      Integer maxActiveConcurrent, String retryMode, Long retryCapMs, Long retryBudgetMs) {
    if (retryMode != null && !retryMode.isBlank()
        && !(retryMode.equals("fixed") || retryMode.equals("linear") || retryMode.equals("exponential"))) {
      throw new IllegalArgumentException("retryMode must be one of fixed|linear|exponential");
    }
    if (retryCapMs != null && retryCapMs < 0) throw new IllegalArgumentException("retryCapMs must be >= 0");
    if (retryBudgetMs != null && retryBudgetMs < 0) throw new IllegalArgumentException("retryBudgetMs must be >= 0");
    if (timeoutSeconds != null && timeoutSeconds < 0) throw new IllegalArgumentException("timeoutSeconds must be >= 0");
    if (backoffMs != null && backoffMs < 0) throw new IllegalArgumentException("backoffMs must be >= 0");
    if (maxRetries != null && maxRetries < 0) throw new IllegalArgumentException("maxRetries must be >= 0");
    if (shardCount != null && shardCount < 1) throw new IllegalArgumentException("shardCount must be >= 1");
    if (maxActiveConcurrent != null && maxActiveConcurrent < 1) {
      throw new IllegalArgumentException("maxActiveConcurrent must be >= 1");
    }
    validateCron(cron);
  }
```

两处调用点(第 59、85 行)补 3 参:`req.retryMode(), req.retryCapMs(), req.retryBudgetMs()`。

- [ ] **Step 4: 运行验证通过**

Run: `mvn -o -pl scheduler-server -am test -Dtest=ApiIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(含新增测试 + 既有全量)。

- [ ] **Step 5: 提交**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java
git commit -m "feat(server): 任务 DTO 暴露 retry_mode/retry_cap_ms/retry_budget_ms + 校验"
```

---

### Task 3: RetryPolicy 退避三模式 + 任务级封顶

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/RetryPolicy.java:36-48`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/FailureResolver.java:35`(唯一调用点换新签名)
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/retry/RetryPolicyTest.java`

**Interfaces:**
- Consumes: `Task.retryMode`/`retryCapMs`/`backoffMs`(Task 1)。
- Produces: `long RetryPolicy.delayMs(Task task, int attempt)`(签名由 `delayMs(long,int)` 变更;`shouldRetry` 不变)。

- [ ] **Step 1: 写失败的单元测试**

`RetryPolicyTest` 加 Task 帮助(源文件已有 13 参 `new Task(1L,"t",...)` 在第 19 行)——替换其现有指数断言 + 新增模式断言:

```java
  private Task task(long backoffMs) { return task(backoffMs, null, null); }
  private Task task(long backoffMs, String mode, Long cap) {
    return new Task(1L, "t", "kind", "handler", null, 1, 60, 3, backoffMs, null, 8, true, false,
        mode, cap, null);
  }

  @Test void exponentialMode_isExistingDefaultBackoff() {
    assertEquals(200L, policy.delayMs(task(200L), 1));
    assertEquals(400L, policy.delayMs(task(200L), 2));
    assertEquals(800L, policy.delayMs(task(200L), 3));
    assertEquals(3600_000L, policy.delayMs(task(1L << 40, "exponential", null), 40), "溢出守卫 → 全局 1h");
    assertEquals(3600_000L, policy.delayMs(task(1L << 30, "exponential", null), 40), "超上限 → 封顶 1h");
  }

  @Test void fixedMode_constantBackoffCapped() {
    assertEquals(200L, policy.delayMs(task(200L, "fixed", null), 1));
    assertEquals(200L, policy.delayMs(task(200L, "fixed", null), 6), "fixed 不随 attempt 增长");
    assertEquals(1000L, policy.delayMs(task(200L, "fixed", 1000L), 100), "任务级 cap 覆盖 fixed");
  }

  @Test void linearMode_growsWithAttemptCapped() {
    assertEquals(200L, policy.delayMs(task(200L, "linear", null), 1));
    assertEquals(400L, policy.delayMs(task(200L, "linear", null), 2));
    assertEquals(800L, policy.delayMs(task(200L, "linear", null), 4));
    assertEquals(3600_000L, policy.delayMs(task(1L << 50, "linear", null), 40), "linear 乘法溢出守卫 → 全局 1h");
  }

  @Test void perTaskCap_overridesGlobalDefault() {
    assertEquals(4000L, policy.delayMs(task(2000L, "exponential", 6000L), 2), "2^1*2000=4000 未超 cap");
    assertEquals(6000L, policy.delayMs(task(2000L, "exponential", 6000L), 3), "3 次 8000 超 cap 6000 → 取 cap");
    assertEquals(3600_000L, policy.delayMs(task(1L << 30, "exponential", null), 40), "任务未设 cap → 兜底全局 1h");
  }
```

(删除原 `delayMs(long,int)` 断言的 5 处调用与对应方法体,替换为上。)

- [ ] **Step 2: 运行验证失败**

Run: `mvn -o -pl scheduler-persistence -am test -Dtest=RetryPolicyTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `RetryPolicy` 无 `delayMs(Task,int)` 符号;`FailureResolver` 仍调旧签名。

- [ ] **Step 3: 实现三模式 + 封顶**

`RetryPolicy.java` 重构 `delayMs`:

```java
  /**
   * 按任务配置的退避模式计算下次重试前等待时长(封顶 = 任务级 retryCapMs,未设则全局 1h)。
   * fixed: backoff;linear: backoff*attempt;exponential: backoff*2^(attempt-1)(缺省,含 mode 为 null/未知)。
   * 乘法溢出守卫:首次即到顶或必超封顶 → 直接返回 cap,防移位溢出 long。
   */
  public long delayMs(Task task, int attempt) {
    long cap = task.retryCapMs() != null ? task.retryCapMs() : CAP;
    long backoff = task.backoffMs();
    if ("fixed".equals(task.retryMode())) {
      return Math.min(backoff, cap);
    }
    if ("linear".equals(task.retryMode())) {
      if (backoff >= cap) return cap;                    // 首次即到顶
      if (attempt > 1 && backoff > cap / attempt) return cap; // 乘法必超封顶 → 取顶,防溢出
      return Math.min(backoff * attempt, cap);
    }
    // exponential(缺省)
    if (attempt <= 1) {
      return Math.min(backoff, cap);
    }
    long result = backoff;
    for (int i = 1; i < attempt; i++) {
      if (result > cap / 2) {
        return cap;
      }
      result <<= 1;
    }
    return Math.min(result, cap);
  }
```

`FailureResolver.java:35` 换新签名调用:

```java
        clock.instant().plusMillis(retryPolicy.delayMs(task, attempt)), detail);
```

- [ ] **Step 4: 运行验证通过**

Run: `mvn -o -pl scheduler-persistence -am test -Dtest=RetryPolicyTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(含新增模式断言 + 既有指数回归)。

- [ ] **Step 5: 提交**

```bash
git add scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/RetryPolicy.java scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/FailureResolver.java scheduler-persistence/src/test/java/dev/scheduler/persistence/retry/RetryPolicyTest.java
git commit -m "feat(persistence): RetryPolicy 退避三模式(fixed/linear/exponential) + 任务级封顶 — delayMs(Task,int)"
```

---

### Task 4: 整轮重试预算 W(scheduleRetry 置窗 + retryBudgetExhausted + claim 重置 started_at + FailureResolver 预算分支)

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java:75`(scheduleRetry 签名)+ 新增 `retryBudgetExhausted`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java:169-170`(claim started_at)、`285-297`(scheduleRetry)
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/FailureResolver.java`(预算分支)
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java`、Create `scheduler-persistence/src/test/java/dev/scheduler/persistence/retry/FailureResolverTest.java`

**Interfaces:**
- Consumes: `Task.retryBudgetMs`(Task 1)、`RetryPolicy.delayMs(Task,int)`(Task 3)。
- Produces: `boolean ShardRepository.retryBudgetExhausted(long shardId)`;`void ShardRepository.scheduleRetry(long shardId, Instant retryAt, Long retryBudgetMs, String detail)`(签名变更,唯二调用点: FailureResolver + JdbcShardRepositoryTest)。

- [ ] **Step 1: 写失败的持久层测试**

`JdbcShardRepositoryTest`——先更新 4 处 3 参 `scheduleRetry` 调用为 4 参并传 `null` 预算(第 411、435、447 行;第 425 行的调用在测试体):`shardRepo.scheduleRetry(shard0, retryAt, null, "boom")` 等。再加新测试与一个取预算任务的 helper:

```java
  /** 取带整轮预算 W 的任务。 */
  private long newTaskBudget(int shardCount, int maxActiveConcurrent, Long budgetMs) {
    Task t = taskRepo.create(new Task(null, "t" + System.nanoTime(), "cron", "demo", "*/5 * * * *",
        shardCount, 300, 0, 1000, null, maxActiveConcurrent, true, false, null, null, budgetMs));
    return t.id();
  }

  @Test void scheduleRetry_setsRetryBudgetWindowOnce_doesNotExtendOnRetries() {
    long taskId = newTaskBudget(1, 8, 300_000L);
    long shard0 = failedShard(taskId, 1);
    shardRepo.scheduleRetry(shard0, Instant.now().plusSeconds(10), 300_000L, "boom");
    java.sql.Timestamp first = jdbc.queryForObject(
        "SELECT retry_budget_until FROM execution_shard WHERE id=?", java.sql.Timestamp.class, shard0);
    assertNotNull(first, "首次进重试置 retry_budget_until=now()+W");

    // 再失败一次(CAS 无关,直接落地 FAILED),改更大预算重试:窗口不得续后,COALESCE 保持首置。
    jdbc.update("UPDATE execution_shard SET status='FAILED' WHERE id=?", shard0);
    shardRepo.scheduleRetry(shard0, Instant.now().plusSeconds(10), 600_000L, "boom2");
    java.sql.Timestamp second = jdbc.queryForObject(
        "SELECT retry_budget_until FROM execution_shard WHERE id=?", java.sql.Timestamp.class, shard0);
    assertEquals(first, second, "预算窗口自首次失败起算,重试间不续");
  }

  @Test void scheduleRetry_noBudget_leavesWindowNull() {
    long taskId = newTask(1, 8); // 无预算
    long shard0 = failedShard(taskId, 1);
    shardRepo.scheduleRetry(shard0, Instant.now().plusSeconds(10), null, "boom");
    assertNull(jdbc.queryForObject(
        "SELECT retry_budget_until FROM execution_shard WHERE id=?", java.sql.Timestamp.class, shard0),
        "未设 W → 不置预算窗(行为=现状按次数)");
  }

  @Test void retryBudgetExhausted_reflectsDeadlineAgainstDbNow() {
    long taskId = newTaskBudget(1, 8, 300_000L);
    long shard0 = failedShard(taskId, 1);
    shardRepo.scheduleRetry(shard0, Instant.now().plusSeconds(10), 300_000L, "boom");
    assertFalse(shardRepo.retryBudgetExhausted(shard0), "窗口未到 → 未耗尽");

    jdbc.update("UPDATE execution_shard SET retry_budget_until=now() - interval '1 second' WHERE id=?", shard0);
    assertTrue(shardRepo.retryBudgetExhausted(shard0), "预算到点(DB now)> → 耗尽");
  }

  @Test void retryBudgetExhausted_noBudget_isFalse() {
    long taskId = newTask(1, 8);
    long shard0 = failedShard(taskId, 1);
    assertFalse(shardRepo.retryBudgetExhausted(shard0), "未设 W → 恒 false(=现状仅按次数)");
  }

  @Test void claim_resetsStartedAt_eachClaim() {
    long taskId = newTask(1, 8);
    long shard0 = shard(seedParentAndShards(taskId, 1), 0).id();
    claimShard(shard0, taskId, "w1", 8);
    // 回 FAILED 再重试认领:started_at 须重置为认领时刻,而非沿用首次认领时间(超时从本轮起算)。
    jdbc.update("UPDATE execution_shard SET started_at=now() - interval '10 minutes' WHERE id=?", shard0);
    jdbc.update("UPDATE execution_shard SET status='FAILED' WHERE id=?", shard0);
    jdbc.update("UPDATE execution_shard SET status='DUE', next_retry_at=NULL WHERE id=?", shard0);
    claimShard(shard0, taskId, "w1", 8);
    java.sql.Timestamp started = jdbc.queryForObject(
        "SELECT started_at FROM execution_shard WHERE id=?", java.sql.Timestamp.class, shard0);
    assertNotNull(started);
    assertTrue(started.toInstant().isAfter(Instant.now().minusSeconds(5)),
        "重试重新认领 → started_at 重置为 now(),非 10 分钟前的旧值");
  }
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn -o -pl scheduler-persistence -am test -Dtest=JdbcShardRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `ShardRepository` 无 `retryBudgetExhausted`、`scheduleRetry` 无 4 参(编译错误);claim 未重置 started_at。

- [ ] **Step 3: 实现持久层**

`ShardRepository.java` — `scheduleRetry` 换签名 + 新增 `retryBudgetExhausted`:

```java
  /** 失败分片排回:FAILED → DUE 并写 next_retry_at。CAS on status='FAILED':0 行=竞态/非 FAILED,静默跳过不落 outcome。
   *  retryBudgetMs != null 且该 shard retry_budget_until 尚未置(首次进重试)时,置 retry_budget_until=now()+W;
   *  已置保持(窗口自首次失败起算,重试间不续)。 */
  void scheduleRetry(long shardId, Instant retryAt, Long retryBudgetMs, String detail);

  /** 整轮重试预算 W(W 时限的第二个终止条件)是否已耗尽:retry_budget_until 已置且 <= DB now()。未设 W 的对象恒 false。 */
  boolean retryBudgetExhausted(long shardId);
```

`JdbcShardRepository.java` — `scheduleRetry`(第 285-297 行)加预算窗 SQL:

```java
  @Override public void scheduleRetry(long shardId, Instant retryAt, Long retryBudgetMs, String detail) {
    // FAILED -> DUE,期待值时延由调用方(RetryPolicy,注入 clock)预先算好;仓库只写值。
    // 预算窗:首次进重试(COALESCE 未置)置 now()+W;未设预算(nullptr)保持 NULL(仅按次数)。
    java.sql.Timestamp ts = java.sql.Timestamp.from(retryAt);
    tx.executeWithoutResult(s -> {
      int updated = jdbc.update("""
          UPDATE execution_shard SET status='DUE', next_retry_at=?, queued_at=now(),
            retry_budget_until = CASE
              WHEN ? IS NOT NULL AND retry_budget_until IS NULL
                   THEN now() + make_interval(msecs => ?)
              ELSE retry_budget_until END
          WHERE id=? AND status='FAILED'""",
          ts, retryBudgetMs, retryBudgetMs, shardId);
      if (updated == 0) {
        log.debug("scheduleRetry lost CAS race: shard {} no longer FAILED; "
            + "suppressing outcome", shardId);
        return;
      }
      jdbc.update("INSERT INTO execution_shard_outcome (shard_id, status, detail) VALUES (?,?,?)",
          shardId, "DUE", detail);
    });
  }

  @Override public boolean retryBudgetExhausted(long shardId) {
    Boolean v = jdbc.queryForObject(
        "SELECT retry_budget_until IS NOT NULL AND retry_budget_until <= now()"
            + " FROM execution_shard WHERE id=?",
        Boolean.class, shardId);
    return Boolean.TRUE.equals(v);
  }
```

`JdbcShardRepository.java` — `claim`(第 169-170 行)无条件重置 started_at:

```java
      UPDATE execution_shard s SET status='RUNNING', worker_id=?, lease_until=?,
             attempt=attempt+1, started_at=now()
```

(原 `started_at=COALESCE(started_at, now())` → `started_at=now()`;超时从本轮运行起点起算,重试重新认领不沿用首次认领时间。注释就地更新。)

- [ ] **Step 4: 实现 FailureResolver 预算分支 + 写其集成测试**

`FailureResolver.java` `handle` 加预算前置分支 + 抽取死信小方法:

```java
  public void handle(Task task, long shardId, int attempt, String detail) {
    if (shards.retryBudgetExhausted(shardId)) {
      // 整轮重试预算 W 已耗尽 → 即使 maxRetries 有余也直接死信(第二终止条件)。
      deadLetter(shardId, detail);
      return;
    }
    if (retryPolicy.shouldRetry(task, attempt, detail)) {
      shards.scheduleRetry(shardId,
          clock.instant().plusMillis(retryPolicy.delayMs(task, attempt)), task.retryBudgetMs(), detail);
    } else {
      deadLetter(shardId, detail);
    }
  }

  /** 死信 + FAIL_FAST:分片进入 DLQ 即这批已失败,协作取消同父仍在 RUNNING/DUE 的兄弟,避免整批空耗。 */
  private void deadLetter(long shardId, String detail) {
    shards.markDeadLetter(shardId, "dlq:" + detail);
    shards.cancelSiblings(shardId, "fail_fast");
  }
```

新建 `scheduler-persistence/src/test/java/dev/scheduler/persistence/retry/FailureResolverTest.java`(复用 `AbstractPostgresTest`,真实 PG):

```java
package dev.scheduler.persistence.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.AbstractPostgresTest;
import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.ShardRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 失败后统一判定(重试 vs 死信)集成测试:按 Task 携带的预算 W / maxRetries 决策。 */
class FailureResolverTest extends AbstractPostgresTest {
  private final JdbcTaskRepository taskRepo = new JdbcTaskRepository(jdbc);
  private final JdbcShardRepository shardRepo = new JdbcShardRepository(jdbc);
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T10:00:00Z"), ZoneOffset.UTC);

  @BeforeEach void clean() {
    jdbc.update("TRUNCATE app_task, execution, execution_shard, execution_shard_outcome "
        + "RESTART IDENTITY CASCADE");
  }

  private FailureResolver resolver() {
    return new FailureResolver(shardRepo, new RetryPolicy(), CLOCK);
  }

  /** 建任务 + 扇出 + 认领(attempt=1)+ 落 FAILED,返回 task 与 shardId。 */
  private Fixture seed(Long budgetMs, int maxRetries) {
    Task t = taskRepo.create(new Task(null, "t", "cron", "demo", "*/5 * * * *",
        1, 30, maxRetries, 1000, null, 8, true, false, null, null, budgetMs));
    long parentId = shardRepo.createParentWithShards(t.id(), "p:" + System.nanoTime(), 1).id();
    long shard0 = shardRepo.findShards(parentId).get(0).id();
    shardRepo.claim(shard0, t.id(), "w1", Instant.now().plusSeconds(60), 8);
    shardRepo.markStatus(shard0, ExecutionStatus.FAILED, "w1", "boom");
    return new Fixture(t, shard0);
  }

  private record Fixture(Task task, long shardId) {}

  private boolean deadLetterFlag(long sid) {
    return Boolean.TRUE.equals(jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=?", Boolean.class, sid));
  }

  private long dueOutcome(long sid) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='DUE'",
        Long.class, sid);
  }

  /** 预算 W 已耗尽(即便 maxRetries 有余)→ 直接死信,不重试。 */
  @Test void budgetExhausted_goesDeadLetter_evenWithRetriesLeft() {
    Fixture fx = seed(0L, 3);
    jdbc.update("UPDATE execution_shard SET retry_budget_until=now() - interval '1 second' WHERE id=?",
        fx.shardId);

    resolver().handle(fx.task, fx.shardId, 1, "boom");

    assertEquals(ExecutionStatus.FAILED, shardRepo.findShard(fx.shardId).orElseThrow().status());
    assertTrue(deadLetterFlag(fx.shardId), "预算 W 耗尽 → 即便 maxRetries 有余也转死信");
    assertEquals(0L, dueOutcome(fx.shardId), "预算耗尽 → 无 DUE(不重试)");
  }

  /** 预算未耗尽 → 按 maxRetries 重试(现状回归)。 */
  @Test void budgetNotExhausted_retriesByCount() {
    Fixture fx = seed(300_000L, 3);

    resolver().handle(fx.task, fx.shardId, 1, "boom");

    assertEquals(ExecutionStatus.DUE, shardRepo.findShard(fx.shardId).orElseThrow().status(),
        "窗口未耗尽 → 按次数重试");
    assertFalse(deadLetterFlag(fx.shardId));
    assertEquals(1L, dueOutcome(fx.shardId));
  }

  /** 未设 W → 恒按次数重试(现状回归)。 */
  @Test void noBudget_retriesByCount_unchanged() {
    Fixture fx = seed(null, 3);

    resolver().handle(fx.task, fx.shardId, 1, "boom");

    assertEquals(ExecutionStatus.DUE, shardRepo.findShard(fx.shardId).orElseThrow().status(),
        "无 W → 现状按次数重试");
    assertFalse(deadLetterFlag(fx.shardId));
  }

  /** attempt 超 maxRetries → 死信(预算无关的既有分支)。 */
  @Test void maxRetriesExceeded_goesDeadLetter() {
    Fixture fx = seed(null, 1);

    resolver().handle(fx.task, fx.shardId, 2, "boom");

    assertTrue(deadLetterFlag(fx.shardId), "attempt 超 maxRetries → 死信");
  }
}
```

- [ ] **Step 5: 运行验证通过**

Run: `mvn -o -pl scheduler-persistence -am test -Dtest=JdbcShardRepositoryTest,FailureResolverTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(含新增预算/claim 测试 + 既有全量)。

- [ ] **Step 6: 提交**

```bash
git add scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/FailureResolver.java scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java scheduler-persistence/src/test/java/dev/scheduler/persistence/retry/FailureResolverTest.java
git commit -m "feat(persistence): 整轮重试预算 W — scheduleRetry 置窗 + retryBudgetExhausted + FailureResolver 预算分支;claim 每轮重置 started_at"
```

---

### Task 5: 分布式执行超时(findOverRuntime + Reconciler 超时分支)

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java`(新增 `findOverRuntime`)
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java`(impl)
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/reconcile/Reconciler.java:40-58`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java`、`scheduler-server/src/test/java/dev/scheduler/server/reconcile/ReconcilerTest.java`

**Interfaces:**
- Consumes: `Task.timeoutSeconds`(既有)、`FailureResolver`(Task 4 起含预算分支)、`markStatus`/`ExpiredShard`。
- Produces: `List<ExpiredShard> ShardRepository.findOverRuntime(long taskId, int timeoutSeconds)`。

- [ ] **Step 1: 写失败的持久层测试**

`JdbcShardRepositoryTest` 加:

```java
  @Test void findOverRuntime_returnsOnlyClaimedLongRunningShards() {
    long taskId = newTask(4, 8);
    long parentId = seedParentAndShards(taskId, 3);
    long overId = shard(parentId, 0).id();
    long freshId = shard(parentId, 1).id();
    long unclaimedId = shard(parentId, 2).id();
    // 超时:已认领(started_at 拨旧 10 分钟)
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w1',"
        + " started_at=now() - interval '10 minutes' WHERE id=?", overId);
    // 未超时:已认领但 started_at 新鲜
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w1',"
        + " started_at=now() WHERE id=?", freshId);
    // 未认领(worker_id null)的异常 RUNNING 行不参与超时
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=NULL,"
        + " started_at=now() - interval '10 minutes' WHERE id=?", unclaimedId);

    var expired = shardRepo.findOverRuntime(taskId, 60);

    assertEquals(1, expired.size(), "仅已认领且超时的分片进入超时判定");
    assertEquals(overId, expired.get(0).id());
  }
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn -o -pl scheduler-persistence -am test -Dtest=JdbcShardRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `ShardRepository` 无 `findOverRuntime` 符号。

- [ ] **Step 3: 实现 findOverRuntime**

`ShardRepository.java`:

```java
  /** 超时的 RUNNING shard(生命周期网关):仅当分片已认领(worker_id 非空)且 started_at 距今超过 timeoutSeconds。
   *  返回对账所需的 id 与 attempt。 */
  List<ExpiredShard> findOverRuntime(long taskId, int timeoutSeconds);
```

`JdbcShardRepository.java`:

```java
  @Override public List<ExpiredShard> findOverRuntime(long taskId, int timeoutSeconds) {
    return jdbc.query(
        "SELECT s.id, s.attempt FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
            + " WHERE e.task_id=? AND s.status='RUNNING'"
            + " AND s.worker_id IS NOT NULL"            // 已认领才算运行(未认领异常行不参与超时)
            + " AND now() - s.started_at > make_interval(secs => ?)",
        (rs, i) -> new ExpiredShard(rs.getLong("id"), rs.getInt("attempt")),
        taskId, (double) timeoutSeconds);
  }
```

- [ ] **Step 4: 运行验证通过**

Run: `mvn -o -pl scheduler-persistence -am test -Dtest=JdbcShardRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(含新增 findOverRuntime 测试)。

- [ ] **Step 5: 写失败的 Reconciler 测试**

`ReconcilerTest` 加 helper + 4 个超时用例:

```java
  /** §3 timeoutSeconds>0 的任务。 */
  private long createTaskTimed(int timeoutSeconds) {
    return tasks.create(new Task(null, "t", "cron", "demo", "0 */5 * * * *",
        1, timeoutSeconds, 0, 1000, null, 5, true, false)).id();
  }

  /** §3 分布式超时:设了 timeoutSeconds>0,RUNNING 且 started_at 超限 → 被铁 FAILED + runtime timeout。 */
  @Test void shardOverRuntime_isReclaimedAsFailed() {
    long taskId = createTaskTimed(3);
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w',"
        + " lease_until=now()+interval '60 seconds', attempt=1,"
        + " started_at=now() - interval '10 minutes' WHERE id=?", sid);

    assertEquals(1, reconciler().scanOnce(), "超时 → 回收");
    assertEquals(ExecutionStatus.FAILED, shards.findShard(sid).orElseThrow().status());
    assertEquals(1L, shardOutcomeCount(sid, "FAILED"));
    Long rt = jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='FAILED' AND detail='runtime timeout'",
        Long.class, sid);
    assertEquals(1L, rt, "failure detail = runtime timeout");
  }

  /** §3 对照:timeoutSeconds=0(不超时)即便 started_at 极旧也不被铁。 */
  @Test void timeoutZero_shardNeverTimedOut() {
    long taskId = createTaskTimed(0);
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w',"
        + " lease_until=now()+interval '60 seconds', attempt=1,"
        + " started_at=now() - interval '10 minutes' WHERE id=?", sid);

    assertEquals(0, reconciler().scanOnce());
    assertEquals(ExecutionStatus.RUNNING, shards.findShard(sid).orElseThrow().status());
    assertEquals(0L, shardOutcomeCount(sid, "FAILED"));
  }

  /** §3:未认领(worker_id NULL)的异常 RUNNING 行不参与超时判定。 */
  @Test void unclaimedRunningShard_notTimedOut() {
    long taskId = createTaskTimed(3);
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=NULL,"
        + " lease_until=now()+interval '60 seconds',"
        + " started_at=now() - interval '10 minutes' WHERE id=?", sid);

    assertEquals(0, reconciler().scanOnce());
    assertEquals(ExecutionStatus.RUNNING, shards.findShard(sid).orElseThrow().status());
  }

  /** §3:worker 心跳仍新鲜但已超时 → 仍被铁(超时与活性两分支互不干扰,都汇入统一 fail 路径)。 */
  @Test void liveButOverRuntime_shardStillTimedOut() {
    long taskId = createTaskTimed(3);
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    String owner = "w-live";
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=?,"
        + " lease_until=now()+interval '60 seconds', attempt=1,"
        + " started_at=now() - interval '10 minutes' WHERE id=?", owner, sid);
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status)"
        + " VALUES (?, '', now() - interval '5 seconds', 'ALIVE')", owner);

    assertEquals(1, reconciler().scanOnce(), "活 worker 但超时 → 仍铁 FAILED");
    assertEquals(ExecutionStatus.FAILED, shards.findShard(sid).orElseThrow().status());
  }
```

- [ ] **Step 6: 运行验证失败**

Run: `mvn -o -pl scheduler-server -am test -Dtest=ReconcilerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `shardOverRuntime_isReclaimedAsFailed` 断言 scan 返回 1 却得 0(Reconciler 尚无超时分支)。

- [ ] **Step 7: 实现 Reconciler 超时分支**

`Reconciler.java` `scanOnce` 的逐任务循环内、`findExpiredRunning` 回收段之后追加:

```java
    for (Task task : tasks.findAll()) {
      for (ExpiredShard run : shards.findExpiredRunning(task.id(), staleAfterSeconds)) {
        try {
          if (shards.markStatus(run.id(), ExecutionStatus.FAILED, workerId,
                  "owner unresponsive or lease expired")) {
            failureResolver.handle(task, run.id(), run.attempt(),
                "owner unresponsive or lease expired");
            reclaimed++;
          }
        } catch (IllegalStateException alreadyMovedOn) {
          // 快照后该行已被 owner 抢先完成(或已非法迁移)→ 已迁移,跳过这一行,不中止整批回收。
        }
      }
      // §3 分布式执行超时:timeoutSeconds>0 的任务,已认领且运行超限的 RUNNING 分片走同一条失败路径
      // (重试/死信/FAIL_FAST)。worker 迟到写回由 owner 归属守卫拒(worker_id 已被改写)→ 硬超时语义。
      // 与租约/活性回收互不干扰、同汇入 fail 路径;同 trigger 片二次查或已 FAILED 时由 canTransition 抛
      // IllegalStateException 被上方 catch 结构吞掉(这里独立 catch,与活性分支同口径)。
      if (task.timeoutSeconds() > 0) {
        for (ExpiredShard run : shards.findOverRuntime(task.id(), task.timeoutSeconds())) {
          try {
            if (shards.markStatus(run.id(), ExecutionStatus.FAILED, workerId, "runtime timeout")) {
              failureResolver.handle(task, run.id(), run.attempt(), "runtime timeout");
              reclaimed++;
            }
          } catch (IllegalStateException alreadyMovedOn) {
            // 快照后已终态/已迁移 → 跳过,不中止整批。
          }
        }
      }
    }
```

- [ ] **Step 8: 运行验证通过**

Run: `mvn -o -pl scheduler-server -am test -Dtest=ReconcilerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(4 个超时用例 + 既有全量)——既有任务 timeoutSeconds=300 且种子 RUNNING 行 started_at 为 NULL(`now()-NULL` 为 NULL,`NULL > interval` 为 false)故不被误收,回归安全。

- [ ] **Step 9: 提交**

```bash
git add scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java scheduler-server/src/main/java/dev/scheduler/server/reconcile/Reconciler.java scheduler-server/src/test/java/dev/scheduler/server/reconcile/ReconcilerTest.java
git commit -m "feat(runtime): 分布式执行超时 — findOverRuntime + Reconciler 超时分支(reuse timeout_seconds)"
```

---

### Task 6: 全量构建 + 回归验证

**Files:** 无代码变更(验证任务)。

- [ ] **Step 1: 全仓库编译 + 全量测试**

Run: `mvn -o -T1C install -DskipTests`
Expected: BUILD SUCCESS(全部 6 模块编译)。

Run: `mvn -o test`
Expected: BUILD SUCCESS,全部既有 + 新增测试绿(重试策略化 + 超时纵深回归)。

- [ ] **Step 2: 核对 spec 测试计划覆盖**

- RetryPolicy 三模式/cap/兜底/溢出 → Task 3 ✅
- 持久层 retryBudgetExhausted + claim started_at 重置 → Task 4 ✅
- Reconciler 超时(timeoutSeconds>0 铁 FAILED、0 不铁、活 worker 超时铁、worker_id null 不收)→ Task 5 ✅
- FailureResolver 预算耗尽死信 + 未设 W 回归 → Task 4 ✅
- 配置回归(老任务默认行为不变)→ Task 6 全量绿 ✅

- [ ] **Step 3: 提交(如有残留)**

```bash
git status --porcelain
```

确认工作树清洁;无新文件则此步 no-op。

---

## Self-Review

- **Spec coverage:** §1 任务配置(retry_mode/cap/budget 新列 + DTO)→ Task 1/2;timeout_seconds 复用强制执行 → Task 5。§2 重试引擎(三模式+封顶 → Task 3;整轮预算 W 的 `retry_budget_until` 置窗 + `retryBudgetExhausted` + FailureResolver 预算分支 → Task 4)。§3 分布式超时(claim 重置 started_at → Task 4;findOverRuntime + Reconciler → Task 5)。防双跑/DB now/attempt 语义 → 全任务采用既有 CAS + 骨架,无新增机制。
- **Placeholder scan:** 每步含可运行代码 + 确切文件/行;测试断言具体(值断言、非空断言)。
- **Type consistency:** `delayMs(Task,int)` 在 Task 3 连同唯一调用点 FailureResolver 一并改;`scheduleRetry(long,Instant,Long,String)` 在 Task 4 连同 FailureResolver + JdbcShardRepositoryTest 一并改;`retryBudgetExhausted(long)`/`findOverRuntime(long,int)` 签名跨任务一致。Task record 13 参便捷构造保持既有 13 参构造点(Persistence 测试、DagEngineTest/TriggerEngineTest/ReconcilerTest/ExecutorWorkerTest、TaskController create/update)原样编译。
- **已知取舍(记 ledger,非缺陷):** 既有 ReconcilerTest 种子 RUNNING 行 started_at 为 NULL,`now()-NULL`=NULL → 超时查询不命中,故 timeoutSeconds=300 的既有用例不被误收(靠 NULL 隔离,非靠显式禁用超时)。此为隔离低风险,未新增显式 `timeoutSeconds=0` 的既有用例改造。
- **次要裁决(逐条记录 ledger):** COALESCE 兜 `retry_mode` 缺省;`deadLetter` 死信小方法抽取复用;超时 detail 统一 `"runtime timeout"`;预算窗自首次失败起算(重试间不续)。