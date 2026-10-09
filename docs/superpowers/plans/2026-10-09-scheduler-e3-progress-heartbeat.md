# Scheduler E3 — progress-heartbeat 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 handler 一个显式 `touch()` 心跳,落成 `execution_shard.last_progress_at`,并把 stuck-alive 判定从「距启动多久」改成「距最近推进多久」,从而区分「慢而健康」与「真卡住」。

**Architecture:** 纯观测信号、零新线程。handler 经新增 `HandlerContext.touch()` → 置位 `ProgressSignal`(AtomicBoolean);worker 既有续租线程每次 `renewLease` 成功后 `consume()` 并合并 flush 成一次 `recordProgress`(DB `now()` 盖章,attempt+所有权栅栏)。stuck 谓词改成渐进式:未 opt-in 任务退化为 E2B 现状,opt-in 任务改按 `COALESCE(last_progress_at, started_at)` 判定。任务新增可空字段 `progress_timeout_seconds`。

**Tech Stack:** Java 21, Spring Boot 3.2.5, PostgreSQL 16 (Spring JDBC + Flyway), React 19 + Vite 6 + Tailwind 4。零新增依赖,仅一个 Flyway 迁移 V32。

**Spec:** `docs/superpowers/specs/2026-10-09-scheduler-e3-progress-heartbeat-design.md`

## Global Constraints

- **单一时钟(DB):** `last_progress_at` 由 DB `now()` 盖章;JVM 只传「有没有推进」的布尔语义,不传时间戳(同 e2b `7d18230` 的 abandon 活性门纪律)。
- **不碰活性/超时/回收:** `touch()` 不改 `lease_until`/`worker.status`/`last_seen`,不给 run 续命,不触发任何自动回收。回收仍由 E1 活性门 + attempt 栅栏独家判定;force-abandon 门控仍只看 owner ALIVE。
- **零新告警规则:** `stuck-alive` 规则与阈值不变——`countStuckAliveRunning` 的谓词内部升级,AlertEngine 不感知。
- **可空退化:** `progress_timeout_seconds` 与 `last_progress_at` 皆可空;NULL/0 = 未启用 = 退化 E2B 现状,对既有任务/handler 零行为变化。
- **命名:** 新方法 `recordProgress` 对齐 `recordResultPayload`/`renewLease`;attempt 栅栏参数名 `expectedAttempt` 对齐 `renewLease`/`markStatusOwned`。
- **测试门:** 离线 `mvn -o`,server→`-pl scheduler-server -am`,persistence→`-pl scheduler-persistence -am`,worker→`-pl scheduler-worker -am`,`-Dsurefire.failIfNoSpecifiedTests=false`(root pom 已传 `api.version=1.40`)。前端 `npm run build` 常绿。persistence/server/worker 测试需 Docker;core 纯 JUnit。

---

### Task 1: V32 迁移 — 两列可空字段

**Files:**
- Create: `scheduler-persistence/src/main/resources/db/migration/V32__progress_heartbeat.sql`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/SchemaSmokeTest.java`

**Interfaces:**
- Produces: `execution_shard.last_progress_at timestamptz`(NULL = handler 从未 touch)、`app_task.progress_timeout_seconds int`(NULL/0 = 不启用 progress 监控)。Task 3/5/6 依赖这两列存在。

- [ ] **Step 1: 写迁移文件**

```sql
-- V32: E3 progress-heartbeat —— 区分「慢而健康」vs「真卡住」。
-- execution_shard.last_progress_at:handler 最近一次 touch() 落库的时间(NULL=从未 touch)。
--   由 DB now() 单一时钟盖章,每续租周期合并 flush 一次(见 ShardRepository.recordProgress)。
-- app_task.progress_timeout_seconds:进度陈旧预算(NULL/0=不启用 progress 监控,退化 E2B 现状)。
ALTER TABLE execution_shard ADD COLUMN last_progress_at timestamptz;
ALTER TABLE app_task       ADD COLUMN progress_timeout_seconds int;
```

- [ ] **Step 2: 加 SchemaSmokeTest 断言(先写失败测试)**

在 `SchemaSmokeTest` 类内新增(复用既有 `jdbc()` 辅助):

```java
  @Test void progressHeartbeatColumnsExist() {
    var jdbc = jdbc();
    assertEquals("timestamp with time zone",
        jdbc.queryForObject("SELECT data_type FROM information_schema.columns"
            + " WHERE table_schema='public' AND table_name='execution_shard' AND column_name='last_progress_at'",
            String.class));
    assertEquals("integer",
        jdbc.queryForObject("SELECT data_type FROM information_schema.columns"
            + " WHERE table_schema='public' AND table_name='app_task' AND column_name='progress_timeout_seconds'",
            String.class));
  }
```

- [ ] **Step 3: 跑测试确认失败(未建列前)**

Run: `mvn -o test -pl scheduler-persistence -am -Dtest=SchemaSmokeTest#progressHeartbeatColumnsExist -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL —— 查询 `last_progress_at` 列不存在(No column / empty result)。

(注:若 Step 1 已建迁移后再跑,则直接 PASS;先建文件再跑亦属正常,因 Flyway 迁移在建库时已执行。此处不必强造失败——迁移本身就是 fixtures 的源头。可按序:先写迁移、再跑通 SchemaSmokeTest 全类。)

- [ ] **Step 4: 跑通 SchemaSmokeTest 全类**

Run: `mvn -o test -pl scheduler-persistence -am -Dtest=SchemaSmokeTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(含既有 `tablesExist`/`reliabilityColumnsExist` 等)。

- [ ] **Step 5: Commit**

```bash
git add scheduler-persistence/src/main/resources/db/migration/V32__progress_heartbeat.sql \
        scheduler-persistence/src/test/java/dev/scheduler/persistence/SchemaSmokeTest.java
git commit -m "feat(e3): V32 migration — last_progress_at + progress_timeout_seconds (nullable)"
```

---

### Task 2: Task 领域模型 + 持久化 (progress_timeout_seconds)

**Files:**
- Modify: `scheduler-core/src/main/java/dev/scheduler/core/Task.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcTaskRepository.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcTaskRepositoryTest.java`

**Interfaces:**
- Consumes: V32 迁移的 `app_task.progress_timeout_seconds` 列(Task 1)。
- Produces: `Task` record 新增末位组件 `Integer progressTimeoutSeconds`(可空;NULL = 未启用)。Task 5(TaskController 落库字段)、Task 6 不受类型影响。

- [ ] **Step 1: 扩展 Task record(末位追加,保既有 helper 构造)**

`Task.java` 三处编辑:

1) 规范构造头部(第 15-16 行)末尾追加:

```java
    String timezone, Integer intervalSeconds, List<String> eventRoutes,
    String successPolicyType, Integer successPolicyValue,
    Integer progressTimeoutSeconds) {
```

2) 15 参 helper(第 40 行 body)末尾追加一个 `null`:

```java
        retryMode, retryCapMs, retryBudgetMs, "UTC", null, List.of(), "NONE", null, null);
```

3) 18 参 helper(第 52 行 body)末尾追加一个 `null`:

```java
        retryMode, retryCapMs, retryBudgetMs, timezone, intervalSeconds, List.of(), "NONE", null, null);
```

(注:域内不校验 progressTimeoutSeconds 的负值——与 `timeoutSeconds` 一致,负值校验归 TaskController `checkDefinition`,见 Task 5。NULL 是合法值。)

- [ ] **Step 2: JdbcTaskRepository —— MAP / create / update 三处列**

`JdbcTaskRepository.java`:

1) `MAP` RowMapper(第 27 行)末位追加:

```java
      rs.getString("success_policy_type"), (Integer) rs.getObject("success_policy_value"),
      (Integer) rs.getObject("progress_timeout_seconds"));
```

2) `create` INSERT(第 57-74 行):列清单末尾加 `progress_timeout_seconds`,`VALUES` 加一个 `?`,绑定序号顺延:

```java
    jdbc.update(con -> {
      var ps = con.prepareStatement("""
        INSERT INTO app_task (name, kind, handler_ref, cron, shard_count, timeout_seconds,
                              max_retries, backoff_ms, retryable_failure_pattern, max_active_concurrent,
                              retry_mode, retry_cap_ms, retry_budget_ms, timezone, interval_seconds,
                              event_routes, success_policy_type, success_policy_value, progress_timeout_seconds)
        VALUES (?,?,?,?,?,?,?,?,?,?,COALESCE(?, 'exponential'),?,?,?,?,?,?,?,?)""",
          new String[]{"id"});
      ps.setString(1, t.name()); ps.setString(2, t.kind()); ps.setString(3, t.handlerRef());
      ps.setString(4, t.cron()); ps.setInt(5, t.shardCount()); ps.setInt(6, t.timeoutSeconds());
      ps.setInt(7, t.maxRetries()); ps.setLong(8, t.backoffMs());
      ps.setString(9, t.retryableFailurePattern()); ps.setInt(10, t.maxActiveConcurrent());
      ps.setString(11, t.retryMode());
      ps.setObject(12, t.retryCapMs(), java.sql.Types.BIGINT);
      ps.setObject(13, t.retryBudgetMs(), java.sql.Types.BIGINT);
      ps.setString(14, t.timezone());
      ps.setObject(15, t.intervalSeconds(), java.sql.Types.INTEGER);
      ps.setArray(16, stringsToArray(con, t.eventRoutes()));
      ps.setString(17, t.successPolicyType());
      ps.setObject(18, t.successPolicyValue(), java.sql.Types.INTEGER);
      ps.setObject(19, t.progressTimeoutSeconds(), java.sql.Types.INTEGER);
      return ps;
    }, kh);
```

3) `create` 返回值(第 77-81 行)末尾追加:

```java
        t.eventRoutes(), t.successPolicyType(), t.successPolicyValue(),
        t.progressTimeoutSeconds());
```

4) `update` UPDATE(第 122-144 行):SET 清单 `success_policy_value=?` 后加 `progress_timeout_seconds=?`(在 `updated_at=now()` 之前),`WHERE id=?` 序号从 20 顺延到 21:

```java
    int rows = jdbc.update(con -> {
      var ps = con.prepareStatement("""
        UPDATE app_task SET name=?, kind=?, handler_ref=?, cron=?, shard_count=?,
               timeout_seconds=?, max_retries=?, backoff_ms=?,
               retryable_failure_pattern=?, max_active_concurrent=?, paused=?,
               retry_mode=COALESCE(?, 'exponential'), retry_cap_ms=?, retry_budget_ms=?,
               timezone=?, interval_seconds=?, event_routes=?,
               success_policy_type=?, success_policy_value=?, progress_timeout_seconds=?, updated_at=now()
        WHERE id=?""");
      ps.setString(1, t.name()); ps.setString(2, t.kind()); ps.setString(3, t.handlerRef());
      ps.setString(4, t.cron()); ps.setInt(5, t.shardCount()); ps.setInt(6, t.timeoutSeconds());
      ps.setInt(7, t.maxRetries()); ps.setLong(8, t.backoffMs());
      ps.setString(9, t.retryableFailurePattern()); ps.setInt(10, t.maxActiveConcurrent());
      ps.setBoolean(11, t.paused()); ps.setString(12, t.retryMode());
      ps.setObject(13, t.retryCapMs(), java.sql.Types.BIGINT);
      ps.setObject(14, t.retryBudgetMs(), java.sql.Types.BIGINT);
      ps.setString(15, t.timezone());
      ps.setObject(16, t.intervalSeconds(), java.sql.Types.INTEGER);
      ps.setArray(17, stringsToArray(con, t.eventRoutes()));
      ps.setString(18, t.successPolicyType());
      ps.setObject(19, t.successPolicyValue(), java.sql.Types.INTEGER);
      ps.setObject(20, t.progressTimeoutSeconds(), java.sql.Types.INTEGER);
      ps.setLong(21, id);
      return ps;
    });
```

(注意:上面的 `create`/`update` 都用了 `con` 变量名与既有 `stringsToArray`——只是把既有两个 SQL 字符串与绑定序号整体替换,编号必须逐一对齐。)

- [ ] **Step 3: 写 round-trip 测试(先写失败测试)**

`JdbcTaskRepositoryTest.java` 新增(规范构造 22 参,顺序见 Task record 头部):

```java
  /** E3:progress_timeout_seconds 落库回读;NULL(未启用)缺省。 */
  @Test void roundTrip_progressTimeoutSeconds() {
    var repo = new JdbcTaskRepository(jdbc);
    Task created = repo.create(new Task(null, "pt", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false, null, null, null,
        "UTC", null, List.of(), "NONE", null, 900));
    assertEquals(900, created.progressTimeoutSeconds(), "create 应带 progress_timeout_seconds");
    Task cur = repo.findById(created.id()).orElseThrow();
    assertEquals(900, cur.progressTimeoutSeconds(), "progress_timeout_seconds 落库回读");
    Task plain = repo.create(new Task(null, "pt-null", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    assertNull(repo.findById(plain.id()).orElseThrow().progressTimeoutSeconds(),
        "未 opt-in → progress_timeout_seconds 回读 NULL");
  }
```

- [ ] **Step 4: 跑测试确认失败**

Run: `mvn -o test -pl scheduler-persistence -am -Dtest=JdbcTaskRepositoryTest#roundTrip_progressTimeoutSeconds -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败(`Task` 无 `progressTimeoutSeconds` 组件)或断言失败。

- [ ] **Step 5: 跑通全类**

Run: `mvn -o test -pl scheduler-persistence -am -Dtest=JdbcTaskRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(既有 `roundTrip`/`create_persistsTimezoneAndIntervalTrigger`/`update` 等保持绿——helper 构造未改语义)。

- [ ] **Step 6: Commit**

```bash
git add scheduler-core/src/main/java/dev/scheduler/core/Task.java \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcTaskRepository.java \
        scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcTaskRepositoryTest.java
git commit -m "feat(e3): Task gains nullable progressTimeoutSeconds (domain + JDBC upsert)"
```

---

### Task 3: recordProgress + StuckShard.lastProgressAt + 渐进谓词 (persistence)

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java`

**Interfaces:**
- Consumes: V32 两列(Task 1)。
- Produces:
  - `boolean recordProgress(long shardId, String ownerWorkerId, int expectedAttempt)` —— DB `now()` 盖章,ownership + attempt 栅栏镜像 `renewLease`;0 行静默返回 false。
  - `StuckShard(long id, long executionId, int shardIndex, String workerId, int attempt, Instant startedAt, long timeoutSeconds, Instant lastProgressAt)` —— 末位新增 `lastProgressAt`(可空)。
  - `findStuckAliveRunning(Long,int)` / `countStuckAliveRunning(int)` 谓词按 spec §3 渐进式重写。Task 4(worker flush)依赖 `recordProgress`;Task 6(StuckView)依赖 `StuckShard.lastProgressAt`。

- [ ] **Step 1: ShardRepository 接口 —— recordProgress + StuckShard 末位字段**

`ShardRepository.java`:

1) `StuckShard` record(第 74-75 行)追加 `Instant lastProgressAt`:

```java
  record StuckShard(long id, long executionId, int shardIndex, String workerId, int attempt,
                   Instant startedAt, long timeoutSeconds, Instant lastProgressAt) {}
```

2) 在 `renewLease`(第 57 行)之后插入新方法签名(注释镜像 renewLease 的栅栏语义):

```java
  /** E3 进度落库:DB now() 盖章,所有权 + attempt 栅栏(镜像 renewLease);0 行 = 已被接管/重认领,静默。
   *  无 outcome 行。 */
  boolean recordProgress(long shardId, String ownerWorkerId, int expectedAttempt);
```

- [ ] **Step 2: JdbcShardRepository —— recordProgress 实现**

在 `renewLease` 方法(第 228 行)之后插入:

```java
  @Override public boolean recordProgress(long shardId, String ownerWorkerId, int expectedAttempt) {
    // E3 进度落库(镜像 renewLease 每-attempt 栅栏):DB now() 盖章(单一时钟,JVM 不传时间戳)。
    // 仅当仍归本 worker(RUNNING + worker_id 匹配)且 attempt 未变才置 last_progress_at;被接管/重认领则 0 行静默。
    return jdbc.update("""
        UPDATE execution_shard SET last_progress_at = now()
         WHERE id=? AND status='RUNNING' AND worker_id=? AND attempt=?""",
        shardId, ownerWorkerId, expectedAttempt) == 1;
  }
```

- [ ] **Step 3: JdbcShardRepository —— 渐进谓词重写 findStuckAliveRunning**

整段替换现有 `findStuckAliveRunning`(第 260-277 行):

```java
  @Override public List<ShardRepository.StuckShard> findStuckAliveRunning(Long taskId, int staleAfterSeconds) {
    // E3 渐进谓词(spec §3):未 opt-in → 复用 E2B timeout/started_at;opt-in(progress_timeout>0)→ 改按 COALESCE(last_progress_at, started_at)。
    String base = "SELECT s.id, s.execution_id, s.shard_index, s.worker_id, s.attempt, s.started_at,"
        + " t.timeout_seconds, s.last_progress_at"
        + " FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
        + " JOIN app_task t ON t.id = e.task_id";
    String cond = " s.status='RUNNING' AND s.worker_id IS NOT NULL"
        + " AND ("
        + "   (t.progress_timeout_seconds IS NULL OR t.progress_timeout_seconds = 0)"
        + "     AND t.timeout_seconds > 0"
        + "     AND now() - s.started_at > make_interval(secs => t.timeout_seconds::double precision)"
        + "   OR"
        + "   (t.progress_timeout_seconds > 0)"
        + "     AND now() - COALESCE(s.last_progress_at, s.started_at)"
        + "           > make_interval(secs => t.progress_timeout_seconds::double precision)"
        + " )"
        + " AND EXISTS (SELECT 1 FROM worker w"
        + "             WHERE w.id = s.worker_id AND w.status = 'ALIVE'"
        + "             AND w.last_seen >= now() - make_interval(secs => ?))";
    RowMapper<ShardRepository.StuckShard> map = (rs, i) -> new ShardRepository.StuckShard(
        rs.getLong("id"), rs.getLong("execution_id"), rs.getInt("shard_index"),
        rs.getString("worker_id"), rs.getInt("attempt"),
        rs.getTimestamp("started_at").toInstant(), rs.getLong("timeout_seconds"),
        rs.getTimestamp("last_progress_at") != null ? rs.getTimestamp("last_progress_at").toInstant() : null);
    if (taskId != null) {
      return jdbc.query(base + " WHERE e.task_id = ? AND" + cond, map, taskId, (double) staleAfterSeconds);
    }
    return jdbc.query(base + " WHERE" + cond, map, (double) staleAfterSeconds);
  }
```

- [ ] **Step 4: JdbcShardRepository —— 渐进谓词重写 countStuckAliveRunning**

整段替换现有 `countStuckAliveRunning`(第 279-291 行):

```java
  @Override public long countStuckAliveRunning(int staleAfterSeconds) {
    Long n = jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
            + " JOIN app_task t ON t.id = e.task_id"
            + " WHERE s.status='RUNNING' AND s.worker_id IS NOT NULL"
            + " AND ("
            + "   (t.progress_timeout_seconds IS NULL OR t.progress_timeout_seconds = 0)"
            + "     AND t.timeout_seconds > 0"
            + "     AND now() - s.started_at > make_interval(secs => t.timeout_seconds::double precision)"
            + "   OR"
            + "   (t.progress_timeout_seconds > 0)"
            + "     AND now() - COALESCE(s.last_progress_at, s.started_at)"
            + "           > make_interval(secs => t.progress_timeout_seconds::double precision)"
            + " )"
            + " AND EXISTS (SELECT 1 FROM worker w"
            + "             WHERE w.id = s.worker_id AND w.status = 'ALIVE'"
            + "             AND w.last_seen >= now() - make_interval(secs => ?))",
        Long.class, (double) staleAfterSeconds);
    return n == null ? 0L : n;
  }
```

- [ ] **Step 5: 写测试(先写失败测试)**

`JdbcShardRepositoryTest.java` 新增(E2B stuck 测试区末尾,第 1093 行后,复用既有 `newTask`/`seedParentAndShards`/`shard`/`claimShard`/`seedWorker`)。新增 `seedProgressTimeout` helper 与 4 个用例:

```java
  private void seedProgressTimeout(long taskId, int seconds) {
    jdbc.update("UPDATE app_task SET progress_timeout_seconds=? WHERE id=?", seconds, taskId);
  }

  /** opt-in + touch 新鲜 → 不卡(分支 B 接管:即便 timeout_seconds 已超,progress 预算未超)。 */
  @Test void findStuckAliveRunning_optIn_progressFresh_notStuck_evenIfTimeoutShort() {
    long taskId = newTask(1, 8);
    jdbc.update("UPDATE app_task SET progress_timeout_seconds=60, timeout_seconds=5 WHERE id=?", taskId);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w-opt-a',"
        + " started_at=now() - interval '10 minutes', last_progress_at=now() WHERE id=?", sid);
    seedWorker("w-opt-a", 5);

    assertTrue(shardRepo.findStuckAliveRunning(taskId, 30).stream().noneMatch(e -> e.id() == sid),
        "opt-in:距最近推进未超 progress 预算 → 不卡(timeout_seconds 分支 A 已被 B 接管)");
  }

  /** opt-in + 超预算且从未 touch(COALESCE 兜底 started_at)→ 列,lastProgressAt=null。 */
  @Test void findStuckAliveRunning_optIn_neverTouched_overBudget_isStuck() {
    long taskId = newTask(1, 8);
    seedProgressTimeout(taskId, 60);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w-opt-b',"
        + " started_at=now() - interval '10 minutes' WHERE id=?", sid);
    seedWorker("w-opt-b", 5);

    var stuck = shardRepo.findStuckAliveRunning(taskId, 30);
    assertEquals(1, stuck.size());
    assertEquals(sid, stuck.get(0).id());
    assertNull(stuck.get(0).lastProgressAt(), "从未 touch → lastProgressAt null");
  }

  /** opt-in + touch 陈旧(超 progress 预算)→ 列,并带出 lastProgressAt。 */
  @Test void findStuckAliveRunning_optIn_staleProgress_overBudget_isStuck_withLastProgressAt() {
    long taskId = newTask(1, 8);
    seedProgressTimeout(taskId, 60);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w-opt-c',"
        + " started_at=now() - interval '10 minutes', last_progress_at=now() - interval '5 minutes'"
        + " WHERE id=?", sid);
    seedWorker("w-opt-c", 5);

    var stuck = shardRepo.findStuckAliveRunning(taskId, 30);
    assertEquals(1, stuck.size());
    assertNotNull(stuck.get(0).lastProgressAt(), "carries lastProgressAt");
  }

  /** recordProgress 归属/attempt/终态三重栅栏(镜像 renewLease);DB now() 盖章。 */
  @Test void recordProgress_stampsNow_whileOwned_ignoresWhenLost() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    claimShard(sid, taskId, "w1", 8); // attempt -> 1
    assertTrue(shardRepo.recordProgress(sid, "w1", 1), "持有者 recordProgress 应成功");
    assertNotNull(jdbc.queryForObject(
        "SELECT last_progress_at FROM execution_shard WHERE id=?", java.sql.Timestamp.class, sid),
        "DB now() 盖章落 last_progress_at");
    assertFalse(shardRepo.recordProgress(sid, "w2", 1), "非持有者 0 行");
    assertFalse(shardRepo.recordProgress(sid, "w1", 2), "陈旧 attempt 栅栏");
    shardRepo.markStatusOwned(sid, ExecutionStatus.SUCCESS, "w1", 1, "ok");
    assertFalse(shardRepo.recordProgress(sid, "w1", 1), "终态后 recordProgress 0 行");
  }
```

(说明:`findStuckAliveRunning_aliveOwnerOverBudget_isReturned` 等 E2B 既有用例走分支 A(progress_timeout_seconds 为 NULL),谓词升级后语义一致,应保持绿;`countStuckAliveRunning_matchesList` 同样。)

- [ ] **Step 6: 跑测试确认失败**

Run: `mvn -o test -pl scheduler-persistence -am -Dtest=JdbcShardRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL —— 编译错误(`StuckShard` 无 `lastProgressAt`、`recordProgress` 不存在)或新用例 red。

- [ ] **Step 7: 跑通全类**

Run: 同 Step 6。
Expected: PASS(全部 E2B 既有 stuck 用例 + 新 4 用例绿)。

- [ ] **Step 8: Commit**

```bash
git add scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java \
        scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java
git commit -m "feat(e3): recordProgress (DB-stamped) + StuckShard.lastProgressAt + progressive stuck predicate"
```

---

### Task 4: ProgressSignal + HandlerContext.touch() + 合并 flush (worker)

**Files:**
- Create: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/ProgressSignal.java`
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/HandlerContext.java`
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/execute/ExecutorWorker.java`
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/SyncDemoHandler.java`(示范)
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/EchoHandler.java`(示范)
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/MyJobHandler.java`(示范)
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/DemoHandler.java`(示范)
- Test: `scheduler-worker/src/test/java/dev/scheduler/worker/handler/ProgressSignalTest.java`
- Test: `scheduler-worker/src/test/java/dev/scheduler/worker/handler/HandlerContextTest.java`
- Test: `scheduler-worker/src/test/java/dev/scheduler/worker/execute/ExecutorWorkerTest.java`

**Interfaces:**
- Consumes: `ShardRepository.recordProgress`(Task 3)。
- Produces:
  - `ProgressSignal`(`void touch()` 置位;`boolean consume()` 读并清零;AtomicBoolean)。
  - `HandlerContext` 新增第 9 组件 `ProgressSignal progress` + 实例方法 `void touch()`(`progress==null` no-op);`of(...)` 便捷构造照旧(progress=null)。
  - `ExecutorWorker` 续租线程在每次 `renewLease` 成功后 `progress.consume()` → `recordProgress`。

- [ ] **Step 1: 写 ProgressSignal + 单测(先写失败测试)**

`ProgressSignal.java`:

```java
package dev.scheduler.worker.handler;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * E3 进度心跳信号:handler 在自己的批处理循环边界周期调用 {@link #touch()} 置位;worker 的续租线程在每次
 * {@code renewLease} 成功后 {@link #consume()} 读并清零,合并 flush 成一次 {@code recordProgress}(至多每续租周期一次)。
 * 纯布尔语义——只表达「刚有过推进」,不携带时间戳/百分比/阶段。
 */
public final class ProgressSignal {
  private final AtomicBoolean progressed = new AtomicBoolean(false);

  /** handler 侧上报:置位「自上次 flush 后有过推进」。任意线程/任意频率调用皆安全(幂等 set)。 */
  public void touch() { progressed.set(true); }

  /** worker 侧采读:读并清零。返回是否存在未 flush 的推进信号。 */
  public boolean consume() { return progressed.getAndSet(false); }
}
```

`ProgressSignalTest.java`:

```java
package dev.scheduler.worker.handler;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class ProgressSignalTest {
  @Test void touchThenConsume_returnsTrueThenFalse() {
    var p = new ProgressSignal();
    assertFalse(p.consume(), "未 touch → false");
    p.touch();
    assertTrue(p.consume(), "touch 后首 consume → true");
    assertFalse(p.consume(), "consume 清零 → 再 consume false");
  }

  @Test void multipleTouches_singleConsumeCoalesces() {
    var p = new ProgressSignal();
    p.touch(); p.touch(); p.touch();
    assertTrue(p.consume(), "多次 touch 幂等合并");
    assertFalse(p.consume(), "已 flush → false");
  }
}
```

- [ ] **Step 2: 跑 ProgressSignalTest 确认失败(类尚不存在)**

Run: `mvn -o test -pl scheduler-worker -am -Dtest=ProgressSignalTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败(`ProgressSignal` 不存在)。

- [ ] **Step 3: HandlerContext 增组件 + touch()**

`HandlerContext.java` 整段替换:

```java
package dev.scheduler.worker.handler;

/** 一次分片运行的上下文:父 execution + 本分片坐标 + 父 args + 协作取消令牌 + E3 进度心跳。
 *  {@link #touch()} 把「仍在推进」上报给 worker 续租线程合并 flush({@code progress==null} 时 no-op)。 */
public record HandlerContext(long executionId, long shardId, int shardIndex, int shardCount,
                             String shardData, String args, String workerId, CancellationToken token,
                             ProgressSignal progress) {

  /** 便捷构造:默认未取消 token、无 workerId、无进度心跳,供手工/既有测试在不关心协作取消/进度时使用。 */
  public static HandlerContext of(long executionId, long shardId, int shardIndex, int shardCount,
                                  String shardData, String args) {
    return new HandlerContext(executionId, shardId, shardIndex, shardCount, shardData, args, null,
        new CancellationToken(false), null);
  }

  /** E3 进度上报:置位进度信号(重交由 worker 周期合并落库);progress==null 时 no-op。 */
  public void touch() {
    if (progress != null) progress.touch();
  }
}
```

- [ ] **Step 4: HandlerContextTest**

`HandlerContextTest.java`:

```java
package dev.scheduler.worker.handler;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class HandlerContextTest {
  @Test void touch_delegatesToProgress() {
    var p = new ProgressSignal();
    var ctx = new HandlerContext(1, 2, 0, 1, null, null, "w", new CancellationToken(false), p);
    ctx.touch();
    assertTrue(p.consume(), "touch 委托置位 progress");
  }

  @Test void touch_noProgress_isNoop() {
    var ctx = HandlerContext.of(1, 2, 0, 1, null, null); // progress=null
    ctx.touch(); // 不得 NPE
    assertNull(ctx.progress(), "of(...) 便捷构造的 progress 为 null");
  }
}
```

(注:第二条用 `ctx.progress() == null` 直接断言 `of(...)` 便捷构造的 progress 为 null 且 `touch()` 不抛;`of(...)` 语义既定。)

- [ ] **Step 5: ExecutorWorker 合并 flush**

`ExecutorWorker.java` 三处编辑:

1) import 追加:

```java
import dev.scheduler.worker.handler.ProgressSignal;
```

2) 续租调度前声明信号(在 `ScheduledFuture<?> renew = renewer.scheduleWithFixedDelay(...)` 之前,第 82 行前):

```java
      // E3 进度心跳:handler 在推进边界 touch() 置位;续租线程每 tick 合并 flush 一次 recordProgress。
      ProgressSignal progress = new ProgressSignal();
      ScheduledFuture<?> renew = renewer.scheduleWithFixedDelay(() -> {
        try {
          // B1 每-attempt 栅栏:...
          if (!shards.renewLease(shard.id(), workerId, shard.attempt() + 1, clock.instant().plusSeconds(leaseSeconds))) {
            org.slf4j.LoggerFactory.getLogger(ExecutorWorker.class)
                .warn("lease lost for shard {} while running; self-fencing (stale write-back will be suppressed)",
                    shard.id());
          } else if (progress.consume()) {
            // renewLease 成功(仍持有 + attempt 未变)后合并 flush;recordProgress 0 行(已失权)静默。
            shards.recordProgress(shard.id(), workerId, shard.attempt() + 1);
          }
        } catch (Throwable ex) {
          // ...
          org.slf4j.LoggerFactory.getLogger(ExecutorWorker.class)
              .warn("lease renew failed for shard {}", shard.id(), ex);
        }
      }, renewSeconds, renewSeconds, TimeUnit.SECONDS);
```

3) HandlerContext 构造注入 progress(原第 115-116 行):

```java
        HandlerContext ctx = new HandlerContext(shard.executionId(), shard.id(), shard.shardIndex(),
            parent.shardCount(), shard.shardData(), parent.args(), workerId, token, progress);
```

(注意:step 2 中 `catch` 前的注释保留原文;只把 `if` 分支扩成 `if/else if`。)

- [ ] **Step 6: 四个 handler 补 touch() 调用点(示范)**

`SyncDemoHandler.java`:`handle` 内,空表/越界守卫通过后、执行批量插入前(第 44 行 `Integer synced = ...` 之前)加一行:

```java
    ctx.touch(); // E3 上报:本分片即将执行一批同步(慢而健康时不误标 stuck);worker 续租线程合并 flush。
```

`EchoHandler.java` / `MyJobHandler.java` / `DemoHandler.java`:各自 `handle` 的取消检查之后、方法体末尾加一行(纯契约示范,本 handler 无批处理):

```java
    ctx.touch(); // E3 契约示范:推进边界上报(本 handler 即时返回,touch 对短跑无实际效果)。
```

- [ ] **Step 7: ExecutorWorkerTest 合并 flush 时序测试**

`ExecutorWorkerTest.java` 新增(复用 `createTask`/`seedParentAndShards`/`shard`/`shards`;用 `renewSeconds=1` 加速续租 tick):

```java
  /** E3 合并 flush:handler touch → 续租 tick(1s)合并 flush 落 last_progress_at(无 DB 写放大)。 */
  @Test void handlerTouch_flushesProgressOnRenewTick() throws Exception {
    long taskId = createTask("rec", 1, 1);
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var touchHandler = new ExecutionHandler() {
      @Override public String ref() { return "rec"; }
      @Override public void handle(HandlerContext ctx) {
        ctx.touch(); // 上报推进
        entered.countDown();
        try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      }
    };
    var registry = new MapHandlerRegistry(List.of(() -> touchHandler));
    ExecutorWorker w = new ExecutorWorker(tasks, shards, registry, "worker-a",
        new FailureResolver(shards, new RetryPolicy(), CLOCK), CLOCK, 120, 1); // renewSeconds=1

    Thread runner = new Thread(w::workOne, "test-touch");
    runner.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "handler 应已进入(已 touch)");

    java.sql.Timestamp progressAt = null;
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline && progressAt == null) {
      progressAt = jdbc.queryForObject(
          "SELECT last_progress_at FROM execution_shard WHERE id=?", java.sql.Timestamp.class, shardId);
      if (progressAt == null) Thread.sleep(100);
    }
    assertNotNull(progressAt, "handler touch → 续租 tick 合并 flush 落 last_progress_at");
    release.countDown();
    runner.join(10000);
    assertEquals(ExecutionStatus.SUCCESS, shards.findShard(shardId).orElseThrow().status(),
        "handler 正常返回后落 SUCCESS");
  }
```

- [ ] **Step 8: 跑 worker 全量相关测试**

Run: `mvn -o test -pl scheduler-worker -am -Dtest=ProgressSignalTest,HandlerContextTest,ExecutorWorkerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(含既有 `EchoHandlerTest`/`MyJobHandlerTest`——它们走 `HandlerContext.of(...)`、touch no-op,不破)。

- [ ] **Step 9: Commit**

```bash
git add scheduler-worker/src/main/java/dev/scheduler/worker/handler/ProgressSignal.java \
        scheduler-worker/src/main/java/dev/scheduler/worker/handler/HandlerContext.java \
        scheduler-worker/src/main/java/dev/scheduler/worker/execute/ExecutorWorker.java \
        scheduler-worker/src/main/java/dev/scheduler/worker/handler/SyncDemoHandler.java \
        scheduler-worker/src/main/java/dev/scheduler/worker/handler/EchoHandler.java \
        scheduler-worker/src/main/java/dev/scheduler/worker/handler/MyJobHandler.java \
        scheduler-worker/src/main/java/dev/scheduler/worker/handler/DemoHandler.java \
        scheduler-worker/src/test/java/dev/scheduler/worker/handler/ProgressSignalTest.java \
        scheduler-worker/src/test/java/dev/scheduler/worker/handler/HandlerContextTest.java \
        scheduler-worker/src/test/java/dev/scheduler/worker/execute/ExecutorWorkerTest.java
git commit -m "feat(e3): ProgressSignal + HandlerContext.touch() + renewer coalesced recordProgress flush"
```

---

### Task 5: TaskController 字段/校验/审计 (server)

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: `Task.progressTimeoutSeconds`(Task 2)。
- Produces: `CreateTaskRequest`/`UpdateTaskRequest` 新增 `Integer progressTimeoutSeconds`;`checkDefinition` 新增 `Integer progressTimeoutSeconds` 参数并校验 `>= 0`;create/update 构造 Task 时透传;push 进 `/api/v1/tasks` POST/PUT/GET 序列化与审计 meta/before/diff。

- [ ] **Step 1: 请求记录增字段**

`TaskController.java`,`CreateTaskRequest`(第 52-57 行)与 `UpdateTaskRequest`(第 59-64 行)末位各追加:

```java
      String successPolicyType, Integer successPolicyValue,
      Integer progressTimeoutSeconds) {}
```

- [ ] **Step 2: checkDefinition 增参数 + 校验**

签名(第 333-337 行)末位追加 `Integer progressTimeoutSeconds`,并在校验区(第 368 行 `timeoutSeconds ...` 附近)追加:

```java
    if (timeoutSeconds != null && timeoutSeconds < 0) throw new IllegalArgumentException("timeoutSeconds must be >= 0");
    if (progressTimeoutSeconds != null && progressTimeoutSeconds < 0) {
      throw new IllegalArgumentException("progressTimeoutSeconds must be >= 0");
    }
```

两个调用点(create 第 76-80 行、update 第 107-111 行)的 `checkDefinition(...)` 实参末位各追加 `req.progressTimeoutSeconds()`。

- [ ] **Step 3: create/update 构造 Task 透传**

`create`(第 81-92 行)末位追加 `req.progressTimeoutSeconds()`:

```java
        req.eventRoutes() == null ? List.of() : req.eventRoutes(),
        req.successPolicyType(), req.successPolicyValue(), req.progressTimeoutSeconds()));
```

`update`(第 112-129 行)末位追加(`req 为 null → 沿用 existing`):

```java
        req.successPolicyType() == null ? existing.successPolicyType() : req.successPolicyType(),
        req.successPolicyValue() == null ? existing.successPolicyValue() : req.successPolicyValue(),
        req.progressTimeoutSeconds() == null ? existing.progressTimeoutSeconds() : req.progressTimeoutSeconds());
```

- [ ] **Step 4: 审计 meta/before/diff 各加一字段**

`taskMeta`(第 244-264 行)加 `m.put("progressTimeoutSeconds", t.progressTimeoutSeconds());`;
`taskBefore`(第 267-288 行)加 `b.put("progressTimeoutSeconds", t.progressTimeoutSeconds());`;
`taskDiff`(第 291-314 行)加 `putDiff(d, "progressTimeoutSeconds", before.progressTimeoutSeconds(), after.progressTimeoutSeconds());`。

- [ ] **Step 5: 写集成测试(先写失败测试)**

`ApiIntegrationTest.java` 新增(复用既有 `objectMapper`、`mvc`、`session(...)`,POST 任务请求体带 `progressTimeoutSeconds`):

```java
  /** E3:POST/PUT /api/v1/tasks 透传 progress_timeout_seconds;负值 → 400。 */
  @Test void task_createAndValidate_progressTimeoutSeconds() throws Exception {
    String body = "{\"name\":\"e3-progress-task\",\"handlerRef\":\"demo\",\"cron\":\"0 */5 * * * *\","
        + "\"shardCount\":1,\"timeoutSeconds\":300,\"progressTimeoutSeconds\":900}";
    var created = mvc.perform(post("/api/v1/tasks").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.progressTimeoutSeconds").value(900));
    long id = objectMapper.readTree(created.andReturn().getResponse().getContentAsString()).get("id").asLong();

    // 负值 → 400(checkDefinition)
    mvc.perform(post("/api/v1/tasks").cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"bad-progress\",\"handlerRef\":\"demo\",\"cron\":\"0 */5 * * * *\"," +
                "\"shardCount\":1,\"progressTimeoutSeconds\":-1}"))
        .andExpect(status().isBadRequest());

    // PUT 覆写
    mvc.perform(put("/api/v1/tasks/" + id).cookie(session(ALICE_TOKEN))
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"e3-progress-task\",\"handlerRef\":\"demo\",\"cron\":\"0 */5 * * * *\"," +
                "\"shardCount\":1,\"progressTimeoutSeconds\":1800}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.progressTimeoutSeconds").value(1800));
  }
```

(注:需确认 `postTask`/`ALICE_TOKEN`/`session(...)` 命名与既有测试一致——本文件已在用 `ALICE_TOKEN`、`session(...)`、`post(...)`、`put(...)`、`MediaType.APPLICATION_JSON`。若 handlerRef 校验对 `demo` 无活体 worker 而 400,则可仿既有 `postTask(name)` 走真实注册路径,或改用 SQL 直接 UPDATE——以既有用例风格为准,保持请求体字段正确。)

- [ ] **Step 6: 跑测试确认失败**

Run: `mvn -o test -pl scheduler-server -am -Dtest=ApiIntegrationTest#task_createAndValidate_progressTimeoutSeconds -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL —— `CreateTaskRequest` 无 `progressTimeoutSeconds` 或甘认。

- [ ] **Step 7: 跑通 TaskController 相关(全类偏重)**

Run: `mvn -o test -pl scheduler-server -am -Dtest=ApiIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(既有 task CRUD 用例保持绿)。

- [ ] **Step 8: Commit**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java \
        scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java
git commit -m "feat(e3): task progressTimeoutSeconds field + validation + audit on /tasks"
```

---

### Task 6: StuckView 进度两列 (server)

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/ExecutionController.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: `StuckShard.lastProgressAt`(Task 3)。
- Produces: `StuckView` 新增 `Instant lastProgressAt` + `long progressAgeSeconds`;`GET /api/v1/executions/stuck` 响应新增两列(同 `ageSeconds` 并列,让运维区分「慢而健康」vs「真卡住」)。

- [ ] **Step 1: StuckView record 增两字段**

`ExecutionController.java`,`StuckView` record(第 54-56 行)替换:

```java
  public record StuckView(long shardId, long executionId, int shardIndex, String taskName,
                          String handlerRef, String workerId, int attempt, Instant startedAt,
                          long ageSeconds, long timeoutSeconds, Instant lastProgressAt,
                          long progressAgeSeconds) {}
```

- [ ] **Step 2: stuck() 映射补两列**

`stuck()`(第 176-182 行)映射体替换:

```java
    List<StuckView> all = ss.stream().map(x -> {
      TaskRef r = refs.get(x.id());
      long age = Duration.between(x.startedAt(), Instant.now()).getSeconds();
      Instant anchor = x.lastProgressAt() != null ? x.lastProgressAt() : x.startedAt();
      long progressAge = anchor == null ? age : Duration.between(anchor, Instant.now()).getSeconds();
      return new StuckView(x.id(), x.executionId(), x.shardIndex(),
          r == null ? null : r.name(), r == null ? null : r.handlerRef(),
          x.workerId(), x.attempt(), x.startedAt(), age, x.timeoutSeconds(),
          x.lastProgressAt(), progressAge);
    }).toList();
```

(注:`age` 与 `progressAge` 沿用既有 `Instant.now()` 展示口径——展示列非回收/观测权威,单一时钟纪律约束的是 DB 谓词,见 Global Constraints。)

- [ ] **Step 3: 写集成测试(先写失败测试)**

`ApiIntegrationTest.java` 新增(沿用 E2B `seedRunningShard`/SQL 直改):

```java
  /** E3:/stuck 响应带 lastProgressAt + progressAgeSeconds(未 touch → lastProgressAt null)。 */
  @Test void stuckView_optInCarriesProgressColumns() throws Exception {
    long id = postTask("stuck-progress-task");
    jdbc.update("UPDATE app_task SET progress_timeout_seconds=60 WHERE id=?", id);
    long shardId = seedRunningShard(id, "w-prog"); // started_at 10 min ago, last_progress_at NULL
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES ('w-prog','',now(),'ALIVE')");

    mvc.perform(get("/api/v1/executions/stuck").param("taskId", String.valueOf(id)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$.items[0].shardId").value((int) shardId))
        .andExpect(jsonPath("$.items[0].lastProgressAt").value(org.hamcrest.Matchers.nullValue()))
        .andExpect(jsonPath("$.items[0].progressAgeSeconds").isNumber());
  }
```

- [ ] **Step 4: 跑测试确认失败**

Run: `mvn -o test -pl scheduler-server -am -Dtest=ApiIntegrationTest#stuckView_optInCarriesProgressColumns -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL —— `StuckView` 无 `progressAgeSeconds`。

- [ ] **Step 5: 跑通 stuck 相关(全类偏重)**

Run: `mvn -o test -pl scheduler-server -am -Dtest=ApiIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(既有 `stuckView_listghsOnlyAliveOwnerOverBudget`/`abandon_*` 等保持绿)。

- [ ] **Step 6: Commit**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/web/ExecutionController.java \
        scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java
git commit -m "feat(e3): stuck view adds lastProgressAt + progressAgeSeconds columns"
```

---

### Task 7: web 前端 (types / 任务表单 / StuckPage)

**Files:**
- Modify: `web/src/api/types.ts`
- Modify: `web/src/pages/TasksPage.tsx`
- Modify: `web/src/pages/StuckPage.tsx`

**Interfaces:**
- Consumes: 后端 `Task.progressTimeoutSeconds`、`StuckView.lastProgressAt`/`progressAgeSeconds`(Task 5/6)。
- Produces: `Task` 类型 + 任务表单「进度预算」输入;`StuckRow` + StuckPage 两列。

- [ ] **Step 1: types.ts 增字段**

`types.ts`:

1) `Task`(第 1-22 行)加:

```ts
  successPolicyType: string | null;
  successPolicyValue: number | null;
  /** E3:进度陈旧预算秒(null/0 = 不启用 progress 监控)。 */
  progressTimeoutSeconds: number | null;
```

2) `CreateTaskRequest`(第 91-109 行)加:

```ts
  successPolicyType?: string;
  successPolicyValue?: number;
  /** E3:进度陈旧预算秒(非负;null/0 = 不启用)。 */
  progressTimeoutSeconds?: number | null;
```

3) `StuckRow`(第 69-80 行)加:

```ts
  ageSeconds: number;
  timeoutSeconds: number;
  /** E3:最近推进时间(null = 从未 touch)与进度年龄秒。 */
  lastProgressAt: string | null;
  progressAgeSeconds: number;
```

- [ ] **Step 2: TasksPage 表单加「进度预算」输入**

`TasksPage.tsx`:

1) `EMPTY`(第 12-17 行)加 `progressTimeoutSeconds: undefined,`。

2) `beginEdit`(第 74-78 行)加 `progressTimeoutSeconds: t.progressTimeoutSeconds ?? undefined,`。

3) 表单网格(第 151-156 行,`maxActiveConcurrent` 后)加一个 number 字段:

```tsx
          {field('maxActiveConcurrent', '最大并发', 'number')}
          {field('progressTimeoutSeconds', '进度预算 (s)', 'number')}
```

(注:空串时 `field` 经 `String(form[key] ?? '')` → `''`,提交时若 `CreateTaskRequest.progressTimeoutSeconds` 为 `''` 空串会以空串 JSON 序列化。为与后端 `Integer` 解析一致,backend `checkDefinition` 非负校验只挡负数;空串会被 Jackson 反序列化为 null(缺省)或触发 400。为稳妥,提交 `{...form, progressTimeoutSeconds: form.progressTimeoutSeconds ?? undefined}` 已在 `handleSubmit`/`beginEdit` 路径用 `?? undefined` 归一,与 `successPolicyValue` 处理一致。)

- [ ] **Step 3: StuckPage 加两列**

`StuckPage.tsx`:

1) 表头 `超龄`(第 72 行)后加两列:

```tsx
                <th className="num">超龄</th>
                <th className="num">进度年龄</th>
                <th className="num">最近推进</th>
                <th className="num">预算</th>
```

2) 表行 `ageSeconds` 单元格(第 88 行)后加两格:

```tsx
                  <td className="num">{s.ageSeconds}s</td>
                  <td className="num">{s.progressAgeSeconds}s</td>
                  <td className="text-xs font-mono">{s.lastProgressAt ?? '—'}</td>
                  <td className="num">{s.timeoutSeconds}s</td>
```

- [ ] **Step 4: 前端类型/构建绿灯**

Run: `cd web && npm run build`
Expected: PASS(tsc -b 类型检查 + vite build)。

- [ ] **Step 5: Commit**

```bash
git add web/src/api/types.ts web/src/pages/TasksPage.tsx web/src/pages/StuckPage.tsx
git commit -m "feat(e3): web — task progress budget input + stuck page progress columns"
```

---

### Task 8: QA — OpenAPI 快照 + spec 状态 + 全量回归

**Files:**
- Modify: `docs/openapi/api-docs.json`(additive 重导)
- Modify: `docs/superpowers/specs/2026-10-09-scheduler-e3-progress-heartbeat-design.md`(状态标 implemented)

**Interfaces:**
- Consumes: Task 1-7 全部产出。OpenAPI 快照导出手法:`-Dscheduler.openapi.dump` 属性门控 test(见 `ApiIntegrationTest#openApiSnapshot_exportAsCommittableArtifact`)。

- [ ] **Step 1: 全量后端回归**

Run: `mvn -o test -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(core/persistence/server/worker 全绿;需 Docker up)。

- [ ] **Step 2: OpenAPI 快照 additive 重导**

Run: `mvn -o test -pl scheduler-server -am -Dtest=ApiIntegrationTest#openApiSnapshot_exportAsCommittableArtifact -Dscheduler.openapi.dump=docs/openapi/api-docs.json -Dsurefire.failIfNoSpecifiedTests=false`

然后深 diff 确认仅新增:`/api/v1/tasks` 请求/响应 schema 加 `progressTimeoutSeconds` 字段、`/api/v1/executions/stuck` 响应加 `lastProgressAt`/`progressAgeSeconds` 两列;endpoint/schema 无漂移。

```bash
git diff --stat docs/openapi/api-docs.json
```

- [ ] **Step 3: spec 状态标 implemented**

`spec` 头部 `> **状态**:设计中` 改为 `> **状态**:已实现(2026-10-09)`,并在 §9 遗留里勾掉已落地项(不动内容,只更状态)。

- [ ] **Step 4: Commit**

```bash
git add docs/openapi/api-docs.json docs/superpowers/specs/2026-10-09-scheduler-e3-progress-heartbeat-design.md
git commit -m "chore(e3): full qa + openapi snapshot + spec marked implemented"
```

---

## Self-Review

**Spec 覆盖:** V32 迁移 → Task 1;`progress_timeout_seconds` 进领域/仓储 → Task 2;`recordProgress` + 渐进谓词 + `StuckShard.lastProgressAt` → Task 3;`ProgressSignal`/`HandlerContext.touch()`/合并 flush + handler 示范 → Task 4;任务字段/校验/审计 → Task 5;`StuckView` 两列 → Task 6;web types/表单/stuck 列 → Task 7;OpenAPI + spec 状态 → Task 8。spec §7 测试分叉(a/b/c/d/e)——a/b/e 与 recordProgress 栅栏在 Task 3,c/d 与 count 一致性由既有 E2B 用例 + 升级后的谓词覆盖;worker 合并 flush + attempt 栅栏在 Task 4;server /stuck 两列在 Task 6;web `npm run build` 在 Task 7。

**Spec 边界对照:** 「不碰活性/超时/回收」→ `recordProgress` 只改 `last_progress_at`,不动 `lease_until`/status/outcome;「单一时钟」→ SQL `now()` 盖章;「零新告警规则」→ AlertEngine 未改(谓词升级对它透明);「可空退化」→ 两列皆 NULL 默认,分支 A 与 E2B 完全一致。

**文义偏差如实记录:** spec §5.1 点名「4 个 handler 补调用点」,但 echo/demo/myjob 是**无循环的 no-op handler**(无 per-chunk/per-loop 边界)。实现把它们作为「契约示范」各加一行 `ctx.touch()`,真正的有界推进示范落在 `SyncDemoHandler` 的批量插入前。这不改任何运行语义(短跑 handler 立即终态,touch 的 flush 被 status='RUNNING' 栅栏静默)。

**类型一致性:** `progressTimeoutSeconds`(Task record 末位)→ `Task.progressTimeoutSeconds()` 贯穿 MAP/insert/update/create 返回/taskMeta/taskBefore/taskDiff/TaskController create+update;`StuckShard.lastProgressAt`(可空 Instant)→ `StuckView.lastProgressAt` + `progressAgeSeconds`(long 原始)。web 侧 `StuckRow.lastProgressAt: string | null` 与 `progressAgeSeconds: number` 对齐后端序列化(Jackson null → JSON null;long → number)。

**占位符扫描:** 无 TBD/TODO;全部代码块给出实实现;测试命令给出确切 `-Dtest` 目标与期望结果。