# E2-B stuck-alive 卡死分片可观测 + 强制放弃 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让「属主 worker 仍 ALIVE 但已超过自身运行预算(runtime timeout)、卡住的 RUNNING 分片」可见(V31 告警信号 + 卡死视图),并提供显式、活性前门的运维「强制放弃」端点,把它们换干净 attempt 重派。只观测 + 显式运维动作,不自动回收活体,不违反 E1 B1 不变量。

**Architecture:** 三条线——(1) persistence 新查询 `findStuckAliveRunning` / `countStuckAliveRunning`:复用 E1 活性门的取反(要求属主 ALIVE)+ 经 execution→task JOIN 取 task 自身 `timeout_seconds`,零新字段/零迁移。(2) AlertEngine 加 `stuck-alive` 全局规则(镜像 `dlq-depth` 的 RuntimeConfigKeys + Signal 手法)。(3) ExecutionController 加只读 `GET /executions/stuck` 视图 + 写 `POST /shards/{id}/abandon`(镜像 requeue;安全前门=属主仍 ALIVE→409,放行后 markOffline + RUNNING→FAILED("operator force-abandon") + requeue + audit)。前端加卡死页 + 告警下钻。

**Tech Stack:** Java 21 / Spring Boot 3 / Postgres(Testcontainers@Postgres)、React + Vite + TS。

**Spec:** `docs/superpowers/specs/2026-09-30-scheduler-e2b-stuck-alive-observability-design.md`(本 plan 自 spec 论证;执行者同读 spec + plan)。

## Global Constraints

- 零新增依赖、零迁移(V32 无);仅 Java/Spring 既有栈。
- 测试门:离线 `mvn -o`;server→`-pl scheduler-server -am`,persistence→`-pl scheduler-persistence -am`,`-Dsurefire.failIfNoSpecifiedTests=false`;前端 `web/` 内 `npm run build`。
- 提交到 `main`(仓库单一主分支惯例);commit 尾加 `Co-Authored-By: Claude Code <noreply@anthropic.com>`。
- 审计 append-only:`shard.force-abandon` 入 `app_audit`,绝不删。只读端点(audit / alerts / stuck)读开放;写端点(abandon)经 OperatorInterceptor + audit 留痕。
- 单一时钟(DB 持时);stuck-alive 复用 E1 活体判据(stale 窗与 Reconciler/AlertEngine 一致=30s)。
- 活体永不自动回收:E2-B 只观测 + 显式运维;B1 判定语义不改。
- OpenAPI 快照需 additively 重导(加 2 条端点参数)。

---

## Task 1: Persistence — `StuckShard` + `findStuckAliveRunning` / `countStuckAliveRunning`

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java`(add record + 2 方法)
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java`(实现;镜像 findOverRuntime 谓词,活性门取反 + JOIN task 取 timeout)
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java`

**Interfaces:**
- Consumes: `worker.last_seen`/`worker.status='ALIVE'`(E1 活体判据)、`execution.task_id → app_task.timeout_seconds` JOIN、`execution_shard.started_at/worker_id/status`。
- Produces:`ShardRepository.StuckShard(shardId, executionId, shardIndex, workerId, attempt, startedAt, timeoutSeconds)`;`findStuckAliveRunning(Long taskId, int staleAfterSeconds)`(taskId=null=全部);`countStuckAliveRunning(int staleAfterSeconds)`。Task 2 消费 count;Task 3 消费 list + id。

**背景事实(已核实)**:`newTask(int shardCount, int maxActiveConcurrent)` 参数二是 maxConcurrent **非** timeout;故测试须 `UPDATE app_task SET timeout_seconds=?` 显式播种预算。`findOverRuntime`/`findExpiredRunning` 已有 JdbcShardRepository:245-258/230-243 的 `NOT EXISTS(worker ... ALIVE)` 活性门,stuck 只是把 `NOT EXISTS` 换成 `EXISTS` 并补 JOIN task 取 timeout。

- [ ] **Step 1: 写失败测试**

在 `JdbcShardRepositoryTest.java` 末尾(紧接 `findOverRuntime_deadOwnerOverRuntime_isReturned` 之后、类收尾 `}` 之前)追加 helper + 5 个测试:

```java
// ---- E2B 可观测:stuck-alive(findStuckAliveRunning / countStuckAliveRunning)----

private void seedTaskTimeout(long taskId, int timeoutSeconds) {
  jdbc.update("UPDATE app_task SET timeout_seconds=? WHERE id=?", timeoutSeconds, taskId);
}

/** stuck-alive:活属主 + 已超其运行预算 → 列入(被 findOverRuntime 排除的补集,自观测)。 */
@Test void findStuckAliveRunning_aliveOwnerOverBudget_isReturned() {
  long taskId = newTask(1, 8);
  seedTaskTimeout(taskId, 60);
  long parentId = seedParentAndShards(taskId, 1);
  long sid = shard(parentId, 0).id();
  jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w-live',"
      + " started_at=now() - interval '10 minutes' WHERE id=?", sid);
  seedWorker("w-live", 5); // ALIVE, last_seen 5s 前 < stale(30)

  var stuck = shardRepo.findStuckAliveRunning(taskId, 30);
  assertEquals(1, stuck.size());
  assertEquals(sid, stuck.get(0).id());
  assertEquals("w-live", stuck.get(0).workerId());
  assertEquals(60, stuck.get(0).timeoutSeconds());
}

/** 活属主但未超其 timeout 预算 → 不视为卡死(慢而健康不误报)。 */
@Test void findStuckAliveRunning_aliveOwnerUnderBudget_notReturned() {
  long taskId = newTask(1, 8);
  seedTaskTimeout(taskId, 60 * 60); // 1h 预算
  long parentId = seedParentAndShards(taskId, 1);
  long sid = shard(parentId, 0).id();
  jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w-live',"
      + " started_at=now() - interval '10 minutes' WHERE id=?", sid);
  seedWorker("w-live", 5);

  assertTrue(shardRepo.findStuckAliveRunning(taskId, 30).stream().noneMatch(e -> e.id() == sid),
      "活属主但未超预算 → 不列入 stuck");
}

/** 死属主(即便超预算)不入 stuck —— 那是普通回收路径,finstuck 只关心活体卡死。 */
@Test void findStuckAliveRunning_deadOwnerOverBudget_notReturned() {
  long taskId = newTask(1, 8);
  seedTaskTimeout(taskId, 60);
  long parentId = seedParentAndShards(taskId, 1);
  long sid = shard(parentId, 0).id();
  jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w-dead',"
      + " started_at=now() - interval '10 minutes' WHERE id=?", sid);
  seedWorker("w-dead", 120); // ALIVE 但 last_seen 120s 前 > stale(30) → 判死

  assertTrue(shardRepo.findStuckAliveRunning(taskId, 30).stream().noneMatch(e -> e.id() == sid),
      "死属主超预算 → 走普通回收,不列入 stuck");
}

/** 无运行预算(timeout_seconds=0)的 task 不计卡死。 */
@Test void findStuckAliveRunning_noTimeoutBudget_notCounted() {
  long taskId = newTask(1, 8); // 保持 timeout_seconds=0
  long parentId = seedParentAndShards(taskId, 1);
  long sid = shard(parentId, 0).id();
  jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w-live',"
      + " started_at=now() - interval '10 minutes' WHERE id=?", sid);
  seedWorker("w-live", 5);

  assertTrue(shardRepo.findStuckAliveRunning(taskId, 30).stream().noneMatch(e -> e.id() == sid));
}

/** count 与列表一致,且支持全量(taskId=null)。 */
@Test void countStuckAliveRunning_matchesList_andSupportsGlobal() {
  long taskA = newTask(1, 8); seedTaskTimeout(taskA, 60);
  long parentA = seedParentAndShards(taskA, 1);
  long sa = shard(parentA, 0).id();
  jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w-live',"
      + " started_at=now() - interval '10 minutes' WHERE id=?", sa);
  seedWorker("w-live", 5);

  long taskB = newTask(1, 8); seedTaskTimeout(taskB, 60);
  long parentB = seedParentAndShards(taskB, 1);
  long sb = shard(parentB, 0).id();
  jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w-live2',"
      + " started_at=now() - interval '10 minutes' WHERE id=?", sb);
  seedWorker("w-live2", 5);

  assertEquals(1, shardRepo.findStuckAliveRunning(taskA, 30).size());
  assertEquals(2, shardRepo.countStuckAliveRunning(30));
  assertEquals(2, shardRepo.findStuckAliveRunning(null, 30).size());
}
```

- [ ] **Step 2: 跑,确认失败**

Run: `mvn -o -pl scheduler-persistence -am test -Dtest='JdbcShardRepositoryTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — 编译错「cannot find symbol findStuckAliveRunning / countStuckAliveRunning」。

- [ ] **Step 3: 实现**

`ShardRepository.java`(在 `findOverRuntime` 注释后、`markStatus` 前插入;需 `import java.time.Instant` 已在文件头):

```java
/** E2-B stuck-alive 视图投影:属主 worker 仍 ALIVE 但已超过其所属 task 运行预算的 RUNNING 分片。
 *  纯观测/只读,不改状态。timeoutSeconds 取自该分片所属 task。 */
record StuckShard(long id, long executionId, int shardIndex, String workerId, int attempt,
                 Instant startedAt, long timeoutSeconds) {}

/** E2-B 卡死视图:某任务下(或全部,taskId=null)属主 ALIVE 且已超运行预算的 RUNNING 分片。
 *  活性门 = E1 统一判据的取反(要求属性 ALIVE);超时经 execution→task JOIN 取 task.timeout_seconds。 */
List<StuckShard> findStuckAliveRunning(Long taskId, int staleAfterSeconds);

/** E2-B 全局 stuck-alive 计数(告警信号源)。 */
long countStuckAliveRunning(int staleAfterSeconds);
```

`JdbcShardRepository.java`(在 `findOverRuntime` 实现后插入;文件头已 `import dev.scheduler.persistence.ShardRepository.StuckShard` 会因嵌套 record 需要全限定——用 `ShardRepository.StuckShard` 全限定于返回类型即可):

```java
@Override public List<ShardRepository.StuckShard> findStuckAliveRunning(Long taskId, int staleAfterSeconds) {
  String base = "SELECT s.id, s.execution_id, s.shard_index, s.worker_id, s.attempt, s.started_at, t.timeout_seconds"
      + " FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
      + " JOIN app_task t ON t.id = e.task_id";
  String cond = " s.status='RUNNING' AND s.worker_id IS NOT NULL"
      + " AND t.timeout_seconds > 0"
      + " AND now() - s.started_at > make_interval(secs => t.timeout_seconds::double precision)"
      + " AND EXISTS (SELECT 1 FROM worker w"
      + "             WHERE w.id = s.worker_id AND w.status = 'ALIVE'"
      + "             AND w.last_seen >= now() - make_interval(secs => ?))";
  var map = (rs, i) -> new ShardRepository.StuckShard(rs.getLong("id"), rs.getLong("execution_id"),
      rs.getInt("shard_index"), rs.getString("worker_id"), rs.getInt("attempt"),
      rs.getTimestamp("started_at").toInstant(), rs.getLong("timeout_seconds"));
  if (taskId != null) {
    return jdbc.query(base + " WHERE e.task_id = ? AND" + cond, map, taskId, (double) staleAfterSeconds);
  }
  return jdbc.query(base + " WHERE" + cond, map, (double) staleAfterSeconds);
}

@Override public long countStuckAliveRunning(int staleAfterSeconds) {
  Long n = jdbc.queryForObject(
      "SELECT count(*) FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
          + " JOIN app_task t ON t.id = e.task_id"
          + " WHERE s.status='RUNNING' AND s.worker_id IS NOT NULL"
          + " AND t.timeout_seconds > 0"
          + " AND now() - s.started_at > make_interval(secs => t.timeout_seconds::double precision)"
          + " AND EXISTS (SELECT 1 FROM worker w"
          + "             WHERE w.id = s.worker_id AND w.status = 'ALIVE'"
          + "             AND w.last_seen >= now() - make_interval(secs => ?))",
      (double) staleAfterSeconds, Long.class);
  return n == null ? 0L : n;
}
```

（`make_interval(secs => t.timeout_seconds::double precision)`:`timeout_seconds` 为 int,`make_interval(secs)` 需要 double,故显式 cast。）

- [ ] **Step 4: 跑,确认通过**

Run: `mvn -o -pl scheduler-persistence -am test -Dtest='JdbcShardRepositoryTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: X, Failures: 0`(既有多考保持绿 + 5 新例过)。

- [ ] **Step 5: Commit**

```bash
git add scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java \
        scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java
git commit -m "feat(e2b): StuckShard + findStuckAliveRunning/countStuckAliveRunning (liveness flip + task-timeout JOIN)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 2: Alerts — `stuck-alive` 全局规则

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/RuntimeConfigKeys.java`(加 key + DEFAULTS)
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/service/AlertEngine.java`(collectSignals 加分支 + stale 常量)
- Test: `scheduler-server/src/test/java/dev/scheduler/server/service/AlertEngineTest.java`

**Interfaces:**
- Consumes: Task 1 的 `ShardRepository.countStuckAliveRunning(int)`。AlertEngine 已注入 `ShardRepository shards`(:37)。镜像 `dlq-depth` 规则(RuntimeConfigKeys 阈值 + `shards.countDeadLetter()` Signal)。
- Produces: `AlertEngine` 第 9 条规则 `stuck-alive`(全局, targetType=null, severity="medium", value=count)。Task 4 前端据 `rule="stuck-alive"` 加 label + `/stuck` 下钻。

**背景事实**:RuntimeConfigKeys `DEFAULTS` 是 key→默认值白名单(:37-62),未登记 key 的写入会被拒;已登记未覆盖时 `RuntimeConfigService.getLong(key, default)` 回落默认。AlertEngineTest 用无 Mock 依赖(ShardRepository 是 mock),镜像 `p95-latency`/`dlq-depth` 手法喂 `settings` 阈值 + stub `countStuckAliveRunning`。

- [ ] **Step 1: 写失败测试**

在 `AlertEngineTest.java` 加 2 个测试(放在 `open_persistsWhileConditionActive...` 之后、per-task 段之前):

```java
// ---- E2B:stuck-alive 全局规则(镜像 dlq-depth)----

@Test void stuckAlive_aboveThreshold_opensEpisode() {
  Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_STUCK_ALIVE, "1"));
  when(h.shards.countStuckAliveRunning(anyInt())).thenReturn(2L);
  h.engine.evaluateOnce();
  h.engine.evaluateOnce();
  h.engine.evaluateOnce(); // 3 连续 active → OPEN
  AlertEpisode e = h.episode("stuck-alive");
  assertEquals("OPEN", e.status());
  assertEquals(3, e.sampleCount());
  assertEquals("2", e.openedValue());
  assertNull(e.targetType(), "全局规则无 target");
}

@Test void stuckAlive_belowThreshold_noSignal() {
  Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_STUCK_ALIVE, "1"));
  when(h.shards.countStuckAliveRunning(anyInt())).thenReturn(0L);
  h.engine.evaluateOnce();
  assertTrue(h.alerts.active.isEmpty(), "count(0) < 阈值(1)→ 不产出信号");
}

@Test void stuckAlive_thresholdZero_disabled() {
  Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_STUCK_ALIVE, "0"));
  when(h.shards.countStuckAliveRunning(anyInt())).thenReturn(5L); // 即便有卡死也因阈=0 不产出
  h.engine.evaluateOnce();
  assertTrue(h.alerts.active.isEmpty(), "阈=0 → 规则停用");
}
```

- [ ] **Step 2: 跑,确认失败**

Run: `mvn -o -pl scheduler-server -am test -Dtest='AlertEngineTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — 编译错「cannot find symbol ALERT_STUCK_ALIVE」(RuntimeConfigKeys 未定义)。

- [ ] **Step 3: 实现**

`RuntimeConfigKeys.java`:

```java
public static final String ALERT_STUCK_ALIVE = "alert.stuck-alive";
```
并把下行加入 DEFAULTS 静态块(mirror 邻接 alert 键):
```java
DEFAULTS.put(ALERT_STUCK_ALIVE, "1");
```
(class 字段区 :34 后加常量;`DEFAULTS` 块 :61 后加 put。)

`AlertEngine.java`(:34 常量区):
```java
private static final int STUCK_ALIVE_STALE_SECONDS = 30;
```
在 `collectSignals()` 的 `dag-run-failed` 分支(`:163-170`)之后、`return out;` 之前加:

```java
// stuck-alive(全局, medium):存在属主仍 ALIVE 但已超其运行预算的 RUNNING 分片(自观测;B1 不改活体)。
long stuckThr = settings.getLong(RuntimeConfigKeys.ALERT_STUCK_ALIVE, 1L);
if (stuckThr > 0) {
  long n = shards.countStuckAliveRunning(STUCK_ALIVE_STALE_SECONDS);
  out.add(new Signal("stuck-alive", null, null, "medium", Long.toString(n), n >= stuckThr));
}
```

- [ ] **Step 4: 跑,确认通过**

Run: `mvn -o -pl scheduler-server -am test -Dtest='AlertEngineTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: X, Failures: 0`(既有全绿 + 3 新例过)。

- [ ] **Step 5: Commit**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/config/RuntimeConfigKeys.java \
        scheduler-server/src/main/java/dev/scheduler/server/service/AlertEngine.java \
        scheduler-server/src/test/java/dev/scheduler/server/service/AlertEngineTest.java
git commit -m "feat(e2b): stuck-alive global alert rule (countStuckAliveRunning > threshold)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 3: Server — `GET /executions/stuck` 视图 + `POST /shards/{id}/abandon` 强制放弃

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/ExecutionController.java`(注入 WorkerRepository;加 2 端点 + StuckView record + 常量)
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes:Task 1 `ShardRepository.findStuckAliveRunning(Long, int)` + `findTaskRefsByShardIds(Collection)`(已有,返回 `Map<Long, TaskRef>`)、`WorkerRepository.findAllAlive(Instant)` + `markOffline(String, Instant)`(已有)、`markStatus` + `requeueShard` + `auditor.record`(requeue 已有全套模式 ExecutionController:139-151)。
- Produces:`GET /api/v1/executions/stuck`(只读 Page<StuckView>)、`POST /api/v1/executions/shards/{shardId}/abandon`(写,审计 `shard.force-abandon`)。Task 4 前端消费。

**背景事实**:ExecutionController 构造器注入 :48-55 现有 `(ExecutionRepository, ShardRepository, JdbcTemplate, AuditRecorder, CurrentOperator)`;需加 `WorkerRepository`。`Page`/`Paging` 已在 dlq()(:118 `Paging.of`、:132 `new Page<>`)使用 → 同包可用。`notFound`/:142 `current`/:148 `shardBefore(s)` 私有helper 已有。ApiIntegrationTest 用 `jdbc` 字段 + `shards.createParentWithShards` 直接 UPDATE 播种 RUNNING shard。**写请求不带显式 cookie**:requeue 测试 `mvc.perform(post(requeue))` 无 cookie 参数,默认 alice cookie(MockMvcBuilderCustomizer)已带 → abandon 测试同款,不写 `.session(...)`(无此 helper;`session(String token)` 是显式覆盖用)。分页断言用 `Page` 信封 `$.total`/`$.items[]`(dlq 同款)。

- [ ] **Step 1: 写失败测试**

在 `ApiIntegrationTest.java` 加 4 个测试(放在 `requeueMissingShard_returnsNotFound` :1781 之后):

```java
// ---- E2B:卡死视图 + 强制放弃 ----

long seedRunningShard(long taskId, String workerId) {
  long parentId = shards.createParentWithShards(taskId, "e2b:" + System.nanoTime(), 1).id();
  long shardId = shards.findShards(parentId).get(0).id();
  jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=?,"
      + " started_at=now() - interval '10 minutes' WHERE id=?", workerId, shardId);
  return shardId;
}

@Test void stuckView_listsOnlyAliveOwnerOverBudget() throws Exception {
  long id = postTask("stuck-view-task");
  jdbc.update("UPDATE app_task SET timeout_seconds=60 WHERE id=?", id);
  long shardLive = seedRunningShard(id, "w-live");
  jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES ('w-live','',now(),'ALIVE')");
  long shardDead = seedRunningShard(id, "w-dead");
  jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES ('w-dead','',now() - interval '120 seconds','ALIVE')");

  mvc.perform(get("/api/v1/executions/stuck").param("taskId", String.valueOf(id)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.total").value(1))
      .andExpect(jsonPath("$.items[0].shardId").value((int) shardLive));
}

@Test void abandon_aliveOwner_returnsConflict() throws Exception {
  long id = postTask("abandon-alive-task");
  long shardId = seedRunningShard(id, "w-live");
  jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES ('w-live','',now(),'ALIVE')");

  mvc.perform(post("/api/v1/executions/shards/" + shardId + "/abandon"))
      .andExpect(status().isConflict());
  assertEquals("RUNNING", jdbc.queryForObject(
      "SELECT status FROM execution_shard WHERE id=?", String.class, shardId),
      "属主仍 ALIVE → 拒绝,行不被改动");
}

@Test void abandon_deadOwner_downsToDue_withAudit() throws Exception {
  long id = postTask("abandon-dead-task");
  long shardId = seedRunningShard(id, "w-dead");
  jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES ('w-dead','',now() - interval '120 seconds','ALIVE')");

  mvc.perform(post("/api/v1/executions/shards/" + shardId + "/abandon"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.status").value("DUE"))
      .andExpect(jsonPath("$.attempt").value(0)); // requeue 复位 attempt
  // FAILED outcome 'operator force-abandon' 已落;再 requeue → DUE
  assertEquals(1L, (long) jdbc.queryForObject(
      "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND detail='operator force-abandon'",
      Long.class, shardId));
  // 审计留痕
  mvc.perform(get("/api/v1/audits").param("action", "shard.force-abandon").param("targetId", String.valueOf(shardId)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.items.length()").value(1));
}

@Test void abandon_nonRunning_returnsConflict() throws Exception {
  long id = postTask("abandon-not-running-task");
  long parentId = shards.createParentWithShards(id, "e2b-nr:" + System.nanoTime(), 1).id();
  long shardId = shards.findShards(parentId).get(0).id();
  jdbc.update("UPDATE execution_shard SET status='SUCCESS' WHERE id=?", shardId);

  mvc.perform(post("/api/v1/executions/shards/" + shardId + "/abandon"))
      .andExpect(status().isConflict());
}
```

测试辅助确认:`postTask(String)`/:2342、`shards` 字段、`jdbc` 字段在 ApiIntegrationTest 均已存在。写请求(abandon)不带显式 cookie = 默认 alice cookie(MockMvcBuilderCustomizer)承载身份;audit GET 读开放不需 session。

- [ ] **Step 2: 跑,确认失败**

Run: `mvn -o -pl scheduler-server -am test -Dtest='ApiIntegrationTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — 404(端点未实现)及编译期 Optionals.`GET /stuck` 与 `POST /shards/{id}/abandon` 尚不存在。

- [ ] **Step 3: 实现**

`ExecutionController.java`:
- 构造器加参数 `WorkerRepository workers` 并赋值字段 + import `dev.scheduler.persistence.WorkerRepository`(类头 :8 已有 ShardRepository import)。
- 类内加常量与 StuckView record(:40 类内、字段区后):

```java
private static final int STUCK_ALIVE_STALE_SECONDS = 30;

/** E2-B 卡死视图行(persistence StuckShard + task 名/handlerRef + 派生超龄秒)。 */
public record StuckView(long shardId, long executionId, int shardIndex, String taskName,
                        String handlerRef, String workerId, int attempt, Instant startedAt,
                        long ageSeconds, long timeoutSeconds) {}
```

- 在 `requeue`(:151)之后加 2 端点:

```java
/** E2-B 卡死视图:属主仍 ALIVE 但已超运行预算的 RUNNING 分片(只读观测;运维判「慢 vs 真卡」+ 触发强制放弃)。
 *  分页在内存切片(卡死分片通常极少,简单为先)。 */
@GetMapping("/stuck")
public Page<StuckView> stuck(
    @RequestParam(required = false) Long taskId,
    @RequestParam(required = false) Integer limit,
    @RequestParam(required = false) Integer offset) {
  Paging p = Paging.of(limit, offset);
  List<ShardRepository.StuckShard> ss = shards.findStuckAliveRunning(taskId, STUCK_ALIVE_STALE_SECONDS);
  List<Long> ids = ss.stream().map(ShardRepository.StuckShard::id).toList();
  Map<Long, TaskRef> refs = shards.findTaskRefsByShardIds(ids);
  List<StuckView> all = ss.stream().map(x -> {
    TaskRef r = refs.get(x.id());
    long age = Duration.between(x.startedAt(), Instant.now()).getSeconds();
    return new StuckView(x.id(), x.executionId(), x.shardIndex(),
        r == null ? null : r.name(), r == null ? null : r.handlerRef(),
        x.workerId(), x.attempt(), x.startedAt(), age, x.timeoutSeconds());
  }).toList();
  List<StuckView> items = all.stream().skip(p.offset()).limit(p.limit()).toList();
  return new Page<>(items, all.size(), p.offset(), p.limit());
}

/** E2-B 强制放弃:运维显式回收「属主卡住」的 RUNNING 分片(mirror requeue 全套)。
 *  安全前门:属主仍 ALIVE → 409(强制先停 worker 再放弃)。放行后:防御性 markOffline + RUNNING→FAILED
 *  ("operator force-abandon") + requeue(→DUE attempt=0)换干净 attempt,审计留痕。 */
@PostMapping("/shards/{shardId}/abandon")
public ResponseEntity<Shard> abandon(
    @PathVariable long shardId) {
  Shard s = shards.findShard(shardId).orElseThrow(() -> notFound("shard " + shardId));
  if (s.status() != ExecutionStatus.RUNNING || s.workerId() == null) {
    throw new ResponseStatusException(HttpStatus.CONFLICT,
        "shard " + shardId + " is " + s.status() + " and cannot be force-abandoned (only RUNNING with an owner)");
  }
  boolean ownerAlive = workers.findAllAlive(Instant.now().minusSeconds(STUCK_ALIVE_STALE_SECONDS))
      .stream().anyMatch(w -> w.id().equals(s.workerId()));
  if (ownerAlive) {
    throw new ResponseStatusException(HttpStatus.CONFLICT,
        "owner worker " + s.workerId() + " is still ALIVE; stop it before force-abandon");
  }
  workers.markOffline(s.workerId(), Instant.now().minusSeconds(600)); // 防御性摘活(即便守卫已过仍兜底)
  shards.markStatus(shardId, ExecutionStatus.FAILED, s.workerId(), "operator force-abandon");
  shards.requeueShard(shardId);
  auditor.record(current.get(), "shard.force-abandon", TargetType.SHARD, shardId, Map.of(), null, shardBefore(s));
  return ResponseEntity.ok(shards.findShard(shardId).orElseThrow(() -> notFound("shard " + shardId)));
}
```

（`Duration` 需 import:`java.time.Duration`。文件头 :16 已有 `java.time.Instant`,补 Duration。）

- [ ] **Step 4: 跑,确认通过**

Run: `mvn -o -pl scheduler-server -am test -Dtest='ApiIntegrationTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: X, Failures: 0`(既有 137 保持绿 + 4 新例)。加跑 Existing reconfirm:`-pl scheduler-server -am test -Dsurefire.failIfNoSpecifiedTests=false`。

- [ ] **Step 5: Commit**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/web/ExecutionController.java \
        scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java
git commit -m "feat(e2b): GET /executions/stuck view + POST /shards/{id}/abandon (liveness-gated force-abandon)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 4: Frontend — 卡死页 + 告警下钻

**Files:**
- Modify: `web/src/api/types.ts`(加 `StuckRow`)
- Modify: `web/src/api/client.ts`(加 `getStuck` + `forceAbandonShard`)
- Create: `web/src/pages/StuckPage.tsx`(镜像 DlqPage)
- Modify: `web/src/App.tsx`(加 route `/stuck`)
- Modify: `web/src/pages/AlertsPage.tsx`(加 `stuck-alive` label + `/stuck` 下钻)

**Interfaces:**
- Consumes:Task 3 `GET /executions/stuck` + `POST /shards/{id}/abandon`。镜像 DlqPage 的 requeue 按钮(client.ts:161 `requeueShard` → DlqPage.tsx:62-65 `doRequeue`)+ AlertsPage RULE_LABELS:11-20 / RULE_DRILLDOWN:23-32。
- Produces:路由 `/stuck` 卡死页(列出 stuck 分片 + 每行「强制放弃」确认按钮);`stuck-alive` 告警可下钻 `/stuck`。npm build 绿。

- [ ] **Step 1: 加类型 + client**

`web/src/api/types.ts`(DlqRow 附近)加:

```ts
export interface StuckRow {
  shardId: number;
  executionId: number;
  shardIndex: number;
  taskName: string | null;
  handlerRef: string | null;
  workerId: string | null;
  attempt: number;
  startedAt: string | null;
  ageSeconds: number;
  timeoutSeconds: number;
}
```

`web/src/api/client.ts`(:162 `requeueShard` 之后)加:

```ts
/** E2-B 卡死视图:属主仍 ALIVE 但已超运行预算的 RUNNING 分片(只读,驱动卡死页)。 */
export const getStuck = (p: { taskId?: number; limit?: number; offset?: number } = {}): Promise<Page<StuckRow>> =>
  req<Page<StuckRow>>(`/api/v1/executions/stuck${qstr(p)}`);

/** E2-B 强制放弃:属主必须已非 ALIVE(后端 409 兜底);需先停 worker。 */
export const forceAbandonShard = (shardId: number) =>
  req<Shard>(`/api/v1/executions/shards/${shardId}/abandon`, { method: 'POST' });
```

（`StuckRow` 需入 client.ts import 型表 :1-34 与 types.ts 导出。）

- [ ] **Step 2: 新建 `web/src/pages/StuckPage.tsx`(镜像 DlqPage.tsx 结构)**

```tsx
// web/src/pages/StuckPage.tsx —— E2-B 卡死视图:列出「属主仍 ALIVE 但已超运行预算」的 RUNNING 分片,
// 每行提供「强制放弃」按钮(破坏性:要求先停 worker,后端对仍 ALIVE 属主 409 兜底)。
import { useCallback, useEffect, useState } from 'react';
import { forceAbandonShard, getStuck, listTasks } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { StuckRow, Task } from '../api/types';
import StatusBadge from '../components/StatusBadge';
import Pager from '../components/Pager';

const PAGE_SIZE = 20;

export default function StuckPage() {
  const [rows, setRows] = useState<StuckRow[]>([]);
  const [total, setTotal] = useState(0);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [taskId, setTaskId] = useState('');
  const [offset, setOffset] = useState(0);
  const [err, setErr] = useState<string | null>(null);
  const [confirmId, setConfirmId] = useState<number | null>(null);

  useEffect(() => { listTasks({ limit: 100 }).then((p) => setTasks(p.items)).catch(() => {}); }, []);

  const load = useCallback(async () => {
    try {
      const page = await getStuck({ taskId: taskId === '' ? undefined : Number(taskId), limit: PAGE_SIZE, offset });
      setRows(page.items); setTotal(page.total); setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [taskId, offset]);

  useEffect(() => { load(); }, [load]);
  useInterval(load, 5000);

  const doAbandon = useCallback(async (id: number) => {
    try { await forceAbandonShard(id); setErr(null); setConfirmId(null); await load(); }
    catch (e) { setErr(String(e)); }
  }, [load]);

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">运行卡死</h1>
          <p className="page-sub">属主 worker 仍在线但已超过运行预算的 RUNNING 分片 —— 判断「慢而健康」还是「真卡住」;强制放弃须先停掉该 worker</p>
        </div>
        {total > 0 && <div className="text-sm text-slate-500">{total} 条卡死</div>}
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      <div className="toolbar">
        <label className="field">
          <span className="label">任务</span>
          <select className="input" value={taskId} onChange={(e) => { setTaskId(e.target.value); setOffset(0); }}>
            <option value="">全部</option>
            {tasks.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
          </select>
        </label>
      </div>

      {rows.length === 0 ? (
        <div className="card p-10 text-center text-sm text-slate-400">无卡死分片 —— 所有 RUNNING 分片都在运行预算内或属主已离线</div>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>任务</th>
                <th className="num">idx</th>
                <th>状态</th>
                <th className="num">attempt</th>
                <th>worker</th>
                <th className="num">超龄</th>
                <th className="num">预算</th>
                <th className="text-right">操作</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((s) => (
                <tr key={s.shardId}>
                  <td>
                    <div className="font-medium">{s.taskName ?? `任务 #${s.executionId}`}</div>
                    <div className="text-xs text-slate-500">{s.handlerRef ?? '—'} · exec #{s.executionId}</div>
                  </td>
                  <td className="num">{s.shardIndex}</td>
                  <td><StatusBadge status="RUNNING" /></td>
                  <td className="num">{s.attempt}</td>
                  <td className="font-mono text-xs">{s.workerId ?? '—'}</td>
                  <td className="num">{s.ageSeconds}s</td>
                  <td className="num">{s.timeoutSeconds}s</td>
                  <td className="text-right">
                    {confirmId === s.shardId ? (
                      <>
                        <button className="btn-danger"
                          onClick={() => doAbandon(s.shardId)}>确认放弃</button>
                        <button className="btn-secondary ml-2"
                          onClick={() => setConfirmId(null)}>取消</button>
                      </>
                    ) : (
                      <button className="btn-danger" onClick={() => setConfirmId(s.shardId)}>强制放弃</button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <Pager total={total} offset={offset} limit={PAGE_SIZE} onPage={setOffset} />
    </div>
  );
}
```

- [ ] **Step 3: 接线 route + 告警下钻**

`web/src/App.tsx`(:9 已 import DlqPage):加 `import StuckPage from './pages/StuckPage';`,在 `<Route path="/dlq" ...>`(:106)旁加 `<Route path="/stuck" element={<StuckPage />} />`。

`web/src/pages/AlertsPage.tsx` RULE_LABELS(:11-20)加:
```tsx
'stuck-alive': { text: '运行卡死', cls: 'bg-slate-100 text-slate-700' },
```
RULE_DRILLDOWN(:23-32)加:
```tsx
'stuck-alive': '/stuck',
```

- [ ] **Step 4: 构建,确认绿**

Build:`cd web && npm run build`(tsc + vite;不引运行时测试)。
Expected:BUILD 成功,无 TS 编译错。(TS 类型错误 = 前端失败)

- [ ] **Step 5: Commit**

```bash
git add web/src/api/types.ts web/src/api/client.ts web/src/pages/StuckPage.tsx web/src/App.tsx web/src/pages/AlertsPage.tsx
git commit -m "feat(e2b): stuck-alive page with force-abandon + alert drilldown

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 5: QA — 全量回归 + OpenAPI 快照 + spec 状态

**Files:**
- Modify: `docs/openapi/api-docs.json`(additive 重导)
- Modify: `docs/superpowers/specs/2026-09-30-scheduler-e2b-stuck-alive-observability-design.md`(状态→已实现)
- (无代码;若回归暴露问题,回对应 Task 修)

**Interfaces:**
- Consumes:Task 1-4 全部产出。OpenAPI 快照导出手法:`-Dscheduler.openapi.dump` 属性门控 test(见 E1 Task 6 与 `scheduler-v2-openapi-guard` memory)。

- [ ] **Step 1: 全量回归**

Run(离线,逐模块):
```bash
mvn -o -pl scheduler-server -am test -Dsurefire.failIfNoSpecifiedTests=false   # server + core + persistence 依赖
mvn -o -pl scheduler-worker -am test -Dsurefire.failIfNoSpecifiedTests=false  # worker(reactor -am 拉 core/persistence)
cd web && npm run build
```
Expected:全绿(既有 555 体系 + 新例)。若 Testcontainers infra flake(JdbcTask/JdbcWebhook),单独重跑确认非本改动所致。

- [ ] **Step 2: OpenAPI 快照 additive 重导**

用 E1 同款属性门控 test 导出到临时文件,深 diff 确认仅 `GET /api/v1/executions/stuck` 与 `POST /api/v1/executions/shards/{id}/abandon` 两条新增,endpoint/schema 零漂移,再合入 `docs/openapi/api-docs.json`。命令同 `scheduler-v2-openapi-guard` memory:`-Dscheduler.openapi.dump=docs/openapi/api-docs.json` + 对应 export test。

- [ ] **Step 3: spec 状态→已实现**

把计划头 `> **状态**:设计中(2026-09-30)` 改为 `> **状态**:已实现(2026-09-30)`;§4.3 / §6 如有实现细节与原文出入,补一致性注(尤其 abandon 走 requeue(attempt=0) 的实际语义)。

- [ ] **Step 4: Commit**

```bash
git add docs/openapi/api-docs.json docs/superpowers/specs/2026-09-30-scheduler-e2b-stuck-alive-observability-design.md
git commit -m "docs(e2b): mark spec implemented + regenerate OpenAPI snapshot (stuck/abandon)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Self-Review(写完后对照 spec 自查)

**Spec 覆盖**:检测查询(§4.1)→ Task 1;告警规则(§4.2)→ Task 2;视图 + 放弃端点 + 安全前门 + audit(§4.3)→ Task 3;前端(§4.4在 spec 未单列,并入 §6 视图)→ Task 4;OpenAPI + spec 状态(§6/头部)→ Task 5。零迁移/零依赖(§5/§10)全程遵循。活体不自动回收边界(§2)在 abandon 活性前门 + 不新增自动回收 loop 严格执行。
**占位符扫描**:无 TBD/TODO;每步含可直接照做的 SQL/Java/TS。
**类型一致性**:Task 1 产出 `StuckShard(id,executionId,shardIndex,workerId,attempt,startedAt,timeoutSeconds)` → Task 3 用 `.id()/.executionId()/.shardIndex()/.workerId()/.attempt()/.startedAt()/.timeoutSeconds()` 一致;`countStuckAliveRunning(int)` → Task 2 消费一致。StuckRow(shardId,…) ↔ StuckView(shardId,…) 字段名一致,无横跨任务签名漂移。