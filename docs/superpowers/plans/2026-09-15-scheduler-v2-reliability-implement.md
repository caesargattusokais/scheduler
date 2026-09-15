# Scheduler v2 — 可靠性纵深(宕机履约与快速收敛)实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把宕机任务的收敛从「分钟级假死」钉到「秒级:要么接续、要么带归因明确失败,零永久 RUNNING」;lease/对账参数收窄并配置化。

**Architecture:** 不改领域模型、不动执行运行时、不加新依赖。Reconciler 失效判定从「只认租约自然过期(lease_until<=now())」升级为「租约过期 **或** owner worker 心跳失联」双信号;防双跑复用既有 `worker_id` 归属守卫与 `renewLease` 0 行静默,不引入新机制。

**Tech Stack:** Java 17、Spring Boot 3.2.5、PostgreSQL 16、Testcontainers(JUnit 5)、Maven(-o 离线)。

**Spec:** `docs/superpowers/specs/2026-09-15-scheduler-v2-reliability-design.md`(本计划论证据此主线;冲突以 spec 为准)。

## Global Constraints

- 判活口径统一:owner 失联 = `worker.last_seen` 距今 > `staleAfterSeconds` **或** `worker.status != 'ALIVE'`;`staleAfterSeconds` 默认 `30`(与 server 现有 active 判活窗口一致,不漂移)。
- 活性分支**仅当 `s.worker_id IS NOT NULL` 时判定**;`worker_id IS NULL` 的 RUNNING(异常数据/测试播种)只走租约兜底,避免被活性分支误收(否则破坏既有「新鲜 RUNNING 不回收」用例)。
- 数据库统一时钟:所有判活/租约比较用 DB `now()`(PG),与 JVM 时钟无关;重试退避用注入 `Clock`(保持既有语义)。
- 归因 detail 统一为 `owner unresponsive or lease expired`(单查询,不做双因拆分)。
- 初始 claim lease +60s 保留(认领后立即被 renewer 提升到 `lease.seconds`),不动 worker 认领行为。
- 默认值收窄仅改 `@Value` 冒号里的默认值,不改缺省时语义;健康 worker 靠续租无感。
- 编译命令:`mvn -o -q -pl <module> -am install -DskipTests`(先装上游),再 `mvn -o -q -pl <module> test -Dtest=<TestClass>`。全量回归:`mvn -o test`。
- 提交以「一个 task 一个 commit」为准,末尾附 `Co-Authored-By: Claude Code <noreply@anthropic.com>`。

> **Plan 范围说明(拆分裁决):** spec §4「扫描分批健壮性」是低优先短活、不阻塞宕机履约主线,且会引入不小改动(游标/分批遍历 TriggerEngine/DagEngine)。按 writing-plans 拆分原则,§4 不作本计划的依赖交付,**独立为后续短活另行起计划**。本计划只交付 §1(活性优先接管)+ §2(宕机履约)+ §3(参数收窄),构成一个可独立验证的完整闭环。

---

### Task 1: 持久层 `findExpiredRunning` 双信号查询(接口 + 实现 + 集成测试)

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java:55-59`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java:220-225`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java`

**Interfaces:**
- Consumes: 既有 `ExpiredShard(long id, int attempt)` record,不变。
- Produces: `List<ExpiredShard> findExpiredRunning(long taskId, int staleAfterSeconds)` — Task 2 的 Reconciler 在 `scanOnce` 传 `staleAfterSeconds` 调用。

- [ ] **Step 1: 扩展接口签名(先改接口,让测试可编译)**

在 `ShardRepository.java` 把方法声明换成带 `int staleAfterSeconds` 参数,并更新 javadoc 为双信号语义:

```java
  /** 对账器扫描到的孤儿 RUNNING shard:租约已过期仍 RUNNING,或 owner worker 心跳失联(stale)。只投影对账所需的 id 与 attempt。 */
  record ExpiredShard(long id, int attempt) {}

  /** 某任务下需回收的孤儿 RUNNING shard,由 Reconciler 逐任务回收。判定双信号:
   *  ① 租约已过期(lease_until<=now());② owner worker 心跳失联(worker.last_seen 距今 > staleAfterSeconds 秒或
   *  status 非 'ALIVE',仅当 worker_id 非空时判定——worker_id 为空的 RUNNING 异常行走租约兜底)。
   *  worker 判活口径与 server active 指标一致。 */
  List<ExpiredShard> findExpiredRunning(long taskId, int staleAfterSeconds);
```

- [ ] **Step 2: 实现先只带新参数、暂不加活性分支(保持绿)** — 在 `JdbcShardRepository.java` 同步签名并实现原租约逻辑,使编译通过、原行为不变:

```java
  @Override public List<ExpiredShard> findExpiredRunning(long taskId, int staleAfterSeconds) {
    return jdbc.query(
        "SELECT s.id, s.attempt FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
            + " WHERE e.task_id=? AND s.status='RUNNING' AND s.lease_until <= now()",
        (rs, i) -> new ExpiredShard(rs.getLong("id"), rs.getInt("attempt")), taskId);
  }
```

> 此时 `staleAfterSeconds` 未用——编译器不报(java int 参数可未用)。若 IDE 告警可忽略,下一步才引用它。

- [ ] **Step 3: 写失败测试** — 追加到 `JdbcShardRepositoryTest.java`(类末尾,`requeueClearsCancelRequested` 之后),用唯一 worker id 防 `@BeforeEach clean()` 不 TRUNCATE `worker` 表的跨用例污染:

```java
  // ---- v2 可靠性纵深 §1:findExpiredRunning 双信号 ----

  private void seedWorker(String id, int lastSeenSecondsAgo) {
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES (?, '',"
        + " now() - make_interval(secs => ?), 'ALIVE')", id, (double) lastSeenSecondsAgo);
  }

  /** 活性优先:owner worker 心跳失联(last_seen 距今>30s)但租约未过期 → 认定孤儿。 */
  @Test void findExpiredRunning_ownerHeartbeatStale_isReturned() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    claimShard(sid, taskId, "w-stale", 8);   // RUNNING, worker_id='w-stale'
    seedWorker("w-stale", 120);              // last_seen 120s 前 > stale(30)

    boolean hit = shardRepo.findExpiredRunning(taskId, 30).stream().anyMatch(e -> e.id() == sid);
    assertTrue(hit, "owner 心跳失联 → 该行列入孤儿(活性优先)");
  }

  /** 活性负例:owner 心跳新鲜(距今<30s)且租约未过期 → 不回收。 */
  @Test void findExpiredRunning_ownerHeartbeatFresh_isSkipped() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    claimShard(sid, taskId, "w-fresh", 8);   // RUNNING, worker_id='w-fresh'
    seedWorker("w-fresh", 5);                // last_seen 5s 前 < stale(30)

    assertTrue(shardRepo.findExpiredRunning(taskId, 30).isEmpty(), "owner 心跳新鲜 → 不回收");
  }

  /** 兜底路径保留:租约已过期即使 owner 心跳新鲜 → 仍回收(lease 兜底)。 */
  @Test void findExpiredRunning_leaseExpiredButOwnerFresh_stillReturned() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    claimShard(sid, taskId, "w-keep", 8);    // RUNNING, worker_id='w-keep'
    seedWorker("w-keep", 5);
    jdbc.update("UPDATE execution_shard SET lease_until = now() - interval '1 hour' WHERE id=?", sid);

    boolean hit = shardRepo.findExpiredRunning(taskId, 30).stream().anyMatch(e -> e.id() == sid);
    assertTrue(hit, "租约过期即使 owner 新鲜 → lease 兜底仍回收");
  }

  /** 兜底:worker_id IS NULL 的 RUNNING(异常数据/测试播种)只走租约;租约新鲜 → 不回收。 */
  @Test void findExpiredRunning_withoutOwnerId_onlyLeaseSignalApplies() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=NULL,"
        + " lease_until=now()+interval '60 seconds' WHERE id=?", sid);

    assertTrue(shardRepo.findExpiredRunning(taskId, 30).isEmpty(), "worker_id 空 + 租约新鲜 → 不回收");
  }
```

- [ ] **Step 4: 运行到失败** — `mvn -o -q -pl scheduler-persistence test -Dtest=JdbcShardRepositoryTest`
  预期:`findExpiredRunning_ownerHeartbeatStale_isReturned` 失败(活性分支未实现),其余 3 个新用例与既有用例通过。

- [ ] **Step 5: 实现活性分支** — 改 `JdbcShardRepository.findExpiredRunning` 为双信号(用 `make_interval(secs => ?)` 参数化秒数,避免 interval 字面量拼接):

```java
  @Override public List<ExpiredShard> findExpiredRunning(long taskId, int staleAfterSeconds) {
    return jdbc.query(
        "SELECT s.id, s.attempt FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
            + " WHERE e.task_id=? AND s.status='RUNNING'"
            + " AND ( s.lease_until <= now()"
            + "     OR ( s.worker_id IS NOT NULL AND NOT EXISTS ("
            + "           SELECT 1 FROM worker w"
            + "            WHERE w.id = s.worker_id"
            + "              AND w.last_seen >= now() - make_interval(secs => ?)"
            + "              AND w.status = 'ALIVE' ) ) )",
        (rs, i) -> new ExpiredShard(rs.getLong("id"), rs.getInt("attempt")),
        taskId, (double) staleAfterSeconds);
  }
```

- [ ] **Step 6: 运行到通过** — `mvn -o -q -pl scheduler-persistence test -Dtest=JdbcShardRepositoryTest`
  预期:Tests run 43(原 39 + 新 4),0 failures。

- [ ] **Step 7: Commit**
  ```
  git add scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java \
          scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java \
          scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java
  git commit -m "feat(persistence): findExpiredRunning 双信号 — 租约过期 OR owner 心跳失联

  Co-Authored-By: Claude Code <noreply@anthropic.com>"
  ```

---

### Task 2: Reconciler 活性优先接管(stale 注入 + 归因 + server 测试)

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/reconcile/Reconciler.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java:195-199`(reconciler bean 加 `@Value`)
- Test: `scheduler-server/src/test/java/dev/scheduler/server/reconcile/ReconcilerTest.java`

**Interfaces:**
- Consumes: Task 1 的 `findExpiredRunning(long taskId, int staleAfterSeconds)`。
- Produces: `Reconciler(TaskRepository, ShardRepository, FailureResolver, String workerId, int staleAfterSeconds)` — Beans 注入 `scheduler.worker.stale-after-seconds:30`。

- [ ] **Step 1: 写失败测试(先改 helper 与新用例)** — 在 `ReconcilerTest.java`,更新 helper 传 stale,并新增两个活性用例。既有用例不改断言、只走 lease 兜底(`setShard` 播种 worker_id 为 null):

```java
  private Reconciler reconciler() {
    return new Reconciler(tasks, shards,
        new FailureResolver(shards, new RetryPolicy(), CLOCK), "reconciler", 30);
  }
```

```java
  /** §1 活性优先接管:owner 心跳失联且租约未过期 → 在租约到期前被立即回收(零永久 RUNNING)。 */
  @Test void orphanWithHeartbeatStaleOwner_reclaimedBeforeLeaseExpiry() {
    long taskId = createTask(1, 0, 1000, null); // maxRetries=0 → FAILED 即 DLQ
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    String owner = "w-gone";
    // 真实 claim 语义:worker_id=owner、RUNNING、租约仍有效(+60s)。
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=?,"
        + " lease_until=now()+interval '60 seconds', attempt=1 WHERE id=?", owner, sid);
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status)"
        + " VALUES (?, '', now() - interval '2 minutes', 'ALIVE')", owner); // 心跳失联 > stale30

    assertEquals(1, reconciler().scanOnce(), "心跳失联 owner → 租约到期前被活性优先回收");
    Shard s = shards.findShard(sid).orElseThrow();
    assertEquals(ExecutionStatus.FAILED, s.status(), "活性优先回收 → FAILED");
    assertEquals(1L, shardOutcomeCount(sid, "FAILED"), "回收落 FAILED outcome(归因)");
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=?", Boolean.class, sid),
        "不可重试 → DLQ 分支");
  }

  /** §1 负例:owner 心跳新鲜 + 租约有效 → 不被活性优先误收。 */
  @Test void shardWithHeartbeatAliveOwner_isLeftUntouched() {
    long taskId = createTask(1, 0, 1000, null);
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    String owner = "w-alive";
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=?,"
        + " lease_until=now()+interval '60 seconds', attempt=1 WHERE id=?", owner, sid);
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status)"
        + " VALUES (?, '', now() - interval '5 seconds', 'ALIVE')", owner);

    assertEquals(0, reconciler().scanOnce(), "owner 心跳新鲜 → 不回收");
    assertEquals(ExecutionStatus.RUNNING, shards.findShard(sid).orElseThrow().status());
  }
```

- [ ] **Step 2: 运行到失败** — `mvn -o -q -pl scheduler-server -am install -DskipTests && mvn -o -q -pl scheduler-server test -Dtest=ReconcilerTest`
  预期:编译失败(Reconciler 构造器少参 + scanOnce 调用签名缺参)。

- [ ] **Step 3: 实现** — `Reconciler.java` 构造器加 `int staleAfterSeconds`,`scanOnce` 改调新签名并统一归因 detail:

```java
  private final int staleAfterSeconds;

  public Reconciler(TaskRepository tasks, ShardRepository shards,
                    FailureResolver failureResolver, String workerId, int staleAfterSeconds) {
    this.tasks = tasks;
    this.shards = shards;
    this.failureResolver = failureResolver;
    this.workerId = workerId;
    this.staleAfterSeconds = staleAfterSeconds;
  }
```

```java
      for (ExpiredShard run : shards.findExpiredRunning(task.id(), staleAfterSeconds)) {
        try {
          if (shards.markStatus(run.id(), ExecutionStatus.FAILED, workerId,
                  "owner unresponsive or lease expired")) {
            failureResolver.handle(task, run.id(), run.attempt(),
                "owner unresponsive or lease expired");
            reclaimed++;
          }
```

- [ ] **Step 4: 接线 Beans** — `Beans.java` 的 reconciler bean 加 `@Value`(文件内已 import `org.springframework.beans.factory.annotation.Value` 则复用,否则补 import):

```java
  @Bean
  Reconciler reconciler(TaskRepository tasks, ShardRepository shards,
                        FailureResolver failureResolver,
                        @Value("${scheduler.worker.stale-after-seconds:30}") int staleAfterSeconds) {
    return new Reconciler(tasks, shards, failureResolver, "reconciler", staleAfterSeconds);
  }
```

- [ ] **Step 5: 运行到通过** — 重跑 `mvn -o -q -pl scheduler-server test -Dtest=ReconcilerTest`
  预期:Tests run 10(原 8 + 新 2),0 failures,全部既有用例(含 `freshLeaseRunningShard_isLeftUntouched`,`multipleExpiredOrphans_allReclaimedAtOnce`)仍绿。

- [ ] **Step 6: 回归持久层 + server 全量** — `mvn -o -q -pl scheduler-persistence test -Dtest=JdbcShardRepositoryTest` 与 `mvn -o -q -pl scheduler-server test -Dtest=ReconcilerTest`(重跑确认),再 `mvn -o test` 全量回归。

- [ ] **Step 7: Commit**
  ```
  git add scheduler-server/src/main/java/dev/scheduler/server/reconcile/Reconciler.java \
          scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java \
          scheduler-server/src/test/java/dev/scheduler/server/reconcile/ReconcilerTest.java
  git commit -m "feat(server): Reconciler 活性优先接管 — owner 心跳失联即回收,零永久 RUNNING

  Co-Authored-By: Claude Code <noreply@anthropic.com>"
  ```

---

### Task 3: 配置默认值收窄(lease / reconcile 周期)+ README

**Files:**
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java:78-79`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`(ReconcileLoop `@Scheduled`)
- Modify: `README.md`(配置表补 `scheduler.worker.stale-after-seconds` + 值收窄注释)

**Interfaces:**
- Produces: 新默认值语义 — `lease.seconds=60`、`lease.renew-seconds=15`、`reconcile.delay-ms=15000`、新增 `scheduler.worker.stale-after-seconds=30`。健康 worker 靠续租无感。

- [ ] **Step 1: worker 侧 lease 默认收窄** — `WorkerConfig.java`:

```java
                               @Value("${scheduler.lease.seconds:60}") int leaseSeconds,
                               @Value("${scheduler.lease.renew-seconds:15}") int renewSeconds
```

- [ ] **Step 2: server 侧对账周期收窄** — `Beans.java` ReconcileLoop tick:

```java
    @Scheduled(fixedDelayString = "${scheduler.reconcile.delay-ms:15000}")
```

- [ ] **Step 3: 运行 worker + server 相关测试** — `mvn -o -q -pl scheduler-worker test -Dtest=WorkerConfigTest` 与 `mvn -o -q -pl scheduler-server test -Dtest=AdvisoryLockLeaderElectionTest`(确认默认值改不破坏既有装配/选举测试);再 `mvn -o test` 全量回归。

- [ ] **Step 4: README 配置表更新**(现状:README worker 配置表未列 lease 默认值,改为精确新增)
  - server 配置表(`SCHEDULER_LOOP_SCAN_DELAY_MS` 行附近)补两行:
    `| SCHEDULER_WORKER_STALE_AFTER_SECONDS | 30 | owner worker 心跳失联判定窗口(活性优先接管) |`
    `| SCHEDULER_RECONCILE_DELAY_MS | 15000 | 对账回收/终态汇聚周期 |`
  - worker 配置表(`SCHEDULER_LOOP_WORK_DELAY_MS` 行附近)补两行:
    `| SCHEDULER_LEASE_SECONDS | 60 | 运行分片在线续租目标(健康 worker 续租周期保住) |`
    `| SCHEDULER_LEASE_RENEW_SECONDS | 15 | worker 续租检查周期 |`

- [ ] **Step 5: Commit**
  ```
  git add scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java \
          scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java \
          README.md
  git commit -m "feat(config): 宕机收敛默认值收窄 — lease 60s/renew 15s/reconcile 15s +worker 失联窗口

  Co-Authored-By: Claude Code <noreply@anthropic.com>"
  ```

---

## Self-review(计划 vs spec)

- **§1 活性优先接管** → Task 1(查询/接口/集成测试)+ Task 2(Reconciler 接入 + server 测试):查询覆盖「租约过期 OR owner 失联」、仅 worker_id 非空判活性、随 task 遍历。✅
- **§2 宕机履约(零永久 RUNNING + 归因)** → Task 2 scanOnce:凡 RUNNING 有 owner 失联或 lease 过期者必回收(FAILED + `owner unresponsive or lease expired` outcome)→ 语义完整。✅
- **§3 参数收窄与配置化** → Task 3(lease 60/15、reconcile 15s、新增 stale 30s)。✅
- **防双跑护栏** → markStatus CAS + renewLease 0 行静默均既有,本计划不新增;Task 1/2 写死不变。✅
- **§4 扫描分批** → 明确不在本计划(spec 拆分裁决,独立短活后续)。✅

**占位符扫描:** 无 TBD/TODO/「适当处理」;所有 SQL POST 列名(`worker_id`/`id`/`last_seen`/`status`/`attempt`)、helper(`newTask`/`seedParentAndShards`/`shard`/`claimShard`/`setShard`/`seedParentWithShards`)、构造签名(`FailureResolver(shards, RetryPolicy, CLOCK)`/`Reconciler`)、默认值(`120/30/30000/30`)均从已读源码核准。唯一从 spec 继承且本计划已落实的 ruling:`worker_id IS NULL → 走租约兜底`、`归因 detail 统一`。

**类型一致:** Task 1 produces `findExpiredRunning(taskId, int)`;Task 2 consumes 同签名并传 `staleAfterSeconds`。`Reconciler` 新构造 `(…, int)` 在 Task 2 定义与 Beans 注入一致。`seedWorker(…, 120/5/…)` 与用例 `stale(30)` 数字自洽。