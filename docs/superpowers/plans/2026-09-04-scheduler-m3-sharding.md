# Sharded Parallel Execution Implementation Plan (M3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 让分布式任务调度器的运行单元从"一个 execution"演进为"一个父 run（execution header）+ N 个并行 shard"，shard 复用完整生命周期，父态由对账器原子推导，FAIL_FAST 硬编码。

**Architecture:** 复用 `execution` 表为父（run header，非认领单元），新增 `execution_shard` 为真实认领/运行原子单元（完整 DUE/RUNNING/SUCCESS/FAILED/CANCELED 生命周期 + 幂等 `(execution_id, shard_index)`）。worker 认领 shard、回写 shard（沿用所有权守卫 `markStatusOwned`）；reconciler 两手——孤儿 shard 回收 + 父汇聚（全 shard 终态时一次原子推进父）；FAIL_FAST：一 shard 耗尽失败 → RUNNING 兄弟协作取消 + DUE 兄弟直取消，父最终 FAILED。

**Tech Stack:** Java 21, Spring Boot 3 + Spring JDBC 手写 SQL（**NO JPA**）, PostgreSQL 16 Testcontainers, Flyway 9, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-09-04-scheduler-m3-sharding-design.md`（权威；plan 论证它）。

## Global Constraints

- **运行单元语义已迁移**：worker 认领/运行/回写的对象从 `execution` 变为 **`execution_shard`**；`execution` 仅作父（run header），其 `status` 只由对账器写终态（DUE→终态，不落 RUNNING），非终态读侧由 shard 推导。
- **keeper 不变式**：每个 shard 状态迁移各在 `execution_shard_outcome` 落一行；父终态推进在 `execution_outcome` 落一行。
- **所有权守卫**：worker 对 shard 的终态回写一律 `markStatusOwned`（CAS 含 `worker_id`）；reconciler 孤儿回收、FAIL_FAST 的 DUE 兄弟直编 CANCELED、父汇聚推进，均走**无条件** `markStatus`/`finalize`（CAS 状态键）。worker 绝不写父。
- **FAIL_FAST 硬编码**（用户裁定）：任一 shard 进入终 FAILED（worker 或孤儿路径，即 FailureResolver 的 DLQ 分支）即取消其余兄弟。
- **单分片重跑端点 defer**（用户裁定）：M3 不做；DLQ/requeue 重指向 **shard**（共享存表能力保留，URL 语义改为 shard 资源）。
- **shardCount=1 也走统一父+单 shard**，无快路径分叉。
- **不变量/惯例**（沿用 M1/M2）：cron 恒 6/7 字段；Java 21；不新增运行时依赖；`mvn -pl <module> install -DskipTests` 一次以同步签名；文件尾换行；测试确定性（真实 PG Testcontainers、TRUNCATE 复位、注入 Clock）。
- **M2 测试迁移基准**：运行时真相看 shard（实时）；父终态断言 = 先 `reconciler.scanOnce()` 再断言父。这是预期重构面，不是回归。

## Pre-flight rulings (写入 ledger)

- `Ruling: <DLQ/requeue 重指向 shard> — 死信单元随运行单元迁移到 shard;M2 的 /dlq 语义改为"列出 FAILED+dead_letter 的 shard",/requeue 改为对 shard(shard FAILED→DUE)。保持 M2 共享存表能力在新单元上存活;单分片重跑端点仍 defer(用户裁定)。代价:端点返回实体从 Execution 变 Shard,API 形状变化。`
- `Ruling: <父非终态由读侧推导> — 父存储 status 只在 DUE(创建)与终态(对账器/取消)间;有 RUNNING shard 的父在 Get 详情时派生显示 RUNNING,不落列。取消判定读 hasRunningShard 决定 200/202。`
- `Ruling: <createParentWithShards 单事务> — 父 + N shard 同事务创建;父幂等键 = task+triggerAt(2参);shard 幂等 = (execution_id, shard_index) 唯一键。`
- `Ruling: <FAIL_FAST DUE 兄弟直取消 CAS on status='DUE'> — 与并发 claim 竞态由状态键解决(已翻 RUNNING 的行落 RUNNING 协作分支),无泄漏(见 spec §5/§7 同款推理)。`

---

### Task 1: V3 迁移 + Shard 实体 + 父/N-shard 创建仓库

**Files:**
- Modify: `scheduler-core/src/main/java/dev/scheduler/core/IdempotencyKeys.java`（`forTrigger` 去 shardIndex，改 2 参）
- Create: `scheduler-core/src/main/java/dev/scheduler/core/Shard.java`
- Create: `scheduler-persistence/src/main/resources/db/migration/V3__sharding.sql`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java`（本任务仅：创建 + 读）
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java`
- Create: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/SchemaSmokeTest.java`

**Interfaces:**
- Consumes: `Task`（`shardCount`/`maxActiveConcurrent`）、`Execution`（父）。
- Produces: `Shard` record；`ShardRepository.createParentWithShards(long taskId, String parentKey, int shardCount) → Execution`；`Optional<Execution> findParent(long)`；`List<Shard> findShards(long executionId)`；`Optional<Shard> findShard(long shardId)`；`IdempotencyKeys.forTrigger(taskId, triggerAt)`。

- [ ] **Step 1: 改幂等键（core）**——`forTrigger(long taskId, Instant triggerAt)`（去 shardIndex 段）：`taskId + ":" + triggerAt.toEpochMilli()`。更新 M2 唯一调用点 `TriggerEngine`（本任务先只保证编译，语义改动留 Task 4）。
- [ ] **Step 2: 建 Shard record（core）**
  ```java
  package dev.scheduler.core;
  import java.time.Instant;
  /** 一个分片执行实例(真实认领/运行单元)。字段对齐 M3 spec §1.2。 */
  public record Shard(
      Long id, Long executionId, int shardIndex, String shardData, ExecutionStatus status,
      int attempt, String workerId, Instant leaseUntil, Instant nextRetryAt,
      boolean cancelRequested, boolean deadLetter, Instant startedAt, Instant finishedAt,
      String resultPayload) {
    public static Shard ofDue(long executionId, int shardIndex) {
      return new Shard(null, executionId, shardIndex, null, ExecutionStatus.DUE,
          0, null, null, null, false, false, null, null, null);
    }
  }
  ```
- [ ] **Step 3: V3 迁移（persistence resource）**——建两表与索引（spec §1.2/1.3）：
  ```sql
  CREATE TABLE execution_shard (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id      BIGINT NOT NULL REFERENCES execution(id) ON DELETE CASCADE,
    shard_index       INT NOT NULL,
    shard_data        TEXT,
    status            TEXT NOT NULL DEFAULT 'DUE',
    worker_id         TEXT,
    lease_until       TIMESTAMPTZ,
    attempt           INT NOT NULL DEFAULT 0,
    next_retry_at     TIMESTAMPTZ,
    cancel_requested  BOOLEAN NOT NULL DEFAULT false,
    dead_letter       BOOLEAN NOT NULL DEFAULT false,
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    result_payload    TEXT,
    UNIQUE (execution_id, shard_index)
  );
  CREATE INDEX idx_shard_exec_status ON execution_shard (execution_id, status);

  CREATE TABLE execution_shard_outcome (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shard_id   BIGINT NOT NULL REFERENCES execution_shard(id) ON DELETE CASCADE,
    status     TEXT NOT NULL,
    detail     TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  CREATE INDEX idx_shard_outcome_shard ON execution_shard_outcome (shard_id);
  ```
  文件尾换行。
- [ ] **Step 4: ShardRepository 接口 + JdbcShardRepository（本任务范围：创建 + 读）**——`JdbcShardRepository` 构造 `(JdbcTemplate)`，内部自建 `AbstractRepos` 同款 `TransactionTemplate`。`RowMapper<Shard>` 对齐 V3 列。`createParentWithShards` 单事务：
  ```java
  @Override public Execution createParentWithShards(long taskId, String parentKey, int shardCount) {
    // 父 execution:创建(状态 DUE,幂等 by parent_key,works on update 兼容多次扫描)
    Long parentId = jdbc.queryForObject("""
      INSERT INTO execution (task_id, status, idempotency_key, shard_index, shard_count)
      VALUES (?, 'DUE', ?, 0, ?)
      ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
      RETURNING id""", Long.class, taskId, parentKey, shardCount);
    final long pid = parentId;
    tx.executeWithoutResult(s -> {
      int existing = jdbc.queryForObject(
          "SELECT count(*) FROM execution_shard WHERE execution_id=?", Integer.class, pid);
      if (existing == 0) { // 首次物化:插入 N 个 shard(幂等:父重复创建不重复插)
        jdbc.batchUpdate("""
          INSERT INTO execution_shard (execution_id, shard_index, status)
          VALUES (?, ?, 'DUE')""",
          java.util.stream.IntStream.range(0, shardCount)
              .mapToObj(i -> new Object[]{pid, i}).toList());
      }
    });
    return findParent(pid).orElseThrow();
  }
  ```
  `findParent`/`findShards`/`findShard` = `SELECT * ...` + MAP。
- [ ] **Step 5: 测试（JdbcShardRepositoryTest + SchemaSmokeTest）**——新建 `JdbcShardRepositoryTest extends AbstractPostgresTest`（shape 镜像 JdbcExecutionRepositoryTest 的 `clean()` + 任务 helper）。用例：
  - `createParentAndShards_oneParentWithNShards`：`shardCount=3` → 1 父 + 3 shard，`shard_index = 0..2`，全 DUE，父 `shard_count=3`。
  - `createParent_sameKey_idempotent_noDupShards`：同 parentKey 两次 → 同一父 id，shard 总数仍 N（不重复插）。
  - `findShards_orderedByIndex`：按 shard_index 升序返回。
  - `createParent_shardCountOne_stillParentPlusOneShard`：N=1 → 1 父 + 1 shard（统一模型）。
  - SchemaSmokeTest：加两表存在断言（模式沿用既有列存在查询）。
- [ ] **Step 6: 编译过 + 单线程绿**。`mvn -q -pl scheduler-core install -DskipTests`（core 签名变）后 `mvn -q -pl scheduler-persistence install -DskipTests`。跑序列化：`mvn -q -pl scheduler-persistence test`。
- [ ] **Step 7: 提交**。`git commit -m "feat(persistence): V3 execution_shard table + Shard entity + parent/N-shard creation repo"`

---

### Task 2: Shard 生命周期持久层（worker 侧）

**Files:**
- Modify: `scheduler-persistence/.../ShardRepository.java`、`JdbcShardRepository.java`（追加生命周期方法）
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java`

**Interfaces:**
- Consumes: Task 1 的 `Shard`/表/repo。
- Produces（worker 侧,作用对象=shard,语义逐条镜像 M2 `JdbcExecutionRepository` 对应方法,改表/join）：
  - `Optional<Shard> findCandidate(long taskId)`（DUE shard,重试闸）
  - `boolean claim(long shardId, long taskId, String workerId, Instant leaseUntil, int maxConcurrent)`
  - `long countActive(long taskId)`
  - `List<ExpiredShard> findExpiredRunning(long taskId)`（record `ExpiredShard(long id, int attempt)`）
  - `boolean markStatus(long shardId, ExecutionStatus to, String workerId, String detail)`（严格,非法迁移抛）
  - `boolean markStatusOwned(long shardId, ExecutionStatus to, String ownerWorkerId, String detail)`（容忍,worker_id 守卫)
  - `void scheduleRetry(long shardId, Instant retryAt, String detail)`
  - `void markDeadLetter(long shardId, String detail)`
  - `boolean isCancelRequested(long shardId)`

- [ ] **Step 1: `findCandidate`（shard）**——`JOIN execution e ON e.id=s.execution_id WHERE e.task_id=? AND s.status='DUE' AND (s.next_retry_at IS NULL OR s.next_retry_at<=now()) ORDER BY s.id LIMIT 1`，MAP。
- [ ] **Step 2: `claim`（shard）**——镜像 M2：`WITH active AS (SELECT count(*) c FROM execution_shard s JOIN execution e ON e.id=s.execution_id WHERE e.task_id=? AND s.status='RUNNING' AND s.lease_until>now()) UPDATE execution_shard s SET status='RUNNING', worker_id=?, lease_until=?, s.attempt=s.attempt+1, started_at=COALESCE(s.started_at, now()) FROM active, execution e WHERE s.id=? AND s.status='DUE' AND e.id=s.execution_id AND e.task_id=? AND active.c < ?`；tx 内落 `execution_shard_outcome('RUNNING','claim')`；返回 updated==1。
- [ ] **Step 3: `countActive`/`findExpiredRunning`（shard）**——COUNT RUNNING shard（join task）；RUNNING shard `lease_until<=now()` → `ExpiredShard(id, attempt)`。
- [ ] **Step 4: `markStatus`（shard,严格）**——`findShard` 读现值 + `ExecutionTransitions.canTransition` 前置校验（非法抛）；tx：`UPDATE execution_shard SET status=?, worker_id=COALESCE(?,worker_id), finished_at=CASE WHEN ? IN ('SUCCESS','FAILED','CANCELED') THEN now() ELSE finished_at END WHERE id=? AND status=?`（CAS 状态键）；0 行→log+return false；落 `execution_shard_outcome`；return updated==1。
- [ ] **Step 5: `markStatusOwned`（shard,容忍）**——同名逻辑,额外 `AND worker_id=?`(owner)；非法迁移（行已在他处终态）或 CAS 0 → return false 静默不落 outcome（M2 fix-wave 契约逐字）。
- [ ] **Step 6: `scheduleRetry`/`markDeadLetter`/`isCancelRequested`（shard）**——`scheduleRetry`: `UPDATE execution_shard SET status='DUE', next_retry_at=? WHERE id=? AND status='FAILED'` + DUE outcome（CAS 0 静默）。`markDeadLetter`: `SET dead_letter=true WHERE id=? AND status='FAILED'`（无 outcome）。`isCancelRequested` 读列。
- [ ] **Step 7: 测试（JdbcShardRepositoryTest）**——逐条镜像 M2 语义写 shard 用例（helper：`seedParentAndShards(taskId, shardCount)` 建父+shards、`claimShard(shardIndex)`）：
  - `findCandidateReturnsDueShard` / `findCandidateSkipsFutureRetryShard` / `findCandidateReturnsNullRetryShard`
  - `claimThenSecondFailsAndOutcome`（claim 落 RUNNING outcome；二次 claim false）
  - `countActiveIsOneAfterClaim`；`excessiveConcurrencyRejectsSecondShard`（maxActiveConcurrent=1 时第二 shard 拒）
  - `markStatusWritesSuccessAndOutcome` / `markStatusOwnedWrongOwner_suppressedNoClobber` / `markStatusOwnedCorrectOwner_singleOutcome` / `markStatusOwnedOnAlreadyTerminal_false`
  - `illegalTransitionThrows`（shard DUE→SUCCESS 抛）
  - `scheduleRetryMovesFailedToDueWithOutcome` / `scheduleRetryOnNonFailedSilent` / `markDeadLetterSetFlagOnFailed` / `isCancelRequested` 读写
- [ ] **Step 8: 编译绿 + `mvn -q -pl scheduler-persistence test` 全绿**（既有 execution 用例不受影响,仅加新用例）。
- [ ] **Step 9: 提交**。`git commit -m "feat(persistence): shard lifecycle repo (candidate/claim/retry/dlq/cancel-flag, ownership guard)"`

---

### Task 3: 父汇聚 + FAIL_FAST + 父取消 + shard DLQ/requeue 持久层

**Files:**
- Modify: `ShardRepository.java`、`JdbcShardRepository.java`
- Test: `JdbcShardRepositoryTest.java`

**Interfaces:**
- Consumes: Task 1/2 的表与 `Shard`。
- Produces（仅对账器/controller 用）：
  - `List<Long> parentsNeedingAggregation()`
  - `boolean finalizeParent(long parentId, ExecutionStatus terminal, String detail)`（父侧 `execution_outcome` 落行）
  - `void cancelSiblings(long shardId, String detail)`（FAIL_FAST）
  - `void cancelParentImmediate(long parentId)`（DUE 父直取消）
  - `boolean requestCancelParent(long parentId)`（RUNNING 父协作取消;返回是否有 RUNNING shard 被置位）
  - `boolean hasRunningShard(long parentId)`
  - `List<Shard> findDeathLetterShards()`
  - `boolean requeueShard(long shardId)`

- [ ] **Step 1: `parentsNeedingAggregation`**——非终态父且有 ≥1 终态 shard：
  ```sql
  SELECT e.id FROM execution e
   WHERE e.status='DUE'
     AND EXISTS (SELECT 1 FROM execution_shard s WHERE s.execution_id=e.id
                 AND s.status IN ('SUCCESS','FAILED','CANCELED'))
   ORDER BY e.id
  ```
- [ ] **Step 2: `finalizeParent`**——父 CAS on `status='DUE'`（父只 DUE→终态,从不存 RUNNING）：
  ```java
  @Override public boolean finalizeParent(long parentId, ExecutionStatus terminal, String detail) {
    return tx.execute(status -> {
      int updated = jdbc.update(
          "UPDATE execution SET status=?, finished_at=now() WHERE id=? AND status='DUE'",
          terminal.name(), parentId);
      if (updated == 0) return false;
      jdbc.update("INSERT INTO execution_outcome (execution_id, status, detail) VALUES (?,?,?)",
          parentId, terminal.name(), detail);
      return true;
    });
  }
  ```
  （`tx.execute` 返回值变体需自建 TransactionTemplate 的 `execute(TransactionCallback)`。）幂等：0 行=已推进/已终态，proceed。
- [ ] **Step 3: `cancelSiblings(shardId, detail)`**——FAIL_FAST，单事务：
  ```java
  tx.executeWithoutResult(s -> {
    // RUNNING 兄弟置协作取消信号
    jdbc.update("""UPDATE execution_shard SET cancel_requested=true
        WHERE execution_id=(SELECT execution_id FROM execution_shard WHERE id=?)
          AND id<>? AND status='RUNNING'""", shardId, shardId);
    // DUE 兄弟直编 CANCELED(CAS on status='DUE';并发已 claim→RUNNING 的行走 RUNNING 分支)+ outcome
    jdbc.update("""
      WITH targets AS (
        SELECT id FROM execution_shard
         WHERE execution_id=(SELECT execution_id FROM execution_shard WHERE id=?)
           AND id<>? AND status='DUE')
      , upd AS (UPDATE execution_shard SET status='CANCELED', finished_at=now()
                WHERE id IN (SELECT id FROM targets) RETURNING id)
      INSERT INTO execution_shard_outcome (shard_id, status, detail)
      SELECT id, 'CANCELED', ? FROM upd""", shardId, shardId, detail);
  });
  ```
- [ ] **Step 4: `requestCancelParent`/`cancelParentImmediate`/`hasRunningShard`**——
  - `requestCancelParent(parentId)`：tx：RUNNING shard `cancel_requested=true`；DUE shard `CANCELED`+outcome(`'cascade cancel'`)（同 Step 3 CTE 形状，execution_id=?）；父 `UPDATE execution SET cancel_requested=true WHERE id=? AND status='DUE'`；返回 `hasRunningShard(parentId)`（事务后读）。
  - `cancelParentImmediate(parentId)`：tx：父 `UPDATE execution SET status='CANCELED', finished_at=now() WHERE id=? AND status='DUE'`（+父 outcome `'cancel parent'`）；全部非终态 shard → CANCELED + outcome（`WHERE execution_id=? AND status IN ('DUE','RUNNING')`）。
  - `hasRunningShard(parentId)`：`SELECT count(*) FROM execution_shard WHERE execution_id=? AND status='RUNNING'` > 0。
- [ ] **Step 5: `findDeathLetterShards`/`requeueShard`**——DLQ 读：`WHERE status='FAILED' AND dead_letter ORDER BY id`；`requeueShard`：`UPDATE execution_shard SET status='DUE', attempt=0, next_retry_at=NULL, dead_letter=false WHERE id=? AND status='FAILED'` + shard DUE outcome(`'requeue'`)；CAS 0 → false。
- [ ] **Step 6: 测试（JdbcShardRepositoryTest）**——
  - `aggregateParent_allShardsSuccess_parentSuccess`：3 shard 全 SUCCESS(+outcome)→ `finalizeParent` 全 SUCCESS 后 `parentsNeedingAggregation` 空、父 SUCCESS、`execution_outcome` 有 SUCCESS。
  - `aggregateParent_oneFailed_parentFailed`:1 shard FAILED+其余终态 → 父 FAILED。
  - `finalizeParent_idempotent_CAS0Skips`：父已 SUCCESS 再 finalize → false 无多余 outcome。
  - `failFast_cancelsRunningAndDueSiblings`:shard0 DLQ 后 `cancelSiblings` → RUNNING 兄弟 cancel_requested=true、DUE 兄弟 CANCELED + outcome；`hasRunningShard` 语义。
  - `requestCancelParent_flagsRunningAndCancelsDue`/`cancelParentImmediate_cancelsParentAndShards`/`requeueShard_movesFailedToDue_reset`。
- [ ] **Step 7: 编译绿 + persistence 全绿**。
- [ ] **Step 8: 提交**。`git commit -m "feat(persistence): parent aggregation + FAIL_FAST + parent-cancel + shard DLQ/requeue"`

---

### Task 4: 触发/手动触发 → 父+N-shard

**Files:**
- Modify: `scheduler-server/.../trigger/TriggerEngine.java`
- Modify: `scheduler-server/.../web/TaskController.java`（trigger 端点）
- Test: `scheduler-server/src/test/java/dev/scheduler/server/trigger/TriggerEngineTest.java`

**Interfaces:**
- Consumes: Task 1 `createParentWithShards`、`IdempotencyKeys.forTrigger(taskId, triggerAt)`（2参）。
- Produces: 触发一次 = 一父 + N shard；手动触发同。

- [ ] **Step 1: TriggerEngine**——在 `scanOnce` 命中分支，`ShardRepository.creates`：注入 `ShardRepository shards`，替换 `executions.createDue(Execution.ofDue(...))` 为 `shards.createParentWithShards(t.id(), IdempotencyKeys.forTrigger(t.id(), fired.toInstant()), t.shardCount())`。构造签名改 `TriggerEngine(TaskRepository, ExecutionRepository, ShardRepository, LeaderElection, Clock)`。
- [ ] **Step 2: TaskController.trigger**——注入 `ShardRepository`；`long pid = shards.createParentWithShards(t.id(), key, t.shardCount()); Execution e = shards.findParent(pid).orElseThrow(...)`（返回父）。`key ="manual:"+t.id()+":"+UUID.randomUUID()`（不变）。
- [ ] **Step 3: Beans**——`TriggerEngine` bean 注入 `shards`；`TaskController` bean 是 controller 由 Spring 扫（构造加参即可）。补 `@Bean ShardRepository shardRepository(JdbcTemplate jdbc)`。
- [ ] **Step 4: 测试（TriggerEngineTest）**——沿用既有固定时钟；`scanOnce` 命中后断言：恰 1 父（execution 表）+ `t.shardCount()` 条 DUE shard；同 tick 再扫幂等（父不变、shard 不增）。更新既有触发用例断言对象（若原断言"一父 execution DUE"，改为"一父 + N shard"）。
- [ ] **Step 5: 编译绿**（core/persistence 已装）。`mvn -q -pl scheduler-server test` 中 TriggerEngineTest 绿（既有 execution-level 集成用例留 Task 7/8 一起改，可能红——本任务如触发既有集成红,在 Task 7 收敛,标注）。
- [ ] **Step 6: 提交**。`git commit -m "feat(server): trigger & manual-trigger fan out parent + N shards"`

---

### Task 5: ExecutorWorker + HandlerContext 迁移到 shard

**Files:**
- Modify: `scheduler-server/.../handler/HandlerContext.java`（增 shard 字段）
- Modify: `scheduler-server/.../execute/ExecutorWorker.java`（认领/运行/回写全移 shard）
- Modify: `scheduler-server/.../config/Beans.java`（executorWorker 注入 ShardRepository）
- Test: `scheduler-server/src/test/java/dev/scheduler/server/execute/ExecutorWorkerTest.java`

**Interfaces:**
- Consumes: Task 2 的 `ShardRepository` 生命周期、`FailureResolver`（Task 6 才移 shard;本任务保持 `FailureResolver.handle` 现签名占位,Task 6 落地——**先以 `FailureResolver` 现签名编译**,Task 6 原子切换）。
- Produces: `HandlerContext(long executionId, long shardId, int shardIndex, int shardCount, String shardData, String args, CancellationToken token)` + `static of(...)` 便利。

- [ ] **Step 1: HandlerContext 扩字段**——record 加 `shardId/shardIndex/shardCount/shardData`（放 executionId 之后）。`of(long executionId, long shardId, int shardIndex, int shardCount, String shardData, String args)` 便利（默认 token false）。M2 既有 `of(long, String)` 签名依调用点迁移。
- [ ] **Step 2: ExecutorWorker**——构造换 `ShardRepository shards`（替换 `ExecutionRepository execs`，若 worker 只需 shard 侧；handler 派发/失败仍走原先）。`workOne()`：
  ```
  for (Task t : tasks.findAll()) {
    var cand = shards.findCandidate(t.id());
    if (cand.isEmpty()) continue;
    Instant lease = clock.instant().plusSeconds(60);
    long parentId = cand.get().executionId();     // 父 id(由 shard 携回,可用 join 读父)
    if (!shards.claim(cand.get().id(), t.id(), workerId, lease, t.maxActiveConcurrent())) continue;
    // 运行前取消检查:shard.cancelRequested → markStatusOwned(shard, CANCELED, "cancelled before run")
    // HandlerContext(cand的executionId, shardId, shardIndex, shardCount, shardData, 父args, token)
    //   —— args：父 args 此处从 findParent(parentId).args() 读;demo handler 不需 args
    // 成功/失败/Cancellation 回写均经 ShardRepository(candidate/owned),FAILED 门控后 failureResolver.handle(...)
  }
  ```
  失败分支与 M2 fix-wave 一致（`markStatusOwned(shard, FAILED)` 门控后才 `failureResolver.handle(shard,...)`）。**删除 worker 内所有对 execution 的写**（不再 markStatus execution）。
- [ ] **Step 3: Beans.executorWorker**——注入 `ShardRepository`（与 `FailureResolver`、`Clock`、`handlerRegistry` 并列）。
- [ ] **Step 4: 测试（ExecutorWorkerTest）**——`worker()` helper 改用 `JdbcShardRepository`；helper 建父+shards。改写/新增：
  - `happyPath_claimsShard_runsWithShardContext_writesSuccess`：单 shard 任务;断言 handler 收到 `shardIndex=0/shardCount=N`;shard 变 SUCCESS;父(经 reconcile)终 SUCCESS。
  - `handlerReceivesShardIndexAndCount`：两 shard 分别 workOne → 两次 handler 各收 index 0/1。
  - `handlerThrowsRetryable_schedulesShardRetry` / `handlerThrowsExhausted_failsAndDeadLetters_shard` / `handlerThrowsPatternMismatch_failsShard`：断言对象=shard。
  - `staleOwnerShard_cannotClobberReownedShard`：模拟 shard 被 reconcile/重认领后旧 worker 迟到回写被抑（CAS worker_id）——断言 shard 不变、无多余 outcome。
  - 协取取消（pre-run / Cancellation / after-run）断言 shard CANCELED。
- [ ] **Step 5: 编译绿 + `mvn -q -pl scheduler-server test`（ExecutorWorkerTest 绿）**。既有集成红态记 Task 7。
- [ ] **Step 6: 提交**。`git commit -m "feat(server): ExecutorWorker claims/runs/writes shards; HandlerContext carries shard context"`

---

### Task 6: FailureResolver + Reconciler 到 shard（含父汇聚）

**Files:**
- Modify: `scheduler-server/.../retry/FailureResolver.java`（对象=shard + FAIL_FAST）
- Modify: `scheduler-server/.../reconcile/Reconciler.java`（孤儿 shard + 父汇聚）
- Modify: `scheduler-server/.../config/Beans.java`（reconciler/failureResolver 注入 ShardRepository）
- Test: `scheduler-server/src/test/java/dev/scheduler/server/reconcile/ReconcilerTest.java`

**Interfaces:**
- Consumes: Task 2/3 的 `ShardRepository`、`RetryPolicy`。
- Produces: `FailureResolver.handle(Task, long shardId, int attempt, String detail)`（对象需为 shard）——DLQ 分支追加 `cancelSiblings(shardId,"fail_fast")`。

- [ ] **Step 1: FailureResolver**——继续判 `shouldRetry`→`scheduleRetry(shardId,...)` 否则 `markDeadLetter(shardId,...)` + `cancelSiblings(shardId,"fail_fast")`（FAIL_FAST 硬编码）。签名对象语义注释改为 shard。前置 FAILED 仍由调用方落（worker `markStatusOwned`、reconciler `markStatus`）。
- [ ] **Step 2: Reconciler**——构造 `(TaskRepository, ShardRepository, FailureResolver, String workerId)`。`scanOnce()`：
  1. 孤儿 shard 回收：每 task `shards.findExpiredRunning(taskId)` → try `shards.markStatus(shardId, FAILED, workerId,"lease expired")` 门控成功后 `failureResolver.handle(task, shardId, run.attempt(), "lease expired")`,count++；（catch `IllegalStateException` 跳过,不中止整批）。
  2. 父汇聚：`for (Long pid : shards.parentsNeedingAggregation())` → 读 `shards.findShards(pid)`,按 status 归类（`isTerminal` 全部为终态才算）：
     ```
     boolean anyFailed = shards.stream().anyMatch(s -> s.status()==FAILED);
     ExecutionStatus parentTerminal = anyFailed ? FAILED
         : shards.stream().anyMatch(s -> s.status()==CANCELED) ? CANCELED : SUCCESS;
     String detail = anyFailed ? "shard failed" : (parentTerminal==CANCELED ? "shard cancelled" : "all shards ok");
     shards.finalizeParent(pid, parentTerminal, detail);
     ```
     幂等：`finalizeParent` CAS 0 则已推进,跳过。
- [ ] **Step 3: Beans**——`failureResolver` 注入 `ShardRepository`；`reconciler` 注入 `ShardRepository` 替换 `ExecutionRepository`。`ReconcileLoop` 不变。
- [ ] **Step 4: 测试（ReconcilerTest）**——helper：`seedParentWithShards(taskId, statuses...)` 造父+shards 相态。用例：
  - `orphanExpiredShard_isReclaimedToDlqOrRetry`（孤儿 shard 回收 → shard FAILED (+DLQ 或 DUE),语义镜像 M2）。
  - `allShardsSuccess_parentBecomesSuccess`：2 shard SUCCESS → `scanOnce` → 父 SUCCESS,`execution_outcome` 有 SUCCESS。
  - `oneShardFailed_parentBecomesFailed_andFailFast_cancelsSiblings`：1 FAILED + 1 RUNNING + 1 DUE → `scanOnce` → FAIL_FAST cancelSiblings(RUNNING→cancel_requested、DUE→CANCELED)、父 FAILED。
  - `oneShardCancelled_parentBecomesCancelled`（无 FAILED 时 CANCELED 优先于 SUCCESS）。
  - `parentNotAllTerminal_staysNonTerminal`：1 SUCCESS + 1 RUNNING + 1 DUE → 父不动。
- [ ] **Step 5: 编译绿 + `mvn -q -pl scheduler-server test`（ReconcilerTest 绿）**。
- [ ] **Step 6: 提交**。`git commit -m "feat(server): Reconciler reclaims orphan shards + derives parent terminal; FailureResolver FAIL_FAST"`

---

### Task 7: 控制面 → 取消级联 + 详情含 shards + DLQ/requeue(到 shard)

**Files:**
- Modify: `scheduler-server/.../web/ExecutionController.java`
- Modify: `scheduler-server/.../service/ExecutionQueryService.java`（详情含 shards）
- Modify: `scheduler-server/.../web/TaskController.java`（若需 shard 相关）
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`（本任务先迁与取消/详情/DLQ 相关的断言）

**Interfaces:**
- Consumes: Task 3 的 cancel/requeue/dlq repo 方法、Task 5 `Shard`。
- Produces: 端点新语义——`GET /executions/{id}` 返回父 + shards；`POST /executions/{id}/cancel` 父级级联；`GET /executions/dlq` 列 shard；`POST /shards/{shardId}/requeue`。

- [ ] **Step 1: ExecutionController**——注入 `ShardRepository shards`。
  - `cancel`：`Execution e = executions.findById(id)...`（父）。`if e.status terminal(CANCELED)→200 幂等;isTerminal→409`。父非终态：`if (shards.hasRunningShard(id)) { shards.requestCancelParent(id); return 202 + 父现态 }`（有 ≥1 RUNNING shard → 协作取消）`else { shards.cancelParentImmediate(id); return 200 + 父现态(CANCELED) }`。
  - `dlq`：返回 `shards.findDeathLetterShards()`（List<Shard>）。
  - 新增 `POST /shards/{shardId}/requeue`：`Shard s=(shards.findShard(id).orElseThrow(404)); if (s.status()!=FAILED) 409; shards.requeueShard(id); return 200+现态`。
  - 保留类 javadoc 更新。
- [ ] **Step 2: ExecutionQueryService.get**——父详情改为父 + shards：新增返回结构（如 `ExecutionDetail(Execution parent, List<Shard> shards)`）或经 `/executions/{id}` 控制器拼。取 `shards.findShards(id)` 附上；若父非终态且有 RUNNING shard,派生把父 status 显示为 RUNNING（读取只映射,不写库）——以最小方式：详情 DTO `{id, taskId, status(派生), shardCount, shards:[...]}`。
- [ ] **Step 3: 测试（ApiIntegrationTest 迁移部分）**——把 `cancelDue/cancelRunning/cancelSuccess/dlqGetAndRequeue/requeue*` 断言迁到新语义：父 cancel 级联断言 shard；`/dlq` 断言 shard(detail 携带父 id+shard_index)；`/shards/.../requeue` 断言 shard DUE。种子 helper 建父+shards。
- [ ] **Step 4: 编译绿 + `mvn -q -pl scheduler-server test`**（本任务涉及的集成用例绿）。
- [ ] **Step 5: 提交**。`git commit -m "feat(server): control-plane — parent-cancel cascade, run detail with shards, shard-scoped DLQ/requeue"`

---

### Task 8: 集成收敛 + 指标（E2E 全迁移 + FAIL_FAST/发散端到端 + prometheus）

**Files:**
- Modify: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`（全面迁移 + 新增）
- Modify: `scheduler-server/.../config/Beans.java`（指标对 shard）
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: Task 1-7 全落地端点/仓库。
- Produces: 全模块绿 + E2E 闭环证明。

- [ ] **Step 1: 指标 → shard**——`Beans.schedulerMetrics`：`scheduler_active_runs` 计数 → `shards.countActive(taskId)`；`scheduler_due_queue_max_age_seconds` → DUE shard 最深年龄（`SELECT EXTRACT(EPOCH FROM (now()-COALESCE(MAX(created_at is absent),now())))` — shard 无 created_at,改 `COALESCE(MAX(s.id),0)` 或按 ordering 用 id 近似——取"最老 DUE shard 的 age"用 `now() - MIN(上一条从未有 created_at)`；**用近似约定：DUE shard 无创建列,以 `MIN(id)` 对应父 created_at 近似**：`SELECT EXTRACT(EPOCH FROM (now() - (SELECT created_at FROM execution e JOIN execution_shard s ON e.id=s.execution_id WHERE s.status='DUE' AND e.task_id=? ORDER BY s.id LIMIT 1)))`）。实现取该近似查询。
- [ ] **Step 2: ApiIntegrationTest 全面迁移**——既有 13 例中每处"断言父 execution 状态/outcome"改为：
  - 触发 → 1 父 + `shardCount` 条 DUE shard（断言 shard 表）。
  - `workOne()` → 断言 **shard** SUCCESS（实时真相）；父终态 = 先 `reconciler.scanOnce()` 后断言父 SUCCESS/FAILED/CANCELED。
  - 手动触发/扫描成功/取消/DLQ/重试/DLQ-闭环/协作取消：照上迁。
- [ ] **Step 3: 新增 E2E**
  - `craftShardFanOut_allSuccess_parentSuccess`：shardCount=3 任务 → `scanOnce` → 3 个 `workOne` → 3 条 shard SUCCESS → reload `reconciler` → 父 SUCCESS；`GET /executions/{id}` 详情含 3 shards。
  - `failFast_endToEnd_oneShardFails_cancelsSiblings_parentFailed`：shardCount=3,任务 handler 使 shard0 抛耗尽失败(maxRetries=0)；shard1/2 运行或 DUE → FAIL_FAST → 父 FAILED;`GET /dlq` 含 shard0(FAILED+dead_letter);已完成 shard SUCCESS outcome 保留。
  - `cooperativeCancel_parentLevel_cascadesToShards`：父 RUNNING(或 shard RUNNING)cancel → 202 → RUNNING shard cancel_requested、DUE shard CANCELED。
  - `prometheus_shardMetrics`：断言 `scheduler_active_runs` 随 RUNNING **shard** 计数（沿用 METRICS_TASK_ID 系列;绑定档 metrics 断言改 shard 口径）。
  - 后续二现:若优先级高可加 `requeueFailedShard_thenRerunSiblingSuccess`(预置 FAILED shard → requeue → workOne → SUCCESS)——非阻塞,可 defer。
- [ ] **Step 4: 载入全模块测试**——`mvn -q -pl scheduler-server test` + `mvn -q -pl scheduler-persistence test` 全绿；`mvn test` 全根绿。
- [ ] **Step 5: 提交**。`git commit -m "test(server): M3 end-to-end shard fan-out, FAIL_FAST, parent-cancel, shard metrics + migration of M2 assertions"`

---

## Self-review

- **Spec 覆盖**：§1.2/1.3 表与 outcome（Task 1/2/3）· §2 触发发散（Task 4）· §3 worker shard 运行时（Task 5）· §4/§6 FAIL_FAST + 对账（Task 3/6）· §7 取消级联（Task 3/7）· §8 端点/指标（Task 7/8）· §9 M2 测试迁（Task 7/8）。无需增任务。
- **占位符**：无 "TBD/TODO/类似 Task N 照抄"。镜像方法均注明"映射 M2 同名方法 + 改表/join"的确定 delta。
- **类型一致**：`ShardRepository` 方法签名在 Task 2/3 固定、Task 5-8 引用同一组；`ExpiredShard(id, attempt)` 全程同名；`IdempotencyKeys.forTrigger` 2 参在 Task 1 改、Task 4 用、唯 M2 调用点在 Task 1 先保证编译。`HandlerContext` 字段序在 Task 5 定、Task 5 测试/worker 用同一序。
- **失败路径次序**：worker FAILED 落 shard（`markStatusOwned`）→ 门控 → `FailureResolver.handle(shard)` → DLQ 时 `cancelSiblings`（FAIL_FAST）→ 父由 Reconciler 汇聚——次序在 Task 5/6 明写。