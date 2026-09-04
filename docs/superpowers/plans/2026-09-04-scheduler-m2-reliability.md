# Scheduler M2 — Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完成失败重试（backoff/上限/可重试模式）、孤儿 executed Reconciler 对账、RUNNING 协作取消（CancellationToken）、死信 DLQ + requeue，并把 GET /executions 裸 SQL 抽到 service 层。

**Architecture:** 全部沿用 DB 认领、无 MQ、无 JPA（Spring JDBC 手写 SQL + Flyway）。重试「同行复用」走既有 `FAILED→DUE` 迁移；`attempt` 由 `claim` 累加；reconciler leader 门控 + CAS 幂等回收过期租约 RUNNING；协作取消用 DB 标志 `cancel_requested`；死信用布尔列 `dead_letter`。四个判重试/取消点共用一套小决策类（RetryPolicy / CancellationToken）。

**Tech Stack:** Java 21, Spring Boot 3.2.5 (spring-context 6.1.x, spring-jdbc), Flyway 9.22, PostgreSQL 16 Testcontainers (Docker API >= 1.40), JUnit 5, Micrometer。

**Spec:** `docs/superpowers/specs/2026-09-04-scheduler-m2-reliability-design.md`（本计划论证它。M1 计划：`docs/superpowers/plans/2026-09-03-scheduler-m1-core-scheduling.md` 的零件已合入 main。）

## Global Constraints

（来自 spec 与 M1 既有约定，每任务隐含要求此节。）

- **Java 21、JUnit 5、Spring JDBC 手写 SQL，禁止 JPA/ORM。**
- **审计不变量**：每次状态迁移必须 INSERT 一条 `execution_outcome`（claim 转 RUNNING、markStatus 转 DUE/terminal 均已写；M2 的 `FAILED→DUE` 重试、reconciler 回收、requeue 同样要落 outcome）。
- **所有权纪律**：只有「认领成功的 worker」回写 execution；reconciler 回收过期租约用 `UPDATE ... WHERE status='RUNNING'` CAS，0 行=他方已回收，静默跳过不落误导 outcome（沿用 M1 markStatus 策略）。
- **Cron 6 字段**（sec min hour dom mon dow）；测试/种子一律 `0 */5 * * * *`。
- **测试确定性**：`scheduler.loop.enabled=false`、`scheduler.reconcile.enabled=false` 关停异步循环；`@Primary MutableClock` 钉时间；每例 `@BeforeEach` TRUNCATE `app_task, execution, execution_outcome RESTART IDENTITY CASCADE`（新加列默认足够）。
- **可注入 Clock**：所有基于墙时逻辑（lease、next_retry_at 计算、cancel 轮询）经注入的 `java.time.Clock`，不直读 `System.currentTimeMillis()`。
- **代码风格**：2-space 缩进、record + 构造注入、方法级 `@Transactional`？否——用 `TransactionTemplate`（沿用 M1），不在持久化方法放大招。
- **真 PG 测试**：每条 SQL 行为必须对 Testcontainers 真 Postgres 断言，不 mock。

---

### Task 1: Flyway V2 迁移（加 cancel_requested / dead_letter 列）

**Files:**
- Create: `scheduler-persistence/src/main/resources/db/migration/V2__reliability.sql`
- Modify: `scheduler-persistence/src/test/java/dev/scheduler/persistence/SchemaSmokeTest.java`（或新建 `MigrationSmokeTest.java`）

**Interfaces:**
- Produces: `execution` 增加两列——`cancel_requested BOOLEAN NOT NULL DEFAULT false`、`dead_letter BOOLEAN NOT NULL DEFAULT false`。后续 Task 4/6/7 依赖此列存在。

- [ ] **Step 1: 写失败测试**（在 SchemaSmokeTest 或新 MigrationSmokeTest 里断言两列存在且默认 false）— 用 Flyway migrate 后的 JDBC 查 information_schema：
  `SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='execution' AND column_name='dead_letter'` 得 `boolean`；`cancel_requested` 同理。并断言「新插入一行 execution 时 `dead_letter`/`cancel_requested` 取 false」。
- [ ] **Step 2: 运行确认失败** — 迁移文件不存在，两列查询为空 → FAIL。
- [ ] **Step 3: 创建 V2 迁移**：
  ```sql
  ALTER TABLE execution ADD COLUMN cancel_requested BOOLEAN NOT NULL DEFAULT false;
  ALTER TABLE execution ADD COLUMN dead_letter      BOOLEAN NOT NULL DEFAULT false;
  ```
- [ ] **Step 4: 运行通过**。Run: `mvn -q -pl scheduler-persistence test -Dtest=SchemaSmokeTest`
- [ ] **Step 5: 提交**。`git add scheduler-persistence && git commit -m "feat(persistence): V2 reliability columns cancel_requested/dead_letter"`

---

### Task 2: 持久化 — scheduleRetry + markDeadLetter 仓库方法

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ExecutionRepository.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcExecutionRepository.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcExecutionRepositoryTest.java`

**Interfaces:**
- Consumes: V2 列（Task 1）；`Execution` record 的 `nextRetryAt`；状态 `FAILED→DUE` 合法。
- Produces:
  - `void scheduleRetry(long id, java.time.Instant retryAt, String workerId, String detail)` — 同事务：`UPDATE execution SET status='DUE', next_retry_at=? WHERE id=? AND status='FAILED'`（CAS）；成功则 INSERT outcome(`DUE`, detail=原始失败原因)；0 行=竞态/非 FAILED，静默跳过（DEBUG log）。
  - `void markDeadLetter(long id, String detail)` — 同事务：`UPDATE execution SET dead_letter=true WHERE id=? AND status='FAILED'`（CAS）；0 行静默跳过。
  - **签名裁决（对 spec 细化）**:`scheduleRetry` 接收**已算好的 `Instant retryAt`**，而非 `(backoffMs, attempt)` —— 延迟计算放到纯 `RetryPolicy`（Task 3），仓库保持“哑存储”只写值；这样 next_retry_at 用注入 clock 算、测试才确定（与 TriggerEngine 的 clock 裁决一脉相承）。`markDeadLetter` 由 worker（Task 4）在判不可重试时调用。

- [ ] **Step 1: 写失败测试**（JdbcExecutionRepositoryTest 新增）:
  - `scheduleRetry`：手工插一条 FAILED 的 execution → 调 `scheduleRetry(id, now+10s, "w", "boom")` → 断言转 DUE、`next_retry_at≈now+10s`、outcome 有一条 `DUE`、`attempt` 不变、`worker_id` 不变。
  - `scheduleRetry` 对非 FAILED（RUNNING）行 CAS 0 行：状态不动、无 outcome——测后仍 RUNNING。
  - `markDeadLetter`：FAILED 行 → `dead_letter=true`；RUNNING 行 → 不动。
- [ ] **Step 2: 运行确认失败**。Run: `mvn -q -pl scheduler-persistence test -Dtest=JdbcExecutionRepositoryTest`
- [ ] **Step 3: 实现**（仿照现有 `markStatus` 的 `TransactionTemplate` + CAS-0-行-静默模式）。
- [ ] **Step 4: 运行通过**。
- [ ] **Step 5: 提交**。`git commit -m "feat(persistence): scheduleRetry (FAILED->DUE + next_retry_at) and markDeadLetter repo methods"`

---

### Task 3: RetryPolicy 纯决策类（server）

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/retry/RetryPolicy.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/retry/RetryPolicyTest.java`

**Interfaces:**
- Consumes: `dev.scheduler.core.Task`。Produces:
  - `boolean shouldRetry(Task task, int attempt, String failureDetail)` — `attempt <= task.maxRetries()` 且（`task.retryableFailurePattern()` 为 null/空白 或 `failureDetail` 上该 pattern 命中）。
  - `long delayMs(long backoffMs, int attempt)` — `backoffMs << (attempt-1)` 指数退避，封顶 1 小时（3600_000L）。
- 语义落死：`maxRetries=0` → 首次失败（attempt=1）即不可重试；pattern 空白=可重试（不过滤）；pattern 非空在 failureDetail 上 substring/正则命中才可重试。

- [ ] **Step 1: 写失败测试**：`shouldRetry` 各分支（可重试 attempt≤max；耗尽 attempt>max →false；pattern 命中→true；不命中→false；pattern 空白→true）+ `delayMs`（1→b、2→2b、3→4b、封顶）。这些是纯函数，单测即可。
- [ ] **Step 2: 跑失败**。Run: `mvn -q -pl scheduler-server test -Dtest=RetryPolicyTest`
- [ ] **Step 3: 实现**。
- [ ] **Step 4: 通过**。
- [ ] **Step 5: 提交**。`git commit -m "feat(server): RetryPolicy pure decision + exponential backoff (capped)"`

---

### Task 4: ExecutorWorker 接入重试 + 注入 Clock + findCandidate 时间闸

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/execute/ExecutorWorker.java`（构造加 `java.time.Clock`；FAIL 路径走 RetryPolicy）
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`（executorWorker 传 clock）
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcExecutionRepository.java`、`ExecutionRepository.java`（findCandidate 加时间闸）
- Test: `scheduler-server/src/test/java/dev/scheduler/server/execute/ExecutorWorkerTest.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcExecutionRepositoryTest.java`（findCandidate 闸）

**Interfaces:**
- Consumes: Task 2 `scheduleRetry`/`markDeadLetter`；Task 3 `RetryPolicy`；`ExecutionRepository.findCandidate`。
- Produces: `ExecutorWorker(TaskRepository, ExecutionRepository, HandlerRegistry, String workerId, Clock clock)`。`findCandidate` 新语义：`status='DUE' AND (next_retry_at IS NULL OR next_retry_at <= now())`。

- [ ] **Step 1: 改 `findCandidate` SQL**（persistence）：加 `AND (next_retry_at IS NULL OR next_retry_at <= now())`；JdbcExecutionRepositoryTest 加用例：一条 `next_retry_at` 在未来（now()+1h）的 DUE 不被 `findCandidate` 返回；一条 `next_retry_at` 为 NULL 的返回。
- [ ] **Step 2: 失败→通过**该 persistence 用例。
- [ ] **Step 3: worker 构造加 Clock**；把 `Instant.now()`（lease）改 `clock.instant()`；`ExecutorWorkerTest` 的 `worker()` helper 和 `Beans.executorWorker` 同步传 testClock / clock。跑既有用例仍绿（行为不变，改造不动语义）。
- [ ] **Step 4: worker FAIL 路径改判重试**：replace 现在的
  `catch (Throwable err) { execs.markStatus(FAILED,...); }` 与 handler 未注册分支，统一为「失败决策」：
  ```java
  int attempt = cand.get().attempt();   // claim 后由 repo 累加
  if (retryPolicy.shouldRetry(task, attempt+1, message)) {
    execs.scheduleRetry(id, clock.instant().plusMillis(retryPolicy.delayMs(task.backoffMs(), attempt+1)), workerId, message);
  } else {
    execs.markStatus(id, ExecutionStatus.FAILED, workerId, message);
    execs.markDeadLetter(id, "dlq:" + message);
  }
  ```
  （handler 未注册 `IllegalArgumentException` 也是失败，走同一判定。`attempt` 用认领后实况：可先 `execs.findById` 后只读到真正 attempt；或直接用 pre-claim `cand.get().attempt()+1` 近似——**用后者**，claim 的 `attempt=attempt+1` 保证本次为 `+1`，避免额外读。）
- [ ] **Step 5: worker 测试**：
  - 可重试（maxRetries=2, pattern 空, handler 抛）→ workOne 后行变 DUE、next_retry_at 非空、outcome 有 DUE、尝试不增改。
  - 耗尽（maxRetries=0, handler 抛）→ FAILED + dead_letter=true，无 DUE outcome。
  - pattern 不匹配（maxRetries=2, pattern="timeout", 抛 "boom"）→ FAILED + dead_letter=true。
  - 既有 happy/missing/missingHandler 用例保持绿（`createTask` helper 需带 maxRetries/backoff/pattern 参数——加 5 参重载，旧 2 参委托给新默认）。
- [ ] **Step 6: 提交**。`git commit -m "feat(server): ExecutorWorker retry via RetryPolicy + Clock injection; findCandidate honors next_retry_at"`

---

### Task 5: Reconciler 对账器（leader 门控 + 循环）

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/reconcile/Reconciler.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`（Reconciler + ReconcileLoop bean）
- Test: `scheduler-server/src/test/java/dev/scheduler/server/reconcile/ReconcilerTest.java`（sibling Testcontainers 基类，模式照抄 AbstractExecutorWorkerTest）

**Interfaces:**
- Consumes: Task 2 `markStatus`；Task 3 `RetryPolicy`；Task 4 时间闸无关（回收只看租约）；`LeaderElection`。
- Produces: `Reconciler(ExecutionRepository, TaskRepository, RetryPolicy, String workerId)`；`int scanOnce()` 返回回收行数。`ReconcileLoop.tick()` 为 `@Scheduled(fixedDelay=30000)` + leader 门控 + try/catch(Throwable)；`@ConditionalOnProperty(scheduler.reconcile.enabled, matchIfMissing=true)`，且非 leader 直接 `return`。

- [ ] **Step 1: 写测试**（ReconcilerTest，真 PG）：
  - 一条过期租约（`lease_until = now()-1h`）RUNNING → `scanOnce` → 转 FAILED，outcome 有 FAILED；同时模式用 worker 同款 RetryPolicy：再断言「转 DUE + 重试时间」的变体（expired RUNNING 且任务可重试 → 走 scheduleRetry）。
  - 两条过期 RUNNING → 都回收（scanOnce 返回 2）。
  - 一条新鲜租约（`lease_until=now()+60s`）RUNNING → 不动（返回 0 不回收）。
- [ ] **Step 2: 失败→实现 `scanOnce`**：`SELECT id FROM execution WHERE status='RUNNING' AND lease_until <= now()`，逐条 `markStatus(FAILED, "reconciler", "lease expired")` + RetryPolicy 判定（复用 Task 4 决策——抽个 `FailureHandler` 或直接在 Reconciler 内部对齐 worker 的判定；**倾向抽 `server.retry.FailureResolver`** 供 worker 与 reconciler 共用，避免两套漂移。若抽共用类，Task 4 的临时内联请并入）。
- [ ] **Step 3: 装配 ReconcileLoop**（Beans；leader 门控 + 兜底）。
- [ ] **Step 4: 通过**。Run: `mvn -q -pl scheduler-server test -Dtest=ReconcilerTest`
- [ ] **Step 5: 提交**。`git commit -m "feat(server): Reconciler reclaims expired-lease RUNNING (leader-gated loop, CAS-idempotent)"`

> 注：`FailureResolver` 若有（Task 5 抽共用），Task 4 的 worker 判定改调它；Task 5 依赖 Task 4 落定其形状，派发时以当时代码为准，裁决记 ledger。

---

### Task 6: 协作取消（DB 标志 + CancellationToken + worker + 端点）

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ExecutionRepository.java`、`JdbcExecutionRepository.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/handler/HandlerContext.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/handler/DemoHandler.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/execute/ExecutorWorker.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/ExecutionController.java`
- Create: `scheduler-server/src/main/java/dev/scheduler/server/handler/CancellationToken.java`
- Test: `JdbcExecutionRepositoryTest`、`ExecutorWorkerTest`、`ApiIntegrationTest`

**Interfaces:**
- Consumes: V2 `cancel_requested` 列；`RUNNING→CANCELED` 合法；
- Produces:
  - `boolean requestCancel(long id)` — `UPDATE execution SET cancel_requested=true WHERE id=? AND status='RUNNING'`；返回命中。**不落 outcome**（取消请求非状态迁移，记 ledger；真转 CANCELED 时 markStatus 落）。
  - `boolean isCancelRequested(long id)` — 读该列。
  - `record CancellationToken(boolean cancelRequested) { boolean isCancelRequested(); }`（无状态弹桶，worker 每次构造/轮询）。
  - `HandlerContext(long executionId, String args, CancellationToken token)`（加第三字段）；给一个便捷静态 `HandlerContext.of(executionId, args)`（token=not-cancelled）供既有测试/手工。
- 语义：DUE cancel → CANCELED（沿用）；RUNNING → `requestCancel`（协作，返回 202 + 现态）；终态 → 409。

- [ ] **Step 1: 持久化 + 测试**：`requestCancel`/`isCancelRequested`（RUNNING 置位、DUE 不置、非 RUNNING 返回 false）。
- [ ] **Step 2: token 接入**：`CancellationToken` + `HandlerContext` 加字段（更新构造点：ApiIntegrationTest/ExecutorWorkerTest 若直接 `new HandlerContext(...)`，改用 `of(...)` 或加 token）。
- [ ] **Step 3: worker 取消**：handler 运行前查 `isCancelRequested` → 若已取消直接 `markStatus(CANCELED, "cancelled before run")` 返回 true；运行时给 `HandlerContext` 传 token；handler 抛 `CancellationException` → `markStatus(CANCELED, "cancelled by handler")`（**不**走重试/FAILED）；handler 正常返回后若 `isCancelRequested` → `markStatus(CANCELED)`. DemoHandler 加一个慢路径（如 handle 里 sleep 后检查 token 抛 CancellationException），供集成测试协作退出。
- [ ] **Step 4: 端点扩展**（ExecutionController.cancel）：RUNNING → `requestCancel` → 202 + `findById`；DUE → CANCELED（保留）；终态 → 409（更新过时 M2 文案）。workerId 参与 `requestCancel` 作 audit 可选 detail（不落 outcome，不传）。
- [ ] **Step 5: 测试**：
  - ExecutorWorkerTest：预置 RUNNING(cancel_requested=true) → workOne → CANCELED；handler 抛 CancellationException → CANCELED（不重试）。
  - ApiIntegrationTest：POST trigger → RUNNING(经 claim) → cancel → 202 + cancel_requested 已在 DB；终态 cancel → 409。
- [ ] **Step 6: 提交**。`git commit -m "feat(server): cooperative RUNNING cancel via cancel_requested flag + CancellationToken"`

---

### Task 7: DLQ 端到端（findDeadLetters/requeue + GET /dlq + POST /requeue）

**Files:**
- Modify: `scheduler-persistence/.../ExecutionRepository.java`、`JdbcExecutionRepository.java`
- Modify: `scheduler-server/.../web/ExecutionController.java`
- Test: `JdbcExecutionRepositoryTest`、`ApiIntegrationTest`

**Interfaces:**
- Consumes: Task 2 `markDeadLetter`；V2 列。Produces:
  - `List<Execution> findDeadLetters()` — `WHERE status='FAILED' AND dead_letter ORDER BY id`。
  - `boolean requeue(long id)` — 同事务 `UPDATE execution SET status='DUE', attempt=0, next_retry_at=NULL, dead_letter=false WHERE id=? AND status='FAILED'`（CAS）+ outcome(`DUE`, "requeue")；0 行=非 FAILED，静默返回 false。
- 端点：`GET /api/v1/executions/dlq`（findDeadLetters）；`POST /api/v1/executions/{id}/requeue`（requeue，204/202 + 现态；非法 → 409）。

- [ ] **Step 1: 持久化 + 测试**：findDeadLetters 只列失败死信；requeue 复位 attempt/next_retry_at/dead_letter 且落 DUE outcome；非 FAILED requeue 返回 false 不动。
- [ ] **Step 2: 端点 + 测试**（ApiIntegrationTest）：建死信（直接 JDBC 置 `FAILED+dead_letter` 或走 0 重试任务失败）→ GET /dlq 含它 → POST /requeue → 变 DUE、attempt=0、GET /dlq 不再含；再 workOne → 可成功跑（SUCCESS）。
- [ ] **Step 3: 提交**。`git commit -m "feat: DLQ read + requeue endpoints (GET /executions/dlq, POST /executions/{id}/requeue)"`

---

### Task 8: GET /executions 抽 ExecutionQueryService（只读）

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/service/ExecutionQueryService.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/ExecutionController.java`（list/get 用 service；删重复 ROW mapper）
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`（若需 Bean）——若非 Bean 化，controller 直接 new service（简单，倾向 new）。

**Interfaces:**
- Consumes: `JdbcTemplate`（只读）。Produces: `List<Execution> list(Long taskId, String status)`、`Optional<Execution> get(long id)`。RowMapper 收敛到 service 单处，供 controller / dlq 复用。

- [ ] **Step 1: 实现 service**（把 ExecutionController.list 的 SQL + ROW 挪进 service；get 用 `findById`）。
- [ ] **Step 2: controller 改调 service**；删 controller 内 ROW 复制。dlq 端点（Task 7）若已有 ROW 也改引 service 的 mapper。
- [ ] **Step 3: 跑 ApiIntegrationTest 保持绿**（list/get/dlq 行为不变）。
- [ ] **Step 4: 提交**。`git commit -m "refactor(server): move GET /executions raw SQL into ExecutionQueryService"`

---

### Task 9: M2 端到端集成测试收敛

**Files:**
- Modify: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: Task 1–8 全部。Produces: 真 Boot 上下文的 M2 快乐路径背书。

- [ ] **Step 1: 添加集成用例**（沿用 M1 的 `@SpringBootTest(properties="scheduler.loop.enabled=false")` + `@Primary MutableClock` + 每例 TRUNCATE + 持锁）：
  - 建任务（maxRetries=1, backoffMs=1000, pattern 空）→ POST trigger → workOne 使 handler 抛 → 断言行 DUE + next_retry_at；把 next_retry_at 手工拨到过去 → 再 workOne → SUCCESS（重试闭环）。
  - 建 maxRetries=0 任务 → 失败 → GET /dlq 含 → POST /requeue → 再 workOne → SUCCESS。
  - 建任务 → trigger → claim(RUNNING) → POST cancel → 202 → 手动置 cancel_requested 为真(或经 requestCancel) → workOne → CANCELED。
  - 断言 `/actuator/prometheus` 仍含 `scheduler_active_runs{task_id=`（防 M2 回归指标）。
- [ ] **Step 2: 满模块跑**：`mvn -q -pl scheduler-server test` 全绿。
- [ ] **Step 3: 提交**。`git commit -m "test(server): M2 end-to-end integration (retry, dlq/requeue, cooperative cancel)"`

---

## Cross-task notes

- **共享 Testcontainers 基类**：Task 5 的 ReconcilerTest 照抄 `AbstractExecutorWorkerTest`；M1 已记「AbstractTriggerEngineTest 包私有致基类重复」为 minor——本计划各包继续就地复制（不重构基类，符合 surgical）。
- **`api.version`**：根 pom `<property>` 已定义，各 surefire 引用 `${api.version}`；M2 测试在 Windows 下无需重复配置。
- **`HandlerContext` 构造点**：加第三字段会破 `new HandlerContext(id,args)` 调用；提供 `HandlerContext.of(id,args)` 便捷工厂，把所有直接构造的地儿改成工厂（surgical，不动 M1 语义）。
- **ExecutorWorkerTest.createTask**：加 5 参重载（handlerRef, maxActive, maxRetries, backoffMs, pattern），旧 2 参委托 `(h,m,0,1000,null)`，既有用例零改动。
- 每任务以「既有用例保持绿」为前提，改动尽量窄。