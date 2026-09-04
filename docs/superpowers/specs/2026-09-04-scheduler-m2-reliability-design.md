# Scheduler M2 — 可靠性 (Reliability) 设计

## Goal

在 M1（DB 认领调度 + 状态机 + 审计）之上补齐调度器的可靠性四件套：**失败重试（backoff/上限/可重试模式）、孤儿/挂死执行的 Reconciler 对账、RUNNING 协作取消（CancellationToken）、死信 DLQ + requeue**，并把 GET /executions 的裸 SQL 抽到 service 层。全部沿用 DB 认领、无 MQ、无 JPA。

## Architecture

M1 已铺好的接缝被本节真正启用：`execution.attempt`（claim 时 +1）、`execution.next_retry_at`、`app_task.max_retries/backoff_ms/retryable_failure_pattern`、`execution_outcome` 审计链、状态机 `FAILED → DUE` / `RUNNING → CANCELED`。M2 新增两个 Flyway 列（V2），并提供 worker 侧重试、独立 Reconciler 循环、协作取消、DLQ 端点、service 只读层。

## 全局决策（Design Rulings，已与用户确认）

- **R1 重试「同行复用」**：重试不新建 execution 行，而是 `FAILED → DUE` 复用当前行。理由：`idempotency_key` UNIQUE 天然不冲突、`attempt` 在 claim 处自然累加、`findCandidate(ORDER BY id)` 让被重排的行保持在队头，无需任何键/序号特殊处理。代价：同一行多次入队，人工重跑需显式 requeue（已有 `FAILED→DUE`、`CANCELED→DUE` 迁移）。
- **R2 Reconciler 跑在 leader**：周期扫 + 改孤儿 RUNNING，leader 门控（用 M1 的 LeaderElection）。理由：它是写路径非执行路径，leader 单点扫+改最省惊群、审计最干净；代价为失效恢复最坏延迟一个 reconciler 周期（30s），可接受。回收仍以 `UPDATE ... WHERE status='RUNNING'` CAS 幂等，即使误配多节点也安全。
- **R3 DLQ 用布尔列**：`execution.dead_letter BOOLEAN DEFAULT false`，重试耗尽的终态 FAILED 置 true；端点列表/requeue。不建独立表。代价：DLQ 统计需 `WHERE status='FAILED' AND dead_letter` 过滤，牺牲一点索引性换取最小 schema 增量。
- **R4 协作取消用 DB 标志**：`execution.cancel_requested BOOLEAN`，worker 轮询 DB 而非 MQ/内存传播；与 DB 认领架构自洽。硬超时（`timeout_seconds` 强杀）**留 M3**，不在 M2 同塞两套取消/超时机制。

## 数据模型变更（Flyway V2）

```sql
ALTER TABLE execution ADD COLUMN cancel_requested BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE execution ADD COLUMN dead_letter      BOOLEAN NOT NULL DEFAULT false;
```

无其他列/表变更。`task_dependency` 仍是 M4；`execution` 的列已够 M2。

## 状态迁移（重试/回收/取消共用现有 ExecutionTransitions，不变）

- `FAILED → DUE`：已存在，重试与 requeue 走它（同事务写 `next_retry_at` 落 DUE outcome）。
- `RUNNING → CANCELED`：已存在，协作取消走它。
- `RUNNING → FAILED`（reconciler 的 lease-expired 回收）：已存在。
- 无新增迁移；新逻辑只复用。

## 组件与职责

### worker 侧重试（改 `ExecutorWorker`）

`workOne()` 捕获 handler FAIL（`Throwable` 或 handler 未注册）时不再无条件落终态 FAILED，改由判定：

1. 计算本行 `attempt`（claim 已 +1，读 `cand` 或认领后实况）。
2. `retryable = task.retryableFailurePattern()==null || pattern 匹配失败消息`；`canRetry = attempt <= task.maxRetries()`。
3. 若 `retryable && canRetry` → `execs.scheduleRetry(id, task.backoffMs(), attempt, task.retryable..., failedDetail)`：`FAILED→DUE` + `next_retry_at = now + backoffMs × 2^(attempt-1)` + 落 `DUE` outcome（detail 记失败原因），同事务。返回 true。
4. 否则（不可重试）→ `execs.markStatus(id, FAILED, …)`，并把该行标 `dead_letter=true`（见 DLQ）。

> DLQ 判定一句话：**终态 FAILED 且「不会再自动重试」即进死信**。两条不重试路径都算——(a) attempts 耗尽（`attempt > maxRetries`）、(b) `retryable_failure_pattern` 非空但不匹配。注意含 `maxRetries=0`（M1 默认）的任务：首次失败即不可重试 → 直接进 DLQ，语义上「FAILED 且不重试」本就是死信。`GET /executions?status=FAILED` 与 `/dlq` 的区别仅是 `dead_letter` 位。

**接口**：`ExecutionRepository.scheduleRetry(long id, long backoffMs, int attempt, String failedDetail)`。

### findCandidate 尊重重试时间（改 `JdbcExecutionRepository.findCandidate`）

```sql
SELECT * FROM execution
 WHERE task_id=? AND status='DUE'
   AND (next_retry_at IS NULL OR next_retry_at <= now())
 ORDER BY id LIMIT 1
```

未到 `next_retry_at` 的 DUE 不被抢——迟到重试按 backoff 落闸。

### Reconciler（新 `Reconciler` + `ReconcileLoop`，leader 门控）

- `Reconciler.scanOnce()`：
  1. `SELECT id FROM execution WHERE status='RUNNING' AND lease_until <= now()`（租约过期 = 孤儿）。
  2. 每行 `markStatus(id, FAILED, workerId="reconciler", detail="lease expired")` 的 CAS（UPDATE WHERE status='RUNNING'）。0 行＝已被他方回收，静默跳过。然后**复用 worker 的重试判定**（reconciler 也产失败 → 可重试则走 scheduleRetry）。
- `ReconcileLoop`：`@Scheduled(fixedDelay=30000)` + `try/catch(Throwable)`，`@ConditionalOnProperty(scheduler.reconcile.enabled, matchIfMissing=true)`，且 `leaderElection.isLeader()` 才真正扫（与非 leader 快速返回）。
- **复用而非复制**：重试判定逻辑从 worker 抽成一个可共用的小决策（`RetryPolicy.shouldRetry(task, attempt, failureDetail)`），worker 与 reconciler 都调，避免两套判定漂移。
- workerId 用 `"reconciler"` or `"system:reconciler"`。

### 协作取消（改 `ExecutorWorker` + `HandlerContext` + cancel 端点 + cancel 仓库方法）

- V2 加 `cancel_requested` 列。
- 仓库：
  - `boolean requestCancel(long id)`：`UPDATE execution SET cancel_requested=TRUE WHERE id=? AND status='RUNNING'`，返回是否更新（RUNNING 才可请求；DUE/终态请求返回 false → 端点相应提示）。落一条 outcome？取消请求不是状态迁移，审计链可选——**不落** outcome（避免噪音），只在真正转 CANCELED 时落。记 ledger 决策。
  - `boolean isCancelRequested(long id)`：worker 轮询。
- `HandlerContext` 增加 `CancellationToken token` 字段（`token.isCancelRequested()`）。DemoHandler 在适当点检查并抛出协作取消信号（如 `throw new CancellationException` 或返回一个标记）。
- `ExecutorWorker`：handler 运行前查一次 `isCancelRequested`；handler 返回后若 `isCancelRequested` 为真且未正常终结 → `markStatus(CANCELED, "cancel requested")`。handler 抛出的取消信号同样映射 `CANCELED`（需区分「取消信号」与「普通失败」——用专用异常类型 `CancellationException`，worker catch 单独处理，不落入 retry 判定）。
- 端点：`POST /api/v1/executions/{id}/cancel` 扩展：RUNNING → 置 `cancel_requested`（协作），返回 202；DUE → 直接 `CANCELED`（沿用 M1，合法迁移）；已 CANCELED → 幂等 200；SUCCESS/FAILED → 409（终态不可取消）。

### DLQ（schema 列 + 端点）

- 重试耗尽（pattern 不匹配或 attempt 超上限）的终态 FAILED → `markStatus` 后置 `dead_letter=true`（同一事务或紧接着），落一条 outcome detail="dlq"。
- 仓库：
  - `List<Execution> findDeadLetters()`：`WHERE status='FAILED' AND dead_letter ORDER BY id`。
  - `boolean requeue(long id)`：`FAILED → DUE`，`attempt=0`、`next_retry_at=NULL`、`dead_letter=false`，落 `DUE` outcome detail="requeue"。仅对终态 FAILED 且 dead_letter 生效（非法终态返回 false）。
- 端点（spec §5.1 补齐）：
  - `GET /api/v1/executions/dlq` — 列死信。
  - `POST /api/v1/executions/{id}/requeue` — requeue 一条 POST 到底;204/202。

### GET /executions 抽 service（改 web 层）

- 新 `ExecutionQueryService`（只读，server 侧）：从 controller 接手 `GET /api/v1/executions`（taskId/status 过滤）与 `GET /executions/{id}`，内部用 `RowMapper`（替掉 controller 里复制出来的 ROW MAP）。controller 只留路由 + 错误语义。
- 兑现 M1 终审 defer 项；不动 persistence。

## REST 增量汇总

| 方法 | 路径 | M2 行为 |
|---|---|---|
| `POST` | `/api/v1/executions/{id}/cancel` | 扩展：RUNNING → 202 协作取消（置 cancel_requested）；DUE → CANCELED；终态 → 409 |
| `GET` | `/api/v1/executions/dlq` | 列死信 FAILED |
| `POST` | `/api/v1/executions/{id}/requeue` | FAILED(DLQ) → DUE 重跑 |
| `GET` | `/api/v1/executions` | 行为不变，实现移到 service |

## 错误语义

- 取消失败（终态）→ 409；requeue 非法终态 → 409；DLQ 列表空 → 200 空数组。
- `scheduleRetry`/`requestCancel`/`requeue` 内部 CAS 0 行 = 竞态丢失，静默跳过（沿用 M1 markStatus 策略），不抛误异常。
- 全部 `@Scheduled` 循环（scan/work/**reconcile**）外层 `try/catch(Throwable)`，防 DB 抖动杀循环（沿用 M1）。

## 测试（真 PG 16 Testcontainers）

- **Retry**：attempt<max 且 pattern 匹配 → `FAILED→DUE`+next_retry_at 正确且 outcome 有 DUE 行；未到 next_retry_at 的 DUE 不被 `findCandidate` 抢；attempt 耗尽 → 终态 FAILED+dead_letter。
- **Reconciler**：预置一条过期 lease 的 RUNNING → scanOnce → 终态 FAILED（或按 retry 重排）；两条过期的行争抢 → CAS 只终态一次（outcome 仅一条）。
- **Cancel**：DUE cancel → CANCELED；RUNNING 置 cancel_requested → 真 DB 可见；DemoHandler 收到 token 且协作退出 → CANCELED；终态 cancel → 409。
- **DLQ**：耗尽 → 出现 `/dlq`；`/requeue` → 变 DUE、attempt 归零、出列、可再认领跑通。
- **findCandidate 时间闸**：next_retry_at 在未来 → 不被 claim。
- 每块沿用 M1 确定性策略：`scheduler.loop.enabled=false`、`@Primary MutableClock`、每例 TRUNCATE。reconciler 用 `scheduler.reconcile.enabled` 关停以确定性单测。

## 里程碑边界 / 明确 defer

- **不在 M2**：硬超时强杀（timeout_seconds）→ M3；Cron/CronTrigger 时间闸增强（非 M 性）→ 不动；取消只协作不强制，无 MQ。
- RETRY 的 `retryable_failure_pattern` 匹配语义用**子串/正则**：pattern 非空即在失败消息上 `find`，命中即可重试（文档写死，防两读）。
- 手动 `POST /tasks/{id}/trigger` 与 cron 触发产出的 execution 都走同一 retry 判定（不区分来源）。

## Open items for plan

- 重试决策抽取 `RetryPolicy` 的位置（worker 侧 vs core 模块）；倾向 server 侧类，避免动 core。
- `AppDelegate`/Boot 装配新增 reconciler 循环 + `RetryPolicy` Bean。
- V2 migration 文件名 `V2__reliability.sql`。