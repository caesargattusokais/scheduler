# Scheduler M3 — Sharded Parallel Execution (分片并行执行) 设计

> 分片并行的里程碑细化。主 spec（`2026-09-01-distributed-task-scheduler-design.md` §3）已锁定本里程碑的形状；本文档把 §3 落到可实现的确定性规格，并做 v1 裁剪。Governing：**正确性优先 + 数据完整优先**（用户全局指令），每项设计无歧义、可被 Plan 逐字引用。

## 0. 方针与 v1 裁剪（已获用户裁定）

- **父为汇聚头、shard 为认领单元**：一次触发 → 一条父 `execution`（run header，shardCount=N）+ 同事务 N 条 `execution_shard`（真正被 worker 认领/运行的原子单元）。父表**复用**既有 `execution`，不发明 run/subscription 新实体；仅新增 `execution_shard` 一种行。
- **父终态只由对账器推导写入**，任一 shard worker 绝不写父 —— 杜绝并发抢写覆盖部分结果。非终态父的 `DUE`/`RUNNING` 由读侧观测 shard 推导，不落父 RUNNING 列。
- **shardFailPolicy：v1 硬编码 FAIL_FAST**（用户裁定），不做 per-task 配置字段。语义：任一 shard 耗尽重试进入终 FAILED → 取消其余兄弟（RUNNING 兄弟协作取消、DUE 兄弟直接 CANCELED）。
- **单分片重跑端点 defer**（用户裁定）：M3 不做；「部分结果不丢、只重跑失败 shard」由 `execution_shard_outcome` 保留证据支撑，重跑端点随管理后台（React SPA）里程碑。
- `shardCount=1` 也走统一父+单 shard 模型，**不做快路径分叉**（避免 worker 双路径漂移；任务日后可从 1 调 N，模型恒一）。代价：父态推进有 ≤对账周期延迟；父是汇聚头，实时工作态看 shard，可接受。
- 沿用主 spec：并行度受 worker 数与 per-task `maxActiveConcurrent` 双重约束；v1 无跨 shard 顺序/依赖（留给 M4 工作流）。

## 1. 数据模型（V3 迁移）

### 1.1 父 `execution`（复用既有表，语义变更）

现作为认领单元的意义删除，改任**汇聚头**。沿用既有列；`status` 只由对账器写终态（DUE→终态；不落 RUNNING）：

- 父幂等键 = `taskId + triggerAt`（毫秒）—— **去掉**既有 `IdempotencyKeys.forTrigger` 的 shardIndex 段，父为「该次触发」的唯一代表。
- `execution.shard_count` = N（既有列）；`execution.shard_index` 父行不再有意义（保持 0，不读）。
- `args` 沿用父参数（handler 的公共入参）；`result_payload` 父行不写（细节在 shard）。

### 1.2 新表 `execution_shard`（真实运行单元）

```sql
CREATE TABLE execution_shard (
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  execution_id      BIGINT NOT NULL REFERENCES execution(id) ON DELETE CASCADE,
  shard_index       INT NOT NULL,             -- 0..N-1
  shard_data        TEXT,                     -- 轻量定位符（键/谓词），Partitioner 产出；不搬数据
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
CREATE INDEX idx_shard_claim ON execution_shard (execution_id, status);
```

- **shard 幂等** = `(execution_id, shard_index)` 唯一约束（父已确立唯一触发，shard 天然唯一、天然幂等重放单 shard）。
- shard 复用 `ExecutionStatus` 与 `ExecutionTransitions`（DUE/RUNNING/SUCCESS/FAILED/CANCELED，与 M1/M2 同迁移图）。

### 1.3 新表 `execution_shard_outcome`（shard 审计）

```sql
CREATE TABLE execution_shard_outcome (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  shard_id   BIGINT NOT NULL REFERENCES execution_shard(id) ON DELETE CASCADE,
  status     TEXT NOT NULL,
  detail     TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

沿用 M2 的不变式：**每个 shard 状态迁移各落一行**。父侧审计沿用既有 `execution_outcome`（终态由对账器落）。「部分结果不丢」= 已完成 shard 的 SUCCESS 证据留在 `execution_shard_outcome`。

## 2. 触发：一次 tick → 一父 + N shard

`TriggerEngine`（leader 门控、幂等）命中一个 tick 时，**单事务**内：

1. 父 `execution`：`createDue`，幂等键 = `IdempotencyKeys.forTrigger(taskId, firedAt)`（2 参），`shard_count=N`。
2. 逐 N 条 `execution_shard`：`shard_index=i`（0..N-1），`status=DUE`，`shard_data` 本阶段为空（Partitioner 接口后续里程碑接）。

- 背压策略不变：触发处不设配额闸，积压由认领侧 CAS+配额消化（沿用 M1 §2.4）。

## 3. 运行：Worker 认领 shard

- `findCandidate` / `claim` 迁移到 `execution_shard`：
  - 候选：`JOIN execution e ON e.id=s.execution_id WHERE e.task_id=? AND s.status='DUE' AND (s.next_retry_at IS NULL OR s.next_retry_at<=now()) ORDER BY s.id LIMIT 1`。
  - 认领 CAS：`active.c < maxConcurrent`（活跃 = 该 task 的 **RUNNING shard** 数，`lease_until>now()`），`s.status='DUE'→'RUNNING'`，置 `worker_id/lease_until/attempt+1/started_at`。
- `HandlerContext` 增字段：携带 `executionId`（父 id）、`shardIndex`、`shardCount`、`shardData`、`args`、`CancellationToken`（本 shard 的 `cancel_requested`）。handler 凭 `shardIndex/shardData` 自取切片（路由器式；shard_data 是轻量谓词，不进表搬数据）。
- 三种回写（成功 / 失败 / 取消）均落 **shard** 行，复用 M2 的**所有权守卫 `markStatusOwned`**（CAS 加 `shard.worker_id`）：旧 worker 租约过期后被回收、兄弟重认领，其迟到回写被抑制，杜绝重复完成/误标失败。父永不在此被写。

### 3.1 worker 逐 shard 行为（等价 M2 每条 execution 的语义，作用对象=shard）

- 运行前 `isCancelRequested(shard)` → `markStatusOwned(shard, CANCELED, "cancelled before run")`。
- handler 抛 `CancellationException` → `markStatusOwned(shard, CANCELED, "cancelled by handler")`（在 catch(Throwable) 之前，不落 retry/DLQ）。
- 正常返回后复查 `isCancelRequested(shard)` → CANCELED，否则 SUCCESS。
- 通用 `catch(Throwable)` → `markStatusOwned(shard, FAILED)` 守卫且门控成功后才走 `FailureResolver.handle`。

## 4. 决策：FailureResolver（对象=shard）

- `handle(Task task, long shardId, int attempt, String detail)`（沿用 M2 形态，作用对象换成 shard 行）：
  - `shouldRetry(task, attempt, detail)` → `scheduleRetry(shardId, clock.instant()+delay, detail)`（shard `FAILED→DUE` + next_retry_at）。
  - 否则 `markDeadLetter(shardId, "dlq:"+detail)`，并**触发 FAIL_FAST**（见 §5）。
- 前置由调用方落 FAILED：worker 经 `markStatusOwned(shard, FAILED)`（CAS worker_id），reconciler 孤儿回收经无条件 `markStatus(shard, FAILED)`（reclaim 换 owner）。保持 M2「先落 FAILED 再判定」次序与 CAS 语义。

## 5. FAIL_FAST（v1 硬编码）

任一 shard 进入**终 FAILED**（worker 路径或对账器孤儿路径，即 FailureResolver 的 `markDeadLetter` 分支）后，单事务内：

1. RUNNING 兄弟（`execution_id` 同父、`status='RUNNING'`、同父不同 shard）→ 置 `cancel_requested=true`（协作取消信号）。
2. DUE 兄弟 → `markStatus(shard, CANCELED)`（CAS on `status='DUE'`，见 §7 同款：若并发 claim 已把该行翻成 RUNNING，谓词不命中、安全落到 RUNNING 协作分支 —— 状态键解决认领竞态，无泄漏；未运行即取消，不必等认领再协作）。

可达终态闭环：RUNNING 兄弟协作 → CANCELED；DUE 兄弟 → CANCELED；→ 全 shard 终态、含 ≥1 FAILED → 对账器聚父 FAILED。已完成 shard 的 SUCCESS outcome 证据保留。

## 6. 对账器（Reconciler）：两手

沿用 leader 门控、复用共享 FailureResolver（同一重试判定，worker/reconciler 无漂移）。`scanOnce` 每周期：

### (a) 孤儿 shard 回收
`findExpiredRunning(taskId)` → RUNNING 且 `lease_until<=now()` 的 **shard**；逐条无条件 `markStatus(shard, FAILED, "lease expired")` 并门控，成功才 `failureResolver.handle(...)`（重试→DUE / 否则 DLQ+FAIL_FAST）。逐条捕获 `IllegalStateException`（快照后被 owner 抢先完成）跳过不中止整批（沿用 M2 fix wave 语义）。

### (b) 父汇聚（本节为 M3 新增核心）
交接面：只处理父 `execution` 满足「非终态且有 ≥1 shard 已终态」，即 `e.status NOT IN 终态 AND EXISTS(terminale shard)`。

对每个待聚父，读其全部 shard 按状态归类，**一次原子推进父**（CAS on 父 current status）：
- 全终态：
  - 含 ≥1 `FAILED` → 父 `FAILED`（detail `"shard <i> failed"`之类可审计），落 `execution_outcome`（父 FAILED）。
  - 否则含 ≥1 `CANCELED` → 父 `CANCELED`。
  - 否则（全 SUCCESS）→ 父 `SUCCESS`。
- 未全终态 → 本轮不动（父非终态即「仍在跑/等待」，非泄漏；永续 DUE 积压由 DUE 年龄指标曝光）。

父终态推进幂等：达终态的父不再入待聚集合；多对账器并发由父 CAS 兜底（0 行=已被推进，跳过）。

## 7. 取消（父级级联 shard）

- 父 `REST /executions/{id}/cancel` 沿用：终态幂等 200、失败态→409；父非终态时，读侧观测 shard 派生父态：`DUE`（无 shard 运行过）→ 200 直接取消；有 ≥1 RUNNING shard（工作在途）→ **202** 请求协作取消（`requestCancel`）。
- **取消动作为父级级联**，且保证「父已取消后任一 shard 不再运行」。父非终态取消 → 单事务内对全部**非终态** shard：RUNNING 兄弟置 `cancel_requested=true`（协作），DUE 兄弟直接 `status='DUE'→'CANCELED'`（CAS on DUE，未运行、安全直接取消；若 claim 竞态已把该行翻成 RUNNING，则谓词不命中、走 RUNNING 协作分支 —— 状态键解决竞态，无泄漏）。父自身在 RUNNING 情形置 `cancel_requested=true`，否则 DUE→CANCELED 直接终态。
- 终止闭环：RUNNING 兄弟协作→CANCELED、DUE 兄弟→CANCELED → 全 shard 终态 → 对账器聚父 CANCELED。父终态幂等：达终态父不再入待聚集合。

## 8. 端点与指标增量

| 端点 | M3 变更 |
|------|---------|
| `POST /tasks` | 不变（shardCount 已可配） |
| `GET /executions/{id}` | 详情扩为含父态 + shards 列表（each: shard_index/shard_data/status/attempt） |
| `POST /executions/{id}/cancel` | 作用于父、级联 shard |

- 每 task 指标改为对 **shard** 计数：活跃 RUNNING shard 数、DUE shard 队列最深年龄；保留 `task_id/task_name` 标签（沿用 M1 §5.2 的点式注册）。
- `markStatusOwned` / 新仓库方法均对 `execution_shard` 生效；M1/M2 现 `execution` 仓库的 worker/retry/孤儿语义整体迁移到 shard 表。

## 9. 既有测试的预期折腾（非回归）

M2 的 `ApiIntegrationTest`/`ExecutorWorkerTest`/`ReconcilerTest` 直接断言父 `execution` 在 `workOne()` 后即 SUCCESS/FAILED → 迁移后父要等对账器推进。改造基准：**断言 shard 状态为运行时真相（实时）**；父终态断言 = 手动触发对账（`reconciler.scanOnce()`）后父 SUCCESS/FAILED/CANCELED。这是 M3 预期的重构面，不是回归。

## 10. 明确不做（v1 范围外）

- per-task `shardFailPolicy` 配置（硬编码 FAIL_FAST）。
- `Partitioner` 框架接口与 shard_data 填充（shard_data 本阶段空，切片由 handler 凭 shardIndex 自取；`Partitioner` 统一接口、路由器/静态分桶两式随后续排程增强）。
- 单失败 shard 手动重跑端点。
- 跨 shard 顺序 / 依赖 / 子流程（M4 工作流）。
- 分片数据源与执行数据源合并读取（shard_data 只放轻量定位符）。