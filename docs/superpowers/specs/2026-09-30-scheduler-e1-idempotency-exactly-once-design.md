# Scheduler E1 — 调度幂等 / Exactly-once 设计(A 层触发幂等 + B1 层活性门控回收·栅栏)

> **规划路径**:E(调度纵深)子项第一件,按「调度器能做满 Exactly-once」的可验证边界推进。
> **前置**:sharding/分片(M3/M6)、DLQ 治理(V28)、运行超时(v2 runtime)、跨 DAG 依赖(V24)、告警引擎(V31)。
> **状态**:已实现(2026-09-30)。
> **更新日期**:2026-09-30

---

## 1. 背景与问题

调度器采用**两层模型**(本设计依据,已核实源码):

- `execution` 行 = 父运行,`status` 仅 DUE→终态;创建幂等由 `execution.idempotency_key UNIQUE`(V1__schema.sql:23)保证。
- `execution_shard` N 行 = 实际工作单元(认领/租约/写回)。认领 `claim`(JdbcShardRepository:169-196)已原子:`DUE→RUNNING` CAS + task `FOR UPDATE` 串行化锚点,双 worker 不会同时持有同一 shard。
- 无 shard 级幂等键;无客户端 request-id on manual 触发;`attempt` 未纳入属主守卫。

对照探查,存在**两类重复窗口**:

**A 层(触发层,产生两条父运行):**
1. 双 `POST /tasks/{id}/trigger` → 键 `manual:{taskId}:{UUID}` 每次新 UUID → 两条父。
2. 双 `POST /dags/{id}/trigger`(manual)→ `dag:{dagId}:manual:{UUID}` → 两条 dag_run。
3. 双 `POST /executions/{id}/rerun` → `rerun:{srcId}:{UUID}` → 两条父。
4. 双 DAG node rerun → `dag:{runId}:node:{nodeKey}:rerun:{UUID}` → 两轮 execution。

**B 层(投递层,同一条物理分片被双跑):**
5. RECONCILER 用**任意属主**回收「运行超时」分片(`findOverRuntime:243-251`,不看活性)→ `markStatus(FAILED)`(:262-276,CAS 仅旧 status)→ DLQ/重试把同一条物理 shard 重派 → **原 handler 可能仍活着在跑** → 外部副作用双跑。写回被 `markStatusOwned`(:283-299,`status AND worker_id` 守卫)静默压制(DB 安全),但副作用不幂等。
6. `findExpiredRunning` 的 **lease-expiry 分支**(`lease_until<=now`,:229-241)不验活性 → 活体-writer 续租 blink 超窗即被回收 → 同上双跑。仅 heartbeat 分支验 ALIVE。
7. `renewLease` 尽力而为(失败仅记日志,ExecutorWorker:76-80)→ worker 心跳正常但续租瞬断即触发 lease-expiry 回收。
8. **DLQ replay**(requeueShard,:472-488)把 FAILED→DUE 重派同一物理 shard:`clean` worker 自报 FAILED 无重叠,但来源为「回收落 FAILED」时带窗 5 重叠。

**大多数触发路径已有强去重**(cron `{taskId}:{epochMs}`、interval、事件 `event:{dedupeKey}`、跨 DAG `dep:{上游runId}`、scheduled dag `dag:{dagId}:{epochMs}`)→ 全走 `createParentWithShards ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key=EXCLUDED...`(:67-70,同 key 复现原父、不重复插 shard)。

---

## 2. 设计原则与 Exactly-once 边界(先说破)

只要调度器允许「崩溃后回收重跑」(为了不丢活),它对**外部系统副作用**(发请求、写外库)就给不了严格 exactly-once —— 除非消费者副作用本身幂等。因此本设计把承诺**明确分层**:

- **调度器端(本设计覆盖,可验证)**:活体 worker 的分片绝不同时双跑;真死 worker 靠活性回收重跑(不丢活);陈旧写/续租被栅栏整体拒绝。
- **Handler 端(契约,不做)**:外部副作用的严格 exactly-once 由 handler 以其自身幂等性保证。本设计不承诺也不可能承诺 arbitrary 副作用的严格单次。

**本设计选择 B1(活性门控回收 + 栅栏)**;A 层无 B 层这种取舍,直接做。

---

## 3. A 层:触发幂等(重复触发去重)→ 客户端 request-id

**目标**:同一次客户端请求重试(网络重传、双按钮、重放)只产生一条父运行/一轮 execution;不同请求仍各成一轮。

**机制**:复用现有 `createParentWithShards` 的 `ON CONFLICT (idempotency_key)` + `execution.idempotency_key UNIQUE` / `dag_run.idempotency_key UNIQUE`。四个端点各加**可选** `idempotencyKey` 请求参数:

| 端点 | 默认键(无参数,现状不变) | 带参数键 |
|---|---|---|
| `POST /tasks/{id}/trigger` | `manual:{taskId}:{UUID}` | `manual:{taskId}:{requestToken}` |
| `POST /executions/{id}/rerun` | `rerun:{srcId}:{UUID}` | `rerun:{srcId}:{requestToken}` |
| `POST /dags/{id}/trigger` | `dag:{dagId}:manual:{UUID}` | `dag:{dagId}:manual:{requestToken}` |
| DAG node rerun(无独立 HTTP 端点则沿用现有门) | `dag:{runId}:node:{nodeKey}:rerun:{UUID}` | `dag:{runId}:node:{nodeKey}:rerun:{requestToken}` |

- **prefix 隔离**:manual/rerun/dag 各在自己的前缀命名空间,不会与 cron(`{taskId}:{epochMs}`)、dep(`dep:*`)、event(`event:*`)撞键。
- **requestToken 语义**:客户端自管理唯一标识(建议 36 字内 ASCII 字符串;UUID/业务单号均可)。服务端不解析其内容,仅作幂等键段。
- **缺省路径零破坏**:未带 `idempotencyKey` → 维持 UUID 行为(每次新父),兼容既有调用/前端。
- **实现锚点**:`TaskController.manualRun(id, suffix)` 已有 suffix 参数槽(:231);`ExecutionController.rerun` 直接替换 inline UUID(:101)。新增的键格式化辅助放入 `core/IdempotencyKeys`(镜像现有 forTrigger/forManualDagRun/forNodeRerun)。
- **同键重复**:返回**原父**(`ON CONFLICT ... RETURNING id` 已返回原行 id),不 bump 内容、不重复插 shard。可用 `forDagRun` 的 manual 分支复用同 mechanism。

**A 层不改为**:cron/interval/事件/跨 DAG 依赖(已有强去重,无需 request-id)。

> **node rerun 同键不对称性(实现注)**:DAG node rerun 对**已运行中**节点重复触发,即便带上同一 `idempotencyKey`,也返回 **HTTP 409**(既有非终态守卫先于键构造触发,什么都不创建)——**不是** A 层的 id-replay(同键返回原父)。此不对称是有意为之:节点守卫在 `IdempotencyKeys` 构造之前就拦截,故不会走到去重路径;而 manual task / rerun / manual dag 三者没有该前置守卫,才走「同键复用原父」的去重。文档/前端应据此区分:重放已结束节点用同键重取原父,重放运行中节点则先等其进入终态。

---

## 4. B1 层:投递/认领幂等 → 活性门控回收 + 每-attempt 栅栏 + 强制续租

### 4.1 统一活性门控回收(单规则)

**规则**:调度器只在属主 worker **非 ALIVE**(heartbeat 陈旧或 worker 行不存在/非 ALIVE)时,才回收(reclaim→FAIL)一条 RUNNING 分片。活体不因「慢 / 超时」被杀。

- `findOverRuntime`:加活体门 —— 仅回收「超时 **且** 属主非 ALIVE」:
  `... AND NOT EXISTS (SELECT 1 FROM worker w WHERE w.id=s.worker_id AND w.status='ALIVE' AND w.last_seen >= now() - interval)`。
- `findExpiredRunning`:现只有 heartbeat 分支验 ALIVE;**lease-expiry 分支也要求属主非 ALIVE**(统一到同一 NOT EXISTS ALIVE 门)。
- **副作用**:调度器端——「真死的 worker → 回收重跑(不丢活)」;「活的 worker → 慢/超时/续租瞬断都不被回收(不双跑)」。两个回收源共用一条活性判据,杜绝「A 通道有活性门 B 通道没有」的旁路。

### 4.2 每-attempt 栅栏(陈旧写整体拒绝)

`attempt` 在 `claim` 时 `attempt+1`(:174)。把它纳入所有**属主守卫**,使「被回收重派后的旧 attempt 迟写、或同 worker 二次认领后的旧写」都被拒:

- `markStatusOwned` 守卫:`WHERE id=? AND status=? AND worker_id=?` → 加 `AND attempt=?`(属主 = worker_id **且** attempt)。
- `renewLease` 守卫:`WHERE id=? AND status='RUNNING' AND worker_id=?` → 加 `AND attempt=?`。
- 被拒时保持现有静默返回/`log.debug` 抑制语义,不误导 outcome。

**效果**:即使同一 worker 二次认领同 shard(attempt 已递增),旧 attempt 的 write-back/renew 仍被栅栏拒 —— 回收/重派后不可能有来自旧尝试的污染写。

### 4.3 强制续租(自栅栏)

`ExecutorWorker` 续租改为**强制**:`renewLease` 返回 false(属主或 attempt 已失)→ 该 work unit 立停当前副作用推进(本 tick 不再执行 handler 动作),让位给回收判定。不再「尽力而为记日志继续」。

- **粒度裁决(默认,温和)**:仅停**触发栅栏的这一条** work unit;不动同一 worker 的其它在途分片。若需严格全停(同 worker 任一续租失败即停全部在途),仅往上限加力,当前不配。

### 4.4 B1 语义与恢复

- **真死 worker**:heartbeat 停 → ALIVE 消失 → 分片被活性门回收(RUNNING→FAILED)→ DLQ/重试重派 → 新 worker 执行(原 worker 已死,无重叠)。
- **活但 handler 冻结**(worker 进程活着、heartbeat 正常,handler 卡死):不可回收 → 分片滞留直到 worker 停/被标死。**修复依赖** worker 重启或运维介入。这是「拒绝对活体双跑」的成本,如实接受并在告警引擎(可观测)可见滞留(见 §7)。
- **DLQ replay 无重叠**:FAILED 来源仅「属主确认死亡(活性门)」或「handler 自报(clean,已返回)」两类,均无活体双跑。

---

## 5. 迁移

**零新表/零新列**:A 层复用 `idempotency_key`;B1 复用 `attempt`/`worker_id`/`lease_until`/`worker.last_seen`。无 V32。

---

## 6. API 变更

- 4 个写入端点上加**可选** `idempotencyKey` 请求参数(带则幂等键,缺省 UUID 行为)。
- 无新增读端点;无字段/信封变化。OpenAPI 快照需 additively 重导(这 4 条请求参数)。

---

## 7. 可观测

- `findOverRuntime`/`findExpiredRunning` 活性门命中(log/debug)。
- 「活体滞留超规」可被告警引擎消费:活体 RUNNING 超时滞留可作一项信号(与 V31 alert 规则对齐,可选,不阻塞本设计)。

---

## 8. 测试

**A 层(触发去重):**
- 同 `idempotencyKey` 双 `POST /tasks/{id}/trigger` → 单父 + 同 id;异 key → 两父。
- 同 key 双 rerun / 双 manual dag → 单轮;不同 key → 多轮。
- 未带 key → 维持现状(每次新建)。
- 幂等键 prefix 隔离:manual key 与 cron key 不互相拦截。

**B1 层(活性门 + 栅栏):**
- **死 worker 回收**:worker 非 ALIVE → 超时/lease-expiry 分片被回收重派;新 attempt 认领成功执行。
- **活体不回收**:worker ALIVE(即便超时)→ 分片不被回收、不被重派。
- **栅栏拒旧写**:回收重派后(attempt+1),旧 attempt 的 `markStatusOwned(SUCCESS)` 返回 false、不落 outcome;`renewLease` 返回 false。
- **强制续租自栅栏**:renew false → ExecutorWorker 该 tick 不推进 handler(行为断言)。
- 既有认领/配额/超时测试保持绿(行为向后兼容)。

---

## 9. 自审对照(写 plan 时收敛)

- 双层模型已核实;A 层覆盖 4 个「裸 UUID」路径(窗口 1-4),B1 覆盖窗口 5-8。
- 强去重路径(cron/interval/事件/dep)不动。
- 零迁移;全部复用现有列/机制。
- Handler 端副作用幂等为契约,本设计不做(明确边界,不打包超出可验证的承诺)。
- 兼容:所有改动向后兼容(默认行为不变),既有测试应保持绿。

---

## 10. 全局约束

- 版本/依赖:零新增依赖、零迁移;仅 Java/Spring 既有栈。
- 命名:密钥辅助入 `core/IdempotencyKeys`,镜像既有 `forTrigger`/`forManualDagRun`/`forNodeRerun`。
- 平台:单一时钟(DB 持时);worker ALIVE 判据复用现有 heartbeat 语义。
- 测试门:离线 Maven `-o`,`-pl scheduler-server -am`,`-Dsurefire.failIfNoSpecifiedTests=false`;前端 `npm run build` 常绿。