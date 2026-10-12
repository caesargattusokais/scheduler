# Scheduler E3 — 对账行为可观测化(reconciler observability)

> **规划路径**:E 系列(调度纵深)第三件第三题,接 E3#1(progress-heartbeat)、E3#2(handler 投递坐标)之后。
> **前置**:E1 统一活性门控(B1)、E2B stuck-alive 可观测、告警子系统(V31)、审计子系统(V10)、Micrometer/Prometheus 指标面。
> **状态**:设计中
> **更新日期**:2026-10-12

---

## 1. 背景与问题

对账器(Reconciler)是正确性最关键的「裁决者」:它回收租约过期/属主失联的孤儿 RUNNING 分片,并对收敛的父级做终态汇聚。但它当前**完全不可观测**——`scanOnce()` 只返回一个 `int` 回收数,而 `ReconcileLoop.loopOnce()` 把它直接丢弃(`if (!leader.isLeader()) return; reconciler.scanOnce();`)。

**本设计的缺口(对照已核实源码):**

- 全局 Prometheus 指标(`Beans.java` 的 `schedulerMetrics`/`dagMetrics`/`schedulerGlobalMetrics`/`schedulerSliMetrics`)只覆盖活跃分片、DUE 队龄、DAG 活跃、DLQ 深度、worker 存活、SLO——**没有一个反映对账器自身行为**。
- `Reconciler.scanOnce()` 三趟(超时回收 → 孤儿回收 → 父级汇聚)的决策无任何留痕;只有 `execution_shard_outcome.detail`(仅 FAILED 行、仅 `runtime timeout`/`owner unresponsive`)间接泄露了回收归因,聚合决策(SUCCESS/FAILED/CANCELED/PARTIAL_SUCCESS)与 FAIL_FAST 取消兄弟全无记录。
- `FailureResolver.handle()` 返回 `void`,对账器**无从知道自己回收的那片最终走了「重排」还是「死信」**——这是决策留痕最想要的归因恰好被丢弃。
- `markStatus` CAS 返回 false / 抛 `IllegalStateException` 的 **near-miss 竞态**(被 worker 先手)被静默吞掉,毫无信号。
- 无「当前待回收积压」信号:对账器下一趟要回收多少、最老的一条已滞留多久,运维全靠翻 `execution_shard`。

**解法一句话:** 把「对账器的一次行为」固化为 append-only 决策原子(`reconcile_decision` 表),四面对齐——指标是对决策的计数、留痕是决策的行、告警读积压、活动视图列决策流水。

---

## 2. 设计原则与边界(先说破)

- **纯观测,不改回收语义。** 除 `FailureResolver.handle()` 返回枚举这一处契约增强外,回收/汇聚/FAIL_FAST 逻辑零改动。对账器不因可观测而扩大或缩小回收面。
- **决策原子、一等公民存储。** `reconcile_decision` 表只增不改不删(append-only),单一 DB 时钟 `created_at`。
- **指标分两类、各归其源。** 决策派生 → 懒 gauge 查 DB(scrape 时求值,与既有 `scheduler_*` 全部一致);scan 健康(耗时/失败)→ 进程内 Micrometer Timer/Counter(瞬态、无行可落)。
- **单一时钟。** 决策时间戳、积压龄、告警阈值全部读 DB `now()`,不依赖 JVM 墙钟。
- **先观测后处置。** 本设计不做「自动处置积压」——只加告警 + 视图;处置是显式运维动作(镜像 E2B 的分层承诺)。

---

## 3. 决策原子与存储(V33)

### 3.1 `reconcile_decision` 表

| 列 | 类型 | 说明 |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `created_at` | timestamptz NOT NULL DEFAULT now() | DB 时钟 |
| `phase` | text NOT NULL | `timeout_reclaim` / `orphan_reclaim` / `parent_aggregate` / `near_miss` |
| `target_type` | text NOT NULL | `shard` / `execution`(父) |
| `target_id` | bigint NOT NULL | shard id 或 execution id |
| `task_id` | bigint NULL | 关联任务 |
| `reason` | text NOT NULL | `runtime timeout` / `owner unresponsive` / 汇聚归因 |
| `outcome` | text NULL | `REQUEUED` / `DEAD_LETTER` / `SUCCESS` / `FAILED` / `CANCELED` / `PARTIAL_SUCCESS` / `SKIPPED` |
| `from_status` | text NULL | 迁移前状态 |
| `to_status` | text NULL | 迁移后状态 |
| `attempt` | int NULL | 分片 attempt |
| `detail` | text NULL | 如 `3/4 shards ok (1 partial failed)`、超龄秒数 |

索引:`(created_at)`(支撑 1h 窗口计数与活动视图倒序分页);`(target_id)`(取证定位)。可选:`(task_id)`。

### 3.2 `FailureOutcome` 枚举(persistence.retry)

```java
public enum FailureOutcome { REQUEUED, DEAD_LETTER }
```

`FailureResolver.handle(...)` 返回它:`scheduleRetry` 分支 → `REQUEUED`,`deadLetter` 分支 → `DEAD_LETTER`(该分支内部已含 `cancelSiblings` FAIL_FAST,故 `DEAD_LETTER` 即隐含 fail-fast,不单独记逐片取消行)。worker 侧(`ExecutorWorker`)调用点忽略返回值,零行为变更。

---

## 4. 决策点接线

`Reconciler.scanOnce()` 在以下位置经 `ReconcileTelemetry.record(...)` 落行:

1. **超时回收** — `markStatus(FAILED,"runtime timeout")` 为 true → `failureResolver.handle(...)` 返回 `outcome` → `record(phase=timeout_reclaim, target=shard, reason="runtime timeout", outcome, from=RUNNING, to=FAILED, attempt)`。
2. **孤儿回收** — `markStatus(FAILED,"owner unresponsive")` 为 true → 同上落 `phase=orphan_reclaim, reason="owner unresponsive"`。
3. **父级汇聚** — `finalizeParent(pid, parentTerminal, detail)` → `record(phase=parent_aggregate, target=execution, taskId, outcome=parentTerminal, from=DUE, to=parentTerminal, detail)`。仅在成功迁移时记;「父尚未全终态」或「finalizeParent CAS=0 幂等跳过」不记(纯噪声)。
4. **near-miss 竞态** — 阶段 1/2 的 `markStatus` 返回 false 或抛 `IllegalStateException` → `record(phase=near_miss, target=shard, reason, outcome=SKIPPED, from=RUNNING, to=RUNNING)`。

## 5. 组件

- **`ReconcileDecision`(persistence)**:决策行值记录(与 `OutboundNotification` 同层的 persistence 记录)。
- **`ReconcileDecisionRepository`(persistence 接口)+ `JdbcReconcileDecisionRepository`**:
  - `void record(ReconcileDecision d)`
  - `long countPhaseSince(String phase, Instant since)`(决策派生 gauge 用)
  - `List<ReconcileDecision> findRecent(int limit, int offset)` / `long count()`(活动视图分页用)
- **`ShardRepository` 增**:
  - `long countReclaimableRunning(Long taskId, int staleAfterSeconds)`(null=全局;gauge + 告警 + 视图 total 同源)
  - `long maxReclaimableAgeSeconds(int staleAfterSeconds)`(全局最老待回收片,积压龄 gauge)
  - `List<ReclaimableShard> findReclaimableRunning(Long taskId, int staleAfterSeconds, int limit, int offset)`(积压视图分页)
  - `record ReclaimableShard(long id, long executionId, long taskId, String workerId, int attempt, long ageSeconds)`
- **`ReconcileTelemetry`(server.reconcile)**:包 `ReconcileDecisionRepository` + `MeterRegistry`,给 Reconciler 单一接缝:
  - `void record(ReconcileDecision d)` → repo.record
  - `<T> T timedScan(Supplier<T> scan)` → Timer + 失败 Counter(Timer 启动 `scheduler_reconcile_scan_duration`;异常 `scheduler_reconcile_scan_failures_total` 自增后重抛,保留既有 ConfigurableLoop「兜底不杀线程」语义)
- **`Reconciler`**:注入 `ReconcileTelemetry`,三决策点 + near-miss 调用 `record`,scan 体用 `timedScan` 包裹。

**积压语义(重要):** 「待回收」= **属主已死(非 ALIVE)且仍 RUNNING 的已认领分片**,任务无关——这正是 B1 活性门在下一趟会回收的集合(`findExpiredRunning` 谓词的全局形态;超时回收的片亦属死属主,故天然被涵盖,不被排除)。**积压龄** = 该片自「进入可回收」以来的秒数,锚点为其属主 worker 的心跳失联时点 `last_seen`;属主行缺失(已认领却无 worker 行)时锚 `lease_until`。

---

## 6. 指标

### 6.1 决策派生(lazy gauge,`Beans.java` 新增一个 `MeterBinder` bean,读 `ReconcileDecisionRepository` + `ShardRepository`)

- `scheduler_reconcile_reclaimed_1h` — 近 1h `phase IN (timeout_reclaim, orphan_reclaim)` 计数。
- `scheduler_reconcile_timeout_reclaimed_1h` — 近 1h `timeout_reclaim` 计数。
- `scheduler_reconcile_orphan_reclaimed_1h` — 近 1h `orphan_reclaim` 计数。
- `scheduler_reconcile_aggregated_1h` — 近 1h `parent_aggregate` 计数。
- `scheduler_reconcile_near_miss_1h` — 近 1h `near_miss` 计数。
- `scheduler_reconcile_backlog` — `countReclaimableRunning(null, stale)`。
- `scheduler_reconcile_backlog_max_age_seconds` — `maxReclaimableAgeSeconds(stale)`。

### 6.2 scan 健康(进程内 Micrometer)

- `scheduler_reconcile_scan_duration`(Timer,自带 `_count`/`_sum`/`_max`)
- `scheduler_reconcile_scan_failures_total`(Counter)

---

## 7. 告警(V31)

- `RuntimeConfigKeys` 白名单 + `DEFAULTS` 加 `ALERT_RECONCILE_BACKLOG = "alert.reconcile-backlog"`,默认 `"3"`。
- `AlertEngine.collectSignals` 加全局规则 `reconcile-backlog`(镜像 `stuck-alive`):

```java
long rThr = settings.getLong(RuntimeConfigKeys.ALERT_RECONCILE_BACKLOG, 3L);
if (rThr > 0) {
  long n = shards.countReclaimableRunning(null, STUCK_ALIVE_STALE_SECONDS);
  out.add(new Signal("reconcile-backlog", null, null, "high", Long.toString(n), n >= rThr));
}
```

episode 状态机(PENDING→OPEN→RESOLVED)、全局 key(`rule` 裸名)、严重级 `high` 全部复用。阈值 =「存在 ≥N 条待回收积压」→ 对账器滞后(默认容忍 3:低于此可能是正常认领窗口抖动)。

---

## 8. 视图(两个只读端点,审计读开放)

新增 `ReconcileController`(`/api/v1/reconcile`),读查询、无副作用:

- `GET /api/v1/reconcile/backlog` — 当前待回收积压明细,返回 `Page<ReclaimableShard>`(`{items,total,offset,limit}`)。字段:`id/executionId/taskId/workerId/attempt/ageSeconds`。公共 `limit`(默认 100,clamp 1–500)/`offset`/可选 `taskId` 过滤;驱动前端「对账积压」页。
- `GET /api/v1/reconcile/activity` — 最近 `reconcile_decision` 流水,`Page<ReconcileDecision>` 倒序(`created_at DESC, id DESC`)。驱动前端「对账流水」页。

`MetricsController` 不变(它读 `ExecutionMetrics`,非对账)。控制台 `/metrics` 页新增一张「对账」卡展示 §6.1 的 gauge(前端)。

---

## 9. 迁移

**V33 `reconcile-decision`:** 建 `reconcile_decision` 表 + 索引(§3.1),无既有表改造、无数据迁移(append-only 新增表)。

---

## 10. 明确不做

- **不**改回收/汇聚/FAIL_FAST 判定逻辑(除 `handle()` 返回枚举)。
- **不**自动处置积压(只观 + 告警;处置另立 spec)。
- **不**记录「无动作扫描」/「父尚未全终态跳过」/「finalizeParent 幂等跳过」为决策(噪声)。
- **不**单独记 FAIL_FAST 逐片取消(由 `outcome=DEAD_LETTER` 隐含)。
- **不**做 worker↔reconciler 双向竞态账(只记对账器侧被抢的 near-miss)。
- **不**给 `reconcile_decision` 引入清理策略/保留期(append-only,留存交由后续审计保留里程碑统一处理)。

---

## 11. 测试策略

- **persistence**:`JdbcReconcileDecisionRepository` 的 record/countPhaseSince/findRecent/count;`JdbcShardRepository` 的 `countReclaimableRunning`(全局+task 过滤)/`maxReclaimableAgeSeconds`/`findReclaimableRunning` 分页(Testcontainers)。
- **`FailureResolver` 单测**:`handle` 返回 REQUEUED vs DEAD_LETTER 分界(retryBudget 耗尽 / maxRetries 越界)。
- **server**:`Reconciler` 每次回收/汇聚/near-miss 落对应 `reconcile_decision` 行(phase/outcome/reason/from/to 精确);`timedScan` 计时不吞异常;near-miss 播种竞态(markStatus 被先手)落 `SKIPPED` 行。
- **alert**:`reconcile-backlog` 阈值命中/恢复(镜像 E2B stuck-alive 全链路)。
- **endpoint**:`backlog`/`activity` 分页 + task 过滤 + 只读(集成测试)。
- **回归**:既有 Reconciler/告警/DLQ 测试保持绿;离线 `mvn -o test` 全绿 + 前端 `npm run build` 常绿。

---

## 12. 全局约束

- **依赖**:零新增第三方依赖(仅既有 Micrometer/Spring JDBC/Flyway)。
- **命名**:`reconcile_decision` 表、`scheduler_reconcile_*` 指标前缀、`reconcile-backlog` 告警键、`Reconcile*` 类型统一;对齐既有 `scheduler_*` gauge 与 `alert.*` 键惯例。
- **平台**:单一时钟(DB `now()`);积压判据复用 E1 的活体窗(stale 秒与 Reconciler 同源,默认 30)。
- **API**:只读端点审计读开放;遵循 `Page<T>` 分页信封(limit 默认 100,clamp 1–500)。README API 表与 `docs/openapi/api-docs.json` 同步新增两条。
- **测试门**:离线 Maven `-o`;persistence/server → `-pl scheduler-persistence|-pl scheduler-server -am`,`-Dsurefire.failIfNoSpecifiedTests=false`;前端 `npm run build`。