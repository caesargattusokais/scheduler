# Scheduler v2 — 可靠性纵深(宕机履约与快速收敛)设计

> **Goal:** 把宕机任务的收敛从「分钟级假死」钉到「秒级:要么接续、要么带归因明确失败,零永久 RUNNING」;
> 同时把大任务集的扫描改为分批,单 tick 不重不漏。
>
> **Architecture:** 不改领域模型、不动执行运行时。核心是 server 侧 Reconciler 的失效判定升级——
> 从「只认租约自然过期」改为「租约过期 **或** owner worker 心跳失联」双信号;配合 lease 参数收窄与
> worker 新失联判定窗口。防双跑继续复用既有的 owner 归属守卫,不引入新机制。
>
> **Tech Stack:** Java 17、Spring Boot、PostgreSQL 16(DB `now()` 统一时钟)。无新增依赖。
>
> **Spec → plan 关系:** 本文件是 v2「可靠性纵深先行」第 1 子项目的 spec;执行运行时深化、企业化为后续独立子项目,不在本 spec 范围。

## 基线:现状与时延推导

| 环节 | 现状值 | 位置 |
|------|--------|------|
| worker 心跳写 `worker.last_seen` | 每 10s | `ExecutorWorker` / worker 注册器 |
| server 判定 worker 活性 | `last_seen ≤ 30s` 且 `status='ALIVE'`(指标 `scheduler_worker_active` 口径) | server 读视图 |
| shard 租约 `lease_until` | 初始 +60s(硬编码),在线续约至 +`scheduler.lease.seconds:120`,每 `renew-seconds:30` | `ExecutorWorker.claim` / `renewLease` |
| 孤儿回收判定 | `findExpiredRunning` = `status='RUNNING' AND lease_until <= now()` | `JdbcShardRepository` |
| 回收/汇聚周期 | `scheduler.reconcile.delay-ms:30000` | `ReconcileLoop` |

**宕机收敛最坏时延推导**:worker 死(不再续租/心跳)→ lease 停在最后一次成功续约(≤ +120s)→
Reconciler 每 30s 撞到 `lease_until <= now()`。最坏 ≈ 2-3 分钟,平均 ≈ 1.5-2 分钟。→ 目标收敛到 ≈45s 内。

## 关键列名(已核准,不猜测)

- `execution_shard.worker_id` = 当前归属 worker(owner)。renewLease 已用它做所有权守卫。
- `execution_shard.attempt` = 本次运行号(worker claim-前 attempt+1;reconciler 回收原样传入)。
- `worker.id` = worker 主键(字符串 workerId);`worker.status` / `worker.last_seen`。
- JOIN 用 `worker.id = execution_shard.worker_id`。

---

## §1 活性优先接管(核心)

失效判定从单信号(租约过期)升级为双信号(租约过期 **或** owner 心跳失联)。

**改 `findExpiredRunning`** 的 WHERE,扩展 owner 失联分支:

```sql
SELECT s.id, s.attempt
FROM execution_shard s JOIN execution e ON e.id = s.execution_id
WHERE e.task_id=?
  AND s.status='RUNNING'
  AND (
    s.lease_until <= now()                       -- 兜底:租约自然过期
    OR ( s.worker_id IS NOT NULL                 -- 主路径:owner 心跳停滞(仅当有归属时判定;
         AND NOT EXISTS (                        --  worker_id 为 null 的 RUNNING 异常行走 lease 兜底,不被活性分支误收)
          SELECT 1 FROM worker w
          WHERE w.id = s.worker_id
            AND w.last_seen >= now() - interval '<stale-seconds> seconds'
            AND w.status = 'ALIVE'
       ))
  )
```

- 判定信号 = `worker.last_seen`(DB 心跳表,权威活性来源;team 已用它判 active ≤30s,口径一致)。
- `stale-seconds` 默认 `30`(心跳 10s → 容 3 拍抖动),配置键 `scheduler.worker.stale-after-seconds`。
  - 对齐 server 现有 active 判定窗口,避免两套口径漂移。
- `ExpiredShard(id, attempt)` 签名不变(§2 不需要额外字段)。查询 `JOIN execution` 到 task 不变。
- **回收动作不变**:Reconciler 对每行仍 `markStatus(FAILED)` CAS + 共享 `FailureResolver.handle(task, id, attempt, detail)`(重试 / 死信 / FAIL_FAST,与 worker 共用同套判定)。
- **归因**:detail 统一为 `owner unresponsive or lease expired`(单查询不区分两因;outcome 行 detail 可见,足够排查;避免为实现「二元 detail」增加查询复杂度)。

**收敛预算**:死 worker → 心跳停滞 ≤30s + reconcile 周期(下修到默认 15s)→ **≈45s 内必回收**,比现状快 3-4 倍。

## 防双跑护栏(不变量,复用,不新增)

- worker 写回终态前校验 owner 归属(worker_id 匹配)。
- `renewLease` 带 `status='RUNNING' AND worker_id=?` 条件:0 行静默。
- **推论**:即便因心跳抖动误判慢 worker 死亡、抢走其 shard,该 worker 后续写回/续租均被拒,不产生双写;被抢 shard 由 DUE 幂等保证重跑、不重。

> 这就是「误判牺牲结果、绝不双跑」的取舍:被打断的慢 worker 结果丢弃,由新 owner 幂等重跑。

## §2 宕机履约(零永久 RUNNING + 归因)

§1 的查询覆盖所有「owner 失联 **或** lease 过期」的 RUNNING;配合下修后的 reconcile 周期,遍历 `tasks.findAll()`:

> **不变量**:对每个任务,任一 RUNNING shard 要么 owner 活着、要么在一个有限窗口(≤ stale + reconcile 周期 + 宽限)内必进回收。→ 语义上「零永久 RUNNING」。

- 极慢但存活(degraded yet alive)的 worker 由 lease 兜底路径覆盖,不误杀。
- 每个回收都落 `execution_shard_outcome`(FAILED + 归因),可解释、可追踪、可审计 → 成为 v2 后续「运维治理」子项目的数据基础。

## §3 lease 参数收窄与配置化

默认收紧,健康 worker 靠续租无感;全部经 `@Value` 读取(现已有 `scheduler.lease.seconds` / `scheduler.lease.renew-seconds`)。

| 配置键 | 现状 | v2 默认 | 说明 |
|---|---|---|---|
| `scheduler.lease.seconds` | 120 | 60 | 在线续租目标(健康 worker 续租周期保住) |
| `scheduler.lease.renew-seconds` | 30 | 15 | worker 续租周期(健康 worker 无感) |
| `scheduler.reconcile.delay-ms` | 30000 | 15000 | 回收/汇聚周期 |
| `scheduler.worker.stale-after-seconds` | —(新增) | 30 | owner 失联判定窗口(§1) |

> 初始 claim lease 的硬编码 +60s 保留(认领后立即被 renewer 提到 lease.seconds),不动 worker 认领行为。

## §4 扫描分批健壮性(低优先短活,与 §1-3 不耦合)

- TriggerEngine `scanOnce` 遍历 enabled cron 任务、DagEngine `scanPropagation` 遍历活跃 run:改为**游标/分批**,使大任务集单 tick 不重不漏、崩在中途可续。
- 触发侧幂等已保证「不重」(幂等键),本项补「不漏 + 单 tick 不拖垮」。
- 定位低优先短活:可与 §1-3 并行排期,或独立后续。**不阻塞**宕机履约主线。

## 配置变更汇总(新增/收窄)

| 变更 | 类型 |
|---|---|
| `scheduler.worker.stale-after-seconds:30` | 新增(§1) |
| `scheduler.lease.seconds:120→60` | 收窄(§3) |
| `scheduler.lease.renew-seconds:30→15` | 收窄(§3) |
| `scheduler.reconcile.delay-ms:30000→15000` | 收窄(§3) |

## 测试计划

自测用现有 Testcontainers 每模块集成测试 + DB `now()` 时钟(JVM 内 Clock 与实际 DB now 并存,判活依赖 DB now,测试用 testcontainers PG 真实 `now()`)。

1. **持久层**:`JdbcShardRepositoryTest` — 新查询(SQL 直接):
   - RUNNING 且 owner 心跳陈旧(`last_seen < stale`)→ 被 `findExpiredRunning` 返回;
   - RUNNING 且 owner 心跳新鲜(last_seen 距今 < stale)→ 不被误收;
   - RUNNING 且 lease 过期但 owner 新鲜 → 仍被返回(兜底路径保留);
   - 非 ALIVE 的 worker(`status='ALIVE'` 外的行)→ 视为失联被收。
2. **server**:`ReconcilerTest` — 活性优先接管:seed task+parent+shard,claim 给 worker X,把 `worker` 表 X 的 `last_seen` 拨到 stale 之前(RUNNING、lease 未过期)→ `scanOnce` 应回收该 shard(`markStatus` FAILED + outcome)。对照组:last_seen 新鲜 → 不被回收。
3. **server**:`AdvisoryLockLeaderElection` 不受影响(不回归)。
4. **配置**:默认值收窄的断言(读 `@Value` 注入参数)。

## Non-goals(YAGNI 明确排除;本 spec 不做)

- **server 多节点满分担**:单机扫全量对单业务库非瓶颈,引入跨节点一致性复杂度属负性价比,方案 C 讨论时明确否掉。
- **PG 高可用联动(主备切换重新选主)**:advisory lock 已心跳兜住主连接断开;「切换后新主重新选主」是部署层(连接串指向 VIP/故障转移)而非库代码,列为独立运维主题。
- **治理/告警/审计 UI**:属独立子项目,本 spec 仅通过 outcome 归因留数据基础。

## Spec 自审记录

- 占位符扫描:无 TBD/TODO;所有 SQL、列名、配置键、默认值均已核准(worker 表 `id`/`status`/`last_seen`,shard `worker_id`/`attempt`)。
- 内部一致性:§1 查询口径与 server「active ≤30s」口径对齐;防双跑复用既有归属守卫,不新增机制。
- 范围:聚焦宕机履约 + 分批,排除满分担/PG-HA/治理。单实现计划可控。
- 歧义:归因 detail 统一定为 `owner unresponsive or lease expired`(已 ruling,避免双因拆分复杂度);初始 claim +60s 保留(已 ruling);活性分支仅当 `worker_id IS NOT NULL` 时生效——`worker_id IS NULL` 的 RUNNING 为异常数据(claim 必写 owner),走 lease 兜底、不被活性分支误收,且保护测试中未 claim 的播种 RUNNING 行(已 ruling)。