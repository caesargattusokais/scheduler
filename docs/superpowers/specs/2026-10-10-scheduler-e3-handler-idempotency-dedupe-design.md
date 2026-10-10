# Scheduler E3 — handler 侧幂等去重契约

> **规划路径**:E 系列(调度纵深)第三件第二题,接在 E3#1(progress-heartbeat)之后。
> **前置**:E1 统一幂等(`IdempotencyKeys` + 唯一约束 + attempt 栅栏)、E2B stuck-alive、E3#1(已给 `HandlerContext` 加 `progress`/`touch()` 九组件)。
> **状态**:已实现(2026-10-10)
> **更新日期**:2026-10-10

---

## 1. 背景与问题

E1 已把「行级 exactly-once」做扎实:同一次逻辑触发只落一条 `execution`/`execution_shard`(`IdempotencyKeys` + 唯一约束),双触发/重认领不会造出两行。

但这只保证 **调度器侧不多建行**,不保证 **handler 侧不多跑**。一个分片在其生命周期内仍会被**多次投递给 handler**:

- 租约到期 → 崩溃回收走重试策略 → 换个 worker 重跑一遍;
- 显式重试(瞬时故障);
- 未注册 handler / 抛错后按策略重抛(仍可能重投)。

而 handler 的**副作用**(写下游库、调外部接口)可能不幂等:上一把 attempt 已落地一半副作用,这一把又跑一遍。

**缺口(对照已核实源码)**:

- `HandlerContext`(九组件)完全不暴露 `attempt` —— handler 无从判断「这次是不是重投递」。
- handler 也没有一个稳定的、平台统一的幂等键(虽然它自己能拼 `executionId:shardIndex`)。

**解法**:给 `HandlerContext` 加「投递坐标」—— `attempt`(第几次投递)+ 派生稳定幂等键 `idempotencyKey()` —— 把「这次是不是重投递、同一个逻辑单元」讲清楚,handler 自己拿这个坐标对自己下游做幂等。平台**只给信号、不替 handler 判定**。

---

## 2. 设计原则与边界(先说破)

- **纯契约信号,不是强制执行。** 平台不做去重注册表、不做结果缓存回放、不拦重投递。真正幂等必须落在 handler 自己的下游事务边界里。
- **不改可靠性三元组。** 重试/重认领/租约/`attempt` 栅栏语义全部原样;`attempt` 本来就是 `execution_shard.attempt`,这里只是把它暴露出来。
- **重试与重认领不做细分。** 在 worker 层,崩溃回收走的就是重试策略(`attempt` 递增);两类都统一表现为「又一次 attempt」。若未来确需区分 RETRY vs RECLAIM,另立 spec(worker 当前拿不到可靠区分依据)。
- **可加、向后兼容。** 新字段有缺省,既有 handler 完全不受影响(它们本就不读 attempt)。
- **无新列、无迁移。** `attempt` 已在列;`idempotencyKey` 是派生的。

---

## 3. 契约语义

### 3.1 `HandlerContext` 新增

```java
public record HandlerContext(
    long executionId, long shardId, int shardIndex, int shardCount,
    String shardData, String args, String workerId, CancellationToken token,
    ProgressSignal progress,
    // E3#2 新增:本次投递的在途 attempt(1-based)
    int attempt
) {

  /** 稳定幂等键:本分片"逻辑单元"的标识,跨 attempt/重认领不变(= executionId : shardIndex)。 */
  public String idempotencyKey() { return executionId + ":" + shardIndex; }

  /** 是否重投递(attempt > 1;attempt == 1 即首次投递)。 */
  public boolean redelivery() { return attempt > 1; }
}
```

- `attempt` 是**新构造组件**(不可由既有组件派生,必须材质化);`idempotencyKey()`/`redelivery()` 是**派生方法**(零构造链变更)。
- `of(...)` 便捷构造补 `attempt = 1`(首次投递语义);`ExecutorWorker` 的真实投递构造点传 `shard.attempt()+1`(见 §4),其余 canonical 构造点(测试/示范 handler)补尾参缺省 `1`。

### 3.2 `attempt` 语义

- 在途 attempt = `shard.attempt() + 1`(与 `ExecutorWorker` 现有 renewLease/markStatusOwned 的传法一致:认领时 claim 已 attempt+1,`shard` 是认领前(DUE)快照)。
- `attempt == 1` → 首次投递;`attempt > 1` → 重投递(重试或崩溃重认领,视作「同一逻辑单元的又一次」)。

### 3.3 契约实质(handler 的期望)

- handler 拿 `idempotencyKey()`(本分片逻辑单元)做下游幂等锚点;需要更细粒度时自行拼 `attempt`。
- **平台不消重投递**:重试本来就是要重跑,去重粒度只有 handler 自己知道。平台只负责把坐标给准、给稳。

---

## 4. 数据流

- `ExecutorWorker.workOne()` 构造 `HandlerContext` 时额外传两样:
  - `attempt`: `shard.attempt() + 1`;
  - `idempotencyKey`:派生方法,构造点无需传(由 `executionId`/`shardIndex` 现算)。
- 无迁移;`execution_shard.attempt` 既有列直接用。

---

## 5. 明确不做

- **不做**平台强制去重 / 去重注册表 / 结果缓存回放(即设计期否决的 Option A/C)。
- **不做**重试与重认领的细分(RETRY vs RECLAIM)。
- **不改** retry/reclaim/lease/attempt 栅栏语义。
- **不做**值语义进度 / 百分比(那是 E3#1 的边界,依旧不做)。

---

## 6. 测试策略

- **worker**:`HandlerContextTest` —— `attempt` 默认值、`redelivery()` 分界(`attempt==1` false / `>1` true)、`idempotencyKey()` 稳定格式;`ExecutorWorkerTest` —— 首次 vs 重投递 context 的 `attempt` 正确(在途 = 快照+1)。
- **demo handler**:示范用 `ctx.idempotencyKey()`(+ `ctx.attempt()`)对下游做幂等的写法。
- 无 persistence/schema 改动 → 无迁移测试。

---

## 7. 遗留 / 后续

- 若未来要「平台强制去重」或「结果缓存回放」,另立 spec(本设计明确的否决项,不在此期展开)。
- 第三题(对账行为可观测化)另立 spec。