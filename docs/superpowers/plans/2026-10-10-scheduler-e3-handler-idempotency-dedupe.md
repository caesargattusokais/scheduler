# Handler 侧幂等去重契约(投递坐标)实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 `HandlerContext` 加「投递坐标」—— `attempt`(在途第几次投递)+ 派生 `idempotencyKey()`/`redelivery()` —— 让 handler 能判断自己是否是重投递并要求 handler 对自己下游做幂等。

**Architecture:** `HandlerContext` record 增一个构造组件 `int attempt`(1-based,首次=1);`idempotencyKey()`(=`executionId:shardIndex`)与 `redelivery()`(=attempt>1)做成派生方法,零构造链变更。`ExecutorWorker` 在真实投递点传 `shard.attempt()+1`(与现有 renewLease/markStatusOwned 的在途 attempt 传法一致)。纯契约信号:平台不强制去重、不做结果缓存、不拦重投递、不改可靠性三元组。无新列、无迁移。

**Tech Stack:** Java 21, Spring Boot 3.2.5, JUnit 5(纯单元 + Testcontainers postgres:16-alpine), Maven 多模块。

**Spec:** `docs/superpowers/specs/2026-10-10-scheduler-e3-handler-idempotency-dedupe-design.md`

## Global Constraints

- **纯契约信号,不强制**:平台不做去重注册表、不做结果缓存回放、不拦重投递。
- **不改可靠性三元组**:重试/重认领/租约/`attempt` 栅栏语义全部原样;`attempt` 本就是 `execution_shard.attempt`,这里只是暴露出来。
- **重试与重认领不细分**:两类都统一表现为「又一次 attempt」(`attempt` 递增)。
- **无新列、无迁移**:`attempt` 已在列;`idempotencyKey` 派生,不写库。
- **契约取值的精确约定**:幂等键 = `executionId + ":" + shardIndex`(字符串拼接);`attempt` 1-based,首次投递 = 1,重投递 = >1。

---

### Task 1: `HandlerContext` 投递坐标 + 派生方法 + 集成接线

**Files:**
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/HandlerContext.java`
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/execute/ExecutorWorker.java:120-122`
- Modify: `scheduler-worker/src/test/java/dev/scheduler/worker/handler/HandlerContextTest.java`
- Modify: `scheduler-worker/src/test/java/dev/scheduler/worker/handler/EchoHandlerTest.java:20,29`
- Modify: `scheduler-worker/src/test/java/dev/scheduler/worker/handler/MyJobHandlerTest.java:20,30`

**Interfaces:**
- Consumes: 既有 `HandlerContext` 九组件 record + `of(...)` 便捷构造(spec §3.1);`ExecutorWorker` 既有构造点在 `shard`(DUE 快照)上读 `executionId()/id()/shardIndex()/attempt()`。
- Produces: `HandlerContext` 十组件 record(末位 `int attempt`);实例方法 `String idempotencyKey()`、`boolean redelivery()`;`of(...)` 默认 `attempt=1`。后续 Task 2/3 消费这些方法。

- [ ] **Step 1: 扩展 `HandlerContext` record(加组件 + 两个派生方法)**

把整个 `HandlerContext.java` 替换为下面内容(注意:组件顺序、`of(...)` 尾参 `1`、两个派生方法放在 record body):

```java
package dev.scheduler.worker.handler;

/** 一次分片运行的上下文:父 execution + 本分片坐标 + 父 args + 协作取消令牌 + E3 进度心跳。
 *  {@link #touch()} 把「仍在推进」上报给 worker 续租线程合并 flush({@code progress==null} 时 no-op)。
 *  E3#2 投递坐标:{@link #attempt()} 为本次投递的在途 attempt(1-based,1=首次,>1=重投递),
 *  {@link #idempotencyKey()} 为本分片逻辑单元的稳定标识,{@link #redelivery()} 即 attempt>1。 */
public record HandlerContext(long executionId, long shardId, int shardIndex, int shardCount,
                             String shardData, String args, String workerId, CancellationToken token,
                             ProgressSignal progress, int attempt) {

  /** 便捷构造:默认未取消 token、无 workerId、无进度心跳、attempt=1(首次投递),供手工/既有测试在不关心
   *  协作取消/进度/投递坐标时使用。 */
  public static HandlerContext of(long executionId, long shardId, int shardIndex, int shardCount,
                                  String shardData, String args) {
    return new HandlerContext(executionId, shardId, shardIndex, shardCount, shardData, args, null,
        new CancellationToken(false), null, 1);
  }

  /** E3 进度上报:置位进度信号(重交由 worker 周期合并落库);progress==null 时 no-op。 */
  public void touch() {
    if (progress != null) progress.touch();
  }

  /** E3#2 稳定幂等键:本分片逻辑单元的标识,跨 attempt/重认领不变(= executionId : shardIndex)。 */
  public String idempotencyKey() {
    return executionId + ":" + shardIndex;
  }

  /** E3#2 是否重投递(attempt > 1;attempt == 1 即首次投递)。 */
  public boolean redelivery() {
    return attempt > 1;
  }
}
```

- [ ] **Step 2: `ExecutorWorker` 构造点传在途 attempt = `shard.attempt() + 1`**

把 `ExecutorWorker.java` 121-122 行的构造调用从 9 参改 10 参(追加 `attempt`):

```java
        HandlerContext ctx = new HandlerContext(shard.executionId(), shard.id(), shard.shardIndex(),
            parent.shardCount(), shard.shardData(), parent.args(), workerId, token, progress,
            shard.attempt() + 1);
```

- [ ] **Step 3: 修复既有 canonical 构造点(补尾参 `1`)**

`HandlerContextTest.java` 第 10 行:

```java
    var ctx = new HandlerContext(1, 2, 0, 1, null, null, "w", new CancellationToken(false), p, 1);
```

`EchoHandlerTest.java` 第 20 行、第 29 行(各补 `, 1`):

```java
    var ctx = new HandlerContext(1L, 2L, 3, 5, "data", "hello", "worker-a", new CancellationToken(false), null, 1);
```

```java
    var ctx = new HandlerContext(1L, 2L, 0, 1, null, null, "worker-a", new CancellationToken(true), null, 1);
```

`MyJobHandlerTest.java` 第 20 行、第 30 行(各补 `, 1`,与 EchoHandlerTest 同构):

```java
    var ctx = new HandlerContext(1L, 2L, 3, 5, "data", "hello", "worker-a", new CancellationToken(false), null, 1);
```

```java
    var ctx = new HandlerContext(1L, 2L, 0, 1, null, null, "worker-a", new CancellationToken(true), null, 1);
```

- [ ] **Step 4: `HandlerContextTest` 加新字段/派生方法单元测试**

在 `HandlerContextTest.java` 里,把 import 扩成含 `assertEquals`、`assertFalse`(保留现有 `assertNull`/`assertTrue`),再追加三个测试方法:

```java
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
```

```java
  @Test void attempt_defaultsToFirstDelivery_viaOf() {
    var ctx = HandlerContext.of(1, 2, 0, 1, null, null);
    assertEquals(1, ctx.attempt(), "of(...) 便捷构造默认首次投递 attempt=1");
    assertFalse(ctx.redelivery(), "attempt=1 不应标记为重投递");
  }

  @Test void redelivery_trueWhenAttemptGreaterThanOne() {
    var ctx = new HandlerContext(1, 2, 0, 1, null, null, "w", new CancellationToken(false), null, 2);
    assertEquals(2, ctx.attempt());
    assertTrue(ctx.redelivery(), "attempt=2 应标记为重投递");
  }

  @Test void idempotencyKey_isStableExecutionIdColonShardIndex() {
    var first = new HandlerContext(42, 7, 3, 5, null, null, "w", new CancellationToken(false), null, 1);
    var retry = new HandlerContext(42, 7, 3, 5, null, null, "w", new CancellationToken(false), null, 9);
    assertEquals("42:3", first.idempotencyKey(), "幂等键 = executionId:shardIndex");
    assertEquals(first.idempotencyKey(), retry.idempotencyKey(), "幂等键跨 attempt 不变");
  }
```

- [ ] **Step 5: 跑 worker 纯单元测试(无需 Docker)**

Run: `mvn -o test -pl scheduler-worker -am -Dtest=HandlerContextTest,EchoHandlerTest,MyJobHandlerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(`HandlerContextTest` 5 用例 + `EchoHandlerTest` 3 + `MyJobHandlerTest` 3 全绿;这些是纯 JUnit,不依赖 Testcontainers)。

- [ ] **Step 6: Commit**

```bash
git add scheduler-worker/src/main/java/dev/scheduler/worker/handler/HandlerContext.java \
        scheduler-worker/src/main/java/dev/scheduler/worker/execute/ExecutorWorker.java \
        scheduler-worker/src/test/java/dev/scheduler/worker/handler/HandlerContextTest.java \
        scheduler-worker/src/test/java/dev/scheduler/worker/handler/EchoHandlerTest.java \
        scheduler-worker/src/test/java/dev/scheduler/worker/handler/MyJobHandlerTest.java
git commit -m "feat(e3): HandlerContext delivery coordinate (attempt + idempotencyKey + redelivery)"
```

---

### Task 2: `ExecutorWorkerTest` 投递坐标验证(首次=1,重试=2,幂等键稳定)

**Files:**
- Modify: `scheduler-worker/src/test/java/dev/scheduler/worker/execute/ExecutorWorkerTest.java`

**Interfaces:**
- Consumes: Task 1 产出的 `HandlerContext.attempt()/idempotencyKey()/redelivery()`;既有 fixture(`RecordingHandler`、`createTask(handlerRef,maxRetries,backoffMs,pattern,shardCount,maxActiveConcurrent)`、`seedParentAndShards`、`shard(parentId,idx)`、`worker(registry)`)。
- Produces: 集成回归铺证 in-flight `attempt` 与稳定 `idempotencyKey` 端到端正确。

- [ ] **Step 1: 给 `happyPath` 补投递坐标断言**

在 `happyPath_claimsShard_runsWithShardContext_writesSuccess` 里,紧接既有 `assertEquals(1, ctx.shardCount()...)` 之后(约 148 行)追加三行:

```java
    assertEquals(1, ctx.attempt(), "首次投递 attempt=1");
    assertFalse(ctx.redelivery(), "首次投递 redelivery=false");
    assertEquals(parentId + ":0", ctx.idempotencyKey(), "幂等键 = executionId:shardIndex");
```

- [ ] **Step 2: 新增重试驱动的投递坐标测试**

在 `handlerReceivesShardIndexAndCount` 之后追加下面测试(一次性失败 handler 镜像既有 `futureBackoffGate_...` 的 gate 拨回手法):

```java
  /** E3#2 投递坐标:重试驱动的第二次投递 attempt 递增到 2、幂等键稳定不变(同 executionId:shardIndex)。 */
  @Test void handlerReceivesInFlightAttempt_andStableIdempotencyKey_acrossRetry() {
    var attempts = new ArrayList<Integer>();
    var keys = new ArrayList<String>();
    var oneShot = new ExecutionHandler() {
      boolean failNext = true;
      @Override public String ref() { return "rec"; }
      @Override public void handle(HandlerContext ctx) {
        attempts.add(ctx.attempt());
        keys.add(ctx.idempotencyKey());
        if (failNext) { failNext = false; throw new RuntimeException("first fail"); }
      }
    };
    var registry = new MapHandlerRegistry(List.of(() -> oneShot));
    long taskId = createTask("rec", 1, 1000, null, 1, 1); // maxRetries=1 → 可重试一次
    long parentId = seedParentAndShards(taskId, 1);
    long shardId = shard(parentId, 0).id();

    assertTrue(worker(registry).workOne(), "首调 attempt=1,抛错 → 调度重试(DUE)");
    jdbc.update("UPDATE execution_shard SET next_retry_at = now() - interval '1 second' WHERE id=?", shardId);
    assertTrue(worker(registry).workOne(), "次调 attempt=2,成功");

    assertEquals(List.of(1, 2), attempts, "两次投递的在途 attempt 依次为 1、2");
    assertEquals(parentId + ":0", keys.get(0), "幂等键 = executionId:shardIndex");
    assertEquals(keys.get(0), keys.get(1), "幂等键跨投递稳定不变");
    assertEquals(ExecutionStatus.SUCCESS, shards.findShard(shardId).orElseThrow().status(), "次调成功 → SUCCESS");
  }
```

- [ ] **Step 3: 跑 `ExecutorWorkerTest`(需 Docker up)**

Run: `mvn -o test -pl scheduler-worker -am -Dtest=ExecutorWorkerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(全用例绿;需 Docker 起 postgres:16-alpine Testcontainers)。

- [ ] **Step 4: Commit**

```bash
git add scheduler-worker/src/test/java/dev/scheduler/worker/execute/ExecutorWorkerTest.java
git commit -m "test(e3): prove in-flight attempt + stable idempotency key across retry"
```

---

### Task 3: `DemoHandler` 措示范 + `DemoHandlerTest`

**Files:**
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/DemoHandler.java`
- Create: `scheduler-worker/src/test/java/dev/scheduler/worker/handler/DemoHandlerTest.java`

**Interfaces:**
- Consumes: Task 1 产出的 `HandlerContext.attempt()/idempotencyKey()/redelivery()` 与 `of(...)` 默认 `attempt=1`。
- Produces: `DemoHandler.resultPayload(HandlerContext)` 回显投递坐标(纯示范,无状态、无下游写)。

- [ ] **Step 1: `DemoHandler` 加投递坐标示范**

把整个 `DemoHandler.java` 替换为下面内容(保留既有 cancellation 检查与 `touch()`,新增 `resultPayload` 回显投递坐标):

```java
package dev.scheduler.worker.handler;

import java.util.concurrent.CancellationException;

public class DemoHandler implements ExecutionHandler {
  @Override public String ref() { return "demo"; }

  @Override public void handle(HandlerContext ctx) {
    if (ctx.token() != null && ctx.token().isCancelRequested()) {
      // 协作取消:token 置位即抛取消信号,worker 单独捕获映射 CANCELED(不落入重试/死信)。
      throw new CancellationException("demo cancelled");
    }
    // no-op demo handler
    ctx.touch(); // E3 契约示范:推进边界上报(本 handler 即时返回,touch 对短跑无实际效果)。
    // E3#2 契约示范:对下游做幂等时,以 ctx.idempotencyKey() 为锚点、ctx.redelivery() 判重投递;
    // 重投递时只需保证下游按该锚点幂等(如 INSERT ... ON CONFLICT DO NOTHING),平台不替 handler 消重。
  }

  @Override public String resultPayload(HandlerContext ctx) {
    return "{\"idempotencyKey\":\"" + ctx.idempotencyKey() + "\",\"attempt\":" + ctx.attempt()
        + ",\"redelivery\":" + ctx.redelivery() + "}";
  }
}
```

- [ ] **Step 2: 新建 `DemoHandlerTest`**

`DemoHandlerTest.java`:

```java
package dev.scheduler.worker.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class DemoHandlerTest {
  @Test void ref_isDemo() { assertEquals("demo", new DemoHandler().ref()); }

  @Test void resultPayload_reportsDeliveryCoordinate() {
    var first = HandlerContext.of(42, 7, 3, 5, "data", "args"); // attempt=1 首次投递
    String p1 = new DemoHandler().resultPayload(first);
    assertTrue(p1.contains("\"idempotencyKey\":\"42:3\""), "幂等键 = executionId:shardIndex");
    assertTrue(p1.contains("\"attempt\":1"));
    assertTrue(p1.contains("\"redelivery\":false"));

    var retry = new HandlerContext(42, 7, 3, 5, "data", "args", "w", new CancellationToken(false), null, 2);
    String p2 = new DemoHandler().resultPayload(retry);
    assertTrue(p2.contains("\"attempt\":2"));
    assertTrue(p2.contains("\"redelivery\":true"), "重投递 attempt=2 → redelivery=true");
  }
}
```

- [ ] **Step 3: 跑 `DemoHandlerTest`(无需 Docker)**

Run: `mvn -o test -pl scheduler-worker -am -Dtest=DemoHandlerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS(纯 JUnit,2 用例,不依赖 Testcontainers)。

- [ ] **Step 4: Commit**

```bash
git add scheduler-worker/src/main/java/dev/scheduler/worker/handler/DemoHandler.java \
        scheduler-worker/src/test/java/dev/scheduler/worker/handler/DemoHandlerTest.java
git commit -m "feat(e3): DemoHandler demonstrates delivery coordinate self-dedupe contract"
```

---

## Self-Review

**Spec 覆盖:** `HandlerContext` 加 `attempt` 组件 + `idempotencyKey()`/`redelivery()` 派生方法 → Task 1;`ExecutorWorker` 传 `shard.attempt()+1` → Task 1 Step 2;非目标(平台不强制去重/不结果缓存/不改三元组)由 Global Constraints 约束;§6 测试分叉:`HandlerContextTest`(默认值/redelivery 分界/idempotencyKey 格式)→ Task 1 Step 4;`ExecutorWorkerTest`(首次 vs 重投递 attempt 正确,在途=快照+1)→ Task 2;demo handler 示范 → Task 3。spec §3.3「平台不消重投递」是负约束,代码不落任何判定逻辑。

**Spec 边界对照:** 「不细分 RETRY vs RECLAIM」→ 代码不引入 reason 枚举,统一 `attempt` 递增;「无新列/无迁移」→ 三个任务零 SQL 迁移、零新列(`attempt` 读既有列);「幂等键派生」→ `idempotencyKey()` 纯字符串拼接,不材质化。

**占位符扫描:** 无 TBD/TODO;每个 Step 给出完整代码块与确切 `-Dtest` 目标 + 期望结果。

**类型一致性:** `attempt` 为 `int` 组件,`shard.attempt()`(core `Shard` 的 `int attempt`)→ `+1` 后仍 `int`,经 10 参构造传入,无跨模块类型漂移;幂等键为 `long + ":" + int` 的字符串拼接,与 spec §2「= executionId : shardIndex」逐一对应。

**命令一致性:** Task 1/3 为纯 JUnit(无 Testcontainers),命令不要求 Docker;Task 2 用 Testcontainers,命令注明「需 Docker up」。均用模块级离线 gate `mvn -o test -pl scheduler-worker -am -Dtest=... -Dsurefire.failIfNoSpecifiedTests=false`(沿用项目既有做法)。