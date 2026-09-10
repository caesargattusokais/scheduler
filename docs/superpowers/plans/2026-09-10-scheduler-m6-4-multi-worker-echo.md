# M6.4 多 worker 实跑 + 第二个真实 handler —— 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 M6"横向扩展 + 加任务类型零改动"两条承诺坐实到可运行：同一控制面下多个 worker 进程并行分摊同一任务的分片；新增第二个真实 handler `echo`（带执行归属），不改 server/前端即多出一种任务类型。

**Architecture:** 全部主代码落 `scheduler-worker`（workerId 进程唯一 + `echo` handler + `result_payload` 写通路）与 `scheduler-persistence`（一个新增仓储写方法）；`scheduler-server` 与 DB schema 零改动。多 worker 分摊复用既有 DB 原子认领；echo 归属经 `HandlerContext.workerId` + `ExecutionHandler.resultPayload()` 默认方法 + `result_payload` 归属守卫回写。

**Tech Stack:** Java 17、Spring Boot 3.2.5、JUnit 5、Testcontainers(Postgres 16)、React/TS 前端（可选一列展示，后端先行验证）。

**Spec:** `docs/superpowers/specs/2026-09-10-scheduler-m6-4-multi-worker-echo-design.md`

## Global Constraints

- **server 与 DB schema 零改动**（M6.4 spec §0）。不得触碰 `scheduler-server/src`、Flyway `V*.sql`、`execution_shard`/`worker` 表结构。
- **业务 handler 只存在于 `scheduler-worker`**，不 import 控制面任何类；不引 JSON 库（payload 用字符串拼接即可）。
- `echo` 依赖只允许 `core`/`persistence`/JDK（handler 类纪律，沿 M6 spec §0）。
- Java 17、Spring Boot 3.2.5、JUnit 5、Testcontainers(postgres:16-alpine)（沿用 worker 测试基类）。
- 全 reactor 保持绿：core / persistence / server / worker。
- 每个 commit 以 `Co-Authored-By: Claude Code <noreply@anthropic.com>` 结尾。

## 任务依赖与共享接口（实现者按此契约）

| 任务 | 产出接口（后续任务依赖） |
|------|--------------------------|
| T1 | `WorkerConfig.defaultWorkerId()` 改为包可见 `static`，返回含 hostname+pid 的默认 id |
| T2 | 新增 `ShardRepository.recordResultPayload(long, String, String)`；`HandlerContext` 增加 `String workerId` 组件（`of(...)` 默认 null）；`ExecutionHandler` 增加 `default String resultPayload(HandlerContext){return "";}`；`ExecutorWorker` SUCCESS 时回写非空 payload |
| T3 | `EchoHandler`（ref `echo`，ctor `(Clock)`）+ `WorkerConfig.echoHandler(Clock)` bean |
| T4 | 依赖 T1 的显式 workerId 构造（T2 不依赖 T3） |
| T5 | 依赖 T1–T4 全绿 |

---

### Task 1: workerId 进程唯一（`worker@<hostname>:<pid>`）

**Files:**
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java:52-65`（`defaultWorkerId`）
- Test: `scheduler-worker/src/test/java/dev/scheduler/worker/config/WorkerConfigTest.java`（新建）

**Interfaces:**
- Consumes: `ProcessHandle.current().pid()`
- Produces: `WorkerConfig.defaultWorkerId()`（包可见 `static`，返回 `worker@<hostname>:<pid>`；异常回退 `worker@<uuid>`）

- [ ] **Step 1: 写失败测试**

```java
package dev.scheduler.worker.config;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class WorkerConfigTest {
  @Test void defaultWorkerId_isWorkerAtHostnameColonPid() {
    String id = WorkerConfig.defaultWorkerId();
    assertTrue(id.startsWith("worker@"), "默认 id 前缀 vote@ : " + id);
    assertTrue(id.contains(":"), "应含 ':pid' 使同机多 worker 唯一 : " + id);
  }
}
```

- [ ] **Step 2: 跑测试，确认失败**

Run: `mvn -o -pl scheduler-worker -am test -Dtest=WorkerConfigTest`
Expected: FAIL（`defaultWorkerId` 是 `private`，编译 X：包外不可见；且现实现无 `:`）

- [ ] **Step 3: 实现**

`WorkerConfig.java` 中把 `private static String defaultWorkerId()` 改为包可见，并拼 pid：

```java
static String defaultWorkerId() {
  try {
    return "worker@" + java.net.InetAddress.getLocalHost().getHostName()
        + ":" + java.lang.ProcessHandle.current().pid();
  } catch (Throwable t) {
    log.warn("could not resolve hostname for worker-id, falling back to uuid", t);
    return "worker@" + java.util.UUID.randomUUID();
  }
}
```

（仅此一改；`schedulerWorkerId` bean 及其 `@Value("${scheduler.worker-id:}")` 覆盖逻辑不变。）

- [ ] **Step 4: 跑测试，确认通过**

Run: `mvn -o -pl scheduler-worker -am test`
Expected: PASS（新增 1 个，全 worker 绿）

- [ ] **Step 5: Commit**

```bash
git add scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java \
        scheduler-worker/src/test/java/dev/scheduler/worker/config/WorkerConfigTest.java
git commit -m "feat(worker): workerId 默认 worker@<hostname>:<pid>，同机多 worker 唯一 (M6.4 T1)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: result_payload 写通路（HandlerContext.workerId + ExecutionHandler.resultPayload + 归属守卫回写）

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java`（接口加方法）
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java`（实现）
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/HandlerContext.java`（加 `workerId`）
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/ExecutionHandler.java`（加默认方法）
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/execute/ExecutorWorker.java:73-82`（SUCCESS 回写）
- Test: `scheduler-worker/src/test/java/dev/scheduler/worker/execute/ExecutorWorkerTest.java`（加 1 个测试）

**Interfaces:**
- Produces:
  - `ShardRepository.recordResultPayload(long shardId, String ownerWorkerId, String payload)` → `boolean`（SUCCESS 且归 owner 才写，否则 false）
  - `HandlerContext` record 第 8 组件 `String workerId`；`HandlerContext.of(...)` 五参便捷构造将其置 null
  - `ExecutionHandler.resultPayload(HandlerContext)` 默认返回 `""`
  - `ExecutorWorker`：SUCCESS 回写后，若 `payload` 非空则 `recordResultPayload(shard.id(), workerId, payload)`
- Consumes: `Shard.resultPayload()`、`Shard.workerId()`（core record 已有）

- [ ] **Step 1: 写失败测试（ExecutorWorker SUCCESS 回写 result_payload）**

在 `ExecutorWorkerTest` 的 `happyPath_claimsShard_runsWithShardContext_writesSuccess` 之后加：

```java
@Test void handlerResultPayload_writtenToShard_onSuccess() {
  long taskId = createTask("rec", 3, 8);
  long exec = seedParentAndShards(taskId, 3);
  var payloadHandler = new ExecutionHandler() {
    @Override public String ref() { return "rec"; }
    @Override public void handle(HandlerContext ctx) { }
    @Override public String resultPayload(HandlerContext ctx) {
      return "{\"workerId\":\"" + ctx.workerId() + "\",\"shardIndex\":" + ctx.shardIndex() + "}";
    }
  };
  var registry = new MapHandlerRegistry(List.of(() -> payloadHandler));
  assertTrue(worker(registry).workOne());
  var s = shard(exec, 0);
  assertNotNull(s.resultPayload(), "SUCCESS 后 result_payload 应被写回");
  assertTrue(s.resultPayload().contains("\"workerId\":\"worker-a\""),
      "payload 应含调用方 workerId(经 HandlerContext.workerId)");
  assertTrue(s.resultPayload().contains("\"shardIndex\":0"),
      "payload 应含分片下标");
  assertTrue(shards.findShards(exec).stream()
      .noneMatch(x -> x.shardIndex() != 0 && x.resultPayload() != null),
      "仅被认领执行的 shard 写 payload，其余保持空");
}
```

- [ ] **Step 2: 跑测试，确认失败**

Run: `mvn -o -pl scheduler-worker -am test -Dtest=ExecutorWorkerTest#handlerResultPayload_writtenToShard_onSuccess`
Expected: FAIL（`ExecutionHandler` 无 `resultPayload` 方法→编译错；`HandlerContext` 无 `workerId`；`recordResultPayload` 不存在）

- [ ] **Step 3: 实现通路**

`HandlerContext.java`（加组件 + `of` 默认 null）：

```java
public record HandlerContext(long executionId, long shardId, int shardIndex, int shardCount,
                             String shardData, String args, String workerId, CancellationToken token) {

  public static HandlerContext of(long executionId, long shardId, int shardIndex, int shardCount,
                                  String shardData, String args) {
    return new HandlerContext(executionId, shardId, shardIndex, shardCount, shardData, args, null,
        new CancellationToken(false));
  }
}
```

`ExecutionHandler.java`（加默认方法）：

```java
  void handle(HandlerContext ctx);

  /** 运行后续写回 execution_shard.result_payload 的可选结果；默认空（绝大多数 handler 不产出）。 */
  default String resultPayload(HandlerContext ctx) { return ""; }
```

`ShardRepository.java`（接口末尾加）：

```java
  /** SUCCESS 后写回 result_payload，受 worker_id 归属守卫：行已不归该 owner（且非 SUCCESS）则静默返回 false。
   *  payload 不是 outcome，不落 outcome 行。返回是否写入。 */
  boolean recordResultPayload(long shardId, String ownerWorkerId, String payload);
```

`JdbcShardRepository.java`（实现）：

```java
  @Override public boolean recordResultPayload(long shardId, String ownerWorkerId, String payload) {
    int updated = jdbc.update(
        "UPDATE execution_shard SET result_payload=? WHERE id=? AND worker_id=? AND status='SUCCESS'",
        payload, shardId, ownerWorkerId);
    return updated == 1;
  }
```

`ExecutorWorker.java`（SUCCESS 分支，原 77-82 行）：

```java
      try {
        h.handle(ctx);
        if (shards.isCancelRequested(shard.id())) {
          shards.markStatusOwned(shard.id(), ExecutionStatus.CANCELED, workerId, "cancelled after run");
        } else {
          shards.markStatusOwned(shard.id(), ExecutionStatus.SUCCESS, workerId, "ok");
          String payload = h.resultPayload(ctx);
          if (payload != null && !payload.isBlank()) {
            shards.recordResultPayload(shard.id(), workerId, payload);
          }
        }
      } catch (CancellationException cex) {
```

- [ ] **Step 4: 跑测试（新增 + 既有全部）**

Run: `mvn -o -pl scheduler-worker -am test`
Expected: PASS（含上面的新测试；既有 19 个不回归）

- [ ] **Step 5: Commit**

```bash
git add scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java \
        scheduler-worker/src/main/java/dev/scheduler/worker/handler/HandlerContext.java \
        scheduler-worker/src/main/java/dev/scheduler/worker/handler/ExecutionHandler.java \
        scheduler-worker/src/main/java/dev/scheduler/worker/execute/ExecutorWorker.java \
        scheduler-worker/src/test/java/dev/scheduler/worker/execute/ExecutorWorkerTest.java
git commit -m "feat: result_payload 写通路（HandlerContext.workerId + ExecutionHandler.resultPayload + 归属守卫回写）(M6.4 T2)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: 第二个真实 handler `echo` + 注册 bean

**Files:**
- Create: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/EchoHandler.java`
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java`（加 `echoHandler(Clock)` bean，紧跟 `demoHandler()`）
- Test: `scheduler-worker/src/test/java/dev/scheduler/worker/handler/EchoHandlerTest.java`（新建）

**Interfaces:**
- Produces: `EchoHandler(Clock)`，`ref()=="echo"`；`resultPayload` 返回 JSON 字符串 `{"echoed","workerId","shardIndex","at"}`
- Consumes: T2 的 `HandlerContext.workerId()`、`ExecutionHandler.resultPayload`

- [ ] **Step 1: 写失败测试**

```java
package dev.scheduler.worker.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;

class EchoHandlerTest {
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T10:00:00Z"), ZoneOffset.UTC);

  @Test void ref_isEcho() { assertEquals("echo", new EchoHandler(CLOCK).ref()); }

  @Test void resultPayload_recordsWorkerShardAndEcho() {
    var ctx = new HandlerContext(1L, 2L, 3, 5, "data", "hello", "worker-a", new CancellationToken(false));
    String p = new EchoHandler(CLOCK).resultPayload(ctx);
    assertTrue(p.contains("\"workerId\":\"worker-a\""));
    assertTrue(p.contains("\"shardIndex\":3"));
    assertTrue(p.contains("\"echoed\":\"hello\""));
    assertTrue(p.contains("\"at\":\"2026-01-01T10:00:00Z\""));
  }

  @Test void handle_onCancelRequested_throwsCancellation() {
    var ctx = new HandlerContext(1L, 2L, 0, 1, null, null, "worker-a", new CancellationToken(true));
    assertThrows(CancellationException.class, () -> new EchoHandler(CLOCK).handle(ctx));
  }
}
```

- [ ] **Step 2: 跑测试，确认失败**

Run: `mvn -o -pl scheduler-worker -am test -Dtest=EchoHandlerTest`
Expected: FAIL（`EchoHandler` 不存在→编译错）

- [ ] **Step 3: 实现**

`EchoHandler.java`：

```java
package dev.scheduler.worker.handler;

import java.time.Clock;
import java.util.concurrent.CancellationException;

/** M6.4:第二个真实 handler。协作取消感知(demo 同款);resultPayload 记录「哪个 worker 执行了哪个分片」+
 *  回显 args,作为多 worker 分摊的观测仪表,经 T2 通路落 execution_shard.result_payload。 */
public class EchoHandler implements ExecutionHandler {
  private final Clock clock;

  public EchoHandler(Clock clock) { this.clock = clock; }

  @Override public String ref() { return "echo"; }

  @Override public void handle(HandlerContext ctx) {
    if (ctx.token() != null && ctx.token().isCancelRequested()) {
      throw new CancellationException("echo cancelled");
    }
  }

  @Override public String resultPayload(HandlerContext ctx) {
    return "{\"echoed\":\"" + safe(ctx.args()) + "\",\"workerId\":\"" + ctx.workerId()
        + "\",\"shardIndex\":" + ctx.shardIndex() + ",\"at\":\"" + clock.instant().toString() + "\"}";
  }

  private static String safe(String s) { return s == null ? "" : s; }
}
```

`WorkerConfig.java`（`demoHandler()` 之后加）：

```java
  @Bean ExecutionHandler demoHandler() { return new DemoHandler(); }
  @Bean ExecutionHandler echoHandler(Clock clock) { return new EchoHandler(clock); }
```

- [ ] **Step 4: 跑测试 + 全 worker 复绿**

Run: `mvn -o -pl scheduler-worker -am test`
Expected: PASS（EchoHandlerTest 3 个；`handlerRegistry` bean 自动收录 echo，MapHandlerRegistry 重复 ref fail-fast 不触发：demo/echo 不同）

- [ ] **Step 5: Commit**

```bash
git add scheduler-worker/src/main/java/dev/scheduler/worker/handler/EchoHandler.java \
        scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java \
        scheduler-worker/src/test/java/dev/scheduler/worker/handler/EchoHandlerTest.java
git commit -m "feat(worker): 第二个真实 handler echo（带执行归属 resultPayload）(M6.4 T3)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: N-worker 分摊承重集成测试

**Files:**
- Modify: `scheduler-worker/src/test/java/dev/scheduler/worker/execute/ExecutorWorkerTest.java`（worker 重载 + 新测试 + import）

**Interfaces:**
- Consumes: T2 `HandlerContext.workerId`；`Shard.workerId()`/`Shard.shardIndex()`/`Shard.status()`；既有 helper `createTask`/`seedParentAndShards`/`shards`(字段)
- Produces: 包内 `worker(HandlerRegistry, String workerId)` 重载（既有 `worker(registry)` 委托之）

- [ ] **Step 1: 加 worker 重载 + 写失败测试**

替换既有私有 `worker(HandlerRegistry registry)`（现 92-95 行）为重载：

```java
  private ExecutorWorker worker(HandlerRegistry registry) { return worker(registry, "worker-a"); }

  private ExecutorWorker worker(HandlerRegistry registry, String workerId) {
    return new ExecutorWorker(tasks, shards, registry, workerId,
        new FailureResolver(shards, new RetryPolicy(), CLOCK), CLOCK);
  }
```

并在文件内加（子 M3 测试之后任一位置）：

```java
  @Test void twoWorkers_distributeShards_eachClaimsDisjointSubset() {
    long taskId = createTask("rec", 4, 8);
    long exec = seedParentAndShards(taskId, 4);
    var recA = new RecordingHandler(false);
    var recB = new RecordingHandler(false);
    var registryA = new MapHandlerRegistry(List.of(() -> recA));
    var registryB = new MapHandlerRegistry(List.of(() -> recB));
    // 交替驱动,直到无可认领(DUE 清空);DB 原子认领保证每片仅一个 worker 真正执行。
    for (int i = 0; i < 4; i++) { worker(registryA, "worker-a").workOne(); worker(registryB, "worker-b").workOne(); }
    List<Shard> all = shards.findShards(exec);
    assertEquals(Set.of(0, 1, 2, 3),
        all.stream().map(Shard::shardIndex).collect(Collectors.toSet()), "全部分片都被执行");
    assertTrue(all.stream().allMatch(s -> s.status() == ExecutionStatus.SUCCESS));
    var byWorker = all.stream().collect(Collectors.groupingBy(Shard::workerId,
        Collectors.mapping(Shard::shardIndex, Collectors.toSet())));
    assertEquals(2, byWorker.size(), "两个 worker 都分摊到分片");
    assertTrue(byWorker.containsKey("worker-a"));
    assertTrue(byWorker.containsKey("worker-b"));
    long claimed = byWorker.values().stream().mapToLong(Set::size).sum();
    assertEquals(4, claimed, "每个 shard 归属恰一个 worker,无重复认领 (disjoint + covering)");
    assertEquals(2, recA.calls.size(), "worker-a 跑了 2 片");
    assertEquals(2, recB.calls.size(), "worker-b 跑了 2 片");
  }
```

新增 import（文件头随现有 import 区）：

```java
import java.util.Set;
import java.util.stream.Collectors;
```

- [ ] **Step 2: 跑测试，确认失败/先红后绿**

Run: `mvn -o -pl scheduler-worker -am test -Dtest=ExecutorWorkerTest#twoWorkers_distributeShards_eachClaimsDisjointSubset`
Expected: 实现已在 T2 就位 → 此测试应直接 PASS（本任务主要指测验证；若 `worker(registry,id)` 重载缺失则编译错，先补 Step 1 的替换再跑）

- [ ] **Step 3: 全 worker 复绿（含既有）**

Run: `mvn -o -pl scheduler-worker -am test`
Expected: PASS

- [ ] **Step 4: 全 reactor 复绿**

Run: `mvn -o test`
Expected: BUILD SUCCESS（core / persistence / server / worker 全绿）

- [ ] **Step 5: Commit**

```bash
git add scheduler-worker/src/test/java/dev/scheduler/worker/execute/ExecutorWorkerTest.java
git commit -m "test(worker): N-worker 分期分摊承重测试（disjoint + covering + 状态全终态）(M6.4 T4)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5: 全 reactor 验证 + README 增补

**Files:**
- Modify: `README.md`（worker 节增补多 worker 运行 + echo handler 说明、REST 不变）
- 验证：全 reactor 绿已在 T4 Step 4；本任务补 README + live smoke 备注（live smoke 可选，DB 凭证环境对齐）

**Interfaces:**
- Consumes: T1–T4 全绿

- [ ] **Step 1: README 增补**

在 `README.md` 的「独立执行 worker」一节（现有 worker 段落后）追加：

```markdown
- 多 worker 分摊：想增并行度，再多起 worker 进程指向同一 `scheduler` 库即可；散片经 `execution_shard` 原子认领分摊到各 worker，互不重复（执行详情每个 shard 的 `workerId` 可见归属）。
- workerId 默认 `worker@<hostname>:<pid>`（同机多 worker 自动唯一）；固定身份用 `SCHEDULER_WORKER_ID` 覆盖（如 `worker-b`）。
- 第二个真实 handler：`echo`（ref `echo`）。验证「不改控制面/前端加任务类型」——只需 worker 注册即在前端建任务下拉框出现。`echo` 的执行归属写在 `execution_shard.result_payload`：`{"echoed","workerId","shardIndex","at"}`。

示例：起 server(:8080) + 两个 worker(:8081、:8082) 后，建 `echo` 任务 `shardCount=3` 并触发 → 3 个 shard 由两个 worker 分别认领执行、全 SUCCESS。
```

- [ ] **Step 2: 全 reactor 复绿确认**

Run: `mvn -o test`
Expected: BUILD SUCCESS。随后 `git status` 仅 T5 的 README 改动。

- [ ] **Step 3: live smoke（推荐，含 DB 凭证即执行）**

用 `mysql`/原生客户端核对（或沿用 `start-dev.sh` 起 server + 2 个 `SCHEDULER_WORKER_ID` 不同的 worker），验证：
1. `curl :8080/api/v1/handlers` → 含 `echo` 与 `demo`；
2. 建 `echo` 任务 `shardCount=3` 触发 → 详情各 shard SUCCESS、`worker_id` 各异、`result_payload` 有值。

> 若本环境无 DB 凭证/不能起链，则把 live smoke 记为【best-effort、未跑】，由绿色集成测试背书；不可伪造已跑结果。

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs(m6.4): 多 worker 运行 + echo handler 说明 (M6.4 T5)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Self-Review（作者自查）

**1. Spec 覆盖：**
- G1（多 worker 分摊）→ T1（id 唯一，分摊的前提）+ T4（N-worker 承重测试）+ T5 README。
- G2（不改控制面/前端加任务类型）→ T3（echo bean 自动入 `/handlers`）+ T5 验证 `/handlers` 含 echo。
- G3（新 handler 全链走通）→ T2（写通路）+ T3（echo）+ T5 live smoke。
- spec §2（workerId）→ T1；§3（echo + result_payload 通路）→ T2/T3；§5 T2/T3/T4。

**2. Placeholder 扫描：** 无 TBD/TODO；所有代码步骤含逐词内容。

**3. Type/接口一致性：** `recordResultPayload(long,String,String)` 接口与实现签名一致；`resultPayload(HandlerContext)` 默认方法签名在 ExecutionHandler 定义、ExecutorWorker/EchoHandler 调用相同；`HandlerContext` 8 组件构造与 `of(...)` 五参默认一致，ExecutorWorker 构造调用（Step 3）补了 `workerId` 实参。