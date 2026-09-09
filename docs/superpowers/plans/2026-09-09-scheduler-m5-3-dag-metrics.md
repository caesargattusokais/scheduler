# M5.3 DAG 单节点重跑 + 指标面板 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** M5 里程碑最后一块——DAG 单节点重跑（经 DagEngine）+ 指标面板（DLQ 深度 + worker 存活两个新 gauge，面板只读渲染 5 个核心指标）。

**Architecture:** 后端补两处缺口：`POST /api/v1/dags/runs/{runId}/nodes/{nodeId}/rerun`（§1.4，节点重跑经 DagEngine——新建 execution + 重挂新 execution_id + 节点回 RUNNING + 重开 dag_run 至 PENDING，由引擎随扫描重派生终态，不自动复位下游）；指标两新 gauge（§1.5）`scheduler_dlq_depth`（全局死信数）+ `scheduler_worker_active`（0/1，本进程是否持选主锁）。前端在新 3 路由之上加 工作流(/dags) + 指标(/metrics) 两页：DAG 页消费 runs/detail/cancel/节点 rerun，指标面板只读解析 `/actuator/prometheus`（新增 vite proxy `/actuator`），手绘内联 SVG 渲染 5 个核心指标卡，5s 轮询。

**Tech Stack:** Java 21 / Spring Boot web-jdbc / Maven reactor（scheduler-server）· React 19 + Vite + TS（scheduler/web），新增依赖仍仅 `react-router-dom`（已装）。

**Spec:** `docs/superpowers/specs/2026-09-09-scheduler-m5-console-design.md`（本 plan 实现 §1.4 + §1.5 后端 + §2 DAG 页 / 指标面板 / 路由）。DAG 数据模型见 M4 spec `docs/superpowers/specs/2026-09-08-scheduler-m4-workflow-dag-design.md`。

## Global Constraints

（自 spec §0/§1.4/§1.5/§2/§4 逐字引用 + 现状核实，约束本 plan 全部 Task。）

- **控制台只走 `/api/v1` REST，绝不直连执行库。** 例外：指标面板按 §1.5 只读消费 `GET /actuator/prometheus`（或 actuator JSON meter 端点）。
- **DagEngine 仍是 `dag_run`/`dag_run_node` 的唯一写者**（M4 纪律）：单节点重跑的节点态变更必须经引擎，不得在 controller/repository 里直接改写节点状态。worker/Reconciler 对 DAG 无感知，不改执行运行时。
- 单节点重跑：前置条件——节点处**终态**（SUCCESS / FAILED / SKIPPED / CANCELED 均可）；非终态（PENDING / RUNNING）→ **409**。**每次重跑新建一轮 execution**（允许重跑已失败节点、允许成功后再重跑）。
- **节点态写死走 DagEngine。** 重跑机制：建新 execution（`createParentWithShards`）+ 把节点重挂到新 `execution_id` + 节点回 RUNNING（复用 PENDING→RUNNING→终态状态机，**不引入 rerun_token**）+ **重开 `dag_run.status='PENDING'`**（否则已 finalized 的 run 不在 `findActiveRuns()` 内，引擎不会再扫、节点终态无法重派生）；下游不自动复位。
- **不自动复位下游**（M4 §7 不做自动回滚）：重跑节点的下游保持当前态；下游已终态保持不变，未终态 PENDING 待该节点新运行头成功后按既有传播语义推进。
- 端点：`POST /dags/runs/{runId}/nodes/{nodeId}/rerun` → 成功 `200 + 该节点现态（DagRunNode，status RUNNING、新 executionId）`；run/节点不存在 → 404；非终态 → 409（`IllegalStateException` → `ApiExceptionHandler` 已映射 409）。
- 指标：新增 **`scheduler_dlq_depth`**（全局 gauge，非 per-task，v1 明确）+ **`scheduler_worker_active`**（0/1，本进程选主锁持有判定——**v1 不做 per-runner 心跳，这是明确边界**）。面板共 **5 个 gauge family**（既有 `scheduler_active_runs`、`scheduler_due_queue_max_age_seconds`、`scheduler_dag_runs_active` + 新 2）。
- 前端依赖克制：仍仅 react-router；指标面板**手绘内联 SVG/sparkline**，不引图表/UI/状态库；页面本地 state + 5s 轮询。
- **不做**（YAGNI / spec §4）：自动回滚、整轮重试、DAG 超时、自动复位下游、per-runner worker 心跳、count(*) 执行列表总量。

### Ruling 1 —— 指标面板收敛为「5 个 gauge」（spec §1.5 为权威）
§3 验收锚点写「面板渲染 6 指标」，但 §1.5（操作性契约）明确新增的恰是 2 个 gauge（dlq_depth + worker_active），叠加既有 3 gauge（active_runs、due_queue_max_age、dag_runs_active）共 **5 个**；第 6 个指标源在代码与 spec §1.5 中都不存在。**裁决：按 §1.5 契约渲染 5 个 gauge family；§3 的「6」是宽松表述，不新增虚构指标。** 成本若错：面板少一卡，未来有第 6 个真指标源再加。

### Ruling 2 —— 重跑态表示复用 RUNNING 状态机，不引 rerun_token（开放项 §5.1）
节点从终态直接回 RUNNING + 重挂新 execution，靠 `execution_id` 区分"哪轮"；`stepRunningNode` 读新 execution 的 shards 派生终态。无需显式 token。**成本若错**：无法区分"同节点跑了多轮的历史 execution"，但 run 详情/节点 current execution 已满足运维需要。

### Ruling 3 —— 重跑须重开 run 才能重派生（隐含于 §1.4「终态被重新派生」）
`findActiveRuns()` 只返回 stored `status='PENDING'` 的 run；已 finalized（SUCCESS/FAILED/CANCELED）的 run 重跑节点后若不重开，引擎不会再扫、节点新 execution 完成后终态无法回写、节点永挂 RUNNING。**裁决：重跑同事务把 `dag_run.status` 重开为 PENDING（finished_at=NULL），run 终态随引擎扫描重派生**（`finalizeRun` CAS 幂等，追加 outcome 无害）。**成本若错**：一次重跑即固化 run 终态，偏离验收。

---

### 现状（主 `afa8fa3`，已核实）

DAG 运行表写者：`DagEngine`（scheduler-server/.../dag/DagEngine.java）——`scanOnce()` 两阶段（触发 + 传播 `propagateRun`），`spawnNode` 用 `createParentWithShards`，`stepRunningNode` 读 execution 全 shard 全终态后派生节点终态。`findActiveRuns()` = stored `status='PENDING'`。`finalizeRun` CAS on PENDING。

端点（DagController，`/api/v1/dags`）：create/list/get/pause/resume/trigger/runs/`runs/{runId}`(RunDetail)/`runs/{runId}/cancel`。节点 rerun **不存在**。

`DagRepository`（DagRepository.java）已有：`findNode`、`findRun`、`findNodesOfRun`、`markNodeSpawned`(CAS on PENDING)、`markNodeStatus`(FSM CAS)、`finalizeRun`、`findNonTerminalNodes`。**无**「终态节点重置 + 重开 run」方法——需新增。

`IdempotencyKeys`（scheduler-core）：`forTrigger`/`forDagTrigger`/`forManualDagRun`。需新增节点重跑键（新 UUID，每次新建）。

`ShardRepository`（ShardRepository.java）：`createParentWithShards`、`findShards`、`findDeathLetterShards`(FAILED AND dead_letter)。**无**全局死信计数——需新增。

指标（Beans.java）：`schedulerMetrics`(per-task active_runs + due_queue_max_age)、`dagMetrics`(per-dag dag_runs_active)。需新增全局 gauge binder。

`ApiExceptionHandler`：`IllegalArgumentException → 400`，`IllegalStateException → 409`。

前端 `scheduler/web`：`App.tsx` 三路由（/tasks /executions /dlq）；`client.ts` `req<T>` + tasks/executions/DLQ 方法；`types.ts` 有 `Task`/`Execution`/`Shard`/`ExecutionDetail` 等；**无** Dag/DagRun/DagRunNode/RunDetail 类型、无 DAG client 方法、无 metrics 获取、无 `/actuator` 代理。`useInterval(fn, ms)` 已存在。`vite.config.ts` 仅代理 `/api`。

后端 record（scheduler-core）：
- `DagRun(Long id, Long dagId, String idempotencyKey, DagRunStatus status, String triggerReason, boolean cancelRequested, Instant finishedAt, Instant createdAt)`
- `DagRunNode(Long id, Long dagRunId, String nodeKey, Long taskId, Long executionId, DagRunNodeStatus status, int sortOrder, String detail, Instant createdAt, Instant finishedAt)`；`DagRunNodeStatus{ PENDING,RUNNING,SUCCESS,FAILED,CANCELED,SKIPPED }`，`isTerminal()` 见枚举。
- `RunDetail(DagRun run, String status, List<NodeDetail> nodes)`、`NodeDetail(DagRunNode node, List<Shard> shards)`（DagQueryService 内部 record，JSON 形如 `$.nodes[N].node.status`）
- `Shard(Long id, Long executionId, int shardIndex, String shardData, ExecutionStatus status, int attempt, String workerId, Instant leaseUntil, Instant nextRetryAt, boolean cancelRequested, boolean deadLetter, Instant startedAt, Instant finishedAt, String resultPayload)`

E2E 支撑（ApiIntegrationTest.java，M5.2 已扩展至 71+2 用例）：`@Autowired dagEngine`（同步驱动）、`executorWorker`、`reconciler`、`triggerEngine`、`postTask`、`postDag`、`pauseDag`、`triggerDag`、`METRICS_DAG_ID`/`METRICS_TASK_ID`、`promDagGauge` 等 helper。

---

## Task 1: 后端 DAG 单节点重跑（DagEngine + controller + repository + IdempotencyKeys + E2E）

**Interfaces 生产：** `DagEngine.rerunNode(long runId, long nodeId) → DagRunNode`（controller 调）；`DagRepository.rerunNodeToExecution(long runId, long nodeId, long newExecutionId) → boolean`；`IdempotencyKeys.forNodeRerun(long runId, String nodeKey) → String`。Task 3 前端消费新端点。

**Files:**
- Modify: `scheduler-core/.../core/IdempotencyKeys.java`（新增 `forNodeRerun`）
- Modify: `scheduler-persistence/.../persistence/DagRepository.java`（接口新增 `rerunNodeToExecution`）
- Modify: `scheduler-persistence/.../persistence/JdbcDagRepository.java`（实现 `rerunNodeToExecution`）
- Modify: `scheduler-server/.../dag/DagEngine.java`（新增公开 `rerunNode`）
- Modify: `scheduler-server/.../web/DagController.java`（新增端点 + 注入 DagEngine）
- Test: `scheduler-server/.../web/ApiIntegrationTest.java`（新增 2-3 E2E）

### 引用的既有部件（供照抄签名）
- `ShardRepository.createParentWithShards(long taskId, String parentKey, int shardCount) → Execution`
- `TaskRepository.findById(long) → Optional<Task>`；`Task.shardCount()`
- `DagRepository.findNode/findRun/findNodesOfRun`；`markNodeSpawned` 模式（同事务 outcome）
- `ApiExceptionHandler`：`IllegalStateException → 409`

- [ ] **Step 1: 写失败测试（E2E，先驱动接口）**

在 `ApiIntegrationTest.java` 加 3 个测试（追加到 DAG 段）：

```java
/** M5.3:终态节点单节点重跑——节点回 RUNNING(新 execution_id) → worker 跑新 execution → 终态重派生;下游不自动复位。 */
@Test
void dagNodeRerun_terminalRestartsAndDerivesAgain() throws Exception {
  long t = postTask("rerun-success-task"); // demo handler, shardCount 1
  long dagId = postDag("rerun-success-dag", new long[]{t, t}, new String[]{"A", "B"},
      new String[][]{{"A", "B"}});
  pauseDag(dagId);
  long runId = triggerDag(dagId);
  dagEngine.scanOnce();              // spawn A → RUNNING
  assertTrue(executorWorker.workOne());      // A shard 成功
  reconciler.scanOnce();             // A 父 SUCCESS
  dagEngine.scanOnce();              // A 派生 SUCCESS
  dagEngine.scanOnce();              // B 上游 SUCCESS → spawn B
  assertTrue(executorWorker.workOne());      // B shard 成功
  reconciler.scanOnce();             // B 父 SUCCESS
  dagEngine.scanOnce();              // B 派生 SUCCESS → run 终态 SUCCESS

  long aNodeId = dagNodeId(runId, "A");
  long aExecBefore = dagNodeExecutionId(runId, "A");

  mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + aNodeId + "/rerun"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.status").value("RUNNING"))
      .andExpect(jsonPath("$.executionId").value(is(not(aExecBefore))));

  // 下游 B 不自动复位,仍 SUCCESS
  mvc.perform(get("/api/v1/dags/runs/" + runId))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.nodes[1].node.status").value("SUCCESS"))
      .andExpect(jsonPath("$.nodes[0].node.executionId").value(is(not(aExecBefore))));

  // 新 execution 经引擎跑完 → 节点终态重派生(新 execution_id 不变)
  assertTrue(executorWorker.workOne(), "A 的新 execution shard 应被认领并成功");
  reconciler.scanOnce();
  dagEngine.scanOnce();
  mvc.perform(get("/api/v1/dags/runs/" + runId))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.status").value("SUCCESS"))
      .andExpect(jsonPath("$.nodes[0].node.status").value("SUCCESS"))
      .andExpect(jsonPath("$.nodes[0].node.executionId").value(is(not(aExecBefore))));
}

/** M5.3:非终态(PENDING/RUNNING)节点重跑 → 409。 */
@Test
void dagNodeRerun_nonTerminal_rejected409() throws Exception {
  long t = postTask("rerun-terminal-task");
  long dagId = postDag("rerun-terminal-dag", new long[]{t, t}, new String[]{"A", "B"},
      new String[][]{{"A", "B"}});
  pauseDag(dagId);
  long runId = triggerDag(dagId);
  dagEngine.scanOnce(); // A RUNNING;B PENDING

  long aId = dagNodeId(runId, "A");   // RUNNING → 非终态
  long bId = dagNodeId(runId, "B");   // PENDING  → 非终态
  mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + aId + "/rerun"))
      .andExpect(status().isConflict());
  mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + bId + "/rerun"))
      .andExpect(status().isConflict());
}

/** M5.3:已失败(FAILED)终态节点允许重跑 → 200 RUNNING(新建 execution)。 */
@Test
void dagNodeRerun_failedNodeAllowed() throws Exception {
  long t = postTaskWithRetry("rerun-fail-task", "flaky", 0, 1000, ""); // 首调耗尽失败
  long dagId = postDag("rerun-fail-dag", new long[]{t}, new String[]{"A"}, new String[][]{});
  pauseDag(dagId);
  long runId = triggerDag(dagId);
  dagEngine.scanOnce();                 // spawn A
  assertTrue(executorWorker.workOne()); // A shard 耗尽失败
  reconciler.scanOnce();                // A 父 FAILED
  dagEngine.scanOnce();                 // A 派生 FAILED → run FAILED

  long aId = dagNodeId(runId, "A");
  mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + aId + "/rerun"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.status").value("RUNNING"));
  mvc.perform(get("/api/v1/dags/runs/" + runId)).andExpect(status().isOk())
      .andExpect(jsonPath("$.status").value("RUNNING")); // run 已重开
}

private long dagNodeId(long runId, String key) throws Exception {
  String body = mvc.perform(get("/api/v1/dags/runs/" + runId))
      .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
  for (JsonNode n : objectMapper.readTree(body).get("nodes"))
    if (n.get("node").get("nodeKey").asText().equals(key)) return n.get("node").get("id").asLong();
  throw new AssertionError("no node " + key);
}
private long dagNodeExecutionId(long runId, String key) throws Exception {
  String body = mvc.perform(get("/api/v1/dags/runs/" + runId))
      .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
  for (JsonNode n : objectMapper.readTree(body).get("nodes"))
    if (n.get("node").get("nodeKey").asText().equals(key)) return n.get("node").get("executionId").asLong();
  throw new AssertionError("no node " + key);
}
```

（`postTaskWithRetry` 已在 M4 测试存在；如缺补昆。`JsonNode` 测试已用（`objectMapper.readTree`），已 import。断言 `is(not(...))` 需在测试文件顶部追加 `import static org.hamcrest.Matchers.not;`，M5.2 已加 `is`。）

- [ ] **Step 2: 运行验证失败**

Run: `mvn -pl scheduler-server -am test -Dtest='ApiIntegrationTest#dagNodeRerun_terminalRestartsAndDerivesAgain+dagNodeRerun_nonTerminal_rejected409+dagNodeRerun_failedNodeAllowed'`
Expected: FAIL——`DagEngine.rerunNode` 不存在（编译失败，约定可接受）或校验 404/409 断言失败。

- [ ] **Step 3: 新增键**

`scheduler-core/.../core/IdempotencyKeys.java`：
```java
/** dag 节点重跑 → 新 execution 全局幂等键(每次新建 UUID,节点重跑每次独立一轮)。 */
public static String forNodeRerun(long runId, String nodeKey) {
  return "dag:" + runId + ":node:" + nodeKey + ":rerun:" + UUID.randomUUID();
}
```

- [ ] **Step 4: 仓库接口 + 实现（终态重置 + 重开 run，同事务）**

`DagRepository.java`：
```java
/**
 * 单节点重跑(仅 operator):把终态节点回绕到新一轮运行——CAS on status IN (SUCCESS,FAILED,SKIPPED,CANCELED)
 * 置 status='RUNNING'、execution_id 重挂新 execution、finished_at 清 NULL;并把 dag_run 重开为 'PENDING'
 * (finished_at=NULL)使 findActiveRuns 重新纳入、引擎随后重派生。同事务落 dag_run_node_outcome(RUNNING,'node rerun')。
 * CAS 0 行=节点已非终态/竞态 → false,不落 outcome。调用方必须是 DagEngine(引擎是运行表唯一写者)。
 */
boolean rerunNodeToExecution(long runId, long nodeId, long newExecutionId);
```

`JdbcDagRepository.java`：
```java
@Override public boolean rerunNodeToExecution(long runId, long nodeId, long newExecutionId) {
  final boolean[] ok = {false};
  tx.executeWithoutResult(s -> {
    int upd = jdbc.update("""
      UPDATE dag_run_node SET status='RUNNING', execution_id=?, finished_at=NULL
      WHERE id=? AND status IN ('SUCCESS','FAILED','SKIPPED','CANCELED')""",
      newExecutionId, nodeId);
    if (upd == 0) return; // 节点非终态/竞态 → 静默
    jdbc.update("UPDATE dag_run SET status='PENDING', finished_at=NULL WHERE id=? AND status<>'PENDING'",
        runId); // 重开 run,引擎随后重扫重派生(幂等)
    jdbc.update("INSERT INTO dag_run_node_outcome (node_id, status, detail) VALUES (?,?,?)",
        nodeId, "RUNNING", "node rerun");
    ok[0] = true;
  });
  return ok[0];
}
```
注：`dag_run_node_outcome.RUNNING` 与 `markNodeSpawned` 格式一致；`status IN (终态)` 显式限定不误伤非终态节点。

- [ ] **Step 5: DagEngine.rerunNode（节点态变更唯一入口）**

`DagEngine.java` 新增公开方法（顶部 import `java.util.UUID` 已随 `IdempotencyKeys`）：
```java
/**
 * M5.3 §1.4 节点单节点重跑(仅 operator):precondition 节点处终态(else IllegalStateException→409)。
 * 建新 execution(createParentWithShards,键 forNodeRerun)→ 经 dagRepository.rerunNodeToExecution 把节点
 * 回绕到新 execution + 重开 run → 返回回绕后节点现态。下游不自动复位(propagateRun 跳过已终态)。
 * 节点态写仅经本方法(引擎是 dag 运行表唯一写者)。
 */
public DagRunNode rerunNode(long runId, long nodeId) {
  DagRunNode n = dags.findNode(nodeId).orElseThrow(
      () -> new IllegalStateException("no dag_run_node " + nodeId));
  if (!n.dagRunId().equals(runId)) throw new IllegalStateException("node " + nodeId + " not in run " + runId);
  if (!n.status().isTerminal()) throw new IllegalStateException(
      "node " + nodeId + " is " + n.status() + " and cannot be rerun");
  Task task = tasks.findById(n.taskId()).orElseThrow();
  Execution parent = shards.createParentWithShards(
      n.taskId(), IdempotencyKeys.forNodeRerun(runId, n.nodeKey()), task.shardCount());
  if (!dags.rerunNodeToExecution(runId, nodeId, parent.id())) {
    throw new IllegalStateException("node " + nodeId + " was not terminal; rerun cancelled");
  }
  return dags.findNode(nodeId).orElseThrow();
}
```

- [ ] **Step 6: Controller 端点（404 存在性 + 交给引擎）**

`DagController.java`：注入 DagEngine（构造器加参），新增端点（import `DagRunNode` 已 in file）：
```java
/** M5.3 §1.4 单节点重跑:仅终态节点可用;404 缺失,非终态 → 409;成功 200 + 节点现态。节点态变更经 DagEngine。 */
@PostMapping("/runs/{runId}/nodes/{nodeId}/rerun")
public ResponseEntity<DagRunNode> rerunNode(@PathVariable long runId, @PathVariable long nodeId) {
  dags.findRun(runId).orElseThrow(() -> notFound("dag run " + runId));
  DagRunNode node = dags.findNode(nodeId).orElseThrow(() -> notFound("dag run node " + nodeId));
  if (!node.dagRunId().equals(runId)) throw notFound("dag run node " + nodeId);
  return ResponseEntity.ok(dagEngine.rerunNode(runId, nodeId));
}
```
（`dagEngine` 为新增字段；构造器 `public DagController(DagRepository dags, ShardRepository shards, DagEngine dagEngine)`。）

- [ ] **Step 7: 运行验证通过（目标）**

Run: 同 Step 2 命令。Expected: PASS（3 个测试绿）。
E2E 的「经 DagEngine 非直写」由结构保证（controller 只调 `dagEngine.rerunNode`，节点态经 `rerunNodeToExecution` 由引擎调用落 outcome）；测试断言 mutation 行为（换 execution_id、run 重开、终态重派生）即验收锚点。

- [ ] **Step 8: 全 reactor 回归**

Run: `mvn test`。Expected: **BUILD SUCCESS**，core/persistence/server 全绿（server ≥74 用例），Docker/Testcontainers 已启动。

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat(server): DAG 单节点重跑 POST /dags/runs/{runId}/nodes/{nodeId}/rerun（经 DagEngine，终态回 RUNNING + 重开 run）"
```

---

## Task 2: 后端指标 gauges —— `scheduler_dlq_depth` + `scheduler_worker_active`（+ E2E）

**Files:**
- Modify: `scheduler-persistence/.../persistence/ShardRepository.java`（接口 + 全局死信计数）
- Modify: `scheduler-persistence/.../persistence/JdbcShardRepository.java`（实现）
- Modify: `scheduler-server/.../config/Beans.java`（新增全局 gauge binder）
- Test: `scheduler-server/.../web/ApiIntegrationTest.java`（prometheus 断言）

- [ ] **Step 1: 写失败测试（E2E）**

追加到 `ApiIntegrationTest.java` metrics 段（镜像 `prometheus_dagRunActiveMetric` 的 gauge 断言 + `promGauge` helper）：
```java
/** M5.3 §1.5:全局指标 gauge scheduler_dlq_depth 与 scheduler_worker_active。种子一条 FAILED+dead_letter shard。 */
@Test
void prometheus_globalMetrics() throws Exception {
  long parentId = triggerParent(postTask("metrics-dlq-task"));
  long shardId = shards.findShards(parentId).get(0).id();
  jdbc.update("UPDATE execution_shard SET status='FAILED', dead_letter=true WHERE id=? AND status='DUE'", shardId);

  String prom = mvc.perform(get("/actuator/prometheus"))
      .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
  assertTrue(prom.contains("scheduler_dlq_depth"), "missing scheduler_dlq_depth in prometheus:\n" + prom);
  assertEquals(1.0, promGauge(prom, "scheduler_dlq_depth"), 0.0,
      "scheduler_dlq_depth 应计 FAILED+dead_letter shard 数(1)");
  assertTrue(prom.contains("scheduler_worker_active"), "missing scheduler_worker_active in prometheus");
  assertEquals(leader.isLeader() ? 1.0 : 0.0, promGauge(prom, "scheduler_worker_active"), 0.0,
      "scheduler_worker_active 应等于本进程选主锁持有判定");
}

/** 取 prometheus 文本中无标签 gauge 的数值(单 series)。 */
private double promGauge(String prom, String name) {
  for (String line : prom.split("\n")) {
    if (line.startsWith(name + " ")) return Double.parseDouble(line.substring(line.indexOf(' ') + 1));
  }
  throw new AssertionError("no plain-valued gauge " + name + " in:\n" + prom);
}
```
（需 `@Autowired dev.scheduler.server.leader.LeaderElection leader;`。）

- [ ] **Step 2: 运行验证失败**

Run: `mvn -pl scheduler-server -am test -Dtest='ApiIntegrationTest#prometheus_globalMetrics'`
Expected: FAIL——`countDeadLetter` 不存在 / gauge 缺失。

- [ ] **Step 3: 全局死信计数**

`ShardRepository.java`：
```java
/** 全局死信 shard 计数(FAILED 且 dead_letter),指标 scheduler_dlq_depth 源。 */
long countDeadLetter();
```
`JdbcShardRepository.java`：
```java
@Override public long countDeadLetter() {
  Long c = jdbc.queryForObject("SELECT count(*) FROM execution_shard WHERE dead_letter", Long.class);
  return c == null ? 0 : c;
}
```

- [ ] **Step 4: Beans 新增全局 gauge binder（lazy 值，scrape 时读 DB）**

`Beans.java` import `dev.scheduler.server.leader.LeaderElection`(已 in file)。新增（放 `dagMetrics` 之后）：
```java
/** M5.3 §1.5 全局指标:DLQ 深度(全局 gauge) + worker 存活(0/1 = 本进程是否持选主锁)。
 *  死信计数 lazy 读 DB(scrape 时求值);worker 存活判定 isLeader()。v1 无 per-runner 心跳,选主锁持有即活性边界。 */
@Bean
MeterBinder schedulerGlobalMetrics(ShardRepository shards, LeaderElection leader) {
  return registry -> {
    Gauge.builder("scheduler_dlq_depth", () -> (double) shards.countDeadLetter()).register(registry);
    Gauge.builder("scheduler_worker_active", () -> leader.isLeader() ? 1.0 : 0.0).register(registry);
  };
}
```

- [ ] **Step 5: 运行验证通过（目标）**

Run: `mvn -pl scheduler-server -am test -Dtest='ApiIntegrationTest#prometheus_globalMetrics'`
Expected: PASS。全 reactor `mvn test` → **BUILD SUCCESS**（core/persistence/server 全绿；JdbcShardRepositoryTest 计数插入不影响既有用例）。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(server): 指标新增 scheduler_dlq_depth + scheduler_worker_active（全局 gauge，选主锁即活性）"
```

---

## Task 3: 前端 client/types + vite proxy + 路由（DAG + metrics 契约与占位）

Task 4/5 覆写占位页。**只改 `client.ts`/`types.ts`/`vite.config.ts`/`App.tsx`**，不新建页面实现（仅占位壳）。

**Files:**
- Modify: `web/vite.config.ts`（代理加 `/actuator`）
- Modify: `web/src/api/types.ts`（Dag/DagRun/DagRunNode/RunDetail/Metric/NodeDetail）
- Modify: `web/src/api/client.ts`（DAG client fns + fetchMetrics/parsePrometheus）
- Create: `web/src/pages/DagsPage.tsx`（占位壳，Task 4 覆写）
- Create: `web/src/pages/MetricsPage.tsx`（占位壳，Task 5 覆写）
- Modify: `web/src/App.tsx`（+两路由 /dags /metrics +nav）

**Interfaces 生产：** Task 4 消费 DAG client fns + 类型；Task 5 消费 `fetchMetrics`/`parsePrometheus`/`ParsedMetric`。类型字段逐条镜像后端 record（DagRun/DagRunNode/RunDetail 见 Task 1 现状）。

- [ ] **Step 1: vite 代理加 /actuator**

`web/vite.config.ts`：
```ts
server: { port: 5173, proxy: { '/api': 'http://localhost:8080', '/actuator': 'http://localhost:8080' } },
```

- [ ] **Step 2: types.ts 补 DAG + metrics 类型**

```ts
export interface Dag {
  id: number; name: string;
  description: string | null; cron: string;
  enabled: boolean; paused: boolean;
  createdAt: string | null; updatedAt: string | null;
}
/** 镜像 DagController.DagDetail。 */
export interface DagDetail { dag: Dag; nodes: DagNode[]; edges: DagEdge[]; }
export interface DagNode { id: number; dagId: number; nodeKey: string; taskId: number; sortOrder: number; }
export interface DagEdge { id: number; dagId: number; fromNodeId: number; toNodeId: number; }
export interface DagRun {
  id: number; dagId: number; idempotencyKey: string;
  status: string; triggerReason: string; cancelRequested: boolean;
  finishedAt: string | null; createdAt: string | null;
}
export interface DagRunNode {
  id: number; dagRunId: number; nodeKey: string; taskId: number;
  executionId: number | null; status: string; sortOrder: number;
  detail: string | null; createdAt: string | null; finishedAt: string | null;
}
export interface NodeDetail { node: DagRunNode; shards: Shard[]; }
export interface RunDetail { run: DagRun; status: string; nodes: NodeDetail[]; }
/** /actuator/prometheus 解析后的单条系列。 */
export interface ParsedMetric { name: string; tags: Record<string, string>; value: number; }
```

- [ ] **Step 3: client.ts 加 DAG fns + metrics**

```ts
import type { Dag, DagDetail, DagRun, DagRunNode, ParsedMetric, RunDetail } from './types';

// ---- DAG ----
export const listDags = () => req<Dag[]>('/api/v1/dags');
export const getDag = (id: number) => req<DagDetail>(`/api/v1/dags/${id}`);
export const pauseDag = (id: number) => req<Dag>(`/api/v1/dags/${id}/pause`, { method: 'POST' });
export const resumeDag = (id: number) => req<Dag>(`/api/v1/dags/${id}/resume`, { method: 'POST' });
export const triggerDag = (id: number) => req<DagRun>(`/api/v1/dags/${id}/trigger`, { method: 'POST' });
export const listDagRuns = (dagId?: number) =>
  req<DagRun[]>(`/api/v1/dags/runs${dagId ? `?dagId=${dagId}` : ''}`);
export const getRunDetail = (runId: number) => req<RunDetail>(`/api/v1/dags/runs/${runId}`);
export const cancelRun = (runId: number) =>
  req<RunDetail>(`/api/v1/dags/runs/${runId}/cancel`, { method: 'POST' });
export const rerunNode = (runId: number, nodeId: number) =>
  req<DagRunNode>(`/api/v1/dags/runs/${runId}/nodes/${nodeId}/rerun`, { method: 'POST' });

// ---- 指标(只读 /actuator/prometheus, spec §1.5) ----
export const parsePrometheus = (text: string): ParsedMetric[] => {
  const out: ParsedMetric[] = [];
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (!line || line.startsWith('#') || line === 'help' || !line.length) continue;
    const space = line.lastIndexOf(' ');
    if (space < 0) continue;
    const head = line.slice(0, space);
    const value = Number(line.slice(space + 1));
    if (Number.isNaN(value)) continue;
    const brace = head.indexOf('{');
    if (brace === -1) { out.push({ name: head, tags: {}, value }); continue; }
    const name = head.slice(0, brace);
    const tags: Record<string, string> = {};
    for (const pair of head.slice(brace + 1, -1).split(',')) {
      const eq = pair.indexOf('=');
      if (eq > 0) tags[pair.slice(0, eq).trim()] = pair.slice(eq + 1).replace(/^"|"$/g, '');
    }
    out.push({ name, tags, value });
  }
  return out;
};
export const fetchMetrics = async (): Promise<ParsedMetric[]> => {
  const res = await fetch('/actuator/prometheus');
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  return parsePrometheus(await res.text());
};
```

- [ ] **Step 4: 占位壳 + 路由**

`web/src/pages/DagsPage.tsx`（占位，Task 4 覆写）：
```tsx
export default function DagsPage() { return <div>DAG 页（M5.3）</div>; }
```
`web/src/pages/MetricsPage.tsx`（占位，Task 5 覆写）：
```tsx
export default function MetricsPage() { return <div>指标面板（M5.3）</div>; }
```
`web/src/App.tsx`（加 import + nav + route）：
```tsx
import DagsPage from './pages/DagsPage';
import MetricsPage from './pages/MetricsPage';
// nav:
<Link to="/dags">工作流</Link>{' '}
<Link to="/metrics">指标</Link>
// Routes:
<Route path="/dags" element={<DagsPage />} />
<Route path="/metrics" element={<MetricsPage />} />
```

- [ ] **Step 5: 构建验证**

Run: `cd scheduler/web && npm run build`。Expected: PASS（tsc -b + vite build）。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(web): DAG/metrics api client + types + /actuator proxy + 两路由占位"
```

---

## Task 4: DAG 页（批次列表 + run 详情 + 单节点重跑 + 批次终止）——覆写 Task 3 占位

**Files:**
- Modify: `web/src/pages/DagsPage.tsx`（覆写占位）

**Interfaces 消费：** Task 3 产 `listDags/getDag/pauseDag/resumeDag/triggerDag/listDagRuns/getRunDetail/cancelRun/rerunNode` + `Dag/DagRun/DagRunNode/RunDetail` 类型。

- [ ] **Step 1: 实现 DagsPage（列表 + 展开 run + 节点重跑 + 终止）**

`web/src/pages/DagsPage.tsx`（全部代码，镜像 ExecutionsPage 的 `useEffect`+`useInterval` 轮询 + `setErr` 风格；无陈旧闭包——`loadDetail(runId)`/`loadRuns(dagId)` 均显式传参）：
```tsx
import { useEffect, useState } from 'react';
import { cancelRun, getRunDetail, listDagRuns, listDags, rerunNode, triggerDag } from '../api/client';
import type { Dag, DagRun, RunDetail } from '../api/types';
import { useInterval } from '../lib/useInterval';

const TERMINAL = new Set(['SUCCESS', 'FAILED', 'CANCELED', 'SKIPPED']);

export default function DagsPage() {
  const [err, setErr] = useState<string | null>(null);
  const [dags, setDags] = useState<Dag[]>([]);
  const [selected, setSelected] = useState<number | null>(null);
  const [runs, setRuns] = useState<DagRun[]>([]);
  const [detail, setDetail] = useState<RunDetail | null>(null);

  const loadDags = () => listDags().then(setDags).catch((e: unknown) => setErr(String(e)));
  const loadRuns = (dagId: number) => listDagRuns(dagId).then(setRuns).catch((e: unknown) => setErr(String(e)));
  const loadDetail = (runId: number) => getRunDetail(runId).then(setDetail).catch((e: unknown) => setErr(String(e)));

  useEffect(() => { loadDags(); }, []);
  useInterval(loadDags, 5000);
  useEffect(() => { if (selected != null) loadRuns(selected); }, [selected]);
  useInterval(() => { if (selected != null) loadRuns(selected); }, 5000);
  useInterval(() => { const runId = detail?.run?.id; if (runId != null) loadDetail(runId); }, 5000);

  async function onRerun(nodeId: number) {
    const runId = detail?.run?.id;
    if (runId == null) return;
    try { await rerunNode(runId, nodeId); await loadDetail(runId); } catch (e) { setErr(String(e)); }
  }
  async function onCancel() {
    const runId = detail?.run?.id;
    if (runId == null || selected == null) return;
    try { setDetail(await cancelRun(runId)); await loadRuns(selected); } catch (e) { setErr(String(e)); }
  }
  async function onTrigger() {
    if (selected == null) return;
    try { await triggerDag(selected); await loadRuns(selected); } catch (e) { setErr(String(e)); }
  }

  const nodes = detail?.nodes ?? [];
  // 选中 run 时为派生 status(detail.status)，否则用列表的 stored status
  const shownStatus = detail ? detail.status : null;

  return (
    <div>
      <h2>工作流 (DAG)</h2>
      {err && <p style={{ color: 'red' }}>{err}</p>}

      <h3>DAG</h3>
      <select
        value={selected ?? ''}
        onChange={(e) => { setSelected(e.target.value ? Number(e.target.value) : null); setDetail(null); }}
      >
        <option value="">选择 DAG…</option>
        {dags.map((d) => <option key={d.id} value={d.id}>{d.name} (id={d.id})</option>)}
      </select>{' '}
      <button onClick={onTrigger}>手动触发</button>

      <h3>运行批次</h3>
      <ul>
        {runs.map((r) => (
          <li key={r.id}>
            #{r.id} ·{(shownStatus !== null && detail?.run?.id === r.id) ? shownStatus : r.status}{' '}
            <button onClick={() => loadDetail(r.id)}>详情</button>
            {detail?.run?.id === r.id && <button onClick={onCancel}>终止批次</button>}
          </li>
        ))}
      </ul>

      {detail && (
        <div>
          <h3>运行详情 #{detail.run.id}</h3>
          {nodes.map((n) => (
            <div key={n.node.id}>
              <span>{n.node.nodeKey} · {n.node.status}</span>{' '}
              {TERMINAL.has(n.node.status) && <button onClick={() => onRerun(n.node.id)}>重跑节点</button>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: 构建验证**

Run: `cd scheduler/web && npm run build`。Expected: PASS。

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(web): DAG 页——批次列表 + run 详情 + 单节点重跑 + 批次终止，5s 轮询"
```

---

## Task 5: 指标面板（5 核心指标卡 + 手绘内联 SVG）——覆写 Task 3 占位

**Files:**
- Modify: `web/src/pages/MetricsPage.tsx`（覆写占位）

**Interfaces 消费：** Task 3 产 `fetchMetrics`/`parsePrometheus`/`ParsedMetric`。

- [ ] **Step 1: 实现 MetricsPage（5 卡，聚合系列，sparkline）**

`web/src/pages/MetricsPage.tsx`：
```tsx
import { useEffect, useState } from 'react';
import { fetchMetrics } from '../api/client';
import type { ParsedMetric } from '../api/types';
import { useInterval } from '../lib/useInterval';

/** 每卡聚合规则:计数类取系列和(DLQ/活跃),最老队龄取最大值。 */
interface CardDef { key: string; label: string; unit?: string; agg: 'sum' | 'max' | 'raw'; nice?: (v: number) => string; }

const CARDS: CardDef[] = [
  { key: 'scheduler_active_runs', label: '活跃执行', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_due_queue_max_age_seconds', label: 'DUE 最深队龄', unit: 's', agg: 'max', nice: (v) => `${v.toFixed(0)}s` },
  { key: 'scheduler_dag_runs_active', label: '活跃 DAG 批次', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_dlq_depth', label: 'DLQ 深度', agg: 'raw', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_worker_active', label: '调度进程存活', agg: 'raw', nice: (v) => (v >= 1 ? '存活' : '下线') },
];

const HISTORY = 20;

function aggregate(metrics: ParsedMetric[], def: CardDef): number {
  const hits = metrics.filter((m) => m.name === def.key);
  if (!hits.length) return 0;
  const vals = hits.map((m) => m.value);
  if (def.agg === 'max') return Math.max(...vals);
  if (def.agg === 'raw') return vals[0];
  return vals.reduce((a, b) => a + b, 0);
}

function drawPath(vals: number[]): string {
  if (!vals.length) return '';
  const w = 120, h = 36;
  const min = Math.min(...vals), max = Math.max(...vals);
  const span = max - min || 1;
  return vals.map((v, i) =>
    `${i === 0 ? 'M' : 'L'}${(i / (vals.length - 1)) * w},${h - ((v - min) / span) * (h - 4) - 2}`).join(' ');
}

export default function MetricsPage() {
  const [err, setErr] = useState<string | null>(null);
  // 面板数据源路径:每卡保留末 HISTORY 点画 sparkline(5s 轮询追加)
  const [hist, setHist] = useState<Record<string, number[]>>({});

  const tick = async () => {
    try {
      const metrics = await fetchMetrics();
      setHist((prev) => {
        const next: Record<string, number[]> = { ...prev };
        for (const c of CARDS) {
          const v = aggregate(metrics, c);
          next[c.key] = [...(prev[c.key] ?? []), v].slice(-HISTORY);
        }
        return next;
      });
      setErr(null);
    } catch (e) { setErr(String(e)); }
  };

  useEffect(() => { tick(); }, []);
  useInterval(tick, 5000);

  return (
    <div>
      <h2>指标面板</h2>
      {err && <p style={{ color: 'red' }}>{err}</p>}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px,1fr))', gap: 12 }}>
        {CARDS.map((c) => {
          const vals = hist[c.key] ?? [];
          const last = vals.length ? vals[vals.length - 1] : NaN;
          return (
            <div key={c.key} style={{ border: '1px solid #ccc', borderRadius: 6, padding: 10 }}>
              <div style={{ fontWeight: 600 }}>{c.label}</div>
              <div style={{ fontSize: 22 }}>{Number.isNaN(last) ? '—' : c.nice ? c.nice(last) : last}</div>
              <svg width={120} height={36}>
                <polyline points={drawPath(vals)} fill="none" stroke="currentColor" strokeWidth={1.5} />
              </svg>
            </div>
          );
        })}
      </div>
    </div>
  );
}
```
（注：MetricsPage 只读 `/actuator/prometheus`，dev 走 vite `/actuator` 代理，生产随 Spring Boot 静态同源。）

- [ ] **Step 2: 构建验证**

Run: `cd scheduler/web && npm run build`。Expected: PASS。

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(web): 指标面板——5 核心指标卡 + 手绘内联 SVG sparkline，5s 轮询"
```