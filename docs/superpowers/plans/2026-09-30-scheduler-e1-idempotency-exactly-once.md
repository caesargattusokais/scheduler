# Scheduling Idempotency / Exactly-once (E1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 两层幂等防线:A 层给 4 条「裸 UUID」触发路径加可选 `idempotencyKey` 请求参数(同 key 去重复用现父);B1 层把回收统一成「仅当属主 worker 非 ALIVE」活性门 + `attempt` 纳入属主守卫拒陈旧写 + 续租改强制自栅栏。零迁移。

**Architecture:** 复用已有 `createParentWithShards ON CONFLICT (idempotency_key)` + `execution.idempotency_key UNIQUE` / `dag_run.idempotency_key UNIQUE` 做 A 层去重(B 层无),复用 `execution_shard.attempt`/`worker_id` + `worker.last_seen`/`status` 做 B1 活性门与栅栏。

**Tech Stack:** Java/Spring Boot、Postgres、JUnit(离线 Maven `-o`)、前端 React/Vite。

**Spec:** `docs/superpowers/specs/2026-09-30-scheduler-e1-idempotency-exactly-once-design.md`(本 plan 自 spec 论证;执行者同读 spec + plan)。

## Global Constraints(从 spec §10 复制)

- 零新增依赖、零迁移(V32 无);仅 Java/Spring 既有栈。
- 密钥辅助入 `core/IdempotencyKeys`,镜像既有 `forTrigger`/`forManualDagRun`/`forNodeRerun` 手法。
- 单一时钟(DB 持时);worker ALIVE 判据复用现有 heartbeat 语义(worker.status='ALIVE' AND last_seen >= now()-stale)。
- 全向后兼容:缺省行为不变(未带 idempotencyKey → 维持 UUID)。既有测试保持绿。
- 测试门:离线 Maven `-o`,`-pl scheduler-server -am`(server 测试)或 `-pl scheduler-persistence -am`(repo 测试),`-Dsurefire.failIfNoSpecifiedTests=false`;前端 `npm run build` 常绿。
- 提交到 `main`(仓库单一主分支惯例);commit 尾加 `Co-Authored-By: Claude Code <noreply@anthropic.com>`。

---

## File Structure

**A 层(触发幂等):**
- `scheduler-core/.../core/IdempotencyKeys.java` — 加 4 个 request-token 辅助(把现 inline 键收口)。
- `scheduler-server/.../web/TaskController.java` — `trigger` 加可选 `idempotencyKey`。
- `scheduler-server/.../web/ExecutionController.java` — `rerun` 加可选 `idempotencyKey`。
- `scheduler-server/.../web/DagController.java` — `trigger` + `rerunNode` 加可选 `idempotencyKey`。
- `scheduler-persistence/.../DagRepository.java` + `JdbcDagRepository.java` — `createManualRun` 加 token 重载。
- `scheduler-server/.../dag/DagEngine.java` — `rerunNode` 加 token 形参。
- Test:`scheduler-server/.../web/ApiIntegrationTest.java`(A 层幂等用例)。

**B1 层(投递/认领幂等):**
- `scheduler-persistence/.../ShardRepository.java` + `JdbcShardRepository.java` — `findOverRuntime`/`findExpiredRunning` 活性门;`markStatusOwned`/`renewLease` 加 `expectedAttempt`。
- `scheduler-server/.../worker/ExecutorWorker.java` — 强制续租自栅栏 + 传 `expectedAttempt`。
- Test:repo 单测(`JdbcShardRepositoryTest` 或其所在) + `AlertEngine`-style 单测(`ExecutorWorker` 相关)。

**收尾:**
- `docs/openapi/api-docs.json` 重导(additive);spec 标「已实现」。

---

## Task 1: A 层 — IdempotencyKeys 简化 + Task 手动触发 `idempotencyKey`

**Files:**
- Modify: `scheduler-core/src/main/java/dev/scheduler/core/IdempotencyKeys.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java:219-232`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Produces:`IdempotencyKeys.forManualTask(long taskId, String requestToken)` → `"manual:"+taskId+":"+token`;`IdempotencyKeys.forTaskRerun(long srcId, String requestToken)` → `"rerun:"+srcId+":"+token`。
- Consumes:现有 `createParentWithShards(taskId, key, shardCount)` 的 `ON CONFLICT (idempotency_key)`(JdbcShardRepository:54-85)。

- [ ] **Step 1: 写失败测试(先验证 A 去重端点行为)**

`ApiIntegrationTest` 加用例(在既有 task 触发测试旁,复位 DB 手法沿用 resetDb + seedTaskRow):

```java
@Test
void manualTrigger_sameIdempotencyKey_deduplicatesToSingleParent() {
  long t = seedTaskRow("DedupTask", "demo");
  String key = "my-request-001";
  var p1 = mvc.perform(post("/api/v1/tasks/" + t + "/trigger")
          .param("idempotencyKey", key)).andExpect(status().isCreated())
      .andReturn().getResponse().getContentAsString();
  var p2 = mvc.perform(post("/api/v1/tasks/" + t + "/trigger")
          .param("idempotencyKey", key)).andExpect(status().isCreated())
      .andReturn().getResponse().getContentAsString();
  long id1 = objectMapper.readTree(p1).get("id").asLong();
  long id2 = objectMapper.readTree(p2).get("id").asLong();
  assertEquals(id1, id2, "same idempotencyKey must reuse the same parent execution");
  // 全库只应有一条该 key 的父
  Integer rows = jdbc.queryForObject(
      "SELECT count(*) FROM execution WHERE idempotency_key=?", Integer.class, "manual:" + t + ":" + key);
  assertEquals(1, rows, "exactly one parent for the idempotency key");
}

@Test
void manualTrigger_withoutKey_stillMakesNewRunEachTime() {
  long t = seedTaskRow("DedupTaskB", "demo");
  long a = objectMapper.readTree(mvc.perform(post("/api/v1/tasks/" + t + "/trigger"))
      .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
  long b = objectMapper.readTree(mvc.perform(post("/api/v1/tasks/" + t + "/trigger"))
      .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
  assertNotEquals(a, b, "no idempotencyKey -> fresh parent each POST (backward compatible)");
}
```

(测试行 `manual:{t}:{key}` 须与实现键格式严格一致;`seedTaskRow` 若签名不同按既有用法调整。)

- [ ] **Step 2: 跑测,确认 FAIL**

Run: `mvn -o -q -pl scheduler-server -am test -Dtest='ApiIntegrationTest#manualTrigger_sameIdempotencyKey_deduplicatesToSingleParent+manualTrigger_withoutKey_stillMakesNewRunEachTime' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — 控制器今天不认 `idempotencyKey` 参数(键恒 UUID),两 POST 得两条父。

- [ ] **Step 3: IdempotencyKeys 加两个辅助**

`IdempotencyKeys.java`(镜像既有手法,加在同类静态方法之后):

```java
  /** task 手动触发(可选 request-token 幂等):带 token → 同 token 重试复现同父;缺省由调用方决定回落到 UUID。 */
  public static String forManualTask(long taskId, String requestToken) {
    return "manual:" + taskId + ":" + requestToken;
  }

  /** 源 execution 重跑(可选 request-token 幂等):镜像 forManualTask。 */
  public static String forTaskRerun(long srcExecutionId, String requestToken) {
    return "rerun:" + srcExecutionId + ":" + requestToken;
  }
```

- [ ] **Step 4: TaskController.trigger 加可选参**

`trigger` 方法改为带 `@RequestParam(value="idempotencyKey", required=false) String idempotencyKey` 并把后缀从「恒 UUID」换成「token 或新 UUID」:

```java
  @PostMapping("/{id}/trigger")
  public ResponseEntity<Execution> trigger(
      @PathVariable long id,
      @RequestParam(value = "idempotencyKey", required = false) String idempotencyKey) {
    Task before = requireTask(id);
    String suffix = idempotencyKey != null ? idempotencyKey : UUID.randomUUID().toString();
    Execution run = manualRun(id, suffix);
    auditor.record(...); // 原样保留
    return ResponseEntity.status(HttpStatus.CREATED).body(run);
  }
```

`manualRun` 内联键改用辅助(at :231 附近):

```java
    String key = IdempotencyKeys.forManualTask(t.id(), suffix);
```

（若 `manualRun` 已内联 `"manual:" + t.id() + ":" + suffix`,仅将字面量替换为 `forManualTask`;`suffix` 为 null 的路径由调用方保证非 null。修复 `trigger` 传入后 `manualRun` 必收非 null suffix。)

- [ ] **Step 5: 跑测,确认 PASS + 全量回归**

Run: `mvn -o -q -pl scheduler-server -am test -Dtest='ApiIntegrationTest#manualTrigger_sameIdempotencyKey_deduplicatesToSingleParent+manualTrigger_withoutKey_stillMakesNewRunEachTime' -Dsurefire.failIfNoSpecifiedTests=false` → 两例 PASS。
Run: `mvn -o -q -pl scheduler-server -am test -Dsurefire.failIfNoSpecifiedTests=false` → 全绿(不破坏既有 trigger 行为)。

- [ ] **Step 6: 提交**

```bash
git add scheduler-core/src/main/java/dev/scheduler/core/IdempotencyKeys.java \
        scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java \
        scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java
git commit -m "feat(e1): manual task trigger accepts idempotencyKey (dedupe to same parent)"
```

---

## Task 2: A 层 — Execution rerun / Dag manual trigger / Dag node rerun 的 `idempotencyKey`

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/ExecutionController.java:97-105`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/DagController.java:159-166` + `:214-221`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/dag/DagEngine.java:115-127`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/DagRepository.java` + `JdbcDagRepository.java:258-260`
- Test: `ApiIntegrationTest.java`

**Interfaces:**
- Consumes:`IdempotencyKeys.forTaskRerun`(Task 1)、`IdempotencyKeys.forManualDagRun`/`forNodeRerun`(现有)。
- Produces:重载 `DagRepository.createManualRun(long dagId, String requestToken)`;形参化 `DagEngine.rerunNode(long runId, long nodeId, String requestToken)`。

- [ ] **Step 1: 写失败测试**

`ApiIntegrationTest`(镜像 Task 1 手法,各自复位):
- `POST /executions/{id}/rerun` 同 `idempotencyKey` 双发 → 同父 id;键 `rerun:{srcId}:{key}` 全库 1 行。先建一条终态源执行(seedTaskRow + 触发表述终态,复用既有 executeToTerminal helper 若存在)。
- `POST /dags/{id}/trigger` 同 `idempotencyKey` 双发 → 同 dag_run id;键 `dag:{dagId}:manual:{key}` 全库 1 行。
- `POST /dags/runs/{runId}/nodes/{nodeId}/rerun` 同 `idempotencyKey` 双发 → 第二次命中既有守卫(节点已非 terminal)→ 返回 NOT_ACCEPTABLE/409;或(若 rerun 复用)只产一条 execution。断言「重发不新增 execution」为准。
- 无 key 的 rerun / manual-dag → 每次新建(向后兼容)。

- [ ] **Step 2: 跑测,确认 FAIL**

Run: 上面四例(按 helper 名));Expected: FAIL(端点不认参 / key 恒 UUID)。

- [ ] **Step 3: ExecutionController.rerun 加可选参**

```java
  @PostMapping("/{id}/rerun")
  public ResponseEntity<Execution> rerun(
      @PathVariable long id,
      @RequestParam(value = "idempotencyKey", required = false) String idempotencyKey) {
    Execution source = executions.findById(id).orElseThrow(() -> notFound("execution " + id));
    if (!RERUNNABLE.contains(source.status())) {
      throw new ResponseStatusException(HttpStatus.CONFLICT, /* 原样保留 */);
    }
    String suffix = idempotencyKey != null ? idempotencyKey : UUID.randomUUID().toString();
    String key = IdempotencyKeys.forTaskRerun(source.id(), suffix);
    Execution created = shards.createParentWithShards(
        source.taskId(), key, source.shardCount(), source.args(), source.id());
    auditor.record(...); // 原样保留
    return ResponseEntity.status(HttpStatus.CREATED).body(created);
  }
```

(`rerun` 的 UUID import 若不再用,检查 `UUID` 是否别处仍用;若任务文件里成 orphan 则删 import。)

- [ ] **Step 4: Dag 手动触发 + node rerun 透传 token**

`DagRepository` + `JdbcDagRepository` 加重载:

```java
  /** 手动触发(可选 request-token 幂等):带 token 用 token、缺省 UUID 语义(镜像 forManualDagRun)。 */
  DagRun createManualRun(long dagId, String requestToken);
```
```java
  @Override public DagRun createManualRun(long dagId, String requestToken) {
    String key = requestToken != null ? "dag:" + dagId + ":manual:" + requestToken
                                      : IdempotencyKeys.forManualDagRun(dagId);
    return createRun(dagId, key, "manual");
  }
```

`DagController.trigger` 加可选参并透传:

```java
  @PostMapping("/{id}/trigger")
  public ResponseEntity<DagRun> trigger(
      @PathVariable long id,
      @RequestParam(value="idempotencyKey", required=false) String idempotencyKey) {
    Dag before = requireDag(id);
    DagRun run = dags.createManualRun(id, idempotencyKey);
    auditor.record(...); // 原样
    return ResponseEntity.status(HttpStatus.CREATED).body(run);
  }
```

`DagEngine.rerunNode` 形参化(:115):加 `String requestToken` 参数,键路由:

```java
  public DagRunNode rerunNode(long runId, long nodeId, String requestToken) {
    DagRunNode n = dags.findNode(nodeId).orElseThrow(...);
    ... // 原有守卫原样
    String key = requestToken != null
        ? "dag:" + runId + ":node:" + n.nodeKey() + ":rerun:" + requestToken
        : IdempotencyKeys.forNodeRerun(runId, n.nodeKey());
    Execution parent = shards.createParentWithShards(n.taskId(), key, task.shardCount());
    ...
  }
```

`DagController.rerunNode`(:214-221)加可选参并透传 `dagEngine.rerunNode(runId, nodeId, idempotencyKey)`。

- [ ] **Step 5: 跑测,确认 PASS + 全量回归**

Run: `mvn -o -q -pl scheduler-server -am test -Dsurefire.failIfNoSpecifiedTests=false` → 全绿(含既有 DAG/rerun 测试保持)。

- [ ] **Step 6: 提交**

```bash
git add -A
git commit -m "feat(e1): rerun + manual dag + dag node rerun accept idempotencyKey (dedupe)"
```

---

## Task 3: B1 层 — 统一活性门控回收(仅非 ALIVE 属主才回收 RUNNING)

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java:243-251`(findOverRuntime)、`:229-241`(findExpiredRunning)
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/`(JdbcShardRepositoryTest 或所在 repo 测试)

**Interfaces:**
- Consumes:Worker heartbeat 判据(worker.status='ALIVE' AND last_seen >= now()-stale)。
- Produces:两回收源共用一条活性门 —— 语义变化见 §4.1。

- [ ] **Step 1: 写失败测试(先证现状双跑窗)**

repo 测试(seed worker ALIVE + shard RUNNING + 超时/lease 过期):
- `findOverRuntime` 对 **ALIVE 属主超时** shard 返回空(活体慢不回收)。
- `findOverRuntime` 对 **死属主超时** shard 返回该 shard(真死可回收)。
- `findExpiredRunning` 对 **ALIVE 属主 lease 过期** shard 返回空(单一活性门,去掉 lease-only 旁路)。
- `findExpiredRunning` 对死属主返回该 shard。

(seed 手法:jdbc 直插 `worker`/`execution_shard`/`execution` 构造 RUNNING + 属主行;时间回溯用 `now() - interval`。)

- [ ] **Step 2: 跑测,确认 FAIL**

Expected: 现状 `findOverRuntime`(不看活性)会把 ALIVE 超时也返回 → ALIVE 断言 FAIL。

- [ ] **Step 3: 两个查询统一加活性门**

`findOverRuntime` 加 `AND NOT EXISTS (... ALIVE)`:

```sql
SELECT s.id, s.attempt FROM execution_shard s JOIN execution e ON e.id = s.execution_id
   WHERE e.task_id=? AND s.status='RUNNING' AND s.worker_id IS NOT NULL
     AND now() - s.started_at > make_interval(secs => ?)
     AND NOT EXISTS (SELECT 1 FROM worker w
                      WHERE w.id = s.worker_id
                        AND w.status = 'ALIVE'
                        AND w.last_seen >= now() - make_interval(secs => ?))
```
(第三个 `?` 参数 = stale 秒数,复用 `staleAfterSeconds` 或取同值;方法签名相应加参或复用已有聚合值——若改变签名,更新 caller `Reconciler`。)

`findExpiredRunning` 改单一活性门(去掉 lease-only 旁路),谓词只留 `NOT EXISTS (ALIVE)`:

```sql
SELECT s.id, s.attempt FROM execution_shard s JOIN execution e ON e.id = s.execution_id
   WHERE e.task_id=? AND s.status='RUNNING' AND s.worker_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM worker w
                      WHERE w.id = s.worker_id
                        AND w.status = 'ALIVE'
                        AND w.last_seen >= now() - make_interval(secs => ?))
```

(若 `findOverRuntime` 与 `findExpiredRunning` 现签名/调用方(Reconciler :48-75)有差异,按实际重排参数——都返回 `List<ExpiredShard>`,Reconciler 收到后照旧 `markStatus(FAILED)` + failureResolver。)

- [ ] **Step 4: 跑测,确认 PASS + 全量回归**

Run: `mvn -o -q -pl scheduler-persistence -am test -Dtest='<repo 测试类>' -Dsurefire.failIfNoSpecifiedTests=false` → PASS。
Run: `mvn -o -q -pl scheduler-server -am test -Dsurefire.failIfNoSpecifiedTests=false` → 全绿。

- [ ] **Step 5: 提交**

```bash
git add scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java \
        scheduler-persistence/src/test/java/dev/scheduler/persistence/... 
git commit -m "fix(e1): reclaim RUNNING shard only when owner worker not ALIVE (unified liveness gate)"
```

---

## Task 4: B1 层 — 每-attempt 栅栏(拒陈旧写/续租)

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java`(markStatusOwned :283-299、renewLease :213-...)
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/worker/ExecutorWorker.java`(调用点)
- Test: repo 单测

**Interfaces:**
- Consumes:`Shard.attempt`(claim 时 +1,JdbcShardRepository:174)。
- Produces:`markStatusOwned(shardId, to, ownerWorkerId, expectedAttempt, detail)`;`renewLease(shardId, ownerWorkerId, expectedAttempt, leaseUntil)` —— WHERE 均加 `AND attempt=?`。

- [ ] **Step 1: 写失败测试**

repo 测试:先认领(attempt=1)→ 回收置 FAILED → 再认领(attempt=2)→ 用 attempt=1 调 `markStatusOwned(...SUCCESS, owner, 1, ...)` 与 `renewLease(shardId, owner, 1, ...)` → 均应返回 false 且不落 outcome / 不延长 lease。用 attempt=2 调则成功。

- [ ] **Step 2: 跑测,确认 FAIL**

Expected: 现状属主守卫不含 attempt → attempt=1 的旧写仍命中(status+worker_id 配)→ false(新语义)断言 FAIL。

- [ ] **Step 3: 接口 + 实现加 `expectedAttempt`**

`ShardRepository` 两方法签名加 `int expectedAttempt`。
`JdbcShardRepository.markStatusOwned` 的 UPDATE WHERE 加 `AND attempt=?`(传 `expectedAttempt`);`renewLease` 同加。守卫失败维持现有静默/`log.debug` 抑制语义。

- [ ] **Step 4: 更新 ExecutorWorker 调用点**

`ExecutorWorker` 持有已认领 `Shard`(含 `attempt` 字段),两个调用点把该 `attempt` 作为 `expectedAttempt` 传入:
```java
// renew tick (:73-81 区域):把现有 renewLease(shardId, owner, until) 改为
if (!shards.renewLease(shard.id(), ownId, shard.attempt(), until)) { /* 栅栏逻辑于 Task 5 */ }
// write-back (:118-123 区域):markStatusOwned(..., shard.attempt(), detail)
```
用项目既有日志手法记录栅栏命中。若 `ExecutorWorker` 持有 `Shard` 而非分片 id,按其现有字段结构取。

- [ ] **Step 5: 跑测,确认 PASS + 全量回归**

Run: `mvn -o -q -pl scheduler-server -am test -Dsurefire.failIfNoSpecifiedTests=false` → 全绿(既有 ExecutorWorker 认领/写回测试照常,attempt 正在途值)。

- [ ] **Step 6: 提交**

```bash
git add -A
git commit -m "fix(e1): fence stale writes/lease-renews by expected attempt"
```

---

## Task 5: B1 层 — 强制续租自栅栏(续租失败立停该 work unit)

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/worker/ExecutorWorker.java`

**Interfaces:**
- Consumes:Task 4 的 `renewLease(..., expectedAttempt, ...)` 返回语义(属主/attempt 失 → false)。
- Produces:激活该返回值 —— 续租失败 → 本 tick 不推进该 work unit 的 handler 副作用。

- [ ] **Step 1: 写失败测试**

行为断言:构造 renewLease 返回 false(已回收/attempt 不符),验证该 work unit 本 tick 不执行 handler / 不推进副作用。做法:ExecutorWorker 单元测试(若已有)注入假 ShardRepository 使 renewLease=false,断言 handler/副作用未触发;若无既有单测,在 Task 4 基础上加一个聚焦续租失败分支的用例。断言「续租成功 → handler 触发;续租失败 → handler 未触发」双向。

- [ ] **Step 2: 跑测,确认 FAIL**

Expected: 现状续租失败仅 `log.warn` 继续 → handler 仍跑 → 失败断言 FAIL。

- [ ] **Step 3: 续租失败自栅栏**

`ExecutorWorker` 的 per-shard tick 中,`renewLease(...)` 返回 false 时:**跳过该 work unit 本轮 handler 副作用**(record 到日志),不推进本轮动作,让位给回收判定。不停止同 worker 其它在途分片(温和粒度,spec §4.3)。若当前 tick 结构是先续租后执行,把续租守卫提到执行前并 early-continue。

- [ ] **Step 4: 跑测,确认 PASS + 全量回归**

Run: `mvn -o -q -pl scheduler-server -am test -Dsurefire.failIfNoSpecifiedTests=false` → 全绿。

- [ ] **Step 5: 提交**

```bash
git add -A
git commit -m "fix(e1): executor self-fences work unit when lease cannot renew"
```

---

## Task 6: 全量验收 + OpenAPI 快照重导 + spec 标记实现

**Files:**
- Modify: `docs/openapi/api-docs.json`(属性门控重导)
- Modify: `docs/superpowers/specs/2026-09-30-scheduler-e1-idempotency-exactly-once-design.md`(标题下加「已实现」)

- [ ] **Step 1: 全套测试**

Run: `cd /d/ai-project/scheduler && mvn -o -q -pl scheduler-server -am test -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 全绿(既有 + E1 新增)。

- [ ] **Step 2: OpenAPI 快照重导**

Run: `mvn -o -q -pl scheduler-server -am -Dtest=ApiIntegrationTest#openApiSnapshot_exportAsCommittableArtifact -Dscheduler.openapi.dump="$(cygpath -w "$PWD/docs/openapi/api-docs.json")" test -Dsurefire.failIfNoSpecifiedTests=false`
Verify: `grep -o '"idempotencyKey"' docs/openapi/api-docs.json` 出现 ≥4 次(4 端点请求参数,additive)。

- [ ] **Step 3: spec 标记**

`2026-09-30-scheduler-e1-idempotency-exactly-once-design.md` 标题下加一行:`> **状态**:已实现(2026-09-30)。`

- [ ] **Step 4: 前端构建确认**

Run: `cd /d/ai-project/scheduler/web && npm run build`
Expected: 绿(本子项无前端改动,保持常绿)。

- [ ] **Step 5: 提交**

```bash
git add -A && git commit -m "chore(e1): full qa + openapi snapshot + spec marked implemented"
```

---

## 自审(对照 spec)

- §3 A 层 4 路径(Task 1/2):manual task / rerun / manual dag / node rerun 全加可选 idempotencyKey;缺省回 UUID。✓
- §4.1 单一活性门覆盖两回收源(Task 3):findOverRuntime + findExpiredRunning 同一 `NOT EXISTS(ALIVE)`。✓(去掉了 lease-only 旁路.)
- §4.2 每-attempt 栅栏(Task 4):markStatusOwned + renewLease 属主守卫加 attempt。✓
- §4.3 强制续租自栅栏(Task 5):续租失败停该 work unit。✓
- §5 零迁移:Task 全在既有列/方法上。✓
- §6 API:4 端点加可选参,OpenAPI additive(Task 6)。✓
- 向后兼容:无 key 路径行为不变,既有测试保持绿。✓
- 明确边界:副作用严格 exactly-once 交 handler(未打包)。✓