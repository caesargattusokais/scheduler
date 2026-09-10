# M6.3 Server De-wire (Remote Execution) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove every `dev.scheduler.worker.*` import and the in-process executor/handler beans from `scheduler-server`, so the server context has no handler and no executor — the control plane reads/validates execution purely from the shared DB's live-worker registry.

**Architecture:** Relocate the shared retry decision classes (`RetryPolicy`, `FailureResolver`) into `scheduler-persistence` (the one module both server and worker already depend on), then delete the server's worker dependency, its `demoHandler`/`handlerRegistry`/`executorWorker`/`workLoop` beans, and collapse `AvailableHandlerRefs` to a pure live-worker union. Server integration tests that drove the now-removed in-process `ExecutorWorker`/`FlakyHandler` are migrated: execution-plane E2E relocates to the worker module; server tests assert control-plane-only outcomes (`trigger → shard stays DUE`), seeding a live worker row where a valid `handlerRef` is required.

**Tech Stack:** Java 21, Spring Boot 3.2.5 (web/actuator/prometheus/jdbc), Flyway, Testcontainers PG16, Maven multi-module (`scheduler-core → scheduler-persistence → scheduler-server` + `scheduler-worker` sibling).

**Spec:** `docs/superpowers/specs/2026-09-10-scheduler-m6-distributed-worker-design.md` (§1 topology, §3 validation fail-fast, §4 isolation, §5 test migration, §6.M6.3 anchor, §8 open-item 4).

## Global Constraints

- **Server context carries NO `dev.scheduler.worker.*` type and NO `ExecutionHandler`/`ExecutorWorker`**, per spec §1/§6.M6.3. After M6.3, `scheduler-server` must compile, start, and pass tests with ZERO dependency on `scheduler-worker` (the M6.1–M6.2 temporary one-way dep — the server's `pom.xml` in the worker dependency — is removed).
- **Server retains and downs only**: `web/ trigger/ leader/ reconcile/ dag/ config`. `Reconciler` stays in server and keeps its orphan-lease reclamation → it still needs `RetryPolicy`/`FailureResolver`, which M6.3 relocates to `scheduler-persistence` so the server keeps them without importing worker.
- **`AvailableHandlerRefs.refs()` = pure live-worker union** (`findAllAlive(now-30s)` refs); the in-process `HandlerRegistry` leg is deleted. `TaskController` create/update validation and `HandlerController` list both read this single source (unchanged call sites from M6.2).
- **Fail-fast intact**: `MapHandlerRegistry` duplicate-ref `IllegalStateException` stays (worker-side, untouched by M6.3).
- **Test migration per spec §5**: server tests assert `trigger → shard stays DUE (no worker)` and `create/update reject orphan ref (400)`; execution-plane chains (DUE→RUNNING→SUCCESS, retry, DLQ, cancel-coop, fail-fast) live in the worker module. Delete in-process-executor-driven server tests; relocate genuine coverage worker-side; do not silently drop behavior.
- **Full reactor green** at the end: `mvn -o test` → 0 failures (core / persistence / worker / server). Set `api.version` via root-pom surefire property (already wired).
- Spring JDBC cannot bind raw `java.time.Instant` — wrap with `java.sql.Timestamp.from(...)` in SQL writes (existing idiom).
- Worker id prefix distinct from server's `<os>:<hostname>` (M6.2 ruling; server's own `defaultWorkerId` is deleted in T3 — no longer needed once the server has no executor).
- Commit messages end with:
  `Co-Authored-By: Claude Code <noreply@anthropic.com>`

---
## Pre-flight conflict scan

| Tx | Files it touches | vs | Notes |
|----|------------------|----|-------|
| T1 | worker `retry/*`→ persistence `retry/*`; imports in `Beans`, `Reconciler`, `ReconcilerTest`, `WorkerConfig`, `ExecutorWorker`, `ExecutorWorkerTest` | all | single relocation; both modules retain the classes, only package changes |
| T2 | server `ApiIntegrationTest.java` + worker `ExecutorWorkerTest.java` | T1, T3 | consumes relocated imports (T1); **no main-code change** — server main still has in-process executor so tests stay compilable while being migrated |
| T3 | server `pom.xml`, `Beans.java`, `AvailableHandlerRefs.java` | T1, T2 | T2 must run first (it strips test references to the beans T3 deletes); T2 leaves `AvailableHandlerRefs` untouched so T3's collapse is test-neutral |
| T4 | root `README.md` (if needed) | none | authoritative full-reactor + docs |

Ordering T1 < T2 < T3 so every task leaves the reactor green (T2 needs relocation imports from T1 AND server-main beans still present to compile; T3 needs tests already freed of worker references). T4 last.

---
## Task 1: Relocate `RetryPolicy` + `FailureResolver` to `scheduler-persistence`

**Files:**
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/RetryPolicy.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/FailureResolver.java`
- Create: `scheduler-persistence/src/test/java/dev/scheduler/persistence/retry/RetryPolicyTest.java`
- Delete: `scheduler-worker/src/main/java/dev/scheduler/worker/retry/RetryPolicy.java`
- Delete: `scheduler-worker/src/main/java/dev/scheduler/worker/retry/FailureResolver.java`
- Delete: `scheduler-worker/src/test/java/dev/scheduler/worker/retry/RetryPolicyTest.java`
- Modify (import-only): `scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java`, `scheduler-worker/src/main/java/dev/scheduler/worker/execute/ExecutorWorker.java`, `scheduler-worker/src/test/java/dev/scheduler/worker/execute/ExecutorWorkerTest.java`, `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`, `scheduler-server/src/main/java/dev/scheduler/server/reconcile/Reconciler.java`, `scheduler-server/src/test/java/dev/scheduler/server/reconcile/ReconcilerTest.java`

**Interfaces:**
- Produces (later tasks rely on): public classes `dev.scheduler.persistence.retry.RetryPolicy` (methods `boolean shouldRetry(Task,int,String)`, `long delayMs(long,int)` — unchanged) and `dev.scheduler.persistence.retry.FailureResolver` (ctor `(ShardRepository, RetryPolicy, Clock)`, method `void handle(Task,long,int,String)` — unchanged).

- [ ] **Step 1: Move `RetryPolicy` verbatim**, changing only the package declaration from `dev.scheduler.worker.retry` to `dev.scheduler.persistence.retry`. Its deps (`dev.scheduler.core.Task`, `java.util.regex.Pattern`) are already visible from persistence (persistence depends on core). No other change.

- [ ] **Step 2: Move `FailureResolver` verbatim**, changing only the package declaration. Its deps (`dev.scheduler.core.Task`, `dev.scheduler.persistence.ShardRepository`, `java.time.Clock`) are all in/persistence. Keep the javadoc (M3 shard-object note + "供 worker 与 reconciler 复用同一套判定逻辑" — now literally true at the module level).

- [ ] **Step 3: Move `RetryPolicyTest` verbatim** to `scheduler-persistence/src/test/java/dev/scheduler/persistence/retry/RetryPolicyTest.java`, changing only the package. It is pure JUnit (no PG), so persistencence's existing `junit-jupiter` test scope covers it. Keep all 13 assertions untouched.

- [ ] **Step 4: Update the six consumer imports.** In each file listed under `Modify` above, replace `import dev.scheduler.worker.retry.FailureResolver;` → `import dev.scheduler.persistence.retry.FailureResolver;` and `RetryPolicy` likewise. (`WorkerConfig`/`ExecutorWorkerTest` import both; `Beans`/`Reconciler`/`ReconcilerTest` import `FailureResolver`; `Beans` also imports `RetryPolicy`; `ExecutorWorker` imports `FailureResolver`.)

- [ ] **Step 5: Verify the whole reactor compiles and stays green.**

Run: `mvn -o test`
Expected: BUILD SUCCESS, 0 failures (core 6 / persistence 75 / worker 30 / server 58). Counts are the M6.2 baseline; the only delta is package moves — no new/removed tests.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(persistence): relocate RetryPolicy+FailureResolver out of worker into shared persistence module

Both server Reconciler and worker ExecutorWorker consume these pure decision classes;
M6.3 lets the server drop its worker dependency while keeping orphan-lease reclamation.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---
## Task 2: Migrate server integration tests off the in-process executor; relocate execution-plane E2E to worker

**Files:**
- Modify: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`
- Modify: `scheduler-worker/src/test/java/dev/scheduler/worker/execute/ExecutorWorkerTest.java`

**Interfaces:**
- Consumes: relocated `dev.scheduler.persistence.retry.*` (T1). Server main still exposes in-process `ExecutorWorker`/`FlakyHandler`/`demo`/`flaky` — so this task's test edits compile against them and are only *removed*, never used. Do NOT touch `AvailableHandlerRefs` here; its server main still has the in-process leg until T3.
- Produces: server tests that (a) no longer reference `ExecutorWorker`/`ExecutionHandler`/`HandlerContext`/`FlakyHandler`, (b) seed a live `w-demo` worker in `resetDb()` so `postTask(...)`'s `demo` ref passes M6.2 validation, and (c) assert control-plane-only facts (`trigger → shard stays DUE`, `create/update reject orphan 400`). Worker-side, two new tests: fail-fast sibling-cancel + backoff-gate exclusion.

Ruling (this task): the in-process `FlakyHandler`/`ExecutorWorker` E2E tests exercise **execution-plane** behavior. `ExecutorWorkerTest` already covers DUE→SUCCESS, retry scheduling, exhausted→dead_letter, cancel-before-run, multi-shard claim, missing-handler orphan (see `scheduler-worker/.../ExecutorWorkerTest.java`). Two behaviors have NO worker-side coverage and MUST be re-added there so deleting the server E2E loses nothing: (1) **fail-fast sibling cancellation** (3-shard, one exhausts → siblings CANCELED); (2) **backoff-gate exclusion** (future `next_retry_at` → `findCandidate` rejects the shard, verified only in the server's deleted retry test today).

Verified seed interactions you must respect (from the current `ApiIntegrationTest`):
- `resetDb()` already TRUNCATEs `worker` (line 166) and resets `CLOCK.now = BASE` (2026-01-01T10:05:00Z).
- `postTask()`/`postTaskWithRetry()` create tasks with `handlerRef` `demo` via the validating controller, at whatever `CLOCK.now` currently is. After the collapse, `demo` is legal **only** while the `@BeforeEach`-seeded `w-demo` is fresh relative to the live `CLOCK.now` — i.e. only in tests that leave `CLOCK.now` at `BASE`.
- `handlersEndpoint_listsRegisteredRefs` (line 198) and the three M6.2 validation tests (lines 211, 221, 234) move `CLOCK.now` forward to `2026-09-10`. After that move the seeded `w-demo` (last_seen = BASE) is >30 s stale and drops out of the pure-live-worker union — so any assertion relying on `demo` at the moved clock must re-seed its own fresh `w-demo`.

- [ ] **Step 1: Add a live-worker seed to `resetDb()`.**

After the existing `CLOCK.now = BASE;` line, seed a fresh ALIVE worker so task-creation validation (M6.2 `requireAvailableHandlerRef`) accepts the default `demo` ref used by `postTask(...)`:

```java
// M6.3:server 上下文无进程内 handler——测试建任务须先注册一个存活 worker 提供 handlerRef(demo);
//  该 worker 在未拨动 CLOCK.now 的用例中恒存活(见 handlersEndpoint 用例拨钟后须重播种)。
jdbc.update("INSERT INTO worker(id, refs, last_seen, status) VALUES('w-demo','demo',?,'ALIVE')",
    java.sql.Timestamp.from(CLOCK.now));
```

`CLOCK.now` is `BASE` on the line above, so `w-demo` is last-seen fresh.

- [ ] **Step 2: Remove the in-process executor/handler wiring from `ApiIntegrationTest`.**

Delete:
- imports `dev.scheduler.worker.execute.ExecutorWorker`, `dev.scheduler.worker.handler.ExecutionHandler`, `dev.scheduler.worker.handler.HandlerContext` (unused once Step 3/4/5 remove their references).
- the `@Autowired ExecutorWorker executorWorker;` field.
- the `@Autowired FlakyHandler flakyHandler;` field.
- the `TestClockConfig.flakyHandler()` `@Bean` method and the whole `FlakyHandler` static class (lines ~125-131, ~1100-1112).
- `flakyHandler.failNext = true;` in `resetDb()`.
- imports `java.util.List` stays (still used by fan-out/DAG test assertions).

- [ ] **Step 3: Rewrite `handlersEndpoint_listsRegisteredRefs`** — `demo` must now come from a fresh live worker at the test's own clock.

Replace the test body so BOTH refs are seeded live at the test's `base` (the `@BeforeEach` `w-demo` was seeded at `BASE` and is stale once this test moves `CLOCK.now` to `2026-09-10`):

```java
/** M6.3:表单下拉框的数据源 = 纯存活 worker 注册表并集(与 create/update 校验同源;无进程内 handler)。 */
@Test
void handlersEndpoint_listsRegisteredRefs() throws Exception {
  Instant base = Instant.parse("2026-09-10T00:00:00Z");
  CLOCK.now = base;
  // demo 与 ping 都来自存活 worker(@BeforeEach 里的 w-demo 是 BASE 时刻,拨钟后已 stale,须在此重播种)
  jdbc.update("INSERT INTO worker(id, refs, last_seen, status) "
      + "VALUES('w-demo','demo',?,'ALIVE')", java.sql.Timestamp.from(base));
  jdbc.update("INSERT INTO worker(id, refs, last_seen, status) "
      + "VALUES('w-ping','ping',?,'ALIVE')", java.sql.Timestamp.from(base));
  mvc.perform(get("/api/v1/handlers"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$[*]", hasItem("demo")))    // 存活 worker
      .andExpect(jsonPath("$[*]", hasItem("ping")));   // 存活 worker
}
```

Update the two M6.2 validation tests that move the clock but only reference `ping`/`p-ing`: they are already correct and must NOT gain a `demo` dependency — leave `createTask_liveWorkerRef_isAccepted`, `createTask_staleWorkerRef_isRejected`, `createTask_orphanHandlerRef_returnsBadRequest`, `update_orphanHandlerRef_returnsBadRequest` as-is (they use their own inserted workers; `postTask(...)` in `update_orphanHandlerRef` runs at `BASE` where the seeded `w-demo` is fresh).

- [ ] **Step 4: De-wire the individual `workOne()` call sites that touch the requeue shard.** Leave these four validation tests untouched; edit exactly two tests that still drive `executorWorker`:

- `dlqGetAndRequeue_roundTrip` (lines 482-521): it needs no worker to prove the control-plane requeue path. Replace only the trailing block (lines 516-520) that does `executorWorker.workOne()` to flip the requeued shard to SUCCESS with a direct write documenting the simulated external worker:
  ```java
  // 外部 worker 认领并跑成功(server 无执行器;requeue 控制面已在上方断言 DUE + 不再出现在 /dlq)
  jdbc.update("UPDATE execution_shard SET status='SUCCESS', worker_id='w-demo' WHERE id=?", shardId);
  ```
- `leadershipHeldScanOnce_thenWorkOne_reachesSuccess` (lines 383-409): **repurpose** as the canonical spec-§5 server assertion. Rename to `leadershipHeldScanOnce_shardStaysDue_absentWorker`, keep the `triggerEngine.scanOnce()` head (parent DUE + shard DUE + GET /executions shows DUE), then replace the `executorWorker.workOne()` + SUCCESS assertions with the negative: after `reconciler.scanOnce()` the shard is STILL DUE (no worker present to run it):
  ```java
  boolean processed = false; // 无执行器:不再有 workOne
  reconciler.scanOnce();
  assertEquals("DUE", shardStatus(parentId), "无 worker → shard 保持 DUE(汇聚不改变未执行分片)");
  assertEquals("DUE", parentStatus(parentId), "非全部终态 → 父仍 DUE");
  mvc.perform(get("/api/v1/executions").param("taskId", String.valueOf(id)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$[0].status").value("DUE"));
  ```

- [ ] **Step 5: Delete the remaining execution-plane E2E methods** from `ApiIntegrationTest`. Each deleted behavior already lives worker-side (column 2) or is re-added in Step 6 (fail-fast, backoff-gate); the DLQ/requeue *endpoint* path that still needs control-plane coverage is preserved by Step 4's edits to `dlqGetAndRequeue_roundTrip` and the untouched `requeueNonFailed_returnsConflict` / `requeueMissingShard_returnsNotFound`:

| Server test to delete | Worker replacement already present |
|---|---|
| `retryAfterFailure_thenBackoffGate_thenRerunReachesSuccess` | retry scheduling = `ExecutorWorkerTest.handlerThrowsRetryable_schedulesShardRetry`; the **backoff gate half** (lines 583-586) is re-added in Step 6 |
| `exhaustedFailure_landsInDlq_thenRequeueRunsSucceeds` | `ExecutorWorkerTest.handlerThrowsExhausted_failsAndDeadLetters_shard_noDueOutcome` + `dlqGetAndRequeue_roundTrip` (kept, edited) |
| `workerReclaimsCancelRequestedDue_asCanceled` | `ExecutorWorkerTest.cancelRequestedBeforeRun_marksShardCanceled_handlerNeverCalled` |
| `craftShardFanOut_allSuccess_parentSuccess` | `ExecutorWorkerTest.handlerReceivesShardIndexAndCount` + `scheduler-server/.../ReconcilerTest` (parent aggregation) |
| `failFast_endToEnd_oneShardFails_cancelsSiblings_parentFailed` | re-added worker-side in Step 6 |

- [ ] **Step 6: Migrate the four DAG e2e methods** (they interleave `executorWorker.workOne()` with `reconciler.scanOnce()/dagEngine.scanOnce()` and assert control-plane DAG facts — spawn, downstream-SKIPPED, run derivation, rerun-new-execution — that must survive). Replace every `executorWorker.workOne()` call with a single new helper that simulates an external worker driving that node's CURRENT execution shards to a terminal state; the scan sequence and ALL API assertions stay byte-for-byte. Add the helper (next to `dagNodeId`, ~line 987):

```java
/** M6.3:模拟外部 worker 完成某节点本次 execution 的全部分片(server 不再内嵌执行器)。
 *  status 传入 'SUCCESS' 或对失败路径 'FAILED' + dead_letter=true(cancel_siblings 不受影响)。
 *  对 rerun 用例,读的是该节点最新的 execution_id(即重跑新建的那条)。 */
private void completeNodeShards(long runId, String nodeKey, String status) throws Exception {
  long execId = dagNodeExecutionId(runId, nodeKey);
  if ("FAILED".equals(status)) {
    jdbc.update("UPDATE execution_shard SET status='FAILED', dead_letter=true WHERE execution_id=?", execId);
  } else {
    jdbc.update("UPDATE execution_shard SET status=?, worker_id='w-demo' WHERE execution_id=?", status, execId);
  }
}
```

Apply per test (keep every other line; swap only the `workOne()` calls):
- `dagLinear_e2e_runsToSuccess`: the two `assertTrue(executorWorker.workOne(), ...)` (lines 771, 775) → `completeNodeShards(runId, "A", "SUCCESS")` and `completeNodeShards(runId, "B", "SUCCESS")`.
- `dagLinear_failedUpstream_skipsDownstream_failed`: `assertTrue(executorWorker.workOne(), ...)` (line 801) → `completeNodeShards(runId, "A", "FAILED")`.
- `dagNodeRerun_terminalRestartsAndDerivesAgain`: the four `assertTrue(executorWorker.workOne())` (lines 850, 854, 873) → `completeNodeShards(runId, "A", "SUCCESS")`, `completeNodeShards(runId, "B", "SUCCESS")`, and (after rerun, on the NEW execution) `completeNodeShards(runId, "A", "SUCCESS")`.
- `dagNodeRerun_failedNodeAllowed`: `assertTrue(executorWorker.workOne())` (line 909) → `completeNodeShards(runId, "A", "FAILED")`.
- `dagCancel_cascadesToNodesAndExecution` and `dagNodeRerun_nonTerminal_rejected409` and `dag_cycleCreation_rejected400` call NO `workOne()` — leave untouched.

Do not weaken any remaining control-plane assertion (node spawn PENDING→RUNNING, downstream SKIPPED-on-failure, run terminal derivation, rerun execution_id ≠ prior).

- [ ] **Step 7: Run the server module tests** to confirm the migration is green.

Run: `mvn -o -pl scheduler-server -am test`
Expected: BUILD SUCCESS, 0 failures. Total server tests drops from 58 (execution E2E deleted); `grep -n "worker\\.execute\\|FlakyHandler\\|executorWorker" scheduler-server/src/test` returns **only** Step-4/lib cases that use the string `w-demo`/`completeNodeShards` — no active `workOne()` anywhere.

- [ ] **Step 8: Add the two worker-side tests** to `ExecutorWorkerTest` (below the existing exhausted test), preserving behavior that only lived in the deleted server E2E.

**Fail-fast sibling cancel** (replaces `failFast_endToEnd_...`):

```java
/** M6.3 relocate:3-shard、首个被认领的 shard 重试耗尽(FAILED+死信)时,FailureResolver.cancelSiblings
 *  把其余 DUE 兄弟直编 CANCELED —— 原 server ApiIntegrationTest.failFast_endToEnd_... 的执行语义搬至此。 */
@Test void exhaustedShard_cancelSiblingDues() {
  var rec = new RecordingHandler(true);
  var registry = new MapHandlerRegistry(List.of(() -> rec));
  long taskId = createTask("rec", 0, 1000, null, 3, 3); // shardCount=3, maxRetries=0 → 耗尽
  long parentId = seedParentAndShards(taskId, 3);

  boolean processed = worker(registry).workOne(); // 认领 id 最小的一枚 → 耗尽 FAILED + DLQ → FAIL_FAST

  assertTrue(processed);
  var ss = shards.findShards(parentId);
  assertEquals(1, ss.stream().filter(s -> s.status() == ExecutionStatus.FAILED).count(), "恰好一条 FAILED");
  assertEquals(2, ss.stream().filter(s -> s.status() == ExecutionStatus.CANCELED).count(),
      "FAIL_FAST:其余 DUE 兄弟被直编 CANCELED");
  assertEquals(Boolean.TRUE, jdbc.queryForObject(
      "SELECT dead_letter FROM execution_shard WHERE id=? AND status='FAILED'",
      Boolean.class, ss.get(0).id()), "耗尽 shard 置死信");
}
```

**Backoff gate** (replaces the gate assertions the server's retry test deleted, lines 583-586 — a shard whose `next_retry_at` is in the future must NOT be re-claimed, and must stay DUE with a `DUE` outcome recorded):

```java
/** M6.3 relocate:重试调度后 next_retry_at 落未来闸,findCandidate 不得再认领;拨回过去闸才放行。
 *  原 server ApiIntegrationTest.retryAfterFailure_thenBackoffGate_thenRerunReachesSuccess 的闸门语义。
 *  用一次性失败 handler(镜像 server 的 FlakyHandler:首调抛错、次调成功)——RecordingHandler(fail=true)
 *  恒抛,放行后再调度会耗尽而非成功,故不用它。 */
@Test void futureBackoffGate_blocksReclaim_thenPastGate_reruns() {
  final int[] calls = {0};
  var oneShot = new ExecutionHandler() {
    boolean failNext = true;
    @Override public String ref() { return "rec"; }
    @Override public void handle(HandlerContext ctx) {
      calls[0]++;
      if (failNext) { failNext = false; throw new RuntimeException("gate fail"); } // 仅首调抛错
    }
  };
  var registry = new MapHandlerRegistry(List.of(() -> oneShot));
  long taskId = createTask("rec", 1, 1000, null, 1, 1); // maxRetries=1 → 可重试一次
  long parentId = seedParentAndShards(taskId, 1);
  long shardId = shard(parentId, 0).id();

  assertTrue(worker(registry).workOne(), "首调认领并重试调度(落到 next_retry_at)");
  Shard s = shards.findShard(shardId).orElseThrow();
  assertEquals(ExecutionStatus.DUE, s.status(), "可重试 → 回 DUE");
  assertNotNull(s.nextRetryAt(), "重试必须写 next_retry_at");
  assertTrue(s.nextRetryAt().isAfter(CLOCK.instant()), "next_retry_at 在注入时钟未来");

  jdbc.update("UPDATE execution_shard SET next_retry_at = ? WHERE id=?",
      java.sql.Timestamp.from(CLOCK.instant().plusSeconds(3600)), shardId); // 拨到更远的未来
  assertFalse(worker(registry).workOne(), "未来闸 → 候选被排除,不认领");
  assertEquals(ExecutionStatus.DUE, shards.findShard(shardId).orElseThrow().status(), "仍 DUE,未被重认领");

  jdbc.update("UPDATE execution_shard SET next_retry_at = ? WHERE id=?",
      java.sql.Timestamp.from(CLOCK.instant().minusSeconds(1)), shardId); // 拨回过去闸
  assertTrue(worker(registry).workOne(), "过去闸 → 放行,次调成功");
  assertEquals(ExecutionStatus.SUCCESS, shards.findShard(shardId).orElseThrow().status(),
      "calls=2:handler 次调成功 → SUCCESS");
  assertEquals(2, calls[0], "handler 恰被调度两次(首败重试 + 放行后成功)");
}
```

- [ ] **Step 9: Run worker module tests.**

Run: `mvn -o -pl scheduler-worker -am test`
Expected: BUILD SUCCESS, 30 + 1 **new fail-fast** + 1 **new backoff gate** = **32** tests, 0 failures. Verify `RecordingHandler`'s `fail=true` throws a plain `RuntimeException` (it does — line 42 `throw new RuntimeException("boom")`), which binds to the retry path, not `CancellationException`.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "test: migrate server execution E2E off in-process executor; relocate retry-gate + fail-fast to worker

Server ApiIntegrationTest is now control-plane-only per spec §5 (trigger→shard stays DUE, witness
the single live-worker seed). Deleted execution ime E2E is preserved worker-side: retry scheduling
already present, backoff-gate and fail-fast sibling-cancel added in ExecutorWorkerTest so no behavior drops.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

```bash
git add -A
git commit -m "test: migrate server execution E2E off in-process executor; add worker fail-fast sibling-cancel test

Server ApiIntegrationTest no longer drives ExecutorWorker/FlakyHandler (control-plane-only per
spec §5); execution-plane retry/DLQ/cancel/fanout coverage already present worker-side, fail-fast
re-added in ExecutorWorkerTest so no behavior is dropped.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---
## Task 3: De-wire server — remove worker dependency, executor/handler beans, collapse `AvailableHandlerRefs`

**Files:**
- Modify: `scheduler-server/pom.xml`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/AvailableHandlerRefs.java`

**Interfaces:**
- Consumes: T1 relocated `dev.scheduler.persistence.retry.*` imports in `Beans`; T2 stripped the server tests' references to the in-process executor/handler beans. `AvailableHandlerRefs` ctor param list changes to `(WorkerRepository, Clock)` — only `Beans` constructs it.
- Produces: server `pom.xml` with **no** `scheduler-worker` dependency; `Beans.java` with no `dev.scheduler.worker.*` import and no `demoHandler`/`handlerRegistry`/`executorWorker`/`workLoop`/`schedulerWorkerId` beans; `AvailableHandlerRefs.refs()` = pure live-worker union.

- [ ] **Step 1: Remove the `scheduler-worker` dependency from `scheduler-server/pom.xml`.**

Delete the `<dependency>` block for `dev.scheduler:scheduler-worker` (the M6.1 temporary one-way server→worker dep). Expected count post-edit: server depends on persistence + spring-starters only.

- [ ] **Step 2: Remove executor/handler beans and imports from `Beans.java`.**

Delete imports:
```java
import dev.scheduler.worker.execute.ExecutorWorker;
import dev.scheduler.worker.handler.DemoHandler;
import dev.scheduler.worker.handler.ExecutionHandler;
import dev.scheduler.worker.handler.HandlerRegistry;
import dev.scheduler.worker.handler.MapHandlerRegistry;
```
(Keep `import dev.scheduler.persistence.retry.FailureResolver;` and `import dev.scheduler.persistence.retry.RetryPolicy;` from T1.) Also remove now-unused imports that only those beans used: `java.util.function.Supplier` (only `handlerRegistry` used it) and `java.net.InetAddress` (only `defaultWorkerId` used it).

Delete the following bean methods (and inner class + helper), unchanged logic removed:
- `demoHandler()`
- `handlerRegistry(List<ExecutionHandler>)`
- `String schedulerWorkerId(@Value("${scheduler.worker-id:}") ...)` and its private helper `defaultWorkerId()`
- `executorWorker(...)`
- the `public static final class WorkLoop` inner class and the `workLoop(ExecutorWorker)` bean method

Update the `availableHandlerRefs` bean to drop `handlerRegistry`:
```java
/** M6.3:控制面校验的可用 handlerRef = 存活 worker 注册表 refs 并集(纯 worker 视角);
 *  TaskController 建/改 + handlers 下拉框同源。 */
@Bean
AvailableHandlerRefs availableHandlerRefs(WorkerRepository workerRepository, Clock clock) {
  return new AvailableHandlerRefs(workerRepository, clock);
}
```

Keep `retryPolicy()` and `failureResolver(...)` beans (server `Reconciler` needs them — they now construct `dev.scheduler.persistence.retry.*`). Keep `reconciler(...)`, `scanLoop(...)`, `reconcileLoop(...)`, `dagLoop(...)` and their inner loop classes — those are control-plane and stay. Keep `SchedulerApplication`/`@EnableScheduling` (scan/reconcile/dag loops still scheduled; there is simply no work loop). When the last usage of the `@Value("${scheduler.worker-id:}")` annotation disappears, leave `scheduler-server/src/main/resources/application.yml`'s `worker-id:` key in place (harmless override; removed only if the implementer confirms nothing consumes it).

- [ ] **Step 3: Collapse `AvailableHandlerRefs` to pure live-worker.**

Rewrite `scheduler-server/src/main/java/dev/scheduler/server/web/AvailableHandlerRefs.java`:

```java
package dev.scheduler.server.web;

import dev.scheduler.persistence.WorkerRegistration;
import dev.scheduler.persistence.WorkerRepository;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/** M6.3:控制面可校验的 handlerRef = 存活 worker 注册表 refs 并集(仅采信 last_seen 新鲜的 worker)。
 *  进程内不再有任何 handler,故此处坍缩为纯存活-worker 视角,恰合 spec §1/§3。 */
public class AvailableHandlerRefs {
  private static final Duration LIVE_WINDOW = Duration.ofSeconds(30);

  private final WorkerRepository workers;
  private final Clock clock;

  public AvailableHandlerRefs(WorkerRepository workers, Clock clock) {
    this.workers = workers;
    this.clock = clock;
  }

  /** 存活 worker 的 ref 并集。 */
  public Set<String> refs() {
    Set<String> all = new HashSet<>();
    for (WorkerRegistration w : workers.findAllAlive(clock.instant().minus(LIVE_WINDOW))) {
      all.addAll(w.refs());
    }
    return all;
  }
}
```
Remove the `import dev.scheduler.worker.handler.HandlerRegistry;` and the now-unused `inProcess` field. `TaskController`/`HandlerController` call sites are unchanged (they inject `AvailableHandlerRefs`).

- [ ] **Step 4: Verify server compiles and its (already-migrated) tests pass, with no worker dep.**

Run: `mvn -o -pl scheduler-server -am test`
Expected: BUILD SUCCESS, 0 failures. Confirm `mvn -o -pl scheduler-server -am test` resolves nothing from `dev.scheduler.worker` — `grep -rn "dev.scheduler.worker" scheduler-server/src` must return **empty**.

- [ ] **Step 5: Verify the full reactor is green.**

Run: `mvn -o test`
Expected: BUILD SUCCESS. Counts: core 6 / persistence 75 / worker **32** / server = whatever T2's migration settled on (expected ≈53: T2 deletes exactly five execution-E2E methods — `retryAfterFailure…`, `exhaustedFailure…`, `workerReclaimsCancelRequestedDue…`, `craftShardFanOut…`, `failFast_endToEnd…` — and renames `leadershipHeld…`, net −5 from 58). Matches T1 baseline plus T2's two new worker tests (+2).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(server): de-wire execution plane — drop scheduler-worker dep, remove in-process executor/handler beans, AvailableHandlerRefs collapses to pure live-worker union

Server context now has no ExecutionHandler/ExecutorWorker; validation + handlers dropdown read only
the shared DB's live-worker registry (spec §1/§6.M6.3). Reconciler keeps retry policy via
dev.scheduler.persistence.retry.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---
## Task 4: Authoritative full-reactor + README + whole-branch review prep

**Files:**
- Modify: `README.md` (only if it still implies the server runs a handler/executor)

- [ ] **Step 1: Run the authoritative full reactor.**

Run: `mvn -o test` and record the per-module totals and 0-failure result. Verify `grep -rn "dev.scheduler.worker" scheduler-server/src` still empty.

- [ ] **Step 2: Live smoke (best-effort, per M6.2 pattern): boot the worker, confirm the server sees its ref.**

1. Start a worker (`cd scheduler-worker && mvn -o spring-boot:run` or the runnable jar) on 8081 — confirm `/actuator/health` → `{"status":"UP"}` and a `worker@<hostname>` row is registered in PG.
2. `GET /api/v1/handlers` against the running server → the worker's registered ref appears (e.g. `demo`), proving the collapse to live-worker union is observable end-to-end. If the reactor was green and the DB heartbeats, this confirms M6.3's remote-execution read path. Kill the worker afterward; record the result.

- [ ] **Step 3: README honesty check.** If `README.md` anywhere still describes the scheduler-server as running handlers in-process, update the module table line for `scheduler-server` to "控制面:web/trigger/leader/reconcile/dag + 存活-worker 校验;不执行业务 handler". Leave the「独立执行 worker」section as-is (it already documents the worker's real health endpoint).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs(m6.3)+verify: full reactor green; server control-plane-only README; live-worker ref visible via /handlers

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

- [ ] **Step 5: Report for final whole-branch review (SDD)** — the SDD controller dispatches the opus whole-branch review on `feat/m6-distributed-worker` (T1..T4), expecting the M6.2 seam note (in-process ExecutorWorker stealing worker-only shards) to be resolved by the de-wiring.