# M6.2 服务端校验 + fail-fast Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the "拼错/孤儿 handlerRef" hole from the control-plane side — creating/updating a task whose `handlerRef` is served by no live worker (or in-process handler) is rejected 400 at the entry, and a worker startup with duplicate handler refs fails loudly instead of silently overriding.

**Architecture:** Three independent changes, all in the wake of M6.1's control/execution split. (a) `MapHandlerRegistry` becomes fail-fast on duplicate `ref()` — shared class in the worker module. (b) The worker process hardens (P1.1 error-guarded claim+heartbeat loops), gains a real HTTP `/actuator/health` (P1.2, add `spring-boot-starter-web`), and a stable `worker@<hostname>` id (P2.1). (c) The server validates `handlerRef` on create/update against a `AvailableHandlerRefs` union = server in-process `HandlerRegistry.refs()` ∪ live-worker-table refs (`findAllAlive(now-30s)`), and `/api/v1/handlers` serves the same union.

**Tech Stack:** Spring Boot 3.2.5, Maven multi-module (core/persistence/server + worker), Testcontainers for integration tests.

**Spec:** `docs/superpowers/specs/2026-09-10-scheduler-m6-distributed-worker-design.md` (§3, §6.M6.2, §8 items 4/5), plus final-review M6.1 P1.1 / P1.2 / P2.1 rulings recorded in `.superpowers/sdd/2026-09-10-scheduler-m6-1-worker-module/progress.md`.

## Global Constraints

- One-way Maven dep: server→worker (temporary, removed M6.3); worker NEVER depends on server or imports `dev.scheduler.server.*`.
- Migrations single-owner in scheduler-persistence; server runs Flyway, worker `spring.flyway.enabled=false`.
- Worker flies no HTTP today; M6.2 adds `spring-boot-starter-web` so `/actuator/health` (and actuator/prometheus) become real on `server.port=8081`.
- Transitional M6.2 reality: the server STILL runs an in-process `ExecutorWorker` + in-process `HandlerRegistry` (removed M6.3). Therefore validation must accept BOTH in-process refs AND live-worker refs — never strictly live-workers-only (that would break every existing `demo`/`flaky` task test). M6.3 collapses the in-process leg.
- Worker default id must differ from server's `defaultWorkerId()` (`<os>:<hostname>`) to avoid a worker↔control-plane registry collision; use `worker@<hostname>`.
- All tests must stay green on the full reactor (`mvn -o -pl scheduler-server -am test` and finally `mvn -o test`).

---

### Task 1: `MapHandlerRegistry` fail-fast on duplicate ref

**Files:**
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/handler/MapHandlerRegistry.java`
- Modify: `scheduler-worker/src/test/java/dev/scheduler/worker/handler/MapHandlerRegistryTest.java`

**Interfaces:**
- Consumes: existing `ExecutionHandler`, `HandlerRegistry` interface (worker module, migrated M6.1).
- Produces: same constructor `MapHandlerRegistry(List<Supplier<ExecutionHandler>>)`, now THROWS `IllegalStateException` on duplicate `ExecutionHandler.ref()` instead of silently keeping the first.

- [ ] **Step 1: Write the failing test** — add to `MapHandlerRegistryTest`:

```java
@Test
void duplicateRef_failsFast() {
  assertThrows(IllegalStateException.class, () -> new MapHandlerRegistry(List.of(
      (Supplier<ExecutionHandler>) DemoHandler::new,
      (Supplier<ExecutionHandler>) DemoHandler::new)));
}
```

(Add imports: `java.util.function.Supplier` if missing; the test class already imports `DemoHandler`/`MapHandlerRegistry` and static `assertThrows` — add if absent.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-worker test`
Expected: FAIL — current `toMap((a,b)->a)` keeps the first silently; `assertThrows` sees no exception.

- [ ] **Step 3: Implement fail-fast** — replace the collect in `MapHandlerRegistry`:

```java
public MapHandlerRegistry(List<Supplier<ExecutionHandler>> suppliers) {
  Map<String, ExecutionHandler> built = new HashMap<>();
  for (ExecutionHandler h : suppliers.stream().map(Supplier::get).toList()) {
    if (built.putIfAbsent(h.ref(), h) != null) {
      throw new IllegalStateException("duplicate handler ref: " + h.ref());
    }
  }
  handlers = built;
}
```

Keep the rest of the class unchanged (`get`, `refs`). Adjust imports: `Map`/`HashMap` needed; `Collectors` no longer needed — remove it. Keep `Supplier`, `List`.

- [ ] **Step 4: Run to verify it passes**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-worker test`
Expected: PASS (whole worker module; the existing `mapHandlerRegistry_refs_are_sorted` and `get_throws_on_unknown` tests still pass — distinct refs unaffected).

- [ ] **Step 5: Commit**

```bash
git add scheduler-worker/src/main/java/dev/scheduler/worker/handler/MapHandlerRegistry.java scheduler-worker/src/test/java/dev/scheduler/worker/handler/MapHandlerRegistryTest.java
git commit -m "refactor(worker): MapHandlerRegistry fail-fast——重复 handler ref 启动即抛错,取代静默覆盖

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: Worker hardening — error-guarded loops (P1.1) + web starter (P1.2) + stable workerId (P2.1)

**Files:**
- Modify: `scheduler-worker/pom.xml` — add `spring-boot-starter-web` (compile scope).
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java` — error-guard both loops; stable default worker id.
- Modify: `README.md` — the「独立执行 worker」 section: worker now exposes real HTTP `/actuator/health` on 8081; drop the "no HTTP / no probeable health" claims.

**Interfaces:**
- Consumes: existing `WorkerConfig` (M6.1), the server's `Beans.WorkLoop` guard pattern to mirror, `spring-boot-starter-web`.
- Produces: worker loops that survive a transient JDBC error (log-warn, keep ticking); worker id default `worker@<hostname>` (stable, overridable via `SCHEDULER_WORKER_ID`); worker reachable at `GET http://localhost:8081/actuator/health` → `{"status":"UP"}`.

- [ ] **Step 1: Add `spring-boot-starter-web` to worker pom**

Add alongside the existing `spring-boot-starter-jdbc` (in the compile-scope group):

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-web</artifactId>
</dependency>
```

(`spring-boot-starter-actuator` + `micrometer-registry-prometheus` are already present; the existing `application.yml` already has `server.port: 8081` and `management.endpoints.web.exposure.include: health,info,prometheus` — with a web starter those become real. No yml change needed.)

- [ ] **Step 2: Verify the build still compiles with web starter**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-worker -am test-compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Add error guards + stable id to `WorkerConfig`**

Mirror the server's `Beans.WorkLoop` guard exactly (its `try/catch(Throwable) log.warn` shape) for BOTH loops, and replace the random uuid default:

```java
private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(WorkerConfig.class);
```

WorkLoop body → guard `worker.workOne()`:
```java
@Scheduled(fixedDelayString = "${scheduler.loop.work-delay-ms:100}")
public void tick() {
  try {
    worker.workOne();
  } catch (Throwable t) {
    log.warn("worker claim loop tick failed; continuing next tick", t);
  }
}
```

HeartbeatLoop body → guard `registrar.heartbeat()`:
```java
@Scheduled(fixedDelayString = "${scheduler.heartbeat.interval-ms:10000}")
public void tick() {
  try {
    registrar.heartbeat();
  } catch (Throwable t) {
    log.warn("worker heartbeat loop tick failed; continuing next tick", t);
  }
}
```

Replace the `schedulerWorkerId` bean default (uuid → hostname-derived, DISTINCT from server's `<os>:<hostname>`):
```java
@Bean
String schedulerWorkerId(@Value("${scheduler.worker-id:}") String configured) {
  return configured.isBlank() ? defaultWorkerId() : configured;
}

/** 稳定 worker 标识,进程重启后不换 id(在途租约可被接续;避免累积陈旧 ALIVE 行)。与 server 的
 *  defaultWorkerId(<os>:<hostname>) 不同前缀,注册表不冲突。 */
private static String defaultWorkerId() {
  try {
    return "worker@" + java.net.InetAddress.getLocalHost().getHostName();
  } catch (Throwable t) {
    log.warn("could not resolve hostname for worker-id, falling back to uuid", t);
    return "worker@" + java.util.UUID.randomUUID();
  }
}
```

The existing `java.util.UUID` import stays (used in the fallback). Import `java.net.InetAddress` (fully-qualified usage avoids an import — use the FQN above as written).

- [ ] **Step 4: Verify worker module still green + server reactor green (Fail-fast + guards compose)**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test`
Expected: BUILD SUCCESS (server 54, worker 27 — MapHandlerRegistry fail-fast from Task 1 must NOT break ApiIntegrationTest's context (demo+flaky distinct refs)).

- [ ] **Step 5: Update README**

In the 「独立执行 worker」 section, replace the "no HTTP / no probeable health / 8081 reserved" sentences: the worker now exposes real `GET /actuator/health` (and actuator metrics) on `server.port=8081`. Keep the DB-heartbeat-liveness contract as the primary liveness source; note HTTP health is additionally available. Remove "worker 无 HTTP health 端点可探测" and the "8081 仅预留" framing.

- [ ] **Step 6: Commit**

```bash
git add scheduler-worker/pom.xml scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java README.md
git commit -m "feat(worker): 心跳/认领 loop 错误护栏 + web starter 暴露 /actuator/health + 稳定 worker@hostname id

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: Server-side handlerRef validation + live-refs union view

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/web/AvailableHandlerRefs.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java` — add `AvailableHandlerRefs` bean.
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java` — validate `handlerRef` on create+update.
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/HandlerController.java` — serve the union instead of in-process `registry.refs()`.
- Modify: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java` — add validation tests + update handlers-endpoint assertion.

**Interfaces:**
- Consumes: `WorkerRepository` (bean, M6.1/Beans), server's in-process `HandlerRegistry` bean, `Clock` bean, `dev.scheduler.persistence.WorkerRegistration`.
- Produces:
  - `class AvailableHandlerRefs(HandlerRegistry inProcess, WorkerRepository workers, Clock clock)` with `java.util.Set<String> refs()` = union of `inProcess.refs()` and, for every `WorkerRegistration` with `last_seen >= clock.instant() - LIVE_WINDOW` (30s) and status ALIVE (already filtered by `findAllAlive`), that worker's `refs`.
  - `TaskController` create/update: after the existing non-blank `handlerRef` check, reject if not in `available.refs()` → `throw new IllegalArgumentException("handler ref 'X' served by no live worker")` (maps to 400 via `ApiExceptionHandler`).
  - `HandlerController.list()` returns a sorted `List<String>` of `available.refs()` (form dropdown stays consistent with what create/update will accept).

- [ ] **Step 1: Write failing tests first** — add to `ApiIntegrationTest`:

```java
/** M6.2:入口即拒绝孤儿 ref——无存活 worker、非进程内 handler 的 handlerRef 建不进去。 */
@Test
void createTask_orphanHandlerRef_returnsBadRequest() throws Exception {
  String body = "{\"name\":\"orphan-task\",\"kind\":\"cron\",\"handlerRef\":\"no-such-handler\","
      + "\"cron\":\"" + CRON + "\"}";
  mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
      .andExpect(status().isBadRequest())
      .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("no-such-handler")));
}

/** M6.2:注册到 worker 表且有新鲜 last_seen 的 worker 的 ref,是合法可创建目标(控制面只读 DB 注册视图)。 */
@Test
void createTask_liveWorkerRef_isAccepted() throws Exception {
  Instant base = Instant.parse("2026-09-10T00:00:00Z");
  CLOCK.now = base;
  jdbc.update("INSERT INTO worker(id, refs, last_seen, status) "
      + "VALUES('w-ping','ping',?,'ALIVE')", java.sql.Timestamp.from(base));
  String body = "{\"name\":\"live-ref-task\",\"kind\":\"cron\",\"handlerRef\":\"ping\","
      + "\"cron\":\"" + CRON + "\",\"shardCount\":1}";
  mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
      .andExpect(status().isCreated());
}

/** M6.2:同源 stale(>30s)worker 的 ref 不算存活,建任务仍被拒(校验只采信新鲜注册)。 */
@Test
void createTask_staleWorkerRef_isRejected() throws Exception {
  Instant base = Instant.parse("2026-09-10T00:00:00Z");
  CLOCK.now = base;
  jdbc.update("INSERT INTO worker(id, refs, last_seen, status) "
      + "VALUES('w-stale','ping',?,'ALIVE')", java.sql.Timestamp.from(base.minusSeconds(60)));
  String body = "{\"name\":\"stale-ref-task\",\"kind\":\"cron\",\"handlerRef\":\"ping\","
      + "\"cron\":\"" + CRON + "\",\"shardCount\":1}";
  mvc.perform(post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
      .andExpect(status().isBadRequest());
}

/** M6.2:update 到孤儿 ref 同样 400(与 create 同一校验路径)。 */
@Test
void update_orphanHandlerRef_returnsBadRequest() throws Exception {
  long id = postTask("update-orphan"); // 先建一个合法任务(process-in ref demo)
  mvc.perform(put("/api/v1/tasks/" + id).contentType(MediaType.APPLICATION_JSON)
          .content("{\"name\":\"still\",\"kind\":\"cron\",\"handlerRef\":\"no-such-handler\","
              + "\"cron\":\"0 */6 * * * *\",\"shardCount\":1}"))
      .andExpect(status().isBadRequest());
}
```

Also update the existing `handlersEndpoint_listsRegisteredRefs` to assert the live-worker leg:
```java
/** 表单下拉框数据源 = 进程内 ∪ 存活 worker 并集(与 create/update 校验同源)。 */
@Test
void handlersEndpoint_listsRegisteredRefs() throws Exception {
  Instant base = Instant.parse("2026-09-10T00:00:00Z");
  CLOCK.now = base;
  jdbc.update("INSERT INTO worker(id, refs, last_seen, status) "
      + "VALUES('w-ping','ping',?,'ALIVE')", java.sql.Timestamp.from(base));
  mvc.perform(get("/api/v1/handlers"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$[*]", hasItem("demo")))    // 进程内
      .andExpect(jsonPath("$[*]", hasItem("ping")));   // 存活 worker
}
```

(Reset `CLOCK.now` in `@BeforeEach` already pins BASE; these tests set it then let `@BeforeEach` reset. The test class already imports `Instant`/`List`/`hasItem`/`containsString` — only add imports you need.)

- [ ] **Step 2: Run to verify new tests fail**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test -Dtest=ApiIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `createTask_orphanHandlerRef_returnsBadRequest`, `createTask_staleWorkerRef_isRejected`, `update_orphanHandlerRef_returnsBadRequest` FAIL (no validation yet); `createTask_liveWorkerRef_isAccepted` and the handlers-endpoint `ping` assertion FAIL (validation/wiring absent). Existing tests unaffected.

- [ ] **Step 3: Implement `AvailableHandlerRefs` + wire it**

Create `scheduler-server/src/main/java/dev/scheduler/server/web/AvailableHandlerRefs.java`:

```java
package dev.scheduler.server.web;

import dev.scheduler.persistence.WorkerRegistration;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.worker.handler.HandlerRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/** M6.2:控制面可校验的 handlerRef 并集 = 进程内 registry.refs() ∪ 存活 worker 表 refs。
 *  过渡期(server 仍内嵌跑 executor,直到 M6.3):进程内 ref 仍合法;M6.3 移除内嵌后本类坍缩为纯
 *  存活-worker 视角,恰合 spec §3。存活以 last_seen 距今 ≤ LIVE_WINDOW 判定(DB 层 findAllAlive 已滤 status=ALIVE)。 */
public class AvailableHandlerRefs {
  private static final Duration LIVE_WINDOW = Duration.ofSeconds(30);

  private final HandlerRegistry inProcess;
  private final WorkerRepository workers;
  private final Clock clock;

  public AvailableHandlerRefs(HandlerRegistry inProcess, WorkerRepository workers, Clock clock) {
    this.inProcess = inProcess;
    this.workers = workers;
    this.clock = clock;
  }

  /** 进程内 ref ∪ 存活 worker ref 并集。 */
  public Set<String> refs() {
    Set<String> all = new HashSet<>(inProcess.refs());
    for (WorkerRegistration w : workers.findAllAlive(clock.instant().minus(LIVE_WINDOW))) {
      all.addAll(w.refs());
    }
    return all;
  }
}
```

Add the bean to `Beans.java` (near the `workerRepository` bean):
```java
/** M6.2:控制面校验的可用 handlerRef 并集(进程内 ∪ 存活 worker);TaskController 建/改 + handlers 下拉框同源。 */
@Bean
AvailableHandlerRefs availableHandlerRefs(HandlerRegistry handlerRegistry, WorkerRepository workerRepository, Clock clock) {
  return new AvailableHandlerRefs(handlerRegistry, workerRepository, clock);
}
```
(import `dev.scheduler.server.web.AvailableHandlerRefs` — same package as HandlerController/TaskController is `web`, Beans is `config`, so add the import.)

- [ ] **Step 4: Wire validation into `TaskController`**

Inject the seam and validate both create and update, right after the existing non-blank `handlerRef` check:

Add field + ctor param:
```java
private final AvailableHandlerRefs availableRefs;
// ctor becomes: public TaskController(TaskRepository tasks, ShardRepository shards, AvailableHandlerRefs availableRefs)
```

Add a private helper + call it in both `create` and `update` (replace the single-line blank check with a call through the helper):
```java
private void requireAvailableHandlerRef(String handlerRef) {
  if (!availableRefs.refs().contains(handlerRef)) {
    throw new IllegalArgumentException("handler ref '" + handlerRef + "' served by no live worker");
  }
}
```
In `create`, after the `handlerRef` non-blank check (line ~47) add `requireAvailableHandlerRef(req.handlerRef())`. In `update`, after its non-blank check (line ~72) add the same. Keep every other check exactly as-is.

- [ ] **Step 5: Update `HandlerController` to serve the union**

Replace field `HandlerRegistry registry` + `list()` body with:
```java
import java.util.List;
// field:
private final AvailableHandlerRefs availableRefs;
public HandlerController(AvailableHandlerRefs availableRefs) { this.availableRefs = availableRefs; }

@GetMapping
public List<String> list() {
  return availableRefs.refs().stream().sorted().toList();
}
```
(Remove the now-unused `HandlerRegistry` import from HandlerController.)

- [ ] **Step 6: Run full server suite to verify**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test`
Expected: BUILD SUCCESS — all 5 new/updated tests pass AND every existing `demo`/`flaky` handlerRef task test still passes (in-process leg keeps them valid).

- [ ] **Step 7: Commit**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/web/AvailableHandlerRefs.java scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java scheduler-server/src/main/java/dev/scheduler/server/web/HandlerController.java scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java
git commit -m "feat(server): handlerRef 入口即拒绝——孤儿/陈旧 ref 建改任务 400;handlers 下拉框改读存活并集

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: Full-reactor green + live verification of the P1.2 outcome

**Files:**
- No new code. Runs the full reactor and (best-effort) a live worker boot proving `/actuator/health` now answers on 8081.

- [ ] **Step 1: Full reactor test**

Run: `cd /d/ai-project/scheduler && mvn -o test`
Expected: BUILD SUCCESS — core 6 / persistence 75 / worker 27 / server (now +5 tests = 59). All green.

- [ ] **Step 2: Live worker health verification (best-effort)**

If a PG reachable on `localhost:5433` (as in M6.1 T6) exists, start one worker from `scheduler-worker/` (`cp-test.txt` recipe, `dev.scheduler.worker.WorkerApplication`, `DB_URL=jdbc:postgresql://localhost:5433/scheduler ...`), then:
```bash
curl -s http://localhost:8081/actuator/health
```
Expected: `{"status":"UP"}` — proving P1.2 (web starter) truly answers over HTTP, not just the DB heartbeat. Also confirm the `worker` row appears ALIVE. Then kill that worker and confirm no stray worker java process remains. If no PG on 5433, SKIP step 2 and report it (do not block).

- [ ] **Step 3: Commit nothing** (no file changes in this task beyond the prior tasks' commits — if you had to touch anything, commit it clearly and say so; otherwise report "no changes".)

- [ ] **Step 4: Append to ledger** — the controller handles ledger/cleanup; your report just needs the facts.

---

## Self-Review

**1. Spec coverage:** §3 (create/update validation → 400; MapHandlerRegistry fail-fast) ✅ Task 1 + Task 3. §6.M6.2 acceptance (orphan ref 400; dup-ref app startup failure; relevant E2E green) ✅. §8 item 4 (handlers endpoint = live-worker refs union, kept for frontend) ✅ Task 3. §8 item 5 (workerId stability) ✅ Task 2. Final-review P1.1 (loop guards) ✅ Task 2, P1.2 (web starter health) ✅ Task 2 + Task 4, P2.1 ✅ Task 2. **No spec requirement left without a task.**

**2. Placeholder scan:** every changed file has exact code in its task. No TBD/TODO. The only "when/if" is Task 4 Step 2's environment-dependent live check, which is explicitly conditional, never a silent gap.

**3. Type consistency:**
- `AvailableHandlerRefs(HandlerRegistry, WorkerRepository, Clock)` — all three beans exist in server Beans (`handlerRegistry`, `workerRepository`, `clock`); ctor args match Beans wiring order.
- `WorkerRepository.findAllAlive(Instant)` / `WorkerRegistration.refs()` — match persistence M6.1.
- `requireAvailableHandlerRef(String)` added to both create+update with identical signature.
- Fail-fast msg `"duplicate handler ref: " + h.ref()` — stable across Task 1 only (not consumed elsewhere).
- `worker@<hostname>` default id distinct from server's `<os>:<hostname>` — no registry collision.

**4. Transitional seam integrity (the core risk of this plan):** M6.2 validation is intentionally `in-process ∪ live-worker`, NOT strictly live-workers-only, because the server still runs an in-process ExecutorWorker (M6.1 reality) — strict would 400 every existing `demo`/`flaky` task. M6.3 removes the in-process leg (the `AvailableHandlerRefs.inProcess` field + the server's `HandlerRegistry`/`ExecutorWorker` beans together), collapsing to pure live-worker union per spec. The seam is a single field + bean, cleanly deletable. This is deliberate, documented in `AvailableHandlerRefs`'s javadoc, and re-checked in the M6.3 plan.