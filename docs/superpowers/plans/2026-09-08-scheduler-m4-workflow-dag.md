# Scheduler M4 — Workflow DAG (任务依赖编排) 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a workflow DAG layer that schedules registered `app_task`s in dependency order over the existing sharded execution runtime — DagEngine lazily spawns a node's sharded execution when all its upstream nodes succeed, without changing the worker or Reconciler.

**Architecture:** Three new layers on top of the merged M1–M3 runtime. (1) **Persistence** — V4 migration adds definition tables (`app_dag`/`app_dag_node`/`dag_edge`) and runtime tables (`dag_run`/`dag_run_node`/`dag_run_outcome`/`dag_run_node_outcome`), plus `DagRepository`/`JdbcDagRepository`. (2) **Engine** — `DagEngine.scanOnce()` (leader-gated) both fires due DAG cron ticks (creating a `dag_run` + PENDING `dag_run_node`s idempotently) and propagates active runs: spawns ready nodes via the existing `createParentWithShards`, derives node terminal from the spawned execution's shards (read-only), and finalizes the `dag_run` when all nodes are terminal. (3) **Control plane** — `DagController` under `/api/v1/dags` (create/list/get/pause/resume/trigger, runs list/detail, run cancel cascade) + `DagQueryService` + per-dag metrics.

**Tech Stack:** Java 21, Spring Boot + Spring JDBC (hand-written SQL, no JPA), PostgreSQL 16, Flyway (new `V4__dag.sql` only — the applied `V1/V2/V3` migrations are never touched), Testcontainers, Micrometer/prometheus.

**Spec:** `docs/superpowers/specs/2026-09-08-scheduler-m4-workflow-dag-design.md` — the plan argues from this spec; executors read both. DAG semantics (lazy spawn, creation-time cycle rejection, SKIPPED vs CANCELED, cross-cycle convergence) are fixed there and reproduced here.

## Global Constraints

- **Never edit applied migrations** `V1__schema.sql` / `V2__reliability.sql` / `V3__sharding.sql` (Flyway checksum). All schema goes in new `V4__dag.sql`.
- **Reconciler, TriggerEngine, ExecutorWorker, ExecutionController, ShardRepository, `ExecutionStatus`/`ExecutionTransitions`/`Shard` are UNCHANGED.** M4 only *reads* `execution_shard` (node terminal derivation) and *calls* the existing `createParentWithShards` / `requestCancelParent` / `cancelParentImmediate` on `ShardRepository`. No cross-write of the execution/shard tables.
- **`DagEngine` is the sole writer of the dag runtime tables** (`dag_run`, `dag_run_node`, `dag_run_outcome`, `dag_run_node_outcome`). Definition tables (`app_dag`, `app_dag_node`, `dag_edge`) are written once at DAG creation.
- **Every file you touch carries a trailing newline** (`.editorconfig` `insert_final_newline=true`). Byte-check after edits.
- **Lazy execution generation (spec §3.1):** a node becomes RUNNING only when all its direct upstreams are terminal *SUCCESS*. Upstream FAILED/SKIPPED → downstream `SKIPPED` (never spawned); upstream CANCELED (no FAILED/SKIPPED) → downstream `CANCELED`. Cross-cycle convergence: a single `scanOnce` advances at most one layer; child spawns settle on later scans.
- **Determinism in tests:** real PostgreSQL Testcontainers (`postgres:16-alpine`), fixed `Clock` (never wall-clock for lease/backoff/cron), `TRUNCATE ... RESTART IDENTITY CASCADE` in `@BeforeEach`, tests drive `scanOnce`/`workOne` synchronously; `@Scheduled` loops disabled via properties.
- **Do NOT edit `V1/V2/V3`, `Reconciler`, `TriggerEngine`, `ExecutionController` in any task.** If a task seems to need them, stop and record a `Ruling:` in the ledger instead.
- **Node state machine (`DagRunNodeStatus`):** `PENDING → RUNNING → terminal(SUCCESS|FAILED|CANCELED|SKIPPED)`. SKIPPED only reachable from PENDING (upstream failure), never after spawn. Run state machine (`DagRunStatus`): stored `PENDING → terminal(SUCCESS|FAILED|CANCELED)` only — **never stored RUNNING**; RUNNING is a read-side derivation.

---

### Task 1: Data layer — V4 schema + core enums/records + DagRepository/JdbcDagRepository

**Files:**
- Create: `scheduler-core/src/main/java/dev/scheduler/core/DagRunStatus.java`
- Create: `scheduler-core/src/main/java/dev/scheduler/core/DagRunNodeStatus.java`
- Create: `scheduler-core/src/main/java/dev/scheduler/core/DagRunNodeTransitions.java`
- Create: `scheduler-core/src/main/java/dev/scheduler/core/Dag.java`
- Create: `scheduler-core/src/main/java/dev/scheduler/core/DagNode.java`
- Create: `scheduler-core/src/main/java/dev/scheduler/core/DagEdge.java`
- Create: `scheduler-core/src/main/java/dev/scheduler/core/DagRun.java`
- Create: `scheduler-core/src/main/java/dev/scheduler/core/DagRunNode.java`
- Modify: `scheduler-core/src/main/java/dev/scheduler/core/IdempotencyKeys.java` (add 2 methods + `import java.util.UUID`)
- Create: `scheduler-persistence/src/main/resources/db/migration/V4__dag.sql`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/DagRepository.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcDagRepository.java`
- Modify: `scheduler-persistence/src/test/java/dev/scheduler/persistence/SchemaSmokeTest.java` (add V4 assertions)
- Create: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcDagRepositoryTest.java`

**Interfaces:**
- Consumes: existing `dev.scheduler.core.Execution`, `Shard`, `Task`, `IdempotencyKeys` (unchanged); Spring `JdbcTemplate` + `TransactionTemplate` patterns from `JdbcShardRepository`.
- Produces: `DagRepository` (full runtime+definition API below — Tasks 2 and 3 consume it verbatim), the V4 tables, and core enums/records that server code imports.

- [ ] **Step 1: Write the V4 migration.** Create `scheduler-persistence/src/main/resources/db/migration/V4__dag.sql` with exactly this content:

```sql
CREATE TABLE app_dag (
  id          BIGSERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  description TEXT,
  cron        TEXT,
  enabled     BOOLEAN NOT NULL DEFAULT true,
  paused      BOOLEAN NOT NULL DEFAULT false,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE app_dag_node (
  id          BIGSERIAL PRIMARY KEY,
  dag_id      BIGINT NOT NULL REFERENCES app_dag(id) ON DELETE CASCADE,
  node_key    TEXT NOT NULL,
  task_id     BIGINT NOT NULL REFERENCES app_task(id),
  sort_order  INT NOT NULL DEFAULT 0,
  UNIQUE (dag_id, node_key)
);

CREATE TABLE dag_edge (
  id          BIGSERIAL PRIMARY KEY,
  dag_id      BIGINT NOT NULL REFERENCES app_dag(id) ON DELETE CASCADE,
  from_node_id BIGINT NOT NULL REFERENCES app_dag_node(id) ON DELETE CASCADE,
  to_node_id   BIGINT NOT NULL REFERENCES app_dag_node(id) ON DELETE CASCADE,
  UNIQUE (dag_id, from_node_id, to_node_id),
  CHECK (from_node_id <> to_node_id)
);

CREATE TABLE dag_run (
  id              BIGSERIAL PRIMARY KEY,
  dag_id          BIGINT NOT NULL REFERENCES app_dag(id),
  idempotency_key TEXT NOT NULL UNIQUE,
  status          TEXT NOT NULL DEFAULT 'PENDING',
  trigger_reason  TEXT NOT NULL,
  cancel_requested BOOLEAN NOT NULL DEFAULT false,
  finished_at     TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_dag_run_dag_status ON dag_run (dag_id, status);

CREATE TABLE dag_run_node (
  id            BIGSERIAL PRIMARY KEY,
  dag_run_id    BIGINT NOT NULL REFERENCES dag_run(id) ON DELETE CASCADE,
  node_key      TEXT NOT NULL,
  task_id       BIGINT NOT NULL REFERENCES app_task(id),
  execution_id  BIGINT REFERENCES execution(id),
  status        TEXT NOT NULL DEFAULT 'PENDING',
  sort_order    INT NOT NULL DEFAULT 0,
  detail        TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at   TIMESTAMPTZ,
  UNIQUE (dag_run_id, node_key)
);
CREATE INDEX idx_dag_run_node_run_status ON dag_run_node (dag_run_id, status);

CREATE TABLE dag_run_outcome (
  id          BIGSERIAL PRIMARY KEY,
  dag_run_id  BIGINT NOT NULL REFERENCES dag_run(id) ON DELETE CASCADE,
  status      TEXT NOT NULL,
  detail      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE dag_run_node_outcome (
  id          BIGSERIAL PRIMARY KEY,
  node_id     BIGINT NOT NULL REFERENCES dag_run_node(id) ON DELETE CASCADE,
  status      TEXT NOT NULL,
  detail      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- [ ] **Step 2: Write the pyndef core files** (each one file, package `dev.scheduler.core`, trailing newline):

`DagRunStatus.java`:
```java
package dev.scheduler.core;

/** dag_run 状态:存储仅 PENDING(创建)→ 终态;RUNNING 绝不落库,是读侧派生(spec §1.3)。 */
public enum DagRunStatus {
  PENDING, SUCCESS, FAILED, CANCELED;

  public boolean isTerminal() {
    return this == SUCCESS || this == FAILED || this == CANCELED;
  }
}
```

`DagRunNodeStatus.java`:
```java
package dev.scheduler.core;

/** dag_run_node 状态:真实运行单元状态机。SKIPPED 仅从 PENDING 可达(上游失败,绝不 spawn)。 */
public enum DagRunNodeStatus {
  PENDING, RUNNING, SUCCESS, FAILED, CANCELED, SKIPPED;

  public boolean isTerminal() {
    return this == SUCCESS || this == FAILED || this == CANCELED || this == SKIPPED;
  }
}
```

`DagRunNodeTransitions.java`:
```java
package dev.scheduler.core;

public final class DagRunNodeTransitions {
  private DagRunNodeTransitions() {}

  /** 节点迁移:PENDING→RUNNING/SKIPPED/CANCELED;RUNNING→终态;终态不可逆。 */
  public static boolean canTransition(DagRunNodeStatus from, DagRunNodeStatus to) {
    return switch (from) {
      case PENDING  -> to == DagRunNodeStatus.RUNNING || to == DagRunNodeStatus.SKIPPED
                    || to == DagRunNodeStatus.CANCELED;
      case RUNNING  -> to == DagRunNodeStatus.SUCCESS || to == DagRunNodeStatus.FAILED
                    || to == DagRunNodeStatus.CANCELED;
      case SUCCESS, FAILED, CANCELED, SKIPPED -> false;
    };
  }

  /** run 迁移:仅 PENDING→终态;终态不可逆(取消是最终语义)。 */
  public static boolean canRunTransition(DagRunStatus from, DagRunStatus to) {
    return from == DagRunStatus.PENDING && to.isTerminal();
  }
}
```

`Dag.java`, `DagNode.java`, `DagEdge.java`, `DagRun.java`, `DagRunNode.java` (field names must match the V4 columns; mirror `Task`/`Execution`/`Shard` record style):
```java
package dev.scheduler.core;
import java.time.Instant;

/** DAG 定义头(spec §1.1)。 */
public record Dag(Long id, String name, String description, String cron,
    boolean enabled, boolean paused, Instant createdAt, Instant updatedAt) {}
```
```java
package dev.scheduler.core;
/** DAG 节点:引用已注册任务,不做内联定义(spec §0)。 */
public record DagNode(Long id, Long dagId, String nodeKey, Long taskId, int sortOrder) {}
```
```java
package dev.scheduler.core;
/** DAG 有向边:from_parent → to_child(spec §1.1)。 */
public record DagEdge(Long id, Long dagId, Long fromNodeId, Long toNodeId) {}
```
```java
package dev.scheduler.core;
import java.time.Instant;
/** 一次 DAG 执行头(spec §1.2)。存储 status 仅 PENDING→终态。 */
public record DagRun(Long id, Long dagId, String idempotencyKey, DagRunStatus status,
    String triggerReason, boolean cancelRequested, Instant finishedAt, Instant createdAt) {}
```
```java
package dev.scheduler.core;
import java.time.Instant;
/** 一次 DAG 执行中的单个节点实例(spec §1.2);execution_id 惰性填(spawn 后。 */
public record DagRunNode(Long id, Long dagRunId, String nodeKey, Long taskId, Long executionId,
    DagRunNodeStatus status, int sortOrder, String detail, Instant createdAt, Instant finishedAt) {}
```

`IdempotencyKeys.java` — add two methods inside the existing final class (keep `forTrigger` untouched):
```java
  /** dag 调度触发 → dag_run 全局幂等键(镜像 forTrigger 的 task+epochMs 形态)。 */
  public static String forDagTrigger(long dagId, Instant triggerAt) {
    return "dag:" + dagId + ":" + triggerAt.toEpochMilli();
  }

  /** dag 手动触发 → dag_run 全局幂等键。 */
  public static String forManualDagRun(long dagId) {
    return "dag:" + dagId + ":manual:" + UUID.randomUUID();
  }
```
Add `import java.util.UUID;` at the top of the file.

- [ ] **Step 3: Write a failing V4 schema smoke test.** In `SchemaSmokeTest.java`, add a `@Test` (mirror `shardingTablesExist`):
```java
@Test void dagTablesExist() {
  var jdbc = jdbc();
  Integer n = jdbc.queryForObject(
      "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'"
          + " AND table_name IN ('app_dag','app_dag_node','dag_edge','dag_run','dag_run_node',"
          + "                    'dag_run_outcome','dag_run_node_outcome')",
      Integer.class);
  assertEquals(7, n, "V4 must create all 7 dag tables");
}
```

- [ ] **Step 4: Run the migration tests to see the new V4 test fail** (V4 migration not written yet — actually it is; this step confirms V4 applies cleanly and the 7 tables land). Run: `mvn -q -pl scheduler-persistence test -Dtest=SchemaSmokeTest`. Expected: V4 applies, `dagTablesExist` PASS. If Flyway complains about checksums of V1/V2/V3 you edited one — restore it and do not edit migrations again.

- [ ] **Step 5: Write the `DagRepository` interface.** Create `scheduler-persistence/src/main/java/dev/scheduler/persistence/DagRepository.java` with exactly these signatures (imports: `dev.scheduler.core.*`, `java.time.Instant`, `java.util.List`, `java.util.Optional`):
```java
package dev.scheduler.persistence;

import dev.scheduler.core.Dag;
import dev.scheduler.core.DagEdge;
import dev.scheduler.core.DagNode;
import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunNodeStatus;
import dev.scheduler.core.DagRunStatus;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** 工作流 DAG 接口(spec §1)。定义表建 DAG 时写;运行表由 DagEngine 写;DagEngine 是运行表唯一写者。 */
public interface DagRepository {

  // ---- 定义 ----
  record NodeInput(String nodeKey, long taskId, int sortOrder) {}
  record EdgeInput(String from, String to) {}

  /** 建 DAG:dag + 节点 + 边同事务。校验:引用的 task 存在、nodeKey 唯一、边引用已有 nodeKey、无自环、
   *  DFS 无有向环、边唯一。任一违反抛 IllegalArgumentException(400)且整事务回滚。返回建出的 dag 头。 */
  Dag createDag(String name, String description, String cron,
                List<NodeInput> nodes, List<EdgeInput> edges);

  Optional<Dag> findDag(long id);
  List<Dag> findAllDags();
  /** enabled 且非 paused 且 cron 非空(镜像 TaskRepository.findCronEnabled)。 */
  List<Dag> findCronEnabledDags();
  List<DagNode> findNodes(long dagId);
  List<DagEdge> findEdges(long dagId);
  void setPaused(long dagId, boolean paused);

  // ---- 运行 ----
  /** 调度触发:单事务建 dag_run(PENDING)+ 全部 dag_run_node(PENDING, task_id 快照)。幂等 by key(重放自愈)。 */
  DagRun createScheduledRun(long dagId, Instant triggerAt);
  /** 手动触发:同 createScheduledRun,幂等键 = manual:{uuid}。 */
  DagRun createManualRun(long dagId);
  Optional<DagRun> findRun(long runId);
  Optional<DagRunNode> findNode(long nodeId);
  /** run 列表;dagId 空则全部,ORDER BY id DESC。 */
  List<DagRun> findRuns(Long dagId);
  /** 未终态 run(stored status='PENDING'),由 DagEngine 每周期推进。 */
  List<DagRun> findActiveRuns();
  /** 某 run 的全部节点,ORDER BY sort_order, id。 */
  List<DagRunNode> findNodesOfRun(long runId);
  /** 某 run 的非终态节点(status IN (PENDING, RUNNING)),取消级联用。 */
  List<DagRunNode> findNonTerminalNodes(long runId);

  /** 惰性 spawn:置 execution_id + RUNNING,落 node_outcome(RUNNING,'spawned')。CAS on status='PENDING':
   *  0 行=已推进 → 返回 false。 */
  boolean markNodeSpawned(long nodeId, long executionId);

  /** 节点状态迁移:read-validate 后 CAS on 读取到的当前态 + 落 node_outcome;非法迁移抛
   *  IllegalStateException;CAS 0 行=行已被他方改走 → 返回 false,不落误导性 outcome。 */
  boolean markNodeStatus(long nodeId, DagRunNodeStatus to, String detail);

  /** dag_run 终态:CAS on status='PENDING'(stored 只 PENDING→终态),落 run_outcome + finished_at。
   *  CAS 0 行=已终态 → 幂等返回 false,不落误导性 outcome。 */
  boolean finalizeRun(long runId, DagRunStatus terminal, String detail);

  /** 置 run 取消请求标志(cancel_requested=true);仅 PENDING run,静默跳过已终态。 */
  void requestCancelRun(long runId);

  /** 某 dag 下未终态 run 计数(metrics/active gauge)。 */
  long countActiveRuns(long dagId);
}
```

- [ ] **Step 6: Write the failing `JdbcDagRepositoryTest`** (mirror `JdbcShardRepositoryTest` / `ReconcilerTest` harness). Create `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcDagRepositoryTest.java` extending `AbstractPostgresTest`. `@BeforeEach` clears the dag + task + execution tables:
```java
@BeforeEach void clean() {
  jdbc.update("TRUNCATE app_dag, app_dag_node, dag_edge, dag_run, dag_run_node,"
      + " dag_run_outcome, dag_run_node_outcome, execution, execution_shard,"
      + " execution_shard_outcome, execution_outcome, app_task RESTART IDENTITY CASCADE");
}
```
Helper `long newTask(String cron)` via `new JdbcTaskRepository(jdbc)` (mirror `JdbcShardRepositoryTest.newTask`). Write these tests (mirror existing assertion style; the assignable assertions are the point):
1. `createDag_persistsNodesAndEdges` — create a task; `createDag("d","desc","0 */5 * * * *", [A→t1,B→t2], [A→B])`; `findDag(id)` present; `findNodes` = 2 (keys A,B, taskIds t1,t2, sortOrder set); `findEdges` = 1 (fromNodeId=A's id, toNodeId=B's id).
2. `createDag_unknownTask_rejects400` — `createDag` with a node referencing a taskId that doesn't exist → `assertThrows(IllegalArgumentException.class, ...)`, and `findAllDags()` empty (tx rolled back — no partial dag).
3. `createDag_duplicateNodeKey_rejects` — two `NodeInput` with `nodeKey="A"` → `IllegalArgumentException`, no dag persisted.
4. `createDag_duplicateEdge_rejects` — edges `[A→B, A→B]` → `IllegalArgumentException`.
5. `createDag_selfLoop_rejects` — edge `[A→A]` → `IllegalArgumentException`.
6. `createDag_cycle_rejects` — edges `[A→B, B→C, C→A]` → `IllegalArgumentException`; `findAllDags()` empty (atomic rollback).
7. `createScheduledRun_isIdempotentByKey` — create task/dag; call `createScheduledRun(dagId, Instant)` twice with the SAME triggerAt → exactly 1 `dag_run`, and its `findNodesOfRun` = 2 (no duplicate nodes).
8. `createManualRun_createsDistinctRuns` — two `createManualRun(dagId)` calls → 2 runs (distinct UUID keys).
9. `markNodeSpawned_setsExecutionAndRunning` — create run; pick node A; `shardRepo` seed is not needed — just call `markNodeSpawned(id, 999L)` (execution_id 999 need not exist — it's an FK; use a real execution: create one via `execution` INSERT returning id, or pass a valid `createParentWithShards` id). Use a real execution: `long eid = shardRepo.createParentWithShards(taskId,"seed-"+nanoTime(),1).id();` then `assertTrue(markNodeSpawned(nodeId, eid))`; reload node → status RUNNING, execution_id=eid; a `dag_run_node_outcome` row RUNNING/'spawned' exists. A second `markNodeSpawned` → `false` (not PENDING anymore).
10. `markNodeStatus_terminal_derivesWithOutcome` — node RUNNING → `markNodeStatus(id, SUCCESS, "all shards ok")` true; reload → SUCCESS, finished_at non-null; outcome SUCCESS exists. Transition `PENDING→FAILED` throws IllegalStateException.
11. `finalizeRun_cas_idempotent` — run PENDING → `finalizeRun(runId, SUCCESS, "all nodes ok")` true; `run_outcome` SUCCESS row; reload run → SUCCESS, finished_at set; a second `finalizeRun(...)` → false (CAS 0, no extra outcome).
12. `requestCancelRun_setsFlag` — `requestCancelRun` → `cancel_requested=true`; on a terminal run it is a silent no-op (no exception).
13. `countActiveRuns` — 2 PENDING runs + 1 finalized → `countActiveRuns(dagId) == 2`.

- [ ] **Step 7: Run the persistence tests to see them fail** — they fail because `DagRepository`/`JdbcDagRepository`/V4 aren't wired. Run `mvn -q -pl scheduler-persistence test -Dtest=JdbcDagRepositoryTest`. (Fails to compile — no interface yet. That's the expected red.)

- [ ] **Step 8: Implement `JdbcDagRepository`.** Create `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcDagRepository.java`. Pattern (constructor/`tx`/RowMappers) exactly mirrors `JdbcShardRepository`. RowMappers:
```java
private static final RowMapper<Dag> DAG_MAP = (rs, i) -> new Dag(
    rs.getLong("id"), rs.getString("name"), rs.getString("description"), rs.getString("cron"),
    rs.getBoolean("enabled"), rs.getBoolean("paused"),
    rs.getTimestamp("created_at") != null ? rs.getTimestamp("created_at").toInstant() : null,
    rs.getTimestamp("updated_at") != null ? rs.getTimestamp("updated_at").toInstant() : null);
private static final RowMapper<DagNode> NODE_MAP = (rs, i) -> new DagNode(
    rs.getLong("id"), rs.getLong("dag_id"), rs.getString("node_key"), rs.getLong("task_id"),
    rs.getInt("sort_order"));
private static final RowMapper<DagEdge> EDGE_MAP = (rs, i) -> new DagEdge(
    rs.getLong("id"), rs.getLong("dag_id"), rs.getLong("from_node_id"), rs.getLong("to_node_id"));
private static final RowMapper<DagRun> RUN_MAP = (rs, i) -> new DagRun(
    rs.getLong("id"), rs.getLong("dag_id"), rs.getString("idempotency_key"),
    DagRunStatus.valueOf(rs.getString("status")), rs.getString("trigger_reason"),
    rs.getBoolean("cancel_requested"),
    rs.getTimestamp("finished_at") != null ? rs.getTimestamp("finished_at").toInstant() : null,
    rs.getTimestamp("created_at") != null ? rs.getTimestamp("created_at").toInstant() : null);
private static final RowMapper<DagRunNode> RUN_NODE_MAP = (rs, i) -> new DagRunNode(
    rs.getLong("id"), rs.getLong("dag_run_id"), rs.getString("node_key"), rs.getLong("task_id"),
    rs.getLong("execution_id"),
    DagRunNodeStatus.valueOf(rs.getString("status")), rs.getInt("sort_order"),
    rs.getString("detail"),
    rs.getTimestamp("created_at") != null ? rs.getTimestamp("created_at").toInstant() : null,
    rs.getTimestamp("finished_at") != null ? rs.getTimestamp("finished_at").toInstant() : null);
```
`runNode(rs,i)` note: `rs.getLong("execution_id")` returns 0 when NULL — guard: `long eid = rs.getLong("execution_id"); Long exec = rs.wasNull() ? null : eid;`.

Implement the simple reads (SQL shown, mirror `JdbcShardRepository`/`JdbcExecutionRepository`):
- `findDag`/`findAllDags`/`findCronEnabledDags`(`WHERE enabled AND NOT paused AND cron IS NOT NULL ORDER BY id`)/`setPaused`(`UPDATE app_dag SET paused=?, updated_at=now() WHERE id=?`).
- `findNodes`(`WHERE dag_id=? ORDER BY sort_order, id`)/`findEdges`(`WHERE dag_id=? ORDER BY id`).
- `findRun`/`findNode`/`findRuns`(dynamic `WHERE dag_id=?` optional, `ORDER BY id DESC`)/`findActiveRuns`(`WHERE status='PENDING' ORDER BY id`)/`findNodesOfRun`(`WHERE dag_run_id=? ORDER BY sort_order, id`)/`findNonTerminalNodes`(`WHERE dag_run_id=? AND status IN ('PENDING','RUNNING') ORDER BY sort_order, id`).
- `countActiveRuns`(`SELECT count(*) FROM dag_run WHERE dag_id=? AND status='PENDING'`).

Implement the non-trivial methods exactly:

**`createDag`** — all in one `tx.execute`; capture the dag id in a non-final holder. `assertAcyclic` is a private helper:
```java
@Override public Dag createDag(String name, String description, String cron,
                               List<NodeInput> nodes, List<EdgeInput> edges) {
  Long[] dagId = new Long[1];
  tx.executeWithoutResult(s -> {
    Long did = jdbc.queryForObject(
        "INSERT INTO app_dag (name, description, cron) VALUES (?,?,?) RETURNING id",
        Long.class, name, description, cron);
    dagId[0] = did;
    Map<String, Long> byKey = new LinkedHashMap<>();
    for (NodeInput n : nodes) {
      Integer exists = jdbc.queryForObject(
          "SELECT 1 FROM app_task WHERE id=?", Integer.class, n.taskId());
      if (exists == null) throw new IllegalArgumentException(
          "node '" + n.nodeKey() + "' references unknown task " + n.taskId());
      if (byKey.put(n.nodeKey(), -1L) != null) throw new IllegalArgumentException(
          "duplicate node key '" + n.nodeKey() + "'");
      Long nid = jdbc.queryForObject(
          "INSERT INTO app_dag_node (dag_id, node_key, task_id, sort_order) VALUES (?,?,?,?) RETURNING id",
          Long.class, did, n.nodeKey(), n.taskId(), n.sortOrder());
      byKey.put(n.nodeKey(), nid);
    }
    List<long[]> resolved = new ArrayList<>();
    for (EdgeInput e : edges) {
      Long from = byKey.get(e.from());
      Long to = byKey.get(e.to());
      if (from == null || to == null) throw new IllegalArgumentException(
          "edge references unknown node key '" + (from == null ? e.from() : e.to()) + "'");
      if (from.equals(to)) throw new IllegalArgumentException(
          "self-loop edge " + e.from() + "->" + e.to() + " is not allowed");
      boolean dup = resolved.stream().anyMatch(r -> r[0] == from && r[1] == to);
      if (dup) throw new IllegalArgumentException("duplicate edge " + e.from() + "->" + e.to());
      resolved.add(new long[]{from, to});
    }
    assertAcyclic(byKey.values().stream().filter(v -> v > 0).toList(), resolved);
    for (long[] r : resolved) {
      jdbc.update("INSERT INTO dag_edge (dag_id, from_node_id, to_node_id) VALUES (?,?,?)",
          did, r[0], r[1]);
    }
  });
  return findDag(dagId[0]).orElseThrow();
}

/** DFS 无环校验:任一节点有前向边(正在 DFS 栈上)即环 → 抛 IllegalArgumentException(400),回滚整个建 DAG 事务。 */
private void assertAcyclic(List<Long> nodeIds, List<long[]> edges) {
  Map<Long, List<Long>> adj = new HashMap<>();
  for (long[] e : edges) adj.computeIfAbsent(e[0], k -> new ArrayList<>()).add(e[1]);
  Map<Long, Integer> state = new HashMap<>(); // 0=unvisited 1=in-stack 2=done
  for (long nid : nodeIds) {
    if (dfsCycle(nid, adj, state)) throw new IllegalArgumentException(
        "cycle detected: dag must be acyclic");
  }
}
private boolean dfsCycle(long nid, Map<Long, List<Long>> adj, Map<Long, Integer> state) {
  int s = state.getOrDefault(nid, 0);
  if (s == 1) return true;      // back-edge → cycle
  if (s == 2) return false;     // already proven acyclic
  state.put(nid, 1);
  for (long next : adj.getOrDefault(nid, List.of())) {
    if (dfsCycle(next, adj, state)) return true;
  }
  state.put(nid, 2);
  return false;
}
```
(`nodeIds` = all inserted node ids from `byKey.values()` excluding the `-1L` placeholder keys — the placeholder is replaced by the real id at put, so values are all positive; filter defensively.)

**`createScheduledRun`/`createManualRun` + a shared `createRun(dagId, key, reason)`** — the idempotent single-tx, mirror `createParentWithShards`:
```java
@Override public DagRun createScheduledRun(long dagId, Instant triggerAt) {
  return createRun(dagId, IdempotencyKeys.forDagTrigger(dagId, triggerAt), "scheduled");
}
@Override public DagRun createManualRun(long dagId) {
  return createRun(dagId, IdempotencyKeys.forManualDagRun(dagId), "manual");
}
private DagRun createRun(long dagId, String key, String reason) {
  long[] runId = new long[1];
  tx.executeWithoutResult(s -> {
    Long id = jdbc.queryForObject("""
      INSERT INTO dag_run (dag_id, status, idempotency_key, trigger_reason)
      VALUES (?, 'PENDING', ?, ?)
      ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
      RETURNING id""", Long.class, dagId, key, reason);
    int existing = jdbc.queryForObject(
        "SELECT count(*) FROM dag_run_node WHERE dag_run_id=?", Integer.class, id);
    if (existing == 0) { // 首次物化:按定义节点快照 task_id 插入全部 PENDING 节点(重放不重复插)
      List<DagNode> nodes = findNodes(dagId);
      jdbc.batchUpdate("""
        INSERT INTO dag_run_node (dag_run_id, node_key, task_id, sort_order)
        VALUES (?, ?, ?, ?)""",
        nodes.stream().map(n -> new Object[]{id, n.nodeKey(), n.taskId(), n.sortOrder()}).toList());
    }
    runId[0] = id;
  });
  return findRun(runId[0]).orElseThrow();
}
```

**`markNodeSpawned`/`markNodeStatus`/`finalizeRun`/`requestCancelRun`** — mirror `JdbcShardRepository.markStatus`/`finalizeParent` (read-validate, tx, CAS on row, suppress outcome on lost race):
```java
@Override public boolean markNodeSpawned(long nodeId, long executionId) {
  final boolean[] ok = {false};
  tx.executeWithoutResult(s -> {
    int upd = jdbc.update(
        "UPDATE dag_run_node SET status='RUNNING', execution_id=? WHERE id=? AND status='PENDING'",
        executionId, nodeId);
    if (upd == 0) return; // 已非 PENDING(已被推进)→ 静默,不落误导性 outcome
    jdbc.update("INSERT INTO dag_run_node_outcome (node_id, status, detail) VALUES (?,?,?)",
        nodeId, "RUNNING", "spawned");
    ok[0] = true;
  });
  return ok[0];
}

@Override public boolean markNodeStatus(long nodeId, DagRunNodeStatus to, String detail) {
  DagRunNode cur = findNode(nodeId).orElseThrow(() -> new IllegalStateException("no dag_run_node " + nodeId));
  if (!DagRunNodeTransitions.canTransition(cur.status(), to)) {
    throw new IllegalStateException("illegal transition " + cur.status() + " -> " + to);
  }
  final boolean[] ok = {false};
  tx.executeWithoutResult(s -> {
    int upd = jdbc.update("""
      UPDATE dag_run_node SET status=?, detail=?,
        finished_at=CASE WHEN ? THEN now() ELSE finished_at END
        WHERE id=? AND status=?""",
        to.name(), detail, to.isTerminal(), nodeId, cur.status().name());
    if (upd == 0) return; // CAS 0 → 行已被他方改走,静默
    jdbc.update("INSERT INTO dag_run_node_outcome (node_id, status, detail) VALUES (?,?,?)",
        nodeId, to.name(), detail);
    ok[0] = true;
  });
  return ok[0];
}

@Override public boolean finalizeRun(long runId, DagRunStatus terminal, String detail) {
  return tx.execute(s -> {
    int upd = jdbc.update(
        "UPDATE dag_run SET status=?, finished_at=now() WHERE id=? AND status='PENDING'",
        terminal.name(), runId);
    if (upd == 0) return false; // CAS 0 → 已终态,幂等跳过
    jdbc.update("INSERT INTO dag_run_outcome (dag_run_id, status, detail) VALUES (?,?,?)",
        runId, terminal.name(), detail);
    return true;
  });
}

@Override public void requestCancelRun(long runId) {
  jdbc.update("UPDATE dag_run SET cancel_requested=true WHERE id=? AND status='PENDING'", runId);
}
```
Add `import java.util.LinkedHashMap; import java.util.Map; import java.util.ArrayList; import java.util.HashMap;` as needed.

- [ ] **Step 9: Run the persistence tests and make them pass.** `mvn -q -pl scheduler-persistence test`. All `JdbcDagRepositoryTest` + `SchemaSmokeTest` green, existing 58 persistence tests still green.

- [ ] **Step 10: Commit.** 
```bash
git add scheduler-core scheduler-persistence
git commit -m "feat: M4 data layer — V4 dag schema, core enums/records, DagRepository/JdbcDagRepository"
```

**(End of Task 1 self-check:** `DagRepository` is the sole supplier of dag state for Tasks 2 and 3. Verify all method names/signatures you reference later match this file exactly — later tasks consume it verbatim.)

---

### Task 2: Server — DagEngine (trigger + propagation + terminal derivation) + loop bean + engine test

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/dag/DagEngine.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java` (add `dagRepository`/`dagEngine`/`dagLoop` beans)
- Create: `scheduler-server/src/test/java/dev/scheduler/server/dag/DagEngineTest.java`

**Interfaces:**
- Consumes: `DagRepository` (Task 1), `ShardRepository` (existing), `TaskRepository` (existing), `LeaderElection` (existing), `Clock` (existing), `dev.scheduler.core.*`.
- Produces: `DagEngine.scanOnce()` — the trigger + propagation loop that Task 3's E2E drives synchronously; the `dagEngine` bean used by `ApiIntegrationTest`.

- [ ] **Step 1: Write the failing `DagEngineTest`** (mirror `ReconcilerTest` harness: real PG, fixed `CLOCK`, repos, `TRUNCATE` in `@BeforeEach`, construct engine with a real `AdvisoryLockLeaderElection`). Create `scheduler-server/src/test/java/dev/scheduler/server/dag/DagEngineTest.java`. Constants: `CRON_EVERY_5 = "0 */5 * * * *"`, `FIRED = Instant.parse("2026-01-01T10:05:00Z")`, `CLOCK = Clock.fixed(FIRED, ZoneOffset.UTC)`. Helpers mirror `TriggerEngineTest`/`ReconcilerTest` (`clearTables` TRUNCATEs the 7 dag tables + execution tables + app_task; `newTask(shardCount)`; `newDag(taskIdsByKey...)` builds a dag via `dagRepo.createDag`). A helper `setExecutionShardSuccess(taskId, nodeKey)` to make a node's spawned execution's shards terminal by direct SQL (so the engine derives node SUCCESS):
```java
private long seedDag(long... taskIds) { /* not used — see newDag */ }
```
Write these tests:
1. `trigger_cronTick_createsRunWithPendingNodes` — create task + dag (A→t1, no edges). `engine.scanOnce()` (leader). Assert 1 `dag_run` with `trigger_reason='scheduled'`, key `IdempotencyKeys.forDagTrigger(dagId, FIRED)`; `findNodesOfRun` = 1, status PENDING. A second `scanOnce()` adds nothing (idempotent).
2. `linearAtoBtoC_allSuccess_dagRunSuccess` — tasks t1,t2,t3 shardCount 1; dag nodes A→t1, B→t2, C→t3, edges A→B, B→C. `scanOnce()` → A spawned (its `DagRunNode.executionId` non-null, status RUNNING; B,C PENDING). Simulate worker: for the spawned node's execution, mark its shard SUCCESS via SQL (`UPDATE execution_shard SET status='SUCCESS', finished_at=now() WHERE execution_id=(SELECT execution_id FROM dag_run_node WHERE dag_run_id=? AND node_key='A')`). `scanOnce()` → A derived SUCCESS, B spawned RUNNING. Repeat for B then C: after C SUCCESS final `scanOnce()` → `dag_run` status SUCCESS, `dag_run_outcome` row, each node has a `dag_run_node_outcome`.
3. `upstreamFailed_downstreamSkipped_dagRunFailed` — dag A→t1, B→t2, edges A→B. `scanOnce()` spawns A. Force A's execution shard FAILED (`UPDATE ... SET status='FAILED'`), `scanOnce()` → A node FAILED, B node SKIPPED with `execution_id` **NULL** (prove laziness — B never spawned), `dag_run` FAILED (`node failed`).
4. `fanIn_bothDownstreamsWaitForSharedUpstream_Success` — dag A→t1 shared upstream, B→t2, C→t3, edges A→B, A→C. `scanOnce()` → A RUNNING, **B and C both PENDING**. Mark A's shard SUCCESS; `scanOnce()` → A SUCCESS, **both B and C RUNNING** (spawned same scan). Mark B and C shards SUCCESS; `scanOnce()` → both SUCCESS, dag_run SUCCESS.
5. `manualTrigger_cancelCascade_dagRunCancelled` — `dagRepo.createManualRun(dagId)`, `scanOnce()` (spawns A). Call `dagRepo.requestCancelRun` then mirror the Task 3 cascade via direct repo calls: mark each non-terminal node CANCELED + (if spawned) `shards.cancelParentImmediate(executionId)`. Then `scanOnce()` → A node CANCELED, B (PENDING downstream) CANCELED, `dag_run` CANCELED, and A's `dag_run_node` CANCELED even though shards were SUCCESS (cancel-wins determinism, spec §4). (This test documents the cascade outcome; the HTTP endpooint lives in Task 3.)

- [ ] **Step 2: Run to see them fail** — `DagEngine` doesn't exist. Run `mvn -q -pl scheduler-server test -Dtest=DagEngineTest` (compile fails — expected).

- [ ] **Step 3: Implement `DagEngine`.** Create `scheduler-server/src/main/java/dev/scheduler/server/dag/DagEngine.java`:
```java
package dev.scheduler.server.dag;

import dev.scheduler.core.Dag;
import dev.scheduler.core.DagEdge;
import dev.scheduler.core.DagNode;
import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunNodeStatus;
import dev.scheduler.core.DagRunStatus;
import dev.scheduler.core.Execution;
import dev.scheduler.core.Shard;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.leader.LeaderElection;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.scheduling.support.CronExpression;

/** 工作流调度引擎(spec §2/§3):单 scan 两阶段——(1) 触发(leader 门控,cron tick → 幂等建 dag_run +
 *  PENDING 节点,镜像 TriggerEngine);(2) 传播(推进各活跃 run:惰性 spawn 就绪节点、由 spawned execution 的
 *  shards 只读派生节点终态、全节点终态后终结 dag_run,镜像 Reconciler 的父汇聚但作用对象是 dag 表)。
 *  DagEngine 是 dag 运行表唯一写者;worker/Reconciler 对 DAG 无感知,M4 不改执行运行时。 */
public class DagEngine {
  private final DagRepository dags;
  private final TaskRepository tasks;
  private final ShardRepository shards;
  private final LeaderElection leader;
  private final Clock clock;

  public DagEngine(DagRepository dags, TaskRepository tasks, ShardRepository shards,
                   LeaderElection leader, Clock clock) {
    this.dags = dags;
    this.tasks = tasks;
    this.shards = shards;
    this.leader = leader;
    this.clock = clock;
  }

  /** 扫描一次:仅 leader(镜像 TriggerEngine 的 leader 守卫)。 */
  public void scanOnce() {
    if (!leader.isLeader()) return;
    scanTriggers();
    scanPropagation();
  }

  /** 阶段一:对 enabled 且非 paused 的 cron DAG,分钟窗内命中 tick → 幂等建一条 dag_run(含全 PENDING 节点)。 */
  private void scanTriggers() {
    ZoneId zone = clock.getZone();
    ZonedDateTime now = clock.instant().atZone(zone);
    for (Dag d : dags.findCronEnabledDags()) {
      var cron = CronExpression.parse(d.cron());
      ZonedDateTime fired = cron.next(now.minusSeconds(61));
      if (fired != null && !fired.isAfter(now)) {
        dags.createScheduledRun(d.id(), fired.toInstant());
      }
    }
  }

  /** 阶段二:对每个活跃(未终态)run 应用 spec §3 的确定性传播规则。跨周期收敛:单 scan 只推进一层。 */
  private void scanPropagation() {
    for (DagRun run : dags.findActiveRuns()) {
      List<DagRunNode> nodes = dags.findNodesOfRun(run.id());
      if (nodes.isEmpty()) continue;
      propagateRun(run, nodes);
    }
  }

  /** 上游表:nodeKey → 直接上游 nodeKey 列表(读 dag 定义的边 + 节点 id↔key 映射)。 */
  private Map<String, List<String>> upstreamMap(long dagId, Map<Long, String> keyByNodeId) {
    Map<String, List<String>> up = new HashMap<>();
    for (DagEdge e : dags.findEdges(dagId)) {
      String to = keyByNodeId.get(e.toNodeId());
      String from = keyByNodeId.get(e.fromNodeId());
      if (to != null && from != null) up.computeIfAbsent(to, k -> new ArrayList<>()).add(from);
    }
    return up;
  }

  private void propagateRun(DagRun run, List<DagRunNode> nodes) {
    // nodeId → nodeKey(用 dag 定义节点映射边引用的 node id 到 run 内的 node_key)
    Map<Long, String> keyByNodeId = new HashMap<>();
    for (DagNode dn : dags.findNodes(run.dagId())) keyByNodeId.put(dn.id(), dn.nodeKey());
    Map<String, DagRunNode> byKey = new HashMap<>();
    for (DagRunNode n : nodes) byKey.put(n.nodeKey(), n);
    Map<String, List<String>> upstream = upstreamMap(run.dagId(), keyByNodeId);

    for (DagRunNode n : nodes) {
      if (n.status().isTerminal()) continue;
      List<DagRunNode> ups = upstream.getOrDefault(n.nodeKey(), List.of()).stream()
          .map(byKey::get).filter(Objects::nonNull).toList();
      if (n.status() == DagRunNodeStatus.PENDING) {
        stepPendingNode(run, n, ups);
      } else if (n.status() == DagRunNodeStatus.RUNNING) {
        stepRunningNode(n);
      }
    }

    List<DagRunNode> refreshed = dags.findNodesOfRun(run.id());
    if (refreshed.stream().allMatch(u -> u.status().isTerminal())) {
      boolean anyFailSkip = refreshed.stream()
          .anyMatch(u -> u.status() == DagRunNodeStatus.FAILED || u.status() == DagRunNodeStatus.SKIPPED);
      DagRunStatus terminal = anyFailSkip ? DagRunStatus.FAILED
          : refreshed.stream().anyMatch(u -> u.status() == DagRunNodeStatus.CANCELED)
              ? DagRunStatus.CANCELED : DagRunStatus.SUCCESS;
      String detail = anyFailSkip ? "node failed"
          : (terminal == DagRunStatus.CANCELED ? "node cancelled" : "all nodes ok");
      dags.finalizeRun(run.id(), terminal, detail);
    }
  }

  /** §3.1 PENDING 节点:上游失败→SKIPPED(绝不 spawn);上游取消→CANCELED;全 SUCCESS→惰性 spawn RUNNING;否则等。 */
  private void stepPendingNode(DagRun run, DagRunNode n, List<DagRunNode> ups) {
    boolean anyFailSkip = ups.stream()
        .anyMatch(u -> u.status() == DagRunNodeStatus.FAILED || u.status() == DagRunNodeStatus.SKIPPED);
    if (anyFailSkip) { dags.markNodeStatus(n.id(), DagRunNodeStatus.SKIPPED, "upstream failed"); return; }
    boolean anyCancelled = ups.stream().anyMatch(u -> u.status() == DagRunNodeStatus.CANCELED);
    if (anyCancelled) { dags.markNodeStatus(n.id(), DagRunNodeStatus.CANCELED, "upstream cancelled"); return; }
    boolean allTerminal = ups.stream().allMatch(u -> u.status().isTerminal());
    if (allTerminal) { // 无 FAILED/SKIPPED/CANCELED 且全终态 ⇒ 全 SUCCESS → 就绪
      spawnNode(run, n);
    }
    // 否则(有上游未终态)保持 PENDING,本周期不动作,等上游再扫描收敛。
  }

  private void spawnNode(DagRun run, DagRunNode n) {
    Task task = tasks.findById(n.taskId()).orElseThrow();
    String key = "dag:" + run.dagId() + ":run:" + run.id() + ":node:" + n.nodeKey();
    Execution parent = shards.createParentWithShards(n.taskId(), key, task.shardCount());
    dags.markNodeSpawned(n.id(), parent.id()); // CAS on PENDING;崩溃重放时幂等(同一 key 复用既有父+shard)
  }

  /** §3.2 RUNNING 节点:读其 execution 的全部 shard;全终态后派生节点终态(FAILED 先于 CANCELED 先于 SUCCESS)。 */
  private void stepRunningNode(DagRunNode n) {
    if (n.executionId() == null) return;
    List<Shard> ss = shards.findShards(n.executionId());
    if (!ss.stream().allMatch(s -> s.status().isTerminal())) return; // 未全终态 → 保持 RUNNING,等 worker/对账
    boolean anyFailed = ss.stream().anyMatch(s -> s.status() == dev.scheduler.core.ExecutionStatus.FAILED);
    DagRunNodeStatus t = anyFailed ? DagRunNodeStatus.FAILED
        : ss.stream().anyMatch(s -> s.status() == dev.scheduler.core.ExecutionStatus.CANCELED)
            ? DagRunNodeStatus.CANCELED : DagRunNodeStatus.SUCCESS;
    String detail = anyFailed ? "shards failed"
        : (t == DagRunNodeStatus.CANCELED ? "shards cancelled" : "all shards ok");
    dags.markNodeStatus(n.id(), t, detail);
  }
}
```

- [ ] **Step 4: Add the beans to `Beans.java`.** Add three bean methods and a `DagLoop` static class mirroring `ScanLoop` (only the `@ConditionalOnProperty(name = "scheduler.dag.enabled", havingValue = "true", matchIfMissing = true)` gating differs for the loop):
```java
@Bean DagRepository dagRepository(JdbcTemplate jdbc) {
  return new JdbcDagRepository(jdbc);
}

@Bean DagEngine dagEngine(DagRepository dags, TaskRepository tasks, ShardRepository shards,
                          LeaderElection leader, Clock clock) {
  return new DagEngine(dags, tasks, shards, leader, clock);
}

@Bean @ConditionalOnProperty(name = "scheduler.dag.enabled", havingValue = "true", matchIfMissing = true)
DagLoop dagLoop(DagEngine engine) {
  return new DagLoop(engine);
}
```
And the inner class:
```java
/** DAG 调度循环:固定周期触发 DagEngine.scanOnce();失败兜底不杀线程(镜像 ScanLoop)。 */
public static final class DagLoop {
  private static final Logger log = LoggerFactory.getLogger(DagLoop.class);
  private final DagEngine engine;
  DagLoop(DagEngine engine) { this.engine = engine; }
  @Scheduled(fixedDelayString = "${scheduler.dag.delay-ms:5000}")
  public void tick() {
    try { engine.scanOnce(); }
    catch (Throwable t) { log.warn("dag scan loop tick failed; continuing next tick", t); }
  }
}
```
Add imports to `Beans.java`: `dev.scheduler.persistence.DagRepository`, `dev.scheduler.persistence.JdbcDagRepository`, `dev.scheduler.server.dag.DagEngine`.

- [ ] **Step 5: Run the server module to make `DagEngineTest` green** — `mvn -q -pl scheduler-server test -Dtest=DagEngineTest`. (The 57 existing server tests must still pass; some may not because ApiIntegrationTest now fails if the `dagEngine` bean breaks the context — verify, and if `ApiIntegrationTest` breaks it must be because of context wiring, not a behavior change; do not edit `ApiIntegrationTest` here. If the context is broken by the new beans, fix the bean definitions.)

- [ ] **Step 6: Commit.**
```bash
git add scheduler-server/src/main/java/dev/scheduler/server/dag scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java scheduler-server/src/test/java/dev/scheduler/server/dag
git commit -m "feat(server): M4 DagEngine — cron trigger + propagation, lazy node spawn, node/run terminal derivation"
```

**(End of Task 2 self-check:** `DagEngine` compiles, green against real PG, and the 57 existing server tests stay green. Re-run the full server module once to confirm no regression before Task 3.)

---

### Task 3: Control plane + per-dag metrics + E2E convergence

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/service/DagQueryService.java`
- Create: `scheduler-server/src/main/java/dev/scheduler/server/cons/DagController.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java` (add `dagQueryService`-independent controller autowiring is done by Spring; add `dagMetrics` `MeterBinder`)
- Modify: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java` (add `scheduler.dag.enabled=false`, autowire `DagEngine`/`DagRepository`, add dag E2E + cycle-400 + cancel + metrics tests)
- Modify: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java` must not touch V1–V3 behavior.

**Interfaces:**
- Consumes: `DagRepository` (Task 1), `ShardRepository`, `TaskRepository`, `DagEngine` (Task 2), `TaskController.validateCron` (existing static), the `ExecutionController` HTTP idioms (404/409/200/201/202, `ResponseStatusException`, `ResponseEntity`).
- Produces: the `/api/v1/dags` control plane + `scheduler_dag_runs_active` metrics bean + the full green module.

- [ ] **Step 1: Write the failing `DagQueryService` + `DagController`.** (Compile-first: the E2E in Step 3 exercises them.) 

`DagQueryService.java` (mirror `ExecutionQueryService`):
```java
package dev.scheduler.server.service;

import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.ShardRepository;
import java.util.List;
import java.util.Optional;

/** DAG 运行读取服务:GET /dags/runs 列表 与 run 详情(含每节点的 spawned execution 的 shards)。
 *  只读;dag_run 的 RUNNING 是派生值(存储只 PENDING→终态,spec §1.3)。 */
public class DagQueryService {

  private final DagRepository dags;
  private final ShardRepository shards;

  public DagQueryService(DagRepository dags, ShardRepository shards) {
    this.dags = dags;
    this.shards = shards;
  }

  public record NodeDetail(DagRunNode node, List<Shard> shards) {}
  public record RunDetail(DagRun run, String status, List<NodeDetail> nodes) {}

  public List<DagRun> listRuns(Long dagId) { return dags.findRuns(dagId); }

  public Optional<RunDetail> runDetail(long runId) {
    return dags.findRun(runId).map(run -> {
      List<DagRunNode> nodes = dags.findNodesOfRun(runId);
      return new RunDetail(run, deriveStatus(run, nodes),
          nodes.stream().map(n -> new NodeDetail(n,
              n.executionId() == null ? List.of() : shards.findShards(n.executionId()))).toList());
    });
  }

  /** 派生 dag_run 展示用 status:存储终态原样;否则任意节点非终态 → RUNNING(读侧只映射,不写库)。 */
  static String deriveStatus(DagRun run, List<DagRunNode> nodes) {
    if (run.status().isTerminal()) return run.status().name();
    return nodes.stream().anyMatch(n -> !n.status().isTerminal()) ? "RUNNING"
        : run.status().name();
  }
}
```

`DagController.java`:
```java
package dev.scheduler.server.web;

import dev.scheduler.core.Dag;
import dev.scheduler.core.DagEdge;
import dev.scheduler.core.DagNode;
import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunNodeStatus;
import dev.scheduler.core.DagRunStatus;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.DagRepository.EdgeInput;
import dev.scheduler.persistence.DagRepository.NodeInput;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.server.service.DagQueryService;
import dev.scheduler.server.service.DagQueryService.RunDetail;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 工作流 DAG 控制面(spec §5):建/查/pause/resume/手动触发/run 列表详情/取消级联。 */
@RestController
@RequestMapping("/api/v1/dags")
public class DagController {

  private final DagRepository dags;
  private final ShardRepository shards;
  private final DagQueryService query;

  public DagController(DagRepository dags, ShardRepository shards) {
    this.dags = dags;
    this.shards = shards;
    this.query = new DagQueryService(dags, shards);
  }

  public record CreateDagRequest(String name, String description, String cron,
      List<NodeReq> nodes, List<EdgeReq> edges) {}
  public record NodeReq(String nodeKey, Long taskId, Integer sortOrder) {}
  public record EdgeReq(String from, String to) {}
  public record DagDetail(Dag dag, List<DagNode> nodes, List<DagEdge> edges) {}

  /** 建 DAG:name/cron 合法 + task 存在 + 无自环 + DFS 无环 + 键/边唯一(仓库校验,违规 → IllegalArgumentException → 400)。 */
  @PostMapping
  public ResponseEntity<Dag> create(@RequestBody CreateDagRequest req) {
    if (req == null || req.name() == null || req.name().isBlank()) {
      throw new IllegalArgumentException("name is required");
    }
    if (req.cron() == null || req.cron().isBlank()) {
      throw new IllegalArgumentException("cron is required");
    }
    TaskController.validateCron(req.cron());
    if (req.nodes() == null || req.nodes().isEmpty()) {
      throw new IllegalArgumentException("nodes must not be empty");
    }
    List<NodeInput> nodes = req.nodes().stream()
        .map(n -> new NodeInput(n.nodeKey(), n.taskId(), n.sortOrder() == null ? 0 : n.sortOrder()))
        .toList();
    List<EdgeInput> edges = req.edges() == null ? List.of()
        : req.edges().stream().map(e -> new EdgeInput(e.from(), e.to())).toList();
    return ResponseEntity.status(HttpStatus.CREATED).body(
        dags.createDag(req.name(), req.description(), req.cron(), nodes, edges));
  }

  @GetMapping public List<Dag> list() { return dags.findAllDags(); }

  @GetMapping("/{id}")
  public DagDetail get(@PathVariable long id) {
    Dag dag = dags.findDag(id).orElseThrow(() -> notFound("dag " + id));
    return new DagDetail(dag, dags.findNodes(id), dags.findEdges(id));
  }

  @PostMapping("/{id}/pause")  public Dag pause(@PathVariable long id)  { requireDag(id); dags.setPaused(id, true);  return byId(id); }
  @PostMapping("/{id}/resume") public Dag resume(@PathVariable long id) { requireDag(id); dags.setPaused(id, false); return byId(id); }

  /** 手动触发一次:建 dag_run + 全 PENDING 节点,交由 DagEngine 扫描推进。 */
  @PostMapping("/{id}/trigger")
  public ResponseEntity<DagRun> trigger(@PathVariable long id) {
    requireDag(id);
    return ResponseEntity.status(HttpStatus.CREATED).body(dags.createManualRun(id));
  }

  @GetMapping("/runs") public List<DagRun> runs(@RequestParam(required = false) Long dagId) {
    return query.listRuns(dagId);
  }

  @GetMapping("/runs/{runId}")
  public RunDetail runDetail(@PathVariable long runId) {
    return query.runDetail(runId).orElseThrow(() -> notFound("dag run " + runId));
  }

  /** 取消级联(dag → node → execution → shard,spec §4):置 run 取消标志;每个非终态节点置 CANCELED +
   *  outcome;已 spawn 节点再由父取消级联到其 shards;全节点 CANCELED → run 立即终态 CANCELED。 */
  @PostMapping("/runs/{runId}/cancel")
  public ResponseEntity<RunDetail> cancel(@PathVariable long runId) {
    DagRun run = dags.findRun(runId).orElseThrow(() -> notFound("dag run " + runId));
    if (run.status() == DagRunStatus.CANCELED) {
      return ResponseEntity.ok(query.runDetail(runId).orElseThrow()); // 幂等
    }
    if (run.status().isTerminal()) {
      throw new ResponseStatusException(HttpStatus.CONFLICT,
          "dag run " + runId + " is " + run.status() + " and cannot be cancelled");
    }
    dags.requestCancelRun(runId);
    for (DagRunNode n : dags.findNonTerminalNodes(runId)) {
      dags.markNodeStatus(n.id(), DagRunNodeStatus.CANCELED,
          n.executionId() == null ? "cascade cancel (not spawned)" : "cascade cancel");
      if (n.executionId() != null) {
        if (shards.hasRunningShard(n.executionId())) shards.requestCancelParent(n.executionId());
        else shards.cancelParentImmediate(n.executionId());
      }
    }
    dags.finalizeRun(runId, DagRunStatus.CANCELED, "cancelled by operator");
    return ResponseEntity.ok(query.runDetail(runId).orElseThrow());
  }

  private Dag byId(long id) { return dags.findDag(id).orElseThrow(() -> notFound("dag " + id)); }
  private void requireDag(long id) { byId(id); }
  private static ResponseStatusException notFound(String what) {
    return new ResponseStatusException(HttpStatus.NOT_FOUND, what);
  }
}
```

- [ ] **Step 2: Add the `dagMetrics` bean to `Beans.java`** (mirror `schedulerMetrics`; register a per-dag series at context-bind (`restart boundary`, mirror `schedulerMetrics`)):
```java
/** 每 DAG 指标(spec §6):active 未终态 dag_run 计数。已知重启边界:新 DAG 重启后才有 series(镜像 per-task)。 */
@Bean
MeterBinder dagMetrics(DagRepository dags) {
  return registry -> {
    for (Dag d : dags.findAllDags()) {
      Gauge.builder("scheduler_dag_runs_active", () -> (double) dags.countActiveRuns(d.id()))
          .tag("dag_id", Long.toString(d.id()))
          .tag("dag_name", d.name())
          .register(registry);
    }
  };
}
```
Add `import dev.scheduler.core.Dag;`. (This belongs in `Beans` next to `schedulerMetrics`, no controller wiring needed.)

- [ ] **Step 3: Add the E2E tests to `ApiIntegrationTest`.** 
  - Update the `@SpringBootTest` properties to add `"scheduler.dag.enabled=false"` (so the `DagLoop` never races the sync-driven assertions, mirroring `scheduler.loop.enabled=false`).
  - `@Autowired DagEngine dagEngine;` and `@Autowired DagRepository dagRepository;` (mirror the existing `triggerEngine`/`reconciler` autowiring).
  - Add a static dag for the metrics assertion: extend the `@BeforeAll` (which already seeds `METRICS_TASK_ID`) to also seed a disabled dag *before the context binds*, so `scheduler_dag_runs_active` has a tagged series at bind time (mirror `METRICS_TASK_ID`; store `static long METRICS_DAG_ID;`). Because `@BeforeAll` runs before the Spring context for `ApiIntegrationTest`, seed the dag there via raw JDBC (the Flyway+JDBC pattern already in the test) — insert `app_dag` + `app_dag_node` + `dag_edge` rows directly, then read it back after context up.
  - Add three `@Test`s and one metrics test:
    1. `dag_cycleCreation_rejected400` — `POST /api/v1/dags` with nodes `[A→t,B→t,C→t]`, edges `[A→B, B→C, C→A]` → `status().isBadRequest()`.
    2. `dagLinear_e2e_runsToSuccess` — create a task via `POST /api/v1/tasks` (handlerRef `demo`, cron any 6-field, shardCount 1); `POST /api/v1/dags` with two nodes `A→t, B→t`, edge `[A→B]` → 201; `POST /api/v1/dags/{id}/trigger` → 201; then drive deterministically:
       ```java
       dagEngine.scanOnce();                      // spawn A (root) → A RUNNING
       executorWorker.workOne();                  // run A's shard → SUCCESS
       reconciler.scanOnce();                     // finalize A's parent execution (header)
       dagEngine.scanOnce();                      // A derived SUCCESS → spawn B
       executorWorker.workOne();                  // run B's shard → SUCCESS
       reconciler.scanOnce();
       dagEngine.scanOnce();                      // B SUCCESS → dag_run SUCCESS
       ```
       then `GET /api/v1/dags/runs` (dagId filter) → 1 run; `GET /api/v1/dags/runs/{id}` → `status=="SUCCESS"`, nodes both SUCCESS. (For shardCount>1 you would run `workOne` once per shard; use shardCount=1 so `workOne` per node suffices.)
    3. `dagLinear_failedUpstream_skipsDownstream_failed` — same setup with edge `[A→B]`; drive until A's shard fails (use a task whose handler fails on first call — the existing `FlakyHandler` single-line convention, or simpler: seed a task with `maxRetries=0` and a handler that throws — reuse the test's existing failing-handler pattern from the M3 suite). After A fails: `dagEngine.scanOnce()` (A node FAILED via shards, B node SKIPPED, `dag_run` FAILED); assert `GET /runs/{id}` → `status=="FAILED"`, and B's node detail has empty shards (never spawned).
    4. `dagCancel_cascadesToNodesAndExecution` — create dag + trigger; `dagEngine.scanOnce()` (spawn A); `POST /api/v1/dags/runs/{id}/cancel` → 200; `GET /runs/{id}` → `status=="CANCELED"`, all nodes CANCELED.
    5. `prometheus_dagRunActiveMetric` — mirror the existing prometheus metrics test: assert the prometheus endpoint contains `scheduler_dag_runs_active` for `METRICS_DAG_ID`'s series. (Trigger a run on that dag first so the gauge is nonzero, or assert the series name exists.)

  Read the existing prometheus metric test in `ApiIntegrationTest` and mirror its exact assertion shape (the `management.prometheus...` property is already on the class).

- [ ] **Step 4: Run the full server module and make all green.** `mvn -q -pl scheduler-server test`. All new dag tests + all 57 existing server tests pass.

- [ ] **Step 5: Full root reactor green.** From the repo root: `mvn -q test`. Expect `scheduler-persistence` (58 + new) and `scheduler-server` (57 + new) all green, exit 0.

- [ ] **Step 6: Commit.**
```bash
git add scheduler-server/src/main/java scheduler-server/src/test/java
git commit -m "feat(server): M4 control plane — /api/v1/dags create/list/get/pause/resume/trigger, runs list/detail/cancel cascade, per-dag metrics + E2E"
```

---

## Execution Handoff

**Subagent-Driven (recommended, per user's governance):** implement via `superpowers:subagent-driven-development` on branch `feat/m4-workflow-dag` (already created). Fresh implementer per task, per-task review, final whole-branch review, then `superpowers:finishing-a-development-branch`.

Each dispatch brief carries the spec (`docs/superpowers/specs/2026-09-08-scheduler-m4-workflow-dag-design.md`) and the Global Constraints above verbatim, plus the interfaces the task's brief cannot know.