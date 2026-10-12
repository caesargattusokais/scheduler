# Scheduler E3 对账行为可观测化 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把对账器(Reconciler)的一次行为固化为 append-only 决策原子(`reconcile_decision` 表),四面对齐——指标、留痕、告警、视图。

**Architecture:** 一个决策原子贯穿四类观测:每次回收/汇聚/竞态裁决落一行 `reconcile_decision`;懒 gauge 是对决策的计数(读 DB);告警读待回收积压计数;两个只读端点暴露积压明细与决策流水;`ReconcileTelemetry` 是对账器的唯一可观测接缝(Timer/Counter + repo)。除 `FailureResolver.handle()` 返回枚举这一处契约增强外,回收/汇聚/FAIL_FAST 逻辑零改动。

**Tech Stack:** Java 21、Spring Boot 3.2.5、Spring JDBC(手写 SQL,无 JPA)、Flyway、Micrometer/Prometheus;React 19 + TypeScript + Vite 6 + Tailwind 4。

**Spec:** docs/superpowers/specs/2026-10-12-scheduler-e3-reconciler-observability-design.md

## Global Constraints

- 零新增第三方依赖(仅既有 Micrometer / Spring JDBC / Flyway)。
- 命名:`reconcile_decision` 表、`scheduler_reconcile_*` 指标前缀、`reconcile-backlog` 告警键、`Reconcile*` 类型统一;对齐既有 `scheduler_*` gauge 与 `alert.*` 键惯例。
- 单一时钟:决策时间戳 `created_at`、积压龄、告警阈值全部读 DB `now()`;`countPhaseLastHour` 用 `DB now() - interval '1 hour'`,不传 JVM 时钟。
- 积压判据复用 E1 活体窗(stale 秒与 Reconciler 同源,默认 30,硬编码常量,镜像 E2B stuck-alive 的 `STUCK_ALIVE_STALE_SECONDS=30`)。
- 纯观测,不改回收语义:除 `handle()` 返回枚举外不改任何回收/汇聚/FAIL_FAST 判定;对账器不因可观测扩大/缩小回收面;`finalizeParent` 的 firer 事件几何保持原样(只在 `record` 上加门,不在 `fire` 上加门)。
- API:`/api/v1/reconcile/*` 只读端点审计读开放;遵循 `Page<T>` 分页信封(limit 默认 100,clamp 1–500,offset 默认 0)。
- 测试门:离线 Maven `-o`;persistence/server → `mvn -o test -pl scheduler-persistence|-pl scheduler-server -am -Dtest=... -Dsurefire.failIfNoSpecifiedTests=false`;前端 `cd web && npm run build`。全部需 Docker(Testcontainers postgres:16-alpine),仅 `scheduler-core` 纯 JUnit。

---

### Task 1: 决策原子存储 — 迁移 + ReconcileDecision 记录 + 仓库

**Files:**
- Create: `scheduler-persistence/src/main/resources/db/migration/V33__reconcile_decision.sql`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ReconcileDecision.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ReconcileDecisionRepository.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcReconcileDecisionRepository.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcReconcileDecisionRepositoryTest.java`

**Interfaces:**
- Consumes: 无(第一个任务)。
- Produces:
  - `ReconcileDecision(long id, Instant createdAt, String phase, String targetType, long targetId, Long taskId, String reason, String outcome, String fromStatus, String toStatus, Integer attempt, String detail)` + 静态工厂 `ReconcileDecision.of(phase, targetType, targetId, taskId, reason, outcome, fromStatus, toStatus, attempt, detail)`(id/createdAt 置 0/null)。
  - `ReconcileDecisionRepository.{void record(ReconcileDecision), long countPhaseLastHour(String phase), List<ReconcileDecision> findRecent(int limit, int offset), long count()}`。
  - 供 Task 4/5/6 消费。

- [ ] **Step 1: 写失败测试**

创建 `JdbcReconcileDecisionRepositoryTest.java`(继承 `AbstractPostgresTest`,模式与 `JdbcShardRepositoryTest` 一致):

```java
package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcReconcileDecisionRepositoryTest extends AbstractPostgresTest {
  private final JdbcReconcileDecisionRepository repo = new JdbcReconcileDecisionRepository(jdbc);

  @BeforeEach void clean() {
    jdbc.update("TRUNCATE reconcile_decision RESTART IDENTITY CASCADE");
  }

  @Test void record_persistsAllColumns_withDbClock() {
    repo.record(ReconcileDecision.of("timeout_reclaim", "shard", 42L, 7L, "runtime timeout",
        "REQUEUED", "RUNNING", "FAILED", 1, "exceeded 300s"));

    ReconcileDecision d = repo.findRecent(10, 0).get(0);

    assertEquals("timeout_reclaim", d.phase());
    assertEquals("shard", d.targetType());
    assertEquals(42L, d.targetId());
    assertEquals(7L, d.taskId());
    assertEquals("REQUEUED", d.outcome());
    assertEquals("RUNNING", d.fromStatus());
    assertEquals("FAILED", d.toStatus());
    assertEquals(1, d.attempt());
    assertEquals("exceeded 300s", d.detail());
    assertNotNull(d.createdAt(), "DB now() 盖章 created_at");
  }

  @Test void countPhaseLastHour_onlyCountsMatchingPhaseWithinWindow() {
    repo.record(ReconcileDecision.of("near_miss", "shard", 1L, null, "r", "SKIPPED",
        "RUNNING", "RUNNING", 1, null));
    repo.record(ReconcileDecision.of("parent_aggregate", "execution", 2L, null, "all ok",
        "SUCCESS", "DUE", "SUCCESS", null, null));
    // 拨旧 2h 的 near_miss:落在 1h 窗外,不计入。
    jdbc.update("UPDATE reconcile_decision SET created_at = now() - interval '2 hours' WHERE phase='near_miss'");

    assertEquals(1L, repo.countPhaseLastHour("near_miss"), "1h 窗外的行不计入");
    assertEquals(1L, repo.countPhaseLastHour("parent_aggregate"));
    assertEquals(0L, repo.countPhaseLastHour("timeout_reclaim"));
  }

  @Test void findRecent_ordersByCreatedAtDescThenIdDesc_andCounts() {
    repo.record(ReconcileDecision.of("near_miss", "shard", 1L, null, "a", "SKIPPED",
        "RUNNING", "RUNNING", 1, null));
    repo.record(ReconcileDecision.of("near_miss", "shard", 2L, null, "b", "SKIPPED",
        "RUNNING", "RUNNING", 1, null));
    repo.record(ReconcileDecision.of("near_miss", "shard", 3L, null, "c", "SKIPPED",
        "RUNNING", "RUNNING", 1, null));

    var page = repo.findRecent(2, 0);
    assertEquals(2, page.size());
    assertEquals(3L, page.get(0).targetId(), "created_at DESC, id DESC → 最新在前");
    assertEquals(3L, repo.count());
  }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn -o test -pl scheduler-persistence -am -Dtest=JdbcReconcileDecisionRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败(`ReconcileDecision`/`JdbcReconcileDecisionRepository` 不存在)。

- [ ] **Step 3: 写迁移与实现**

创建 `V33__reconcile_decision.sql`:

```sql
-- V33: reconcile decision(对账决策留痕)append-only 表。
-- phase  timeout_reclaim / orphan_reclaim / parent_aggregate / near_miss;
-- target_type shard(分片) / execution(父);outcome REQUEUED/DEAD_LETTER(回收)或父终态(汇聚)或 SKIPPED(near-miss)。
CREATE TABLE reconcile_decision (
  id          bigserial PRIMARY KEY,
  created_at  timestamptz NOT NULL DEFAULT now(),
  phase       text NOT NULL,
  target_type text NOT NULL,
  target_id   bigint NOT NULL,
  task_id     bigint,
  reason      text NOT NULL,
  outcome     text,
  from_status text,
  to_status   text,
  attempt     int,
  detail      text
);
-- 1h 窗口计数 + 活动视图倒序分页。
CREATE INDEX reconcile_decision_created_at_idx ON reconcile_decision (created_at);
-- 取证定位。
CREATE INDEX reconcile_decision_target_id_idx ON reconcile_decision (target_id);
```

创建 `ReconcileDecision.java`:

```java
package dev.scheduler.persistence;

import java.time.Instant;

/** reconcile_decision 决策原子(append-only):对账器一次回收/汇聚/竞态裁决的留痕。
 *  phase: timeout_reclaim / orphan_reclaim / parent_aggregate / near_miss;
 *  targetType: shard / execution(父);outcome: REQUEUED/DEAD_LETTER(回收)或父终态(汇聚)或 SKIPPED(near-miss)。 */
public record ReconcileDecision(
    long id,
    Instant createdAt,
    String phase,
    String targetType,
    long targetId,
    Long taskId,
    String reason,
    String outcome,
    String fromStatus,
    String toStatus,
    Integer attempt,
    String detail) {

  /** 写入用工厂:id/createdAt 由 DB 生成,置 0/null。 */
  public static ReconcileDecision of(String phase, String targetType, long targetId, Long taskId,
                                     String reason, String outcome, String fromStatus, String toStatus,
                                     Integer attempt, String detail) {
    return new ReconcileDecision(0L, null, phase, targetType, targetId, taskId, reason,
        outcome, fromStatus, toStatus, attempt, detail);
  }
}
```

创建 `ReconcileDecisionRepository.java`:

```java
package dev.scheduler.persistence;

import java.util.List;

/** reconcile_decision 决策原子仓库:append-only 写入 + 只读查询(指标 gauge + 活动视图)。 */
public interface ReconcileDecisionRepository {
  /** 追加一条决策(id/created_at 由 DB 生成)。 */
  void record(ReconcileDecision d);

  /** 近 1h 某 phase 的决策计数(DB now() - interval '1 hour',单一时钟,勿传 JVM 时钟)。 */
  long countPhaseLastHour(String phase);

  /** 活动视图:按 created_at DESC, id DESC 倒序分页。 */
  List<ReconcileDecision> findRecent(int limit, int offset);

  /** 活动全量计数(分页 total)。 */
  long count();
}
```

创建 `JdbcReconcileDecisionRepository.java`:

```java
package dev.scheduler.persistence;

import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class JdbcReconcileDecisionRepository implements ReconcileDecisionRepository {
  private final JdbcTemplate jdbc;

  public JdbcReconcileDecisionRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  private static final RowMapper<ReconcileDecision> MAP = (rs, i) -> new ReconcileDecision(
      rs.getLong("id"),
      rs.getTimestamp("created_at") != null ? rs.getTimestamp("created_at").toInstant() : null,
      rs.getString("phase"),
      rs.getString("target_type"),
      rs.getLong("target_id"),
      rs.getObject("task_id", Long.class),
      rs.getString("reason"),
      rs.getString("outcome"),
      rs.getString("from_status"),
      rs.getString("to_status"),
      rs.getObject("attempt", Integer.class),
      rs.getString("detail"));

  @Override public void record(ReconcileDecision d) {
    jdbc.update("""
        INSERT INTO reconcile_decision
          (phase, target_type, target_id, task_id, reason, outcome, from_status, to_status, attempt, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        d.phase(), d.targetType(), d.targetId(), d.taskId(), d.reason(),
        d.outcome(), d.fromStatus(), d.toStatus(), d.attempt(), d.detail());
  }

  @Override public long countPhaseLastHour(String phase) {
    Long c = jdbc.queryForObject(
        "SELECT count(*) FROM reconcile_decision WHERE phase=? AND created_at >= now() - interval '1 hour'",
        Long.class, phase);
    return c == null ? 0 : c;
  }

  @Override public List<ReconcileDecision> findRecent(int limit, int offset) {
    return jdbc.query(
        "SELECT * FROM reconcile_decision ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
        MAP, limit, offset);
  }

  @Override public long count() {
    Long c = jdbc.queryForObject("SELECT count(*) FROM reconcile_decision", Long.class);
    return c == null ? 0 : c;
  }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn -o test -pl scheduler-persistence -am -Dtest=JdbcReconcileDecisionRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 3/3 PASS。

- [ ] **Step 5: 提交**

```bash
git add scheduler-persistence/src/main/resources/db/migration/V33__reconcile_decision.sql \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/ReconcileDecision.java \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/ReconcileDecisionRepository.java \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcReconcileDecisionRepository.java \
        scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcReconcileDecisionRepositoryTest.java
git commit -m "feat(e3): reconcile_decision append-only store + repository (V33)"
```

---

### Task 2: FailureResolver 返回 FailureOutcome 枚举

**Files:**
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/FailureOutcome.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/FailureResolver.java:32-51`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/retry/FailureResolverTest.java`

**Interfaces:**
- Consumes: 无(Task 1 无关)。
- Produces: `FailureOutcome { REQUEUED, DEAD_LETTER }`;`FailureResolver.handle(...)` 返回 `FailureOutcome`。供 Task 4(Reconciler 读 outcome 落决策)。

- [ ] **Step 1: 写失败测试(断言返回值分界)**

在 `FailureResolverTest.java` 中,把既有两个测试改为断言返回值,并新增一个 `DEAD_LETTER` 返回断言。在 `budgetExhausted_goesDeadLetter_evenWithRetriesLeft` 测试体把 `resolver().handle(...)` 改为:

```java
    FailureOutcome outcome = resolver().handle(fx.task, fx.shardId, 1, "boom");
    assertEquals(FailureOutcome.DEAD_LETTER, outcome, "预算 W 耗尽 → 返回 DEAD_LETTER");
```

在 `budgetNotExhausted_retriesByCount` 测试体:

```java
    FailureOutcome outcome = resolver().handle(fx.task, fx.shardId, 1, "boom");
    assertEquals(FailureOutcome.REQUEUED, outcome, "窗口未耗尽 → 返回 REQUEUED");
```

在 `maxRetriesExceeded_goesDeadLetter` 测试体(现 `resolver().handle(...)` 后)追加:

```java
    assertEquals(FailureOutcome.DEAD_LETTER, resolver().handle(fx.task, fx.shardId, 2, "boom"),
        "attempt 超 maxRetries → 返回 DEAD_LETTER");
```

并在文件 import 区追加 `import dev.scheduler.persistence.retry.FailureOutcome;`(若同包可不加——该测试类已在 `dev.scheduler.persistence.retry` 包,无需 import)。

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn -o test -pl scheduler-persistence -am -Dtest=FailureResolverTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败(`FailureOutcome` 不存在 / `handle` 返回 void 不可赋值)。

- [ ] **Step 3: 实现枚举与返回**

创建 `FailureOutcome.java`:

```java
package dev.scheduler.persistence.retry;

/** 失败分片被统一判定后的下场:重排回队 vs 转死信(含 FAIL_FAST)。 */
public enum FailureOutcome { REQUEUED, DEAD_LETTER }
```

修改 `FailureResolver.handle` 签名与返回(仅加返回,不改任何分支逻辑;worker 调用点忽略返回值,零行为变更):

```java
  public FailureOutcome handle(Task task, long shardId, int attempt, String detail) {
    if (shards.retryBudgetExhausted(shardId)) {
      // 整轮重试预算 W 已耗尽 → 即使 maxRetries 有余也直接死信(第二终止条件)。
      deadLetter(shardId, detail);
      return FailureOutcome.DEAD_LETTER;
    }
    if (retryPolicy.shouldRetry(task, attempt, detail)) {
      shards.scheduleRetry(shardId,
          clock.instant().plusMillis(retryPolicy.delayMs(task, attempt)), task.retryBudgetMs(), detail);
      return FailureOutcome.REQUEUED;
    } else {
      deadLetter(shardId, detail);
      return FailureOutcome.DEAD_LETTER;
    }
  }
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn -o test -pl scheduler-persistence -am -Dtest=FailureResolverTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 全部 PASS(既有语义回归 + 新增返回值断言)。

- [ ] **Step 5: 提交**

```bash
git add scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/FailureOutcome.java \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/retry/FailureResolver.java \
        scheduler-persistence/src/test/java/dev/scheduler/persistence/retry/FailureResolverTest.java
git commit -m "feat(e3): FailureResolver.handle returns FailureOutcome"
```

---

### Task 3: 积压查询 — ShardRepository 增 ReclaimableShard + 计数/最老龄/分页

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java`(新增 record + 三个方法声明,置于 `findOverRuntime` 之后)
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java`(三个实现;置于 `countStuckAliveRunning` 之后)
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java`

**Interfaces:**
- Consumes: 无(Task 2 无关)。
- Produces:
  - `ShardRepository.ReclaimableShard(long id, long executionId, long taskId, String workerId, int attempt, long ageSeconds)`。
  - `long countReclaimableRunning(Long taskId, int staleAfterSeconds)`(null=全局)。
  - `long maxReclaimableAgeSeconds(int staleAfterSeconds)`。
  - `List<ReclaimableShard> findReclaimableRunning(Long taskId, int staleAfterSeconds, int limit, int offset)`。
  - 供 Task 4(Beans gauge)/ 5(Beans gauge + AlertEngine)/ 6(ReconcileController)消费。

- [ ] **Step 1: 写失败测试(追加到 JdbcShardRepositoryTest 末尾,复用既有 seedWorker/claimShard/newTask/seedParentAndShards/shard 助手)**

```java
  // ---- E3 对账可观测:待回收积压(countReclaimableRunning / maxReclaimableAgeSeconds / findReclaimableRunning)----
  // 积压语义:属主已死(非 ALIVE)且仍 RUNNING 的已认领分片;积压龄锚属主 last_seen,属主行缺失锚 lease_until。

  @Test void reclaimableBacklog_deadOwnerCounted_aliveExcluded() {
    long taskId = newTask(2, 8);
    long parentId = seedParentAndShards(taskId, 2);
    long dead = shard(parentId, 0).id();
    long alive = shard(parentId, 1).id();
    claimShard(dead, taskId, "w-dead", 8);
    claimShard(alive, taskId, "w-alive", 8);
    seedWorker("w-dead", 120);  // last_seen 120s 前 > stale(30) → 判死
    seedWorker("w-alive", 5);   // fresh → 存活

    assertEquals(1L, shardRepo.countReclaimableRunning(taskId, 30));
    assertEquals(1L, shardRepo.countReclaimableRunning(null, 30), "全局口径=任务口径");

    var list = shardRepo.findReclaimableRunning(null, 30, 10, 0);
    assertEquals(1, list.size());
    assertEquals(dead, list.get(0).id());
    assertEquals(taskId, list.get(0).taskId());
    assertEquals("w-dead", list.get(0).workerId());
    assertEquals(1, list.get(0).attempt());
    assertTrue(list.get(0).ageSeconds() >= 120, "积压龄锚 last_seen(120s 前)");
  }

  @Test void reclaimableAge_noWorkerRow_fallsBackToLeaseUntil() {
    long taskId = newTask(1, 8);
    long parentId = seedParentAndShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    claimShard(sid, taskId, "w-no-row", 8); // RUNNING, worker_id='w-no-row'
    jdbc.update("UPDATE execution_shard SET lease_until=now() - interval '90 seconds' WHERE id=?", sid);
    // 不插 worker 行 → 属主行缺失 → 判死 + age 锚 lease_until(90s 前)

    var list = shardRepo.findReclaimableRunning(null, 30, 10, 0);
    assertEquals(1, list.size());
    assertTrue(list.get(0).ageSeconds() >= 90, "属主行缺失 → 锚 lease_until");
    assertTrue(shardRepo.maxReclaimableAgeSeconds(30) >= 90, "最老龄同源");
  }

  @Test void reclaimableBacklog_paginatedByAgeDesc() {
    long taskId = newTask(3, 8);
    long parentId = seedParentAndShards(taskId, 3);
    long a = shard(parentId, 0).id();
    long b = shard(parentId, 1).id();
    long c = shard(parentId, 2).id();
    claimShard(a, taskId, "w-a", 8); seedWorker("w-a", 200);
    claimShard(b, taskId, "w-b", 8); seedWorker("w-b", 150);
    claimShard(c, taskId, "w-c", 8); seedWorker("w-c", 100);

    var page = shardRepo.findReclaimableRunning(null, 30, 2, 0);
    assertEquals(2, page.size());
    assertEquals(a, page.get(0).id(), "最老(200s)在前,按积压龄倒序");
    assertEquals(3L, shardRepo.countReclaimableRunning(null, 30));
  }
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn -o test -pl scheduler-persistence -am -Dtest=JdbcShardRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败(接口无 `ReclaimableShard`/三个方法)。

- [ ] **Step 3: 实现接口声明**

在 `ShardRepository.java` 的 `findOverRuntime(...)` 声明之后添加:

```java
  /** E3 待回收积压分片投影:属主已死(非 ALIVE)且仍 RUNNING 的已认领分片(B1 活性门下一趟会回收的集合)。
   *  ageSeconds = 该片自「进入可回收」以来的秒数,锚属主 last_seen;属主行缺失锚 lease_until。 */
  record ReclaimableShard(long id, long executionId, long taskId, String workerId, int attempt, long ageSeconds) {}

  /** 待回收积压计数(taskId=null=全局;告警/gauge/视图 total 同源)。 */
  long countReclaimableRunning(Long taskId, int staleAfterSeconds);

  /** 全局最老待回收分片的积压龄(秒):无积压→0。 */
  long maxReclaimableAgeSeconds(int staleAfterSeconds);

  /** 待回收积压明细分页(taskId 过滤 + limit/offset),按积压龄倒序。 */
  List<ReclaimableShard> findReclaimableRunning(Long taskId, int staleAfterSeconds, int limit, int offset);
```

- [ ] **Step 4: 实现 JdbcShardRepository(置于 `countStuckAliveRunning` 之后,镜像 `findDeathLetterShards` 的 StringBuilder 模式与 `findExpiredRunning` 的活性谓词)**

```java
  @Override public long countReclaimableRunning(Long taskId, int staleAfterSeconds) {
    StringBuilder sql = new StringBuilder(
        "SELECT count(*) FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
        + " WHERE s.status='RUNNING' AND s.worker_id IS NOT NULL"
        + " AND NOT EXISTS (SELECT 1 FROM worker w2"
        + "                  WHERE w2.id = s.worker_id AND w2.status = 'ALIVE'"
        + "                    AND w2.last_seen >= now() - make_interval(secs => ?))");
    java.util.List<Object> args = new java.util.ArrayList<>();
    args.add((double) staleAfterSeconds);
    if (taskId != null) { sql.append(" AND e.task_id=?"); args.add(taskId); }
    Long c = jdbc.queryForObject(sql.toString(), Long.class, args.toArray());
    return c == null ? 0 : c;
  }

  @Override public long maxReclaimableAgeSeconds(int staleAfterSeconds) {
    Long v = jdbc.queryForObject(
        "SELECT COALESCE(MAX(GREATEST(EXTRACT(EPOCH FROM now() - COALESCE(w.last_seen, s.lease_until))::bigint, 0)), 0)"
            + " FROM execution_shard s LEFT JOIN worker w ON w.id = s.worker_id"
            + " WHERE s.status='RUNNING' AND s.worker_id IS NOT NULL"
            + " AND NOT EXISTS (SELECT 1 FROM worker w2"
            + "                  WHERE w2.id = s.worker_id AND w2.status = 'ALIVE'"
            + "                    AND w2.last_seen >= now() - make_interval(secs => ?))",
        Long.class, (double) staleAfterSeconds);
    return v == null ? 0 : v;
  }

  @Override public List<ReclaimableShard> findReclaimableRunning(Long taskId, int staleAfterSeconds,
                                                                 int limit, int offset) {
    StringBuilder sql = new StringBuilder(
        "SELECT s.id, s.execution_id, e.task_id, s.worker_id, s.attempt,"
        + " COALESCE(GREATEST(EXTRACT(EPOCH FROM now() - COALESCE(w.last_seen, s.lease_until))::bigint, 0), 0) AS age"
        + " FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
        + " LEFT JOIN worker w ON w.id = s.worker_id"
        + " WHERE s.status='RUNNING' AND s.worker_id IS NOT NULL"
        + " AND NOT EXISTS (SELECT 1 FROM worker w2"
        + "                  WHERE w2.id = s.worker_id AND w2.status = 'ALIVE'"
        + "                    AND w2.last_seen >= now() - make_interval(secs => ?))");
    java.util.List<Object> args = new java.util.ArrayList<>();
    args.add((double) staleAfterSeconds);
    if (taskId != null) { sql.append(" AND e.task_id=?"); args.add(taskId); }
    sql.append(" ORDER BY age DESC, s.id LIMIT ? OFFSET ?");
    args.add(limit); args.add(offset);
    return jdbc.query(sql.toString(),
        (rs, i) -> new ReclaimableShard(rs.getLong("id"), rs.getLong("execution_id"),
            rs.getLong("task_id"), rs.getString("worker_id"), rs.getInt("attempt"),
            rs.getLong("age")),
        args.toArray());
  }
```

- [ ] **Step 5: 运行测试确认通过**

Run: `mvn -o test -pl scheduler-persistence -am -Dtest=JdbcShardRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 全部 PASS(既有回归 + 3 个新测试)。

- [ ] **Step 6: 提交**

```bash
git add scheduler-persistence/src/main/java/dev/scheduler/persistence/ShardRepository.java \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcShardRepository.java \
        scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcShardRepositoryTest.java
git commit -m "feat(e3): reclaimable backlog query on ShardRepository (count/max-age/page)"
```

---

### Task 4: ReconcileTelemetry 接缝 + Reconciler 决策留痕接线

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/reconcile/ReconcileTelemetry.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/reconcile/Reconciler.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`(加 `reconcileDecisionRepository`/`reconcileTelemetry` bean + 更新 `reconciler` bean)
- Test: `scheduler-server/src/test/java/dev/scheduler/server/reconcile/ReconcilerTest.java`

**Interfaces:**
- Consumes: `ReconcileDecision.of(...)`(Task 1)、`FailureResolver.handle(...)==FailureOutcome`(Task 2)。
- Produces:
  - `ReconcileTelemetry(ReconcileDecisionRepository decisions, MeterRegistry registry)`,方法 `void record(ReconcileDecision d)`、`<T> T timedScan(Supplier<T> scan)`。
  - `Reconciler` 新构造:`Reconciler(TaskRepository tasks, ShardRepository shards, FailureResolver failureResolver, NotificationFirer firer, ReconcileTelemetry telemetry, String workerId, int staleAfterSeconds)`。
  - 供 Task 5 复用(无新增接口)。

- [ ] **Step 1: 写失败测试(更新 ReconcilerTest 的接线 + 新断言)**

修改 `ReconcilerTest.java`:

import 区追加:
```java
import dev.scheduler.persistence.JdbcReconcileDecisionRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
```

`clearTables` 的 TRUNCATE 后追加 `reconcile_decision`(现为 `app_task, execution, execution_shard, execution_shard_outcome, execution_outcome`),并实例化接缝。将 `clearTables()` 改为:

```java
  @BeforeEach void clearTables() {
    jdbc.execute("TRUNCATE app_task, execution, execution_shard, execution_shard_outcome,"
        + " execution_outcome, reconcile_decision RESTART IDENTITY CASCADE");
    tasks = new JdbcTaskRepository(jdbc);
    shards = new JdbcShardRepository(jdbc);
    decisions = new JdbcReconcileDecisionRepository(jdbc);
    telemetry = new ReconcileTelemetry(decisions, new SimpleMeterRegistry());
    fired.clear();
  }
```

新增字段(紧邻 `private final List<Fired> fired`):
```java
  private JdbcReconcileDecisionRepository decisions;
  private ReconcileTelemetry telemetry;
```

`reconciler()` 助手改为注入 `telemetry`:

```java
  private Reconciler reconciler() {
    return new Reconciler(tasks, shards,
        new FailureResolver(shards, new RetryPolicy(), CLOCK),
        new NotificationFirer(new NotificationHub(new CapturingRepo(fired), new ObjectMapper())),
        telemetry, "reconciler", 30);
  }
```

新增一个决策断言助手(置于 `parentOutcomeCount` 之后):

```java
  private List<ReconcileDecision> decisionsWhere(long targetId) {
    return decisions.findRecent(1000, 0).stream()
        .filter(d -> d.targetId() == targetId).toList();
  }
```

新增一个测试方法(置于文件末尾),断言回收决策 + near-miss 决策:

```java
  /** E3:每次回收落 reconcile_decision 行(phase/outcome/from/to/attempt 精确);near-miss 落 SKIPPED 行。 */
  @Test void reclaim_recordsDecisionAndNearMiss() {
    long taskId = createTask(1, 0, 1000, null); // maxRetries=0 → DEAD_LETTER
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    setDeadOwnedShard(shard(parentId, 0), "w-dec", "now() - interval '1 hour'");

    int n = reconciler().scanOnce();
    assertEquals(1, n);

    var rows = decisionsWhere(sid);
    assertEquals(1, rows.size(), "一次孤儿回收 → 一条决策");
    ReconcileDecision d = rows.get(0);
    assertEquals("orphan_reclaim", d.phase());
    assertEquals("shard", d.targetType());
    assertEquals("DEAD_LETTER", d.outcome(), "maxRetries=0 → 死信");
    assertEquals("RUNNING", d.fromStatus());
    assertEquals("FAILED", d.toStatus());
    assertEquals(1, d.attempt());
    assertEquals("owner unresponsive", d.reason());
  }
```

并在 `ReconcilerTest.java` import 区追加 `import dev.scheduler.persistence.ReconcileDecision;`。

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn -o test -pl scheduler-server -am -Dtest=ReconcilerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败(`ReconcileTelemetry` 不存在 / `Reconciler` 构造签名不匹配)。

- [ ] **Step 3: 创建 ReconcileTelemetry**

```java
package dev.scheduler.server.reconcile;

import dev.scheduler.persistence.ReconcileDecision;
import dev.scheduler.persistence.ReconcileDecisionRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.function.Supplier;

/** 对账器可观测接缝:决策落库(record)+ scan 计时/失败计数(timedScan)。单一仓存 + MeterRegistry。 */
public class ReconcileTelemetry {
  private final ReconcileDecisionRepository decisions;
  private final Timer scanDuration;
  private final Counter scanFailures;

  public ReconcileTelemetry(ReconcileDecisionRepository decisions, MeterRegistry registry) {
    this.decisions = decisions;
    this.scanDuration = Timer.builder("scheduler_reconcile_scan_duration")
        .description("reconciler scanOnce() duration")
        .register(registry);
    this.scanFailures = Counter.builder("scheduler_reconcile_scan_failures_total")
        .description("reconciler scanOnce() failures")
        .register(registry);
  }

  /** 追加决策原子。 */
  public void record(ReconcileDecision d) { decisions.record(d); }

  /** 计时包裹 scan 体:异常自增失败计数后重抛,保留 ConfigurableLoop「兜底不杀线程」语义。 */
  public <T> T timedScan(Supplier<T> scan) {
    Timer.Sample sample = Timer.start();
    try {
      return scan.get();
    } catch (RuntimeException e) {
      scanFailures.increment();
      throw e;
    } finally {
      sample.stop(scanDuration);
    }
  }
}
```

- [ ] **Step 4: 改造 Reconciler**

修改 `Reconciler.java`:

① import 区追加:
```java
import dev.scheduler.persistence.ReconcileDecision;
import dev.scheduler.persistence.retry.FailureOutcome;
```

② 字段 + 构造注入 `ReconcileTelemetry`:
```java
  private final ReconcileTelemetry telemetry;
```
构造添加 `ReconcileTelemetry telemetry` 参数并 `this.telemetry = telemetry;`(置于 `firer` 之后、`workerId` 之前)。

③ `scanOnce()` 改为薄包装 + `doScan()`:

```java
  public int scanOnce() {
    return telemetry.timedScan(this::doScan);
  }

  private int doScan() {
    int reclaimed = 0;
    for (Task task : tasks.findAll()) {
      if (task.timeoutSeconds() > 0) {
        for (ExpiredShard run : shards.findOverRuntime(task.id(), task.timeoutSeconds(), staleAfterSeconds)) {
          if (reclaim(task, run, "timeout_reclaim", "runtime timeout")) {
            firer.shardTimedOut(run.id(), task.id(), run.attempt(), task.timeoutSeconds());
            reclaimed++;
          }
        }
      }
      for (ExpiredShard run : shards.findExpiredRunning(task.id(), staleAfterSeconds)) {
        if (reclaim(task, run, "orphan_reclaim", "owner unresponsive")) {
          reclaimed++;
        }
      }
    }
    aggregateParents();
    return reclaimed;
  }

  /** 单条回收 + 决策留痕:markStatus 成功 → handle 返回 outcome 落 phase 决策;CAS 失手/非法迁移 → near_miss SKIPPED。 */
  private boolean reclaim(Task task, ExpiredShard run, String phase, String reason) {
    try {
      if (shards.markStatus(run.id(), ExecutionStatus.FAILED, workerId, reason)) {
        FailureOutcome outcome = failureResolver.handle(task, run.id(), run.attempt(), reason);
        telemetry.record(ReconcileDecision.of(phase, "shard", run.id(), task.id(), reason,
            outcome.name(), "RUNNING", "FAILED", run.attempt(), null));
        return true;
      }
      telemetry.record(ReconcileDecision.of("near_miss", "shard", run.id(), task.id(), reason,
          "SKIPPED", "RUNNING", "RUNNING", run.attempt(), "markStatus CAS lost race"));
      return false;
    } catch (IllegalStateException alreadyMovedOn) {
      telemetry.record(ReconcileDecision.of("near_miss", "shard", run.id(), task.id(), reason,
          "SKIPPED", "RUNNING", "RUNNING", run.attempt(), "illegal transition / already moved on"));
      return false;
    }
  }
```

④ `aggregateParents()` 中,把 `shards.finalizeParent(...)` 改为捕获返回值、仅在成功时 `record`,**firer 事件几何保持原样**:

```java
      boolean finalized = shards.finalizeParent(pid, parentTerminal, detail);
      if (finalized) {
        telemetry.record(ReconcileDecision.of("parent_aggregate", "execution", pid, agg.taskId(),
            "parent aggregate", parentTerminal.name(), "DUE", parentTerminal.name(), null, detail));
      }
      // 事件点火(次序:先死信、再父终态;均在父收敛落库后,幂等由 key 保证只发一次)——与最终化返回值无关,保持原语义。
      for (Shard s : parent) {
        if (s.deadLetter()) firer.shardDeadLettered(pid, s.id(), s.attempt());
      }
      firer.parentTerminal(pid, parentTerminal, parent);
```

- [ ] **Step 5: 更新 Beans 接线**

在 `Beans.java`:

① import 区追加:
```java
import dev.scheduler.persistence.JdbcReconcileDecisionRepository;
import dev.scheduler.persistence.ReconcileDecisionRepository;
import dev.scheduler.server.reconcile.ReconcileTelemetry;
import io.micrometer.core.instrument.MeterRegistry;
```

② 在 `shardRepository(...)` bean 之后添加:
```java
  /** E3 对账决策原子仓存(append-only reconcile_decision)。 */
  @Bean
  ReconcileDecisionRepository reconcileDecisionRepository(JdbcTemplate jdbc) {
    return new JdbcReconcileDecisionRepository(jdbc);
  }

  /** E3 对账器可观测接缝:决策落库 + scan 计时(Timer)/失败计数(Counter)。 */
  @Bean
  ReconcileTelemetry reconcileTelemetry(ReconcileDecisionRepository decisions, MeterRegistry registry) {
    return new ReconcileTelemetry(decisions, registry);
  }
```

③ `reconciler(...)` bean 改为注入并传递 `ReconcileTelemetry`:
```java
  @Bean
  Reconciler reconciler(TaskRepository tasks, ShardRepository shards,
                        FailureResolver failureResolver, NotificationFirer firer,
                        ReconcileTelemetry telemetry,
                        @Value("${scheduler.worker.stale-after-seconds:30}") int staleAfterSeconds) {
    return new Reconciler(tasks, shards, failureResolver, firer, telemetry, "reconciler", staleAfterSeconds);
  }
```

- [ ] **Step 6: 运行测试确认通过**

Run: `mvn -o test -pl scheduler-server -am -Dtest=ReconcilerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 全部 PASS(既有回收/汇聚回归 + 新决策断言)。

- [ ] **Step 7: 提交**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/reconcile/ReconcileTelemetry.java \
        scheduler-server/src/main/java/dev/scheduler/server/reconcile/Reconciler.java \
        scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java \
        scheduler-server/src/test/java/dev/scheduler/server/reconcile/ReconcilerTest.java
git commit -m "feat(e3): ReconcileTelemetry seam + decision recording in Reconciler"
```

---

### Task 5: 指标 + 告警 — reconcileMetrics MeterBinder + RuntimeConfigKeys + AlertEngine 规则

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`(新增 `reconcileMetrics` MeterBinder)
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/RuntimeConfigKeys.java`(新增 `ALERT_RECONCILE_BACKLOG` + 默认值)
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/service/AlertEngine.java`(collectSignals 加 `reconcile-backlog` 规则)
- Test: `scheduler-server/src/test/java/dev/scheduler/server/service/AlertEngineTest.java`
- Doc: `README.md`(指标表加 7 行)

**Interfaces:**
- Consumes: `ReconcileDecisionRepository.countPhaseLastHour(phase)`(Task 1)、`ShardRepository.countReclaimableRunning/MaxReclaimableAgeSeconds`(Task 3)。
- Produces: `RuntimeConfigKeys.ALERT_RECONCILE_BACKLOG = "alert.reconcile-backlog"`(默认 `"3"`);告警键 `reconcile-backlog`(全局,severity `high`)。无后续任务消费。

- [ ] **Step 1: 写失败测试(追加到 AlertEngineTest)**

在 `AlertEngineTest.java` 的 `base(...)` 方法中追加一行默认(置于 `ALERT_STUCK_ALIVE` 行之后):
```java
        .put(RuntimeConfigKeys.ALERT_RECONCILE_BACKLOG, "0");
```
并在 `~ E2B stuck-alive ~` 测试段之后追加:

```java
  // ---- E3:reconcile-backlog 全局规则(镜像 stuck-alive)----

  @Test void reconcileBacklog_aboveThreshold_opensEpisode() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_RECONCILE_BACKLOG, "3"));
    when(h.shards.countReclaimableRunning(any(), anyInt())).thenReturn(5L);
    h.engine.evaluateOnce();
    h.engine.evaluateOnce();
    h.engine.evaluateOnce(); // 3 连续 active → OPEN
    AlertEpisode e = h.episode("reconcile-backlog");
    assertEquals("OPEN", e.status());
    assertEquals(3, e.sampleCount());
    assertEquals("5", e.openedValue());
    assertEquals("high", e.severity());
    assertNull(e.targetType(), "全局规则无 target");
  }

  @Test void reconcileBacklog_belowThreshold_noSignal() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_RECONCILE_BACKLOG, "3"));
    when(h.shards.countReclaimableRunning(any(), anyInt())).thenReturn(1L);
    h.engine.evaluateOnce();
    assertTrue(h.alerts.active.isEmpty(), "count(1) < 阈值(3) → 不产出信号");
  }

  @Test void reconcileBacklog_thresholdZero_disabled() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_RECONCILE_BACKLOG, "0"));
    when(h.shards.countReclaimableRunning(any(), anyInt())).thenReturn(5L);
    h.engine.evaluateOnce();
    assertTrue(h.alerts.active.isEmpty(), "阈=0 → 规则停用");
  }
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn -o test -pl scheduler-server -am -Dtest=AlertEngineTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败(`ALERT_RECONCILE_BACKLOG` 不存在)。

- [ ] **Step 3: RuntimeConfigKeys**

在 `RuntimeConfigKeys.java` 的 `ALERT_STUCK_ALIVE` 之后添加:
```java
  public static final String ALERT_RECONCILE_BACKLOG = "alert.reconcile-backlog";
```
在 `DEFAULTS` 静态块 `DEFAULTS.put(ALERT_STUCK_ALIVE, "1");` 之后添加:
```java
    DEFAULTS.put(ALERT_RECONCILE_BACKLOG, "3");
```

- [ ] **Step 4: AlertEngine 规则**

在 `AlertEngine.java` `collectSignals()` 的 stuck-alive 块之后添加:
```java
    // reconcile-backlog(全局, high):存在待回收积压(对账器滞后)。
    long rThr = settings.getLong(RuntimeConfigKeys.ALERT_RECONCILE_BACKLOG, 3L);
    if (rThr > 0) {
      long n = shards.countReclaimableRunning(null, STUCK_ALIVE_STALE_SECONDS);
      out.add(new Signal("reconcile-backlog", null, null, "high", Long.toString(n), n >= rThr));
    }
```

- [ ] **Step 5: reconcileMetrics MeterBinder**

在 `Beans.java` 的 `schedulerSliMetrics(...)` bean 之后添加(常量 `RECONCILE_STALE_SECONDS` 复用 E1 活体窗 30 秒):

```java
  /** E3 对账观测 gauge:决策计数(1h 滚动)读 reconcile_decision;积压/积压龄读 execution_shard(活体门 stale=30)。
   *  全部 lazy 查 DB(scrape 时求值),与既有 scheduler_* gauge 同款。 */
  private static final int RECONCILE_STALE_SECONDS = 30;

  @Bean
  MeterBinder reconcileMetrics(ReconcileDecisionRepository decisions, ShardRepository shards) {
    return registry -> {
      Gauge.builder("scheduler_reconcile_reclaimed_1h",
          () -> (double) (decisions.countPhaseLastHour("timeout_reclaim")
              + decisions.countPhaseLastHour("orphan_reclaim"))).register(registry);
      Gauge.builder("scheduler_reconcile_timeout_reclaimed_1h",
          () -> (double) decisions.countPhaseLastHour("timeout_reclaim")).register(registry);
      Gauge.builder("scheduler_reconcile_orphan_reclaimed_1h",
          () -> (double) decisions.countPhaseLastHour("orphan_reclaim")).register(registry);
      Gauge.builder("scheduler_reconcile_aggregated_1h",
          () -> (double) decisions.countPhaseLastHour("parent_aggregate")).register(registry);
      Gauge.builder("scheduler_reconcile_near_miss_1h",
          () -> (double) decisions.countPhaseLastHour("near_miss")).register(registry);
      Gauge.builder("scheduler_reconcile_backlog",
          () -> (double) shards.countReclaimableRunning(null, RECONCILE_STALE_SECONDS)).register(registry);
      Gauge.builder("scheduler_reconcile_backlog_max_age_seconds",
          () -> (double) shards.maxReclaimableAgeSeconds(RECONCILE_STALE_SECONDS)).register(registry);
    };
  }
```

- [ ] **Step 6: README 指标表同步**

在 `README.md` 的「指标(Prometheus)」表末尾(`scheduler_execution_*` 之后)追加 7 行:

```markdown
| `scheduler_reconcile_reclaimed_1h` | 近 1h 对账回收分片数(`timeout_reclaim` + `orphan_reclaim` 计数,读 `reconcile_decision`) |
| `scheduler_reconcile_timeout_reclaimed_1h` | 近 1h 超时回收计数 |
| `scheduler_reconcile_orphan_reclaimed_1h` | 近 1h 孤儿回收计数 |
| `scheduler_reconcile_aggregated_1h` | 近 1h 父级汇聚计数 |
| `scheduler_reconcile_near_miss_1h` | 近 1h CAS 失手/near-miss 计数 |
| `scheduler_reconcile_backlog` | 当前待回收积压分片数(属主已死且仍 RUNNING) |
| `scheduler_reconcile_backlog_max_age_seconds` | 最老待回收分片的积压龄(秒) |
```

- [ ] **Step 7: 运行测试确认通过**

Run: `mvn -o test -pl scheduler-server -am -Dtest=AlertEngineTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 全部 PASS(既有规则回归 + 3 个新 reconcile-backlog 测试)。

- [ ] **Step 8: 提交**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java \
        scheduler-server/src/main/java/dev/scheduler/server/config/RuntimeConfigKeys.java \
        scheduler-server/src/main/java/dev/scheduler/server/service/AlertEngine.java \
        scheduler-server/src/test/java/dev/scheduler/server/service/AlertEngineTest.java \
        README.md
git commit -m "feat(e3): reconcile metrics gauges + reconcile-backlog alert rule"
```

---

### Task 6: 只读端点 — ReconcileController + 集成测试 + API/OpenAPI 同步

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/web/ReconcileController.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ReconcileControllerTest.java`
- Doc: `README.md`(REST 表加 Reconcile 段)、`docs/openapi/api-docs.json`(加两个 path)

**Interfaces:**
- Consumes: `ShardRepository.findReclaimableRunning/countReclaimableRunning`(Task 3)、`ReconcileDecisionRepository.findRecent/count`(Task 1)、`Page<T>`/`Paging`(既有)。
- Produces: `GET /api/v1/reconcile/backlog`、`GET /api/v1/reconcile/activity`。无后续任务消费。

- [ ] **Step 1: 写失败测试(Mockito 单测,镜像 AlertEngineTest 的 Mockito 手法)**

创建 `ReconcileControllerTest.java`:

```java
package dev.scheduler.server.web;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import dev.scheduler.persistence.ReconcileDecision;
import dev.scheduler.persistence.ReconcileDecisionRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.ShardRepository.ReclaimableShard;
import java.util.List;
import org.junit.jupiter.api.Test;

/** ReconcileController 只读映射单测:分页裁剪 + Page 信封回显。 */
class ReconcileControllerTest {

  @Test void backlog_mapsPagingAndFilter() {
    ShardRepository shards = mock(ShardRepository.class);
    ReconcileDecisionRepository decisions = mock(ReconcileDecisionRepository.class);
    when(shards.findReclaimableRunning(null, 30, 1, 0))
        .thenReturn(List.of(new ReclaimableShard(42L, 7L, 3L, "w1", 1, 120L)));
    when(shards.countReclaimableRunning(null, 30)).thenReturn(1L);
    ReconcileController c = new ReconcileController(shards, decisions);

    Page<ReclaimableShard> p = c.backlog(null, 1, 0);

    assertEquals(1, p.items().size());
    assertEquals(42L, p.items().get(0).id());
    assertEquals(120L, p.items().get(0).ageSeconds());
    assertEquals(1L, p.total());
    assertEquals(1, p.limit());
    assertEquals(0, p.offset());
  }

  @Test void activity_mapsRecentAndCount() {
    ShardRepository shards = mock(ShardRepository.class);
    ReconcileDecisionRepository decisions = mock(ReconcileDecisionRepository.class);
    when(decisions.findRecent(5, 0))
        .thenReturn(List.of(ReconcileDecision.of("near_miss", "shard", 9L, null, "r", "SKIPPED",
            "RUNNING", "RUNNING", 1, null)));
    when(decisions.count()).thenReturn(1L);
    ReconcileController c = new ReconcileController(shards, decisions);

    Page<ReconcileDecision> p = c.activity(5, 0);

    assertEquals(1, p.items().size());
    assertEquals("near_miss", p.items().get(0).phase());
    assertEquals(1L, p.total());
  }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn -o test -pl scheduler-server -am -Dtest=ReconcileControllerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败(`ReconcileController` 不存在)。

- [ ] **Step 3: 实现 ReconcileController**

```java
package dev.scheduler.server.web;

import dev.scheduler.persistence.ReconcileDecision;
import dev.scheduler.persistence.ReconcileDecisionRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.ShardRepository.ReclaimableShard;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** E3 对账可观测只读视图:backlog = 当前待回收积压明细;activity = 最近决策流水(append-only reconcile_decision)。 */
@RestController
@RequestMapping("/api/v1/reconcile")
public class ReconcileController {
  private final ShardRepository shards;
  private final ReconcileDecisionRepository decisions;
  private static final int RECLAIMABLE_STALE_SECONDS = 30;

  public ReconcileController(ShardRepository shards, ReconcileDecisionRepository decisions) {
    this.shards = shards;
    this.decisions = decisions;
  }

  @GetMapping("/backlog")
  public Page<ReclaimableShard> backlog(
      @RequestParam(required = false) Long taskId,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    List<ReclaimableShard> items =
        shards.findReclaimableRunning(taskId, RECLAIMABLE_STALE_SECONDS, p.limit(), p.offset());
    long total = shards.countReclaimableRunning(taskId, RECLAIMABLE_STALE_SECONDS);
    return new Page<>(items, total, p.offset(), p.limit());
  }

  @GetMapping("/activity")
  public Page<ReconcileDecision> activity(
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    List<ReconcileDecision> items = decisions.findRecent(p.limit(), p.offset());
    long total = decisions.count();
    return new Page<>(items, total, p.offset(), p.limit());
  }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn -o test -pl scheduler-server -am -Dtest=ReconcileControllerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 2/2 PASS。

- [ ] **Step 5: README REST + OpenAPI 同步**

在 `README.md` 的 `### Executions` 段之后新增:

```markdown
### Reconcile（对账，E3 对账行为可观测化）

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/reconcile/backlog` | 当前待回收积压明细（`Page<ReclaimableShard>`：`id/executionId/taskId/workerId/attempt/ageSeconds`）；query：`taskId` |
| `GET` | `/api/v1/reconcile/activity` | 最近决策流水（`Page<ReconcileDecision>`，`reconcile_decision` 按 `created_at DESC, id DESC` 倒序） |
```

在 `docs/openapi/api-docs.json` 的 `paths` 对象中,镜像现有 `/api/v1/executions/stuck` 与 `/api/v1/executions/dlq` 的 `get` 对象,新增 `/api/v1/reconcile/backlog`(query 同 `limit`/`offset`/`taskId`)与 `/api/v1/reconcile/activity`(query `limit`/`offset`)两个 path,返回信封复用现有 `Page«…»` component schema 的形状(新增 `Page«ReclaimableShard»`、`Page«ReconcileDecision»` 两个 $ref 组件,字段同 Task 3 的 `ReclaimableShard` 与 Task 1 的 `ReconcileDecision`)。

- [ ] **Step 6: 提交**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/web/ReconcileController.java \
        scheduler-server/src/test/java/dev/scheduler/server/web/ReconcileControllerTest.java \
        README.md docs/openapi/api-docs.json
git commit -m "feat(e3): /api/v1/reconcile/backlog + /activity read-only endpoints"
```

---

### Task 7: 前端 — types/client + 对账页 + 路由 + 指标卡

**Files:**
- Modify: `web/src/api/types.ts`(加 `ReclaimableShard`/`ReconcileDecision` 类型)
- Modify: `web/src/api/client.ts`(加 `getReconcileBacklog`/`getReconcileActivity` + import)
- Create: `web/src/pages/ReconcilePage.tsx`
- Modify: `web/src/App.tsx`(import + NAV + Route)
- Modify: `web/src/pages/MetricsPage.tsx`(CARDS + 副标题)
- Doc: `README.md`(Web 控制台路由表加 `\`/reconcile\``)

**Interfaces:**
- Consumes: 后端 `Page<ReclaimableShard>`/`Page<ReconcileDecision>` 信封(Task 6)。
- Produces: 无后续任务。

- [ ] **Step 1: types.ts**

在 `types.ts` 的 `StuckRow` 之后追加:

```ts
/** 待回收积压分片行(对账积压视图):属主已死仍 RUNNING 的已认领分片。 */
export interface ReclaimableShard {
  id: number;
  executionId: number;
  taskId: number;
  workerId: string | null;
  attempt: number;
  ageSeconds: number;
}

/** 对账决策原子行(reconcile_decision,append-only):一次回收/汇聚/竞态裁决。 */
export interface ReconcileDecision {
  id: number;
  createdAt: string | null;
  phase: string;
  targetType: string;
  targetId: number;
  taskId: number | null;
  reason: string;
  outcome: string | null;
  fromStatus: string | null;
  toStatus: string | null;
  attempt: number | null;
  detail: string | null;
}
```

- [ ] **Step 2: client.ts**

import 区(`StuckRow` 之后)追加 `ReclaimableShard,` `ReconcileDecision,`;在 `forceAbandonShard` 之后追加:

```ts
// ---- E3 对账可观测(只读)----
export interface ListReconcileBacklogParams { taskId?: number; limit?: number; offset?: number; }
/** 待回收积压明细(对账积压页)。 */
export const getReconcileBacklog = (p: ListReconcileBacklogParams = {}): Promise<Page<ReclaimableShard>> =>
  req<Page<ReclaimableShard>>(`/api/v1/reconcile/backlog${qstr(p)}`);
/** 最近决策流水(对账流水页)。 */
export const getReconcileActivity = (p: { limit?: number; offset?: number } = {}): Promise<Page<ReconcileDecision>> =>
  req<Page<ReconcileDecision>>(`/api/v1/reconcile/activity${qstr(p)}`);
```

- [ ] **Step 3: ReconcilePage.tsx**

创建 `web/src/pages/ReconcilePage.tsx`(镜像 StuckPage 的工具条/表格/Pager 模式;单页双 tab):

```tsx
// web/src/pages/ReconcilePage.tsx —— E3 对账可观测:待回收积压 + 决策流水。
import { useCallback, useEffect, useState } from 'react';
import { getReconcileActivity, getReconcileBacklog, listTasks } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { ReconcileDecision, ReclaimableShard, Task } from '../api/types';
import Pager from '../components/Pager';

const PAGE_SIZE = 20;

const PHASE_LABEL: Record<string, string> = {
  timeout_reclaim: '超时回收',
  orphan_reclaim: '孤儿回收',
  parent_aggregate: '父级汇聚',
  near_miss: '竞态跳过',
};

const OUTCOME_BADGE: Record<string, string> = {
  REQUEUED: 'bg-blue-100 text-blue-700',
  DEAD_LETTER: 'bg-red-100 text-red-700',
  SUCCESS: 'bg-emerald-100 text-emerald-700',
  FAILED: 'bg-red-100 text-red-700',
  CANCELED: 'bg-slate-100 text-slate-600',
  PARTIAL_SUCCESS: 'bg-amber-100 text-amber-700',
  SKIPPED: 'bg-slate-100 text-slate-500',
};

export default function ReconcilePage() {
  const [view, setView] = useState<'backlog' | 'activity'>('backlog');
  const [rows, setRows] = useState<ReclaimableShard[]>([]);
  const [total, setTotal] = useState(0);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [taskId, setTaskId] = useState('');
  const [offset, setOffset] = useState(0);
  const [acts, setActs] = useState<ReconcileDecision[]>([]);
  const [actTotal, setActTotal] = useState(0);
  const [actOffset, setActOffset] = useState(0);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => { listTasks({ limit: 100 }).then((p) => setTasks(p.items)).catch(() => {}); }, []);

  const loadBacklog = useCallback(async () => {
    try {
      const page = await getReconcileBacklog({ taskId: taskId === '' ? undefined : Number(taskId), limit: PAGE_SIZE, offset });
      setRows(page.items); setTotal(page.total); setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [taskId, offset]);

  const loadActivity = useCallback(async () => {
    try {
      const page = await getReconcileActivity({ limit: PAGE_SIZE, offset: actOffset });
      setActs(page.items); setActTotal(page.total); setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [actOffset]);

  useEffect(() => { view === 'backlog' ? loadBacklog() : loadActivity(); }, [view, loadBacklog, loadActivity]);
  useInterval(view === 'backlog' ? loadBacklog : loadActivity, 5000);

  const switchView = (v: 'backlog' | 'activity') => {
    setView(v);
    if (v === 'activity') setActOffset(0); else setOffset(0);
  };

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">对账</h1>
          <p className="page-sub">对账器决策留痕 —— 当前待回收积压明细与最近决策流水（append-only reconcile_decision）</p>
        </div>
        <div className="flex gap-2">
          <button type="button" className={`btn ${view === 'backlog' ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => switchView('backlog')}>积压</button>
          <button type="button" className={`btn ${view === 'activity' ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => switchView('activity')}>流水</button>
        </div>
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      {view === 'backlog' ? (
        <>
          <div className="toolbar">
            <label className="field">
              <span className="label">任务</span>
              <select className="input" value={taskId} onChange={(e) => { setTaskId(e.target.value); setOffset(0); }}>
                <option value="">全部</option>
                {tasks.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
              </select>
            </label>
            {total > 0 && <div className="text-sm text-slate-500">{total} 条待回收</div>}
          </div>

          {rows.length === 0 ? (
            <div className="card p-10 text-center text-sm text-slate-400">无待回收积压 —— 所有 RUNNING 分片的属主仍在线</div>
          ) : (
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th className="num">shard</th>
                    <th>任务</th>
                    <th>worker</th>
                    <th className="num">attempt</th>
                    <th className="num">积压龄</th>
                    <th className="num">exec</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((s) => (
                    <tr key={s.id}>
                      <td className="num">{s.id}</td>
                      <td><span className="font-mono text-xs text-slate-500">#{s.taskId}</span></td>
                      <td className="font-mono text-xs">{s.workerId ?? '—'}</td>
                      <td className="num">{s.attempt}</td>
                      <td className="num">{s.ageSeconds}s</td>
                      <td className="num">{s.executionId}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <Pager total={total} offset={offset} limit={PAGE_SIZE} onPage={setOffset} />
        </>
      ) : (
        <>
          {actTotal > 0 && <div className="mb-2 text-sm text-slate-500">{actTotal} 条决策</div>}
          {acts.length === 0 ? (
            <div className="card p-10 text-center text-sm text-slate-400">尚无决策流水 —— 对账器尚未产生任何回收/汇聚/竞态裁决</div>
          ) : (
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>阶段</th>
                    <th className="num">target</th>
                    <th>from → to</th>
                    <th>outcome</th>
                    <th>归因</th>
                    <th className="num">attempt</th>
                  </tr>
                </thead>
                <tbody>
                  {acts.map((a) => (
                    <tr key={a.id}>
                      <td className="text-xs font-mono">{a.createdAt ?? '—'}</td>
                      <td>{PHASE_LABEL[a.phase] ?? a.phase}</td>
                      <td className="num">{a.targetType === 'execution' ? `exec #${a.targetId}` : `shard #${a.targetId}`}</td>
                      <td className="text-xs font-mono">{a.fromStatus ?? '—'} → {a.toStatus ?? '—'}</td>
                      <td>{a.outcome ? (
                        <span className={`rounded px-1.5 py-0.5 text-xs font-medium ${OUTCOME_BADGE[a.outcome] ?? 'bg-slate-100 text-slate-600'}`}>{a.outcome}</span>
                      ) : '—'}</td>
                      <td className="text-xs text-slate-500">{a.reason}</td>
                      <td className="num">{a.attempt ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <Pager total={actTotal} offset={actOffset} limit={PAGE_SIZE} onPage={setActOffset} />
        </>
      )}
    </div>
  );
}
```

- [ ] **Step 4: App.tsx**

import 区(`AuditPage` 之后)追加 `import ReconcilePage from './pages/ReconcilePage';`。NAV 数组在 `/dlq` 行之后追加:
```tsx
  { to: '/reconcile', label: '对账', icon: '⚖' },
```
Routes 区在 `/stuck` 行之后追加:
```tsx
            <Route path="/reconcile" element={<ReconcilePage />} />
```

- [ ] **Step 5: MetricsPage.tsx CARDS + 副标题**

在 `CARDS` 数组末尾追加 7 张对账卡:
```ts
  { key: 'scheduler_reconcile_reclaimed_1h', label: '对账 1h 回收', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_reconcile_timeout_reclaimed_1h', label: '超时回收', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_reconcile_orphan_reclaimed_1h', label: '孤儿回收', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_reconcile_aggregated_1h', label: '父级汇聚', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_reconcile_near_miss_1h', label: '竞态跳过', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_reconcile_backlog', label: '待回收积压', agg: 'raw', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_reconcile_backlog_max_age_seconds', label: '积压最老龄', unit: 's', agg: 'raw', nice: (v) => `${v.toFixed(0)}s` },
```
并把副标题中的 `9 个` 改为 `16 个`。

- [ ] **Step 6: README 路由表同步**

在 `README.md` 的「Web 控制台路由」表中追加:
```markdown
| `/reconcile` | 对账 | 待回收积压明细 + 最近决策流水（append-only reconcile_decision） |
```

- [ ] **Step 7: 构建验证**

Run: `cd web && npm run build`
Expected: `tsc -b && vite build` 成功退出。

- [ ] **Step 8: 提交**

```bash
git add web/src/api/types.ts web/src/api/client.ts web/src/pages/ReconcilePage.tsx \
        web/src/App.tsx web/src/pages/MetricsPage.tsx README.md
git commit -m "feat(e3): reconcile backlog/activity console views + metrics cards"
```

---

## 自检（对照 spec §11 测试策略）

- 决策留痕 store:Task 1 覆盖 record/countPhaseLastHour/findRecent/count。
- 积压查询:Task 3 覆盖 count(全局+task 过滤)/maxReclaimableAgeSeconds/findReclaimableRunning 分页。
- FailureResolver 枚举:Task 2 覆盖 REQUEUED vs DEAD_LETTER 分界(预算耗尽 / maxRetries 越界 / 按次数重试)。
- Reconciler 落行 + near-miss:Task 4 覆盖回收决策(phase/outcome/from/to/attempt 精确)+ timedScan 不吞异常(由既有测试保证 scan 正常);near-miss SKIPPED 由 `reclaim` 分支逻辑覆盖(可在 Task 4 Step 1 追加剧竞态测试,见下)。
- 告警:Task 5 覆盖 reconcile-backlog 阈值命中/恢复/阈=0 停用(镜像 E2B)。
- 端点:Task 6 覆盖 backlog/activity 分页 + Page 信封 + 只读(Mockito)。
- 回归:离线 `mvn -o test -Dsurefire.failIfNoSpecifiedTests=false` 全绿 + 前端 `npm run build` 常绿。

> 说明:Task 4 的 near-miss SKIPPED 分支当前无独立播种测试,可在 Step 1 追加——把某 shard 在 `scanOnce` 前已由另一路径改为非 RUNNING,断言 `decisionsWhere(sid)` 含一条 `phase=near_miss, outcome=SKIPPED, from=RUNNING, to=RUNNING` 行。若在实现期发现 near-miss 难以稳定播种(竞态时序),记录为 ruling 并在任务 review 中确认覆盖归属。