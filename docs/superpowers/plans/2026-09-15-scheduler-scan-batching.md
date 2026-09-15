# Scheduler §4 扫描分批健壮性 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把三个扫描循环(TriggerEngine 任务触发 / DagEngine DAG 触发 / DagEngine 活跃 run 传播)从「单 tick 全量同步处理」改为「游标分批」,大任务集单 tick 不重不漏、不拖垮、崩中可续。

**Architecture:** 每个引擎持一个内存游标(`lastXxxId`,单调 id 递增)加一个批上限(默认 200)。每 tick 只取一页 `id > lastXxxId ORDER BY id LIMIT batch`;页满则推进游标到页末,页未满(含空)则回卷到 0、下轮从头。幂等键(`createParentWithShards` 的 `ON CONFLICT (idempotency_key)`、`markNodeSpawned`)已在崩溃重放时保证「重扫不重」,故游标无需持久化——进程重启即归零、从头重扫,幂等兜底「不重」。

**Tech Stack:** Java 17、Spring Boot、PostgreSQL 16(Testcontainers 集成测试)。无新增依赖。

**Spec:** `docs/superpowers/specs/2026-09-15-scheduler-v2-reliability-design.md` 的 §4(低优先短活,与 §1-3 不耦合)。

## Global Constraints

- 不改领域模型、不动 worker/Reconciler;只动 `trigger/TriggerEngine.java`、`dag/DagEngine.java`、及其 Bean 装配与持久层 finder。
- 批上限用构造函数注入(便于测试注入小批次),默认值 `200` 由 Beans 字面量传入;**不新增配置键**(spec §4 未要求,CLAUDE.md「不添加未被要求的可配置性」)。
- 每 tick 失败兜底保持现状(坏 cron 单独 try/catch 跳任务;循环层 ScanLoop/DagLoop 捕获 Throwable)。
- 判活/时钟不变:`clock.instant()` + DB `now()` 语义不动。
- 现有 `TriggerEngineTest`/`DagEngineTest` 用 5 参构造,批量 200 > 其测试集大小 → 单 tick 行为不变,不回归。

---

### Task 1: 持久层游标分批 finder

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/TaskRepository.java:9`、`JdbcTaskRepository.java:46`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/DagRepository.java:33,52`、`JdbcDagRepository.java:150,222`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcTaskRepositoryTest.java`、`JdbcDagRepositoryTest.java`

**Interfaces:**
- Produces: `List<Task> TaskRepository.findCronEnabledPage(long afterId, int limit)`; `List<Dag> DagRepository.findCronEnabledDagsPage(long afterId, int limit)`; `List<DagRun> DagRepository.findActiveRunsPage(long afterId, int limit)`。游标契约:返回 `id > afterId`、`ORDER BY id` 递增、至多 `limit` 行;`afterId=0` 表示从开头取。

- [ ] **Step 1: 写失败的持久层测试**

在 `JdbcTaskRepositoryTest` 增加分批断言(页序 + 跨页不漏):

```java
@Test void findCronEnabledPage_resumesPastCursor_withoutSkipping() {
  seedTask(); seedTask(); // 两条 enabled cron 任务(id 递增)
  var all = repo.findCronEnabledPage(0L, 1);
  assertEquals(1, all.size());
  assertEquals(repo.findAll().get(0).id(), all.get(0).id());
  var page2 = repo.findCronEnabledPage(all.get(0).id(), 1);
  assertEquals(1, page2.size());
  assertFalse(page2.get(0).id() == all.get(0).id()); // 第二页不重复第一页
  var tail = repo.findCronEnabledPage(page2.get(0).id(), 10);
  assertTrue(tail.isEmpty()); // 扫尽
}
```

在 `JdbcDagRepositoryTest` 增一条(游标续扫 + 空页回卷入口):

```java
@Test void findActiveRunsPage_resumesPastCursor_exhaustsToEmpty() { ... }
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn -o -pl scheduler-persistence test -Dtest=JdbcTaskRepositoryTest,JdbcDagRepositoryTest`
Expected: FAIL — `TaskRepository`/`DagRepository` 无 `findCronEnabledPage`/`findActiveRunsPage` 符号。

- [ ] **Step 3: 实现 finder(3 处 + 删旧 3 处)**

`TaskRepository.java` 接口(把旧 `findCronEnabled` 替换为):

```java
  /** 游标分批:返回 id>afterId 的 enabled+cron 任务,至多 limit 行;afterId=0 从头。配合 §4 扫描分批。 */
  List<Task> findCronEnabledPage(long afterId, int limit);
```

`JdbcTaskRepository.java` 实现:

```java
  @Override public List<Task> findCronEnabledPage(long afterId, int limit) {
    return jdbc.query(
        "SELECT * FROM app_task WHERE enabled AND NOT paused AND cron IS NOT NULL AND id > ?"
            + " ORDER BY id LIMIT ?", MAP, afterId, limit);
  }
```

`DagRepository.java` 把两条旧声明替换为:

```java
  /** 游标分批:返回 id>afterId 的 enabled cron DAG,至多 limit 行;afterId=0 从头。 */
  List<Dag> findCronEnabledDagsPage(long afterId, int limit);
  /** 游标分批:返回 id>afterId 的 PENDING 活跃 run,至多 limit 行;afterId=0 从头。 */
  List<DagRun> findActiveRunsPage(long afterId, int limit);
```

`JdbcDagRepository.java`(旧两法替换):

```java
  @Override public List<Dag> findCronEnabledDagsPage(long afterId, int limit) {
    return jdbc.query(
        "SELECT * FROM app_dag WHERE enabled AND NOT paused AND cron IS NOT NULL AND id > ?"
            + " ORDER BY id LIMIT ?", DAG_MAP, afterId, limit);
  }
  @Override public List<DagRun> findActiveRunsPage(long afterId, int limit) {
    return jdbc.query(
        "SELECT * FROM dag_run WHERE status='PENDING' AND id > ? ORDER BY id LIMIT ?",
        RUN_MAP, afterId, limit);
  }
```

`JdbcTaskRepositoryTest` 中旧 `findCronEnabled()` 的两处断言(第 19、21 行)改用 `findCronEnabledPage(0L, 10)`(含义不变:enabled 计数),否则旧法被删后编译失败。若 test 里有直接调用旧 DAG finder 也一并改页内法。

移除三个新法子无方引用的旧法:`findCronEnabled`、`findCronEnabledDags`、`findActiveRuns`(接口 + 实现)。

- [ ] **Step 4: 运行验证通过**

Run: `mvn -o -pl scheduler-persistence test -Dtest=JdbcTaskRepositoryTest,JdbcDagRepositoryTest`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add scheduler-persistence/src/main/java/dev/scheduler/persistence/{TaskRepository,JdbcTaskRepository,DagRepository,JdbcDagRepository}.java scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcTaskRepositoryTest.java
git commit -m "feat(persistence): 游标分批 finder — findCronEnabled/findActiveRuns 等改 id 分页"
```

---

### Task 2: 引擎游标分批扫描 + 装配 + 测试

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/trigger/TriggerEngine.java:35,40`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/dag/DagEngine.java:43,63,80`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java:90-99`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/trigger/TriggerEngineTest.java`、`scheduler-server/src/test/java/dev/scheduler/server/dag/DagEngineTest.java`

**Interfaces:**
- Consumes: 三个 `...Page(afterId, limit)` finder(Task 1)。`Limit.of()/Integer` 无跨任务新类型。
- Produces: 构造函数新增末参 `int scanBatchSize`(6 参)。Engine 私有游标字段 + 页推进逻辑。

- [ ] **Step 1: 写失败的引擎分批测试**

`TriggerEngineTest` 新增:造 `batchSize=1` 的引擎,两个 enabled cron 任务同 tick 只各触发其命中;两 tick 后两个任务都被触发且无重复 DUE。

```java
@Test void scanOnce_batchesAcrossTicks_coversAllTasksOnce() throws Exception {
  // 两个候选任务(minute 内各命中一次),batch=1 → 每 tick 至多处理一个。
  var engine = new TriggerEngine(tasks, executions, shards, leader, FIXED_CLOCK, 1);
  engine.scanOnce();                      // tick 1:第一页
  engine.scanOnce();                      // tick 2:第二页(游标推进)
  assertEquals(2, executions.count(),     // 两个任务各恰好一条 execution(不重)
      "batch=1 时两 tick 应覆盖两任务且不重复");
}
```

(若 `FIXED_CLOCK` 触发按分钟窗,两个任务 cron 相同则 tick 1/2 各落一条 → count==2 成立。实现里两任务用同一 `* * * * *` cron。)

`DagEngineTest` 新增:造 `batchSize=1`,三条活跃 run,一 tick 只推进第一个 run,三次 scanOnce(游标回卷一次)后三条都被传播(节点非 PENDING)。

- [ ] **Step 2: 运行验证失败**

Run: `mvn -o -pl scheduler-server -am test -Dtest=TriggerEngineTest,DagEngineTest`
Expected: FAIL — `TriggerEngine`/`DagEngine` 无 6 参构造;既有 5 参构造调用点仍编译通过但新测试造不出小批次。

- [ ] **Step 3: 实现引擎分批**

`TriggerEngine` 增字段 + 构造末参:

```java
  private final int batchSize;   // 单 tick 最多处理的任务数(§4 游标分批;页满推进、页未满回卷)
  private long lastTaskId;       // 游标:上次扫到的最大任务 id;回卷=0 表示下轮从头
```

`scanOnce` 体改为页循环 + 游标(其余逐任务逻辑不动):

```java
  public void scanOnce() {
    if (!leader.isLeader()) return;
    ZoneId zone = clock.getZone();
    ZonedDateTime now = clock.instant().atZone(zone);
    List<Task> page = tasks.findCronEnabledPage(lastTaskId, batchSize);
    for (Task t : page) {
      try {
        var cron = CronExpression.parse(t.cron());
        ZonedDateTime fired = cron.next(now.minusSeconds(61));
        if (fired != null && !fired.isAfter(now)) {
          shards.createParentWithShards(t.id(),
              IdempotencyKeys.forTrigger(t.id(), fired.toInstant()), t.shardCount());
        }
      } catch (RuntimeException badCron) {
        log.warn("skipping cron trigger for task {} due to bad cron '{}': {}",
            t.id(), t.cron(), badCron.toString());
      }
    }
    lastTaskId = (page.size() == batchSize) ? page.get(page.size() - 1).id() : 0L;
  }
```

`DagEngine` 增字段(构造末参 `int scanBatchSize`):

```java
  private final int batchSize;
  private long lastDagId;
  private long lastRunId;
```

`scanTriggers` 与 `scanPropagation` 段改页循环 + 各自游标(propagateRun 逐 run 逻辑不动):

```java
  private void scanTriggers() {
    ZoneId zone = clock.getZone();
    ZonedDateTime now = clock.instant().atZone(zone);
    List<Dag> page = dags.findCronEnabledDagsPage(lastDagId, batchSize);
    for (Dag d : page) {
      try {
        var cron = CronExpression.parse(d.cron());
        ZonedDateTime fired = cron.next(now.minusSeconds(61));
        if (fired != null && !fired.isAfter(now)) dags.createScheduledRun(d.id(), fired.toInstant());
      } catch (RuntimeException badCron) {
        log.warn("skipping cron trigger for dag {} due to bad cron '{}': {}",
            d.id(), d.cron(), badCron.toString());
      }
    }
    lastDagId = (page.size() == batchSize) ? page.get(page.size() - 1).id() : 0L;
  }

  private void scanPropagation() {
    List<DagRun> page = dags.findActiveRunsPage(lastRunId, batchSize);
    for (DagRun run : page) {
      List<DagRunNode> nodes = dags.findNodesOfRun(run.id());
      if (nodes.isEmpty()) continue;
      propagateRun(run, nodes);
    }
    lastRunId = (page.size() == batchSize) ? page.get(page.size() - 1).id() : 0L;
  }
```

`Beans.java` 两 bean 加末参 `200`:

```java
  @Bean
  TriggerEngine triggerEngine(TaskRepository tasks, ExecutionRepository execs,
                              ShardRepository shards, LeaderElection leader, Clock clock) {
    return new TriggerEngine(tasks, execs, shards, leader, clock, 200); // §4 单 tick 批上限(5s 周期 ×200)
  }
  @Bean
  DagEngine dagEngine(DagRepository dags, TaskRepository tasks, ShardRepository shards,
                      LeaderElection leader, Clock clock) {
    return new DagEngine(dags, tasks, shards, leader, clock, 200);
  }
```

既有测试的 5 参构造全部补 `, 200`(TriggerEngineTest 第 53/72/89/111/131 行;DagEngineTest 第 97 行 helper),使其语义不变。

- [ ] **Step 4: 运行验证通过**

Run: `mvn -o -pl scheduler-server -am test -Dtest=TriggerEngineTest,DagEngineTest`
Expected: PASS(含新增分批测试 + 既有全量)。

- [ ] **Step 5: 提交**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/trigger/TriggerEngine.java scheduler-server/src/main/java/dev/scheduler/server/dag/DagEngine.java scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java scheduler-server/src/test/java/dev/scheduler/server/trigger/TriggerEngineTest.java scheduler-server/src/test/java/dev/scheduler/server/dag/DagEngineTest.java
git commit -m "feat(server): 触发/DAG 扫描改游标分批 — 单 tick 不拖垮、大任务集不重不漏"
```

---

## Self-Review

- **Spec coverage:** §4 三个循环(TriggerEngine 任务、DagEngine DAG 触发、DagEngine 传播)全覆盖;游标 + 分批 + 崩中可续(幂等重扫)全落地。§1-3(reliability 主干)已合入,不受影响。
- **Placeholder scan:** 无 TBD/TODO;每步含可运行代码与确切文件:行。测试断言明确(非空断言)。
- **Type consistency:** 三 finder 签名 `(long afterId, int limit)` 引擎侧调用一致;`lastTaskId`/`lastDagId`/`lastRunId` 与各 finder 的 id 类型(long)一致;构造末参 `scanBatchSize` 两引擎一致(beans 传 200、测试传 1/200)。
- **已知取舍(记 ledger,非缺陷):** 批上限 200 × 5s 周期 → 全量 pass 时延 = ceil(N/200)×5s;超大任务集(数千+)下尾部任务的分钟窗触发可能被延迟跨窗。这是「单 tick 有界」与「及时触发」的固有权衡,§4 定位低优先短活、接受该权衡;幂等键保证不重复、不永久丢失。