# Alert Episode 子系统实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把库存 AlertsPage(纯前端快照)升级为后端 episode-driven 告警 —— 持续判定、PENDING/OPEN/RESOLVED 生命周期、按任务/DAG 可下钻、热键可调、可查历史并按时清理。

**Architecture:** 服务端 `AlertEngine`(每拍评价信号并对 `alert_episode` 单行做状态机迁移)由 leader 门控的 `Beans.AlertLoop extends ConfigurableLoop` 驱动;信号源从 `MetricsController` 抽成共享 `ExecutionMetrics` + 复用现有仓库(countDeadLetter/count/tasks/integrity/worker);阈值走 `scheduler.alert.*` 热键白名单;`AlertsController` 提供读开放 active/history 端点;前端 `AlertsPage` 改为渲染 episode 并加历史页签。

**Tech Stack:** Spring Boot(server)、JdbcTemplate(persistence)、React+Vite(web)、Flyway(V31 迁移)。

**Spec:** `docs/superpowers/specs/2026-09-29-scheduler-alert-episode-design.md` —— 本计划从 spec 论证,执行者须同时读 spec 与计划。冲突以 spec 为权威。

## Global Constraints

- **离线 Maven**:一律 `mvn -o`。**跑 server 测试必须 `-pl scheduler-server -am`**(否则命中 .m2 过期 persistence,NoSuchMethodError)。`-Dsurefire.failIfNoSpecifiedTests=false`。
- **模块职责**:`scheduler-persistence` 无 Jackson、无 server 容器类型;`Page<T>` 是 server.web 类型,persistence 只返回 `List<T>` + `count`,分页信封由 controller 组装(镜像 AuditController)。
- **共享 domain**:`dev.scheduler.core` 放 `AlertEpisode`/`AlertEpisodeView` 纯 record(persistence 与 server 共用)。
- **写收口**:所有 `app_runtime_config` 写走 `RuntimeConfigService.set`(白名单 + 类型校验 + 记 `runtime-config.update` 审计);读端点开放。
- **循环基建复用**:`ConfigurableLoop`(delay 热键重读 + suspend 门控 + catch Throwable 不杀线程 + destroy 停拍)与 `@ConditionalOnProperty` 现成模式,不另造。
- **背景竞态防护**:`ApiIntegrationTest` 顶层顶部 property 加 `scheduler.alert.enabled=false`,避免 AlertLoop 在测试里后台评价产生非预期 episode 行。
- **严重度**:audit-tamper=critical;dlq-depth/delivery-failed/task-failure-rate/dag-run-failed=high;failure-rate/p95-latency/worker-offline=medium。
- 提交信息以 `Co-Authored-By: Claude Code <noreply@anthropic.com>` 结尾。

---

### Task 1: V31 迁移 + core records + `AlertRepository`/`JdbcAlertRepository`

**Files:**
- Create: `scheduler-persistence/src/main/resources/db/migration/V31__alert_episode.sql`
- Create: `scheduler-core/src/main/java/dev/scheduler/core/AlertEpisode.java`
- Create: `scheduler-core/src/main/java/dev/scheduler/core/AlertEpisodeView.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/AlertRepository.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcAlertRepository.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcAlertRepositoryTest.java`

**Interfaces:**
- Produces: `AlertEpisode`/`AlertEpisodeView`(Task 4/5 消费)、`AlertRepository` 签名(Task 5/7 消费)、V31 表(Task 5 评价落点)。

- [ ] **Step 1: 写迁移文件**

`scheduler-persistence/src/main/resources/db/migration/V31__alert_episode.sql`:
```sql
CREATE TABLE alert_episode (
  id            bigserial PRIMARY KEY,
  key           text NOT NULL,
  rule          text NOT NULL,
  target_type   text NOT NULL DEFAULT 'none',
  target_id     bigint,
  severity      text NOT NULL,
  status        text NOT NULL DEFAULT 'PENDING',
  sample_count  int NOT NULL,
  value         text NOT NULL,
  opened_at     timestamptz,
  opened_value  text,
  resolved_at   timestamptz,
  resolved_value text,
  updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX alert_episode_active_key_uq
  ON alert_episode (key) WHERE status IN ('PENDING','OPEN');
CREATE INDEX alert_episode_status_idx ON alert_episode (status, updated_at);
```

- [ ] **Step 2: 写两个 core record**

`scheduler-core/.../core/AlertEpisode.java`(全字段,后 4 字段事务态可空):
```java
package dev.scheduler.core;
import java.time.Instant;
public record AlertEpisode(
    long id, String key, String rule, String targetType, Long targetId, String severity,
    String status, int sampleCount, String value,
    Instant openedAt, String openedValue, Instant resolvedAt, String resolvedValue) {}
```
`scheduler-core/.../core/AlertEpisodeView.java`(controller 对外形状,查询 join targetName 后组装;resolvedAt/resolvedValue OPEN 时 null,openedValue OPEN 时非 null):
```java
package dev.scheduler.core;
import java.time.Instant;
public record AlertEpisodeView(
    long id, String rule, String targetType, Long targetId, String targetName,
    String severity, String status, String value,
    Instant openedAt, String openedValue, Instant resolvedAt, String resolvedValue) {}
```

- [ ] **Step 3: 写 `AlertRepository` 接口**

```java
package dev.scheduler.persistence;
import dev.scheduler.core.AlertEpisode;
import dev.scheduler.core.AlertEpisodeView;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
public interface AlertRepository {
  Optional<AlertEpisode> findActiveByKey(String key);   // WHERE key=? AND status<>'RESOLVED'
  AlertEpisode insert(AlertEpisode e);                   // 返回含自增 id 的行
  void update(AlertEpisode e);                           // 按 id 覆盖 mutable 列 + updated_at=now()
  void deleteByKey(String key);                          // 仅删活跃行(PENDING 未确认回落的 DELETE 用)
  long pruneResolved(Instant cutoff);                    // DELETE RESOLVED AND resolved_at < cutoff,返回行数
  List<AlertEpisodeView> findActive();                   // status='OPEN'
  List<AlertEpisodeView> findResolved(int limit, int offset); // status='RESOLVED',opened_at DESC
  long countResolved();                                  // findResolved 同过滤全量计数
}
```

- [ ] **Step 4: 写 `JdbcAlertRepository`**

- `insert`: `INSERT (key,rule,target_type,target_id,severity,status,sample_count,value) VALUES (...) RETURNING id,updated_at`,构造带 id 的 AlertEpisode。
- `update`: `UPDATE alert_episode SET status=?,sample_count=?,value=?,opened_at=?,opened_value=?,resolved_at=?,resolved_value=? WHERE id=?`(updated_at 由 SQL `now()` 落)。
- `findActiveByKey`、`findActive`、`findResolved`(LEFT JOIN 出 target_name)、`countResolved`、`pruneResolved`、`deleteByKey` 各齐。行映射:psql 返回 text 列映射 String,`target_id` 可空用 `rs.getObject("target_id", Long.class)`;时间可空同样 `getObject(..., java.sql.Timestamp)?.toInstant()`。
- **join 视图统一用**:
```sql
SELECT e.id, e.rule, e.target_type, e.target_id, e.severity, e.status, e.value,
       e.opened_at, e.opened_value, e.resolved_at, e.resolved_value,
       CASE e.target_type WHEN 'task' THEN t.name WHEN 'dag' THEN d.name ELSE NULL END AS target_name
  FROM alert_episode e
  LEFT JOIN app_task t ON e.target_type='task' AND e.target_id=t.id
  LEFT JOIN app_dag  d ON e.target_type='dag'  AND e.target_id=d.id
  WHERE e.status = ?
  ORDER BY e.opened_at DESC
```
`findResolved` 尾部拼 `LIMIT ? OFFSET ?`;`countResolved` 为 `SELECT count(*) ... WHERE e.status='RESOLVED'`。

- [ ] **Step 5: 写单测 `JdbcAlertRepositoryTest`**(Testcontainers postgres,镜像现 persistence 测试)

- insert→findActiveByKey 往返(key/rule/status=PENDING/sample_count=1)。
- 同 key 第二条非终态 insert → 预期 `DuplicateKeyException`(partial unique 生效)。
- update 迁移到 RESOLVED → findActive 为空、findResolved 含行长(id DESC 可稳定选一条)。
- resolve 后同 key 再 insert 新 PENDING → 允许(partial unique 释放),且 findResolved 现两条历史。
- pruneResolved:插一条 resolved_at=now()-90days 一条 now(),cutoff=now()-30days → 删 1、另一条在。
- deleteByKey:insert 活跃行后 delete → findActiveByKey empty。

- [ ] **Step 6: 跑测**

Run: `cd /d/ai-project/scheduler && mvn -o -q -pl scheduler-persistence -am test -Dtest='JdbcAlertRepositoryTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS。

- [ ] **Step 7: 提交**

```bash
git add -A && git commit -m "feat(alert): V31 alert_episode + AlertRepository/Jdbc impl"
```

---

### Task 2: 抽取 `ExecutionMetrics`(MetricsController 委托)

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/service/ExecutionMetrics.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/MetricsController.java`
- Test: 复用 `ApiIntegrationTest#executionSlo...`(应零改动过)

**Interfaces:**
- Produces: `ExecutionMetrics`(含嵌套 record `ExecutionSlo`/`TaskMetric`/`RecentFailures`)—— Task 5(AlertEngine 信号源)与 MetricsController 共用。

- [ ] **Step 1: 建 `ExecutionMetrics` service**

把 `MetricsController` 的 `snapshots` / `parentPct` / `perTask` 三方法 + 三个嵌套 record(`ExecutionSlo`/`TaskMetric`/`RecentFailures`)原样迁到 `scheduler-server` 的 `dev.scheduler.server.service.ExecutionMetrics`,保留 `JdbcTemplate jdbc` 构造注入、`@Component` 标注、`snapshots(executionSlo)(Integer windowSeconds)` 公开方法、字段名/JSON 输出逐字节不变(前端 `ExecutionSlo` 类型零改动)。

- [ ] **Step 2: `MetricsController` 改为委托**

- 注入 `ExecutionMetrics metrics`;删除本地 SQL 与 record;`snapshots` 改为 `return metrics.snapshots(windowSeconds)`。

- [ ] **Step 3: 跑 SLO 相关测试确认无回归**

Run: `mvn -o -q -pl scheduler-server -am test -Dtest='ApiIntegrationTest#executionSlo_snapshotAggregates+' -Dsurefire.failIfNoSpecifiedTests=false`(用例名按现存 `MetricsController` 测试取,若无则跑整个 ApiIntegrationTest)
Expected: PASS,前端类型不动。

- [ ] **Step 4: 提交**

```bash
git add -A && git commit -m "refactor(metrics): extract ExecutionMetrics service shared by SLO + alerts"
```

---

### Task 3: per-DAG 失败信号补齐(`DagRepository.perDagFailedRunsSince`)

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/DagRepository.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcDagRepository.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcDagRepositoryTest.java`

**Interfaces:**
- Produces: `DagRepository.perDagFailedRunsSince(Instant since)` → `List<DagRunFailedCount>(long dagId, long count)`(Task 5 消费)。

- [ ] **Step 1: 接口加方法 + 新 record**

```java
/** 近窗内每 DAG 终态 FAILED 的 dag_run 计数(告警 per-dag 信号源);since 为窗口起点(含)。 */
List<DagRunFailedCount> perDagFailedRunsSince(Instant since);
record DagRunFailedCount(long dagId, long count) {}
```

- [ ] **Step 2: Jdbc 实现**

```sql
SELECT dag_id, count(*) FROM dag_run
 WHERE status='FAILED' AND finished_at >= ?
 GROUP BY dag_id ORDER BY dag_id
```
(`dag_run.finished_at` 为终态时刻,「近窗失败批次」语义正确;`idx_dag_run_dag_status (dag_id,status)` 覆盖该查询。)

- [ ] **Step 3: 单测 `perDagFailedRunsSince`**

Testcontainers:插 2 个 DAG、若干 dag_run(1 各 FAILED、1 离窗口)、窗口内 FAILED 计数正确、窗口外不计。

- [ ] **Step 4: 跑测**

Run: `mvn -o -q -pl scheduler-persistence -am test -Dtest='JdbcDagRepositoryTest' -Dsurefire.failIfNoSpecifiedTests=false && mvn -o -q -pl scheduler-server -am test -Dtest=ApiIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false`

- [ ] **Step 5: 提交**

```bash
git add -A && git commit -m "feat(alert): per-DAG failed-run count signal in DagRepository"
```

---

### Task 4: `RuntimeConfigKeys` 12 个 alert 键 + `RuntimeConfigService.getDouble`

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/RuntimeConfigKeys.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/service/RuntimeConfigService.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/service/RuntimeConfigServiceTest.java`

**Interfaces:**
- Produces: 常量 + `getDouble(String,double)`(Task 5 读比率阈值用)、白名单 DEFAULTS 条目(Hot Setting 页展示用)。

- [ ] **Step 1: `RuntimeConfigKeys` 添加常量与 DEFAULTS**

```java
public static final String ALERT_DELAY            = "alert.delay-ms";
public static final String ALERT_OPEN_SAMPLES     = "alert.open-samples";
public static final String ALERT_RECOVER_SAMPLES  = "alert.recover-samples";
public static final String ALERT_HISTORY_KEEP_DAYS= "alert.history-keep-days";
public static final String ALERT_AUDIT_TAMPER     = "alert.audit-tamper";
public static final String ALERT_DLQ_DEPTH        = "alert.dlq-depth";
public static final String ALERT_DELIVERY_FAILED  = "alert.delivery-failed";
public static final String ALERT_FAILURE_RATE     = "alert.failure-rate";
public static final String ALERT_P95_LATENCY_MS   = "alert.p95-latency-ms";
public static final String ALERT_WORKER_OFFLINE   = "alert.worker-offline";
public static final String ALERT_TASK_FAILURE_RATE= "alert.task-failure-rate";
public static final String ALERT_DAG_RUN_FAILED   = "alert.dag-run-failed";
```
`DEFAULTS.put` 依次:`15000`、`3`、`3`、`7`、`1`、`1`、`1`、`0.05`、`5000`、`1`、`0.2`、`1`。

- [ ] **Step 2: `RuntimeConfigService.getDouble`**

镜像 `getLong`,用 `Double.parseDouble`;`NumberFormatException` → log.warn + 回落 fallback。加注释「比率阈值(failure-rate 等)用」。

- [ ] **Step 3: `validateValue` 支持比率键**

在现 `ms/sec/days` 分支外补:
```java
} else if (key.equals(RuntimeConfigKeys.ALERT_FAILURE_RATE)
        || key.equals(RuntimeConfigKeys.ALERT_TASK_FAILURE_RATE)) {
  Double.parseDouble(value);
}
```
(其余 alert 计数/毫秒键由原数字分支或 getLong 解析兜底;`alert.open-samples` 等整型走 `interval '1 second'` 无关,getLong 解析即可,不强制在该分支。)

- [ ] **Step 4: 单测**

- 各新键 `RuntimeConfigKeys.isKnown` 为 true;`defaultOf` 正确(`0.05`/`5000`/`3`/`7`)。
- `getDouble(ALERT_FAILURE_RATE, 0.1)` 在 DB 写 `0.05` 时返回 `0.05`;写 `abc` 回落 `0.1`。
- `set(ALERT_FAILURE_RATE, "0.2", op)` 成功;`set(ALERT_FAILURE_RATE, "abc")` → IllegalArgumentException。
- 现 `RuntimeConfigKeysTest`(若存在断言键集合)若硬编码列表则追加 12 键。

- [ ] **Step 5: 跑测**

Run: `mvn -o -q -pl scheduler-server -am test -Dtest='RuntimeConfigServiceTest,RuntimeConfigKeysTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add -A && git commit -m "feat(config): scheduler.alert.* hot keys + getDouble for rate thresholds"
```

---

### Task 5: `AlertEngine`(状态机核心)

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/service/AlertEngine.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/service/AlertEngineTest.java`

**Interfaces:**
- Consumes: `ExecutionMetrics`(Task2)、`ShardRepository.countDeadLetter()`、`NotificationRepository.count(null,"FAILED")`、`AuditRepository.integrity()`、`WorkerRepository.findAllAlive(threshold)`、`DagRepository.perDagFailedRunsSince`(Task3)、`RuntimeConfigService`(Task4)、`AlertRepository`(Task1)。
- Produces: `AlertEngine.evaluateOnce()`(Task6 AlertLoop 每拍调)。

- [ ] **Step 1: 写 `AlertEngine`(含完整状态机)**

`@Component`,`MIN_TOTAL = 3` 常量。私有 record `Signal(String rule, String targetType, Long targetId, String severity, String value, boolean active)` 与 `Rule(String ruleId, String severity, String targetType)`。

`evaluateOnce()`:
```java
public void evaluateOnce() {
  int open = Math.max(Math.toIntExact(settings.getLong(RuntimeConfigKeys.ALERT_OPEN_SAMPLES, 3L)), 1);
  int recover = Math.max(Math.toIntExact(settings.getLong(RuntimeConfigKeys.ALERT_RECOVER_SAMPLES, 3L)), 1);
  long keepDays = settings.getLong(RuntimeConfigKeys.ALERT_HISTORY_KEEP_DAYS, 7L);
  if (keepDays > 0) repo.pruneResolved(Instant.now().minus(Duration.ofDays(keepDays)));
  for (Signal s : collectSignals()) handle(s, open, recover);
}
```

`collectSignals()` 组装 8 条规则(规则 id/severity/target 按 Global Constraints;每条规则阈值读 settings,阈=0/禁用即跳过):
- `audit-tamper`:活性=`!audits.integrity().verified`;`value` = `chained+"/"+total`;阈 `getFlag(ALERT_AUDIT_TAMPER, true)`。
- `dlq-depth`:`depth=shards.countDeadLetter()`;阈 `getLong(ALERT_DLQ_DEPTH,1)`;阈≤0 跳过;活性=`depth >= thr`;`value`=depth。
- `delivery-failed`:`n=notifs.count(null,"FAILED")`;阈 `getLong(ALERT_DELIVERY_FAILED,1)`;阈≤0 跳过;活性=`n >= thr`;`value`=n。
- `failure-rate`:总=Σ perTask.throughput,坏=Σ bad;`total<MIN_TOTAL` → 跳过;`rate=bad/total`;阈 `getDouble(ALERT_FAILURE_RATE,0.05)`;阈≤0 跳过;活性=`rate > thr`;`value`=格式 `String.format("%.0f%%", rate*100)`。
- `p95-latency`:`p95=metrics.p95`;阈 `getLong(ALERT_P95_LATENCY_MS,5000)`;阈≤0 跳过;活性=`p95 > thr`;`value`=ms。
- `worker-offline`:`激活=workers.findAllAlive(now - 30s).isEmpty()`;阈 `getFlag(ALERT_WORKER_OFFLINE,true)`;`value`=`全部下线`/`存活`。
- `task-failure-rate`(per-task):遍历 `metrics.perTask()`,对每项 `t`:total=throughput;`total<MIN_TOTAL` → 跳过;`rate=1-successRate`;阈 `getDouble(ALERT_TASK_FAILURE_RATE,0.2)`;阈≤0 跳过;活性=`rate > thr`;target=task,targetId=t.taskId,`value`=rate%;targetName 交给视图 join。
- `dag-run-failed`(per-dag):遍历 `dags.perDagFailedRunsSince(now - 1h)`(窗口 3600s 与 SLO 一致),每项活性=`count >= thr`(阈 `getLong(ALERT_DAG_RUN_FAILED,1)`,≤0 跳过),target=dag,targetId=dagId,`value`=count。无 dag_run 的 DAG 无 信号(不产生)。

`handle(Signal s, int open, int recover)`(_keyOf_ 全局=`s.rule`,per=`s.rule+":task:"+id` / `+":dag:"+id`):
```java
Optional<AlertEpisode> ex = repo.findActiveByKey(key);
if (ex.isEmpty()) { if (s.active()) repo.insert(newPENDING(key, s)); return; }
AlertEpisode e = ex.get();
if (s.active()) {
  int next = e.sampleCount() > 0 ? e.sampleCount() + 1 : 1;
  if ("PENDING".equals(e.status()) && next >= open) {
    repo.update(with(key, s, "OPEN", next, e, Instant.now(), s.value(), null, null));
  } else {
    repo.update(with(key, s, "PENDING", next, e, null, null, null, null));
  }
} else {
  int next = e.sampleCount() < 0 ? e.sampleCount() - 1 : -1;
  if ("OPEN".equals(e.status()) && next <= -recover) {
    repo.update(with(key, s, "RESOLVED", next, e, e.openedAt(), e.openedValue(), Instant.now(), s.value()));
  } else if ("PENDING".equals(e.status()) && next <= -recover) {
    repo.deleteByKey(key); // 未确认即回落 → 无历史
  } else {
    repo.update(with(key, s, e.status(), next, e, e.openedAt(), e.openedValue(), e.resolvedAt(), e.resolvedValue()));
  }
}
```
私有 `newPENDING`/`with` 构造 AlertEpisode(保留既有 id/时间,迁移状态字段),`sampleCount` 永不越界(活跃分支无上限无害,反向分支以 -recover 为界)。

自审项:状态列与 unique index 一致;`["PENDING"→"OPEN"]` 仅在 `next>=open` 发生;反方向 OPEN/PENDING 均须 `next<=-recover` 才迁移/删除。

- [ ] **Step 2: 写 `AlertEngineTest`**(自足,settings 用 in-memory RuntimeConfigService 假仓储——镜像 LoopWiringTest/既有 service 单测手法;各依赖用轻量假实现或最小 stub)

核心用例**全部为状态机断言**(in-memory AlertRepository 假实现记录 upsert 序列以简化验证):
- 单拍 active(PENDING s=1)后下一拍 !active(PENDING s=-1 且未达 -recover)→ 行仍在、status 仍 PENDING、不入 OPEN。
- 连续 active 到 s=open → status=OPEN、opened_at/opened_value 落。
- OPEN 后连续 !active 到 -recover → RESOLVED、resolved_at/resolved_value 落。
- PENDING 未确认即连续 !active 到 -recover → 行 delete(无 RESOLVED)。
- threshold 0/停用规则 → collectSignals 不产出该规则信号。
- per-task:两个任务同超阈 → 两个独立 key episode;一个超阈一个不超 → 只 1 个。
- per-dag 同 semantics。
- doOnce 含 pruneResolved:注入一个 resolved 行超 keep-days → 被删。

- [ ] **Step 3: 跑测**

Run: `mvn -o -q -pl scheduler-server -am test -Dtest='AlertEngineTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS。

- [ ] **Step 4: 提交**

```bash
git add -A && git commit -m "feat(alert): AlertEngine episode state machine"
```

---

### Task 6: `Beans.AlertLoop` + bean 装配 + 后台竞态防护

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`
- Modify: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`(顶部 property + 至多一个探活断言)
- Test: `scheduler-server/src/test/java/dev/scheduler/server/config/LoopWiringTest.java`(可选补 alert 键断言)

**Interfaces:**
- Consumes: `AlertEngine`(Task5)、`RuntimeConfigService`、`LeaderElection`。
- Produces: `AlertLoop` bean(`@ConditionalOnProperty scheduler.alert.enabled`,缺省 true)—— Task7 前后台接管。

- [ ] **Step 1: `Beans` 加 `AlertRepository`/`ExecutionMetrics`/`AlertEngine`/`AlertLoop` bean**

```java
@Bean AlertRepository alertRepository(JdbcTemplate jdbc) { return new JdbcAlertRepository(jdbc); }

@Bean ExecutionMetrics executionMetrics(JdbcTemplate jdbc) { return new ExecutionMetrics(jdbc); }

@Bean AlertEngine alertEngine(ExecutionMetrics metrics, ShardRepository shards,
    NotificationRepository notifs, AuditRepository audits, WorkerRepository workers,
    DagRepository dags, RuntimeConfigService settings, AlertRepository alertRepo) {
  return new AlertEngine(metrics, shards, notifs, audits, workers, dags, settings, alertRepo);
}
```
`AlertLoop` 内嵌类 + bean:
```java
/** 告警评价循环:leader 门控,周期驱动 AlertEngine.evaluateOnce();失败兜底不杀线程(镜像 NotificationLoop)。 */
public static final class AlertLoop extends ConfigurableLoop {
  private final AlertEngine engine; private final LeaderElection leader;
  AlertLoop(RuntimeConfigService settings, AlertEngine engine, LeaderElection leader) {
    super(settings, RuntimeConfigKeys.ALERT_DELAY, 15000L);
    this.engine = engine; this.leader = leader; start();
  }
  @Override protected void loopOnce() { if (!leader.isLeader()) return; engine.evaluateOnce(); }
}
@Bean @ConditionalOnProperty(name = "scheduler.alert.enabled", havingValue = "true", matchIfMissing = true)
AlertLoop alertLoop(RuntimeConfigService runtimeConfigService, AlertEngine engine, LeaderElection leader) {
  return new AlertLoop(runtimeConfigService, engine, leader);
}
```
(Beans 已注入 `LeaderElection`,`@ConditionalOnProperty` 用现 import。)

- [ ] **Step 2: `ApiIntegrationTest` 顶部加 `scheduler.alert.enabled=false`**

阅现顶部 `@SpringBootTest(properties = {...})` 的单个 properties 块,追 `"scheduler.alert.enabled=false"`(防 AlertLoop 测试期后台评价产生非预期 episode 行)。

- [ ] **Step 3: 探活断言(可选)**

`LoopWiringTest` 若已有 7 环 delay 断言,给 `alertLoop` 构造器形参(延用 `EMPTY` settings + null engine/leader,`loopOnce` 不运行)加同手法断言 `delayKey=="alert.delay-ms"`、fallback 15000,以与其它循环一致;若该测试不覆盖新键则跳过,不强加。

- [ ] **Step 4: 跑测回归**

Run: `mvn -o -q -pl scheduler-server -am test -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 全绿(新增 alert.enabled=false 后无后台竞态)。

- [ ] **Step 5: 提交**

```bash
git add -A && git commit -m "feat(alert): leader-gated AlertLoop bean + test side background guard"
```

---

### Task 7: `AlertsController` — GET active / history

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/web/AlertsController.java`
- Modify: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`
- Test: `ApiIntegrationTest`(新增 `alertsRoundTrip_...` 用例)

**Interfaces:**
- Consumes: `AlertRepository`(Task1 视图查询)。Produces: `GET /api/v1/alerts/active` + `GET /api/v1/alerts/history`,读开放。

- [ ] **Step 1: 控制器**

```java
@RestController @RequestMapping("/api/v1/alerts")
public class AlertsController {
  private final AlertRepository alerts;
  public AlertsController(AlertRepository alerts) { this.alerts = alerts; }

  @GetMapping("/active")
  public List<AlertEpisodeView> active() { return alerts.findActive(); }

  @GetMapping("/history")
  public Page<AlertEpisodeView> history(
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    List<AlertEpisodeView> items = alerts.findResolved(p.limit(), p.offset());
    return new Page<>(items, alerts.countResolved(), p.offset(), p.limit());
  }
}
```
复用 `Paging.of`/`Page<>(items,total,offset,limit)`(镜像 AuditController)。

- [ ] **Step 2: 集成测试(镜像 notification 测试手法)**

`ApiIntegrationTest` 新增用例,resetDb 已清 alert_episode(见下 Step 3)后:
- 直插两条:`jdbc.update("INSERT INTO alert_episode(key,rule,target_type,target_id,severity,status,sample_count,value,opened_at) VALUES('task-failure-rate:task:1','task-failure-rate','task',1,'high','OPEN',5,'33%','2026-09-29T00:00:00Z')")` + 一条 RESOLVED。
- `GET /active` → 长度 1、深 assert `$[0].rule==task-failure-rate`、`$[0].targetType==task`、`targetName` 非空(task 1 若存在)、`status==OPEN`、`value=="33%"`、`openedAt` 在、`resolvedAt` null。
- `GET /history` → total≥1、`status==RESOLVED`、`resolvedAt` 非 null。
- 空态清表后 `GET /active` → `[]`、`GET /history` → total 0、items 空。

- [ ] **Step 3: `JdbcAuditRepository` 同款 resetDb 补 TRUNCATE alert_episode**

`ApiIntegrationTest.resetDb()` 的 TRUNCATE 串追加 `alert_episode`(每用例隔离)。

- [ ] **Step 4: 跑测**

Run: `mvn -o -q -pl scheduler-server -am test -Dtest='ApiIntegrationTest#alertsRoundTrip_...' -Dsurefire.failIfNoSpecifiedTests=false && mvn -o -q -pl scheduler-server -am test -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 新用例 PASS,全套回归绿。

- [ ] **Step 5: 提交**

```bash
git add -A && git commit -m "feat(alert): AlertsController GET active + history (read-open)"
```

---

### Task 8: 前端 —— AlertsPage 渲染 episode + 历史页签

**Files:**
- Modify: `web/src/api/client.ts`
- Modify: `web/src/api/types.ts`
- Modify: `web/src/pages/AlertsPage.tsx`

**Interfaces:**
- Consumes: `GET /api/v1/alerts/active`、`GET /api/v1/alerts/history`(Task7)。

- [ ] **Step 1: types + client**

`types.ts`:
```ts
export interface AlertEpisodeView {
  id: number; rule: string; targetType: string; targetId: number | null;
  targetName: string | null; severity: 'critical' | 'high' | 'medium';
  status: 'PENDING' | 'OPEN' | 'RESOLVED'; value: string;
  openedAt: string | null; openedValue: string | null;
  resolvedAt: string | null; resolvedValue: string | null;
}
```
`client.ts`:
```ts
export const listAlertEpisodes = (): Promise<AlertEpisodeView[]> => req('/api/v1/alerts/active');
export const listAlertHistory = (p: { limit?: number; offset?: number } = {}): Promise<Page<AlertEpisodeView>> =>
  req(`/api/v1/alerts/history${qstr(p)}`);
```
import 加 `AlertEpisodeView`。

- [ ] **Step 2: 重写 `AlertsPage`**

- 删掉前端「算告警」(阈值常量/gauge 聚合/`fetchMetrics`/`getAuditIntegrity`/`getDlq`/`listNotifications` 的本地逻辑),改为 `useState<AlertEpisodeView[]> active` + `useState<Page<...>> history` + `useEffect/useInterval(tick,5000) load active`。
- 规则 id → 人话标题映射常量(镜像 NotificationsPage KIND_LABELS):`audit-tamper`→审计取证链、`dlq-depth`→DLQ 积压、`delivery-failed`→通知投递失败、`failure-rate`→执行失败率、`p95-latency`→父延迟偏高、`worker-offline`→worker 全部离线、`task-failure-rate`→任务失败聚集、`dag-run-failed`→DAG 批次失败;未知回落原文。
- 顶部绿/红横幅:`active.length===0` → 全绿;否则「`{n}` 项需要关注」。active 为空且 history 加载失败时展示空态卡。
- triage 列表每行:severity 点/标签、标题、`targetName`(task/dag 名,否则全局)、当前值 `value`、`openedAt?.toLocaleString()`「始于」、`samples`(取 `value` 之外无需;仅展示 `openedAt`)、下钻链接 `Link`:沿用现 AlertsPage 的 `href` 映射(rule→所属页,见下)。
- **任务范围页签**:`activeTab = 'active'|'history'`;history 页签渲 `listAlertHistory` + `Pager`。history 行多展示 `resolvedAt`、`resolvedValue`。
- 下钻链路:`rule`→ `/audits | /dlq | /notifications | /metrics` 映射(unknown→`/alerts`)。

- [ ] **Step 3: 前端构建**

Run: `cd /d/ai-project/scheduler/web && npm run build`
Expected: `tsc -b && vite build` 绿(模块数 +1)。

- [ ] **Step 4: 提交**

```bash
git add -A && git commit -m "feat(web): AlertsPage renders episodes + history tab"
```

---

### Task 9: 全量验收 + OpenAPI 快照 + spec 标记实现

**Files:**
- Modify: `docs/superpowers/specs/2026-09-29-scheduler-alert-episode-design.md`(文件头加「已实现」行)
- Modify: `docs/openapi/api-docs.json`(属性门控重导)
- Constraint 备忘:内存 `scheduler-v3` 相关。

- [ ] **Step 1: 全套测试**

Run: `cd /d/ai-project/scheduler && mvn -o -q -pl scheduler-server -am test -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 全绿(251 + 新增)。

- [ ] **Step 2: OpenAPI 快照重导**

Run: `mvn -o -q -pl scheduler-server -am -Dtest=ApiIntegrationTest#openApiSnapshot_exportAsCommittableArtifact -Dscheduler.openapi.dump="$(cygpath -w "$PWD/docs/openapi/api-docs.json")" test -Dsurefire.failIfNoSpecifiedTests=false`
Verify: `grep -o '"/api/v1/alerts[^"]*"' docs/openapi/api-docs.json` 含 `/active`、`/history`。

- [ ] **Step 3: spec 标记**

`2026-09-29-scheduler-alert-episode-design.md` 标题下加一行:`> **状态**:已实现(2026-09-29)。`

- [ ] **Step 4: 前端构建确认**

Run: `cd /d/ai-project/scheduler/web && npm run build`
Expected: 绿。

- [ ] **Step 5: 提交**

```bash
git add -A && git commit -m "chore(alert): full qa + openapi snapshot + spec marked implemented"
```

---

## 自审(对照 spec)

- **§1 目标1(生命周期)**:Task5 状态机 + Task1 表 → OK。
- **§1 目标2(粒度)**:Task5 per-task/per-dag 信号 + Task3 dag 计数 → OK。
- **§1 目标3(热键)**:Task4 12 键 + Task5 每拍读 → OK;热键页展示由 DEFAULTS 自动。
- **§1 目标4(历史清理)**:Task5 prune(keep-days)+ Task7 history → OK。
- **§1 目标5(前端渲染)**:Task8 → OK。
- **§3 目录 8 规则**:Task5 collectSignals 全列,严重度/阈 0 禁用一致 → OK。
- **§4 状态机**:Task5 实现与其一致(带符号 sample_count、open/recover、PENDING 未确认 DELETE)→ OK。
- **§5 热键表**:Task4 值与 spec 表对齐。
- **§6 ExecutionMetrics**:Task2 抽取,评价与 SLO 同源 → OK。
- **接口一致性**:`AlertEpisode`/`AlertEpisodeView`(Task1)、`executionMetrics.snapshots/windowSeconds`、`getDouble`(Task4)、`perDagFailedRunsSince`(Task3)在 Task5 恰呼——签名一致。
- **测试完备性**:Task1 表/unique、Task4 热键解析、Task5 状态机、Task7 端点、Task8 前端构建——每 task 独立可验收。