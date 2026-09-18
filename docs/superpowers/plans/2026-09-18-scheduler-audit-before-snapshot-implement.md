# 操作审计子项目3 — before 内容快照(before_meta)实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `app_audit` 追加 `before_meta JSONB` 列,为 5 个 `task.*` 动作(update/pause/resume/delete/trigger)落**操作前全量 Task 定义**快照,供取证回溯/delete 后还原;读 API `AuditEntry` 末尾累进 `before`。

**Architecture:** V12 append-only 加单列;`TaskController` 各写 handler 就地捕获旧态(`update` 用已有 `existing`,`pause`/`resume`/`delete`/`trigger` 用 `requireTask`),新增 `taskBefore(Task)` 产全 16 组件 LinkedHashMap,经 `AuditRecorder` serialize 落 JSON 文本。overload 链在 server 层收敛:**public 8-arg `(meta,diff,before)` 取代 7-arg `(meta,diff)`**(update 唯一调用者迁移后 7-arg 弃置删);persistence 抽象 record 变 8-arg(加 `beforeJson`),6-arg default 委托 before=null,其余调用点零改动。

**Tech Stack:** Java 17、Spring Boot、PostgreSQL 16、Testcontainers、既有 `AuditRepository`/`AuditRecorder`/`AuditEntry`/`Page`、既有 React 审计页。无新增依赖。

**Spec:** `docs/superpowers/specs/2026-09-18-scheduler-audit-before-snapshot-design.md`

## Global Constraints

- **持久层无 Jackson(贯穿 spec)**:meta/diff/before 均 raw JSON 文本 String 贯穿;Jackson 序列化仅 server 层 `AuditRecorder.serialize(Map)`。持久层测试对 JSONB 读回用**去空白语义比较** `" ".replaceAll("\\s+","")`,绝不写精确 JSON 串(PG jsonb 规范化空白,已确立裁决)。
- **审计非阻断**:序列化/落库失败只 `log.warn`,不抛、不改操作结果、不回滚(insert 已在私有 7-arg 的 try/catch 内)。
- `AuditEntry` core 末尾累进 `before`(String,raw JSON 或 null),变 **10 组件**:`(id, occurredAt, operator, action, targetType, targetId, meta, source, diff, before)`;JSON 字段序 meta、source、diff、before,既有位置相对子项目1/2 不变。
- **overload 裁决**:persistence `AuditRepository` 抽象 record 8-arg `(operator, action, targetType, targetId, metaJson, diffJson, beforeJson, source)`,default 6-arg `(… metaJson, source)` 委托 `beforeJson=null`;**无 7-arg default(避免弃置)**;**server 层删 public `record(… Map meta, Map diff)`**(update 迁移 8-arg 后唯一调用者消失);`record(… Map meta)` 5-arg public 保留(其余 9+ 调用点零改动)。
- 仅 5 个 `task.*` 动作 before 非 NULL;`task.create`(无 before)与其余动作恒 NULL。
- 三列语义互不重载:`meta`=后态紧凑,`diff`=字段级变差,`before`=前态全量。
- V12 append-only:`ALTER TABLE app_audit ADD COLUMN before_meta JSONB NULL;` 只加列,不重建索引,不触既有行。
- Maven 离线 `mvn -o`;`-am` 多模块测试需 `-Dsurefire.failIfNoSpecifiedTests=false`。

---

### Task 1: 持久层 — V12 + `AuditEntry.before` + 8-arg record + mapper + 测试

**Files:**
- Create: `scheduler-persistence/src/main/resources/db/migration/V12__audit_before_meta.sql`
- Modify: `scheduler-core/src/main/java/dev/scheduler/core/AuditEntry.java:10-19`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/AuditRepository.java:10-18`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcAuditRepository.java:15-26`
- Modify: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcAuditRepositoryTest.java`

**Interfaces:**
- Consumes: 既有 `AuditEntry`(9 组件)、既有 abstract `record` 7-arg。
- Produces: `AuditEntry` 10 组件末尾 `before`;`AuditRepository.record` abstract 8-arg `(… metaJson, diffJson, beforeJson, source)` + 6-arg default;`JdbcAuditRepository` INSERT 含 `before_meta`、mapper 读 `before`。Task 2 靠它落 before、读 API 透出。

- [ ] **Step 1: V12 迁移(仅加列)**

Create `scheduler-persistence/src/main/resources/db/migration/V12__audit_before_meta.sql`:
```sql
ALTER TABLE app_audit ADD COLUMN before_meta JSONB NULL;
```

- [ ] **Step 2: `AuditEntry` core 末尾加 `before`(10 组件)**

Modify `AuditEntry.java:10-19` — record 声明加 `String before,` 于 `diff` 后一行,注释说明:
```java
    String diff,
    String before) {}
```
并更新类注释行 7(field-line)提及「before 为操作前全量 Task 的 JSON 文本或 null(仅 task.* 动作)」。

- [ ] **Step 3: `AuditRepository` 抽象 record 改 8-arg(加 beforeJson),6-arg default 委托 before=null**

Modify `AuditRepository.java` — 抽象签名加 `String beforeJson`(在 diffJson 后、source 前),Javadoc 增「beforeJson 为操作前全量 Task 快照的 JSON 文本或 null」;6-arg default 的委托调用补 `null`:
```java
  /** 追加一条审计(带 diff 与 before 快照 JSON 文本);occurred_at 由 DB now() 回填。
   *  diffJson 为 before/after diff 的 JSON 文本或 null;beforeJson 为操作前全量 Task 快照的 JSON 文本或 null。 */
  void record(String operator, String action, TargetType targetType, long targetId,
              String metaJson, String diffJson, String beforeJson, String source);

  /** 无 diff/before 的追加(委托 8-arg,both null)——既有调用点与旧测试零改动。 */
  default void record(String operator, String action, TargetType targetType, long targetId,
                      String metaJson, String source) {
    record(operator, action, targetType, targetId, metaJson, null, null, source);
  }
```

- [ ] **Step 4: `JdbcAuditRepository` INSERT 加 before_meta、mapper 读 before**

Modify `JdbcAuditRepository.java:15-23` record。INSERT 显式列序,`before_meta` 用 `?::json`:
```java
  @Override
  public void record(String operator, String action, TargetType targetType,
                     long targetId, String metaJson, String diffJson, String beforeJson, String source) {
    // ?::json 把文本转 JSONB;metaJson/diffJson/beforeJson 为 null 时 NULL::json = NULL。
    jdbc.update("""
        INSERT INTO app_audit (operator, action, target_type, target_id, meta, diff, before_meta, source)
        VALUES (?, ?, ?, ?, ?::json, ?::json, ?::json, ?)""",
        operator, action, targetType.db(), targetId, metaJson, diffJson, beforeJson, source);
  }
```
findPage SELECT(不改签名,只加列):`SELECT id, occurred_at, operator, action, target_type, target_id, meta, source, diff, before_meta …`;mapper 末尾补 `rs.getString("before")`(即 SELECT 别名不移,列名即 `before_meta`,Java 侧参数名无歧义——改读 `rs.getString("before_meta")` 作 ctor 末尾 `before` 参):
```java
            rs.getLong("target_id"), rs.getString("meta"), rs.getString("source"),
            rs.getString("diff"), rs.getString("before_meta")),
```
(注意:保持 SELECT 列序与 ctor 序一致,`meta, source, diff, before_meta` → `meta, source, diff, before`,与 read API JSON 序一致。)

- [ ] **Step 5: 写持久层测试(先 TDD 更新既有 + 新增 before 落库读回)**

Modify `JdbcAuditRepositoryTest.java`:
(a) 既有 `record_withDiff_storesDiffColumn_andReadBackViaFindPage`(line 91-108)的 record 调用改 8-arg(加 beforeJson),并扩断言 before 读回。替换为:
```java
  @Test
  void record_withDiffAndBefore_storesColumns_andReadBackViaFindPage() {
    // 8-arg record 带 meta + diff + before(均 JSON 文本;diff 形如 {field:[before,after]},before 为全量前态)
    audits.record("alice", "task.update", TargetType.TASK, 5L,
        "{\"name\":\"after\",\"shardCount\":1}",
        "{\"cron\":[\"0 * * * * ?\",\"0 */5 * * * ?\"],\"retryCapMs\":[null,10000]}",
        "{\"name\":\"before\",\"shardCount\":1,\"paused\":false}", "cli");
    assertEquals(1L, jdbc.queryForObject(
        "SELECT count(*) FROM app_audit WHERE id=? AND diff IS NOT NULL AND before_meta IS NOT NULL",
        Long.class, jdbc.queryForObject("SELECT id FROM app_audit WHERE action='task.update'", Long.class)));
    List<AuditEntry> rows = audits.findPage(null, "task.update", null, null, null, null, null, null, 10, 0);
    assertEquals(1, rows.size());
    AuditEntry e = rows.get(0);
    assertEquals("{\"cron\":[\"0 * * * * ?\",\"0 */5 * * * ?\"],\"retryCapMs\":[null,10000]}".replaceAll("\\s+", ""),
                 e.diff().replaceAll("\\s+", ""));
    assertEquals("{\"name\":\"before\",\"shardCount\":1,\"paused\":false}".replaceAll("\\s+", ""),
                 e.before().replaceAll("\\s+", ""));
    assertEquals("{\"name\":\"after\",\"shardCount\":1}".replaceAll("\\s+", ""), e.meta().replaceAll("\\s+", ""));
    assertEquals("cli", e.source());
  }
```
(b) 新增(before=null → 列 SQL NULL;6-arg default 不落 before):
```java
  @Test
  void record_withoutBefore_beforeColumnIsSqlNull() {
    audits.record("bob", "task.create", TargetType.TASK, 3L, null, null);
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE before_meta IS NULL AND diff IS NULL", Long.class));
  }
```
(c) `findPage_hasDiffFiltersToRowsWithActualChanges_only`(既有,line 120 区):三个 record 调用经 6-arg default 不变(它们本就走 default,零改动)。

- [ ] **Step 6: 跑持久层测试确认绿**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-persistence -am test -Dtest=JdbcAuditRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: BUILD SUCCESS,全部用例绿(含既有 hasDiff/diffField 过滤)。
> 注:此步会因 `AuditRecorder`(server 模块)仍调旧 7-arg 而需 `-Dtest` + `-am` 只编 persistence;若 IDE/模块编译报 server 缺 7-arg,属预期——Task 2 收敛。

- [ ] **Step 7: Commit**

```bash
git add scheduler-persistence/src/main/resources/db/migration/V12__audit_before_meta.sql \
        scheduler-core/src/main/java/dev/scheduler/core/AuditEntry.java \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/AuditRepository.java \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcAuditRepository.java \
        scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcAuditRepositoryTest.java
git commit -m "feat(audit): V12 app_audit.before_meta 列 + AuditEntry.before + 持久层 8-arg record(6-arg default 委托)"
```

---

### Task 2: server — `AuditRecorder` 8-arg(before)+`taskBefore`+`TaskController` 5 handler 接 before+集成测试

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/service/AuditRecorder.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java`
- Modify: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: Task 1 的 8-arg repo、`AuditEntry.before`。
- Produces: `AuditRecorder` public `record(… Map meta, Map diff, Map before)` 8-arg(私有 8-arg `(meta,diff,before,source)`);`taskBefore(Task)`;`TaskController` 5 handler 记 before。Task 3 依赖读 API JSON 的 `"before"`。

- [ ] **Step 1: `AuditRecorder` 加 public 8-arg、删弃置的 public 7-arg `(meta,diff)`、私有改 8-arg**

Modify `AuditRecorder.java` — 5-arg public(meta)内部委托改私有 8-arg;删 public `(meta, diff)`;新增 public `(meta, diff, before)`;私有 `(meta, diff, before, source)` 补 serialize(before):
```java
  /** source 为 null(保留列,未启用来源追踪)。 */
  public void record(String operator, String action, TargetType target, long targetId, Map<String, Object> meta) {
    record(operator, action, target, targetId, meta, null, null, null);
  }

  /** 带 before/after 字段级 diff + 操作前全量快照(task.update 等 task.* 动作);meta 后态、diff 变差、
   *  before 操作前全量 Task(replace 子项目2 的 (meta,diff) overload)。 */
  public void record(String operator, String action, TargetType target, long targetId,
                     Map<String, Object> meta, Map<String, Object> diff, Map<String, Object> before) {
    record(operator, action, target, targetId, meta, diff, before, null);
  }

  private void record(String operator, String action, TargetType target, long targetId,
                      Map<String, Object> meta, Map<String, Object> diff, Map<String, Object> before, String source) {
    String who = (operator == null || operator.isBlank()) ? "anonymous" : operator;
    try {
      audits.record(who, action, target, targetId, serialize(meta), serialize(diff), serialize(before), source);
    } catch (RuntimeException e) {
      log.warn("audit record failed (non-blocking): {} {} target={} id={}", who, action, target.db(), targetId, e);
    }
  }
```
类注释 line 12-16 的 `meta 为 Map` 一句话补「before 为操作前全量快照」;`serialize` 的 warn 文案保持不改(Minor,不改语义)。

- [ ] **Step 2: `TaskController` 加 `taskBefore(token)`,update 接 before**

Modify `TaskController.java` — 在 `taskMeta`(line 205)后加全量前态方法:
```java
  /** 审计 before = 操作前全量 Task 定义(16 组件):供取证回溯 / delete 后还原;与 taskMeta 后态紧凑互补。 */
  private Map<String, Object> taskBefore(Task t) {
    Map<String, Object> b = new LinkedHashMap<>();
    b.put("id", t.id());
    b.put("name", t.name());
    b.put("kind", t.kind());
    b.put("handlerRef", t.handlerRef());
    b.put("cron", t.cron());
    b.put("shardCount", t.shardCount());
    b.put("timeoutSeconds", t.timeoutSeconds());
    b.put("maxRetries", t.maxRetries());
    b.put("backoffMs", t.backoffMs());
    b.put("retryableFailurePattern", t.retryableFailurePattern());
    b.put("maxActiveConcurrent", t.maxActiveConcurrent());
    b.put("enabled", t.enabled());
    b.put("paused", t.paused());
    b.put("retryMode", t.retryMode());
    b.put("retryCapMs", t.retryCapMs());
    b.put("retryBudgetMs", t.retryBudgetMs());
    return b;
  }
```
`update`(line 121)调用改 8-arg:
```java
    auditor.record(operator, "task.update", TargetType.TASK, saved.id(), taskMeta(saved), taskDiff(existing, saved), taskBefore(existing));
```

- [ ] **Step 3: `TaskController` pause/resume/delete/trigger 捕获 before**

Modify `TaskController.java` — 各 handler 把 `requireTask` 返回值路由给 before:
```java
  // pause
  public Task pause(...) {
    Task before = requireTask(id);
    tasks.setPaused(id, true);
    auditor.record(operator, "task.pause", TargetType.TASK, id, Map.of(), null, taskBefore(before));
    return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  }
```
```java
  // resume — 同理,paused=false
  public Task resume(...) {
    Task before = requireTask(id);
    tasks.setPaused(id, false);
    auditor.record(operator, "task.resume", TargetType.TASK, id, Map.of(), null, taskBefore(before));
    return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  }
```
`delete`(line 165-182):**必须删前捕获**(行将物理删除),`requireTask(id)` 返回值路由:
```java
    Task before = requireTask(id);
    List<String> blockers = new ArrayList<>();
    ...
    if (!tasks.delete(id)) throw notFound("task " + id);
    auditor.record(operator, "task.delete", TargetType.TASK, id, Map.of(), null, taskBefore(before));
```
`trigger`(line 185-191):
```java
    Task before = requireTask(id);
    Execution run = manualRun(id, UUID.randomUUID().toString());
    auditor.record(operator, "task.trigger", TargetType.TASK, id, Map.of(), null, taskBefore(before));
```

- [ ] **Step 4: 集成测试断言 before(file-update 的 update 行 + delete 后留存)**

Modify `ApiIntegrationTest.java` — 在 `taskWriteActions_areAudited` 的 update 断言段(line 607-611)加 before 断言(前置 name=audited-task 旧态存在):
```java
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("audited-task")))
```
在其 after `shardCount` not-contains 断言(line 611)后补 before 不含后态名:
```java
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.not(
            org.hamcrest.Matchers.containsString("renamed-audit"))));
```
新增独立用例 **delete 后 before 留存定义**(取证核心):
```java
  /** 取证:task.delete 物理删除后其完整定义只在 before 留存,可据此还原。 */
  @Test
  void taskDelete_beforeSnapshotRetainsDefinition() throws Exception {
    long delId = objectMapper.readTree(mvc.perform(post("/api/v1/tasks").header("X-Operator", "delsnap")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"snap-del\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":2}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
    mvc.perform(delete("/api/v1/tasks/" + delId).header("X-Operator", "delsnap")).andExpect(status().isNoContent());
    // 任务已物理删除;审计行仍持 before 全量快照
    mvc.perform(get("/api/v1/audits").param("action", "task.delete").param("targetId", String.valueOf(delId)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("snap-del")))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("\"shardCount\":2")));
  }
```
新增独立用例 **同值 update 亦有 before**(diff=`{}` 但 before 为全量前态):
```java
  @Test
  void taskUpdate_identicalFields_hasBeforeThoEmptyDiff() throws Exception {
    long id = objectMapper.readTree(mvc.perform(post("/api/v1/tasks").header("X-Operator", "u2")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"same-before\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
    mvc.perform(put("/api/v1/tasks/" + id).header("X-Operator", "u2")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"name\":\"same-before\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
                + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
        .andExpect(status().isOk());
    mvc.perform(get("/api/v1/audits").param("action", "task.update").param("targetId", String.valueOf(id)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$['items'][0].diff").value("{}"))
        .andExpect(jsonPath("$['items'][0].before").value(org.hamcrest.Matchers.containsString("same-before")));
  }
```

- [ ] **Step 5: 跑 server 集成测试确认绿**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test -Dtest='ApiIntegrationTest#taskWriteActions_areAudited+taskDelete_beforeSnapshotRetainsDefinition+taskUpdate_identicalFields_hasBeforeThoEmptyDiff' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: BUILD SUCCESS;3 用例全绿(含既有 taskWriteActions 全 15 动作断言)。

- [ ] **Step 6: Commit**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/service/AuditRecorder.java \
        scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java \
        scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java
git commit -m "feat(audit): AuditRecorder 8-arg before + taskBefore + TaskController 5 handler 记操作前全量快照(deleted dead 7-arg (meta,diff))"
```

---

### Task 3: 前端 — `AuditEntry.before` + 审计页「快照」列

**Files:**
- Modify: `web/src/api/types.ts`
- Modify: `web/src/pages/AuditPage.tsx`

**Interfaces:**
- Consumes: Task 1/2 的读 API JSON `"before"`(raw JSON 文本或 null)。
- Produces: 前端 `AuditEntry.before: string | null` + 审计页「快照」列(diff 列与 meta 列之间)。

- [ ] **Step 1: `types.ts` `AuditEntry` 加 `before`**

Modify `web/src/api/types.ts` — `AuditEntry` 末尾(`diff` 后)加:
```ts
  diff: string | null;
  before: string | null; // 操作前全量 Task 快照(task.* 动作);其余 null
```

- [ ] **Step 2: `AuditPage.tsx` 加「快照」列**

Modify `web/src/pages/AuditPage.tsx` — 复用 `metaSummary`(line 18-29)同款逻辑渲 before(本身即 JSON 文本):
- 表头(line 135-142)在「变更」(line 140)与「meta」(line 141)之间插 `<th>快照</th>`,列序变 `时间|操作者|动作|目标|变更|快照|meta`。
- 行(line 145-155)在 变更 `<td>`(line 151)与 meta `<td>`(line 152)之间插 `<td className="break-words font-mono text-xs text-slate-500">{metaSummary(r.before)}</td>`(`metaSummary` 对 null → `—`,parse 失败回退 raw,语义即所需)。

- [ ] **Step 3: 前端构建验证**

Run: `cd /d/ai-project/scheduler/web && npm run build`
Expected: `tsc -b` 无类型错 + Vite 构建成功。

- [ ] **Step 4: Commit**

```bash
git add web/src/api/types.ts web/src/pages/AuditPage.tsx
git commit -m "feat(web): 审计页新增「快照」列展示操作前全量 Task 定义(before)"
```

---

### Task 4: 全量构建 + 回归验证

**Files:** (无代码改动,仅验证)

- [ ] **Step 1: 离线安装(暴露编译断点)**

Run: `cd /d/ai-project/scheduler && mvn -o -T1C install -DskipTests`
Expected: BUILD SUCCESS(四模块全过,含 V12 迁移编译)。

- [ ] **Step 2: 全量测试(回归,含既有审计/DAG/execution 用例)**

Run: `cd /d/ai-project/scheduler && mvn -o -T1C test`
Expected: BUILD SUCCESS 全模块绿(V12 每模块 Testcontainers 库干净应用;append-only 表增列不影响既有行为)。

- [ ] **Step 3: 前端构建复核**

Run: `cd /d/ai-project/scheduler/web && npm run build`
Expected: 产物生成无错。

- [ ] **Step 4: 提交(若有回归修正)或确认工作树干净**

Run: `cd /d/ai-project/scheduler && git status`
Expected: 工作树干净(或仅含最后一条验证性提交)。

---

## Self-Review

**Spec 覆盖核对:** §1 数据(V12 before_meta 列)↔ T1;§2 capture/`taskBefore`/overload 链/5 handler ↔ T2;§3 读 API `AuditEntry.before` 末尾 ↔ T1(+T2 走写端);§4 UI「快照」列 ↔ T3;Global Constraints 贯穿(非阻断、持久层无 Jackson、三列语义)。全部覆盖。

**Placeholder 扫描:** 无 TBD/TODO;每步骤给完整 SQL/方法体/断言,签名与既有实况(`Task record` 16 组件、`AuditRecorder` 现行 5-arg/7-arg/私有 7-arg、`JdbcAuditRepository` INSERT/SELECT 列序、`ApiIntegrationTest` 既有 update/delete 断言)对齐。delete 的「删前捕获」、pause/resume 走 5-arg `Map.of()`、trigger「补一次 requireTask」均为已裁分支,非省略。

**类型一致性核对:** `taskBefore` 的 16 字段名与 `Task` record 组件一一对应(含 id);`AuditEntry` 10 组件序在 T1(构造：meta, source, diff, before_meta→before)与 read API(Jackson record accessor 序 meta, source, diff, before)与 T3 前端 `before: string | null` 一致;`AuditRepository.record` 8-arg(beforeJson 在 diff 后、source 前)与 `AuditRecorder` 私有 8-arg 调用一致;6-arg default 语义 = diff/before 均 null → 既有 9+ 调用点无回归。`taskUpdate_identicalFields_hasBeforeThoEmptyDiff` 验证 before 与 `{}` diff 正交(diff 空仍有 before 全量)。

**风险/已知取舍(供执行时 reference):**
- **overload 弃置**:删 public `(meta, diff)` 前须确认唯一调用者(update)已迁 8-arg;grep `\.record\(.*taskDiff` 或直接看 Task 2 Step 2 后无残留。
- **jsonb 读回空白规范化**:持久层断言一律去空白语义比较(Global Constraint),集成测试用 `containsString`/匹配于 JSON 值,不写精确整串。
- **before_meta 空值 JSON**:`{}` 与 NULL 不在此子项目区分——before 只对 5 个 task.* 动作非 NULL,其余恒 NULL;无 no-op 语义(与 diff 的 `{}` 正交)。
- **trigger 双次 requireTask**:为不重构 manualRun 签名,trigger 补一次显式取 before;少。