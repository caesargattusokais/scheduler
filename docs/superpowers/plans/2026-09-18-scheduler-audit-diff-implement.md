# 操作审计 · 子项目2:task.update 字段级 before/after diff — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在既有审计日志之上,为 **`task.update`** 记录**字段级 before/after diff**(哪个字段、从什么改成什么),存新 `app_audit.diff` JSONB 列,审计页新增「变更」列展示。其余 14 动作与 `meta` 语义不变,向后全兼容。

**Architecture:** V11 给 `app_audit` 加 `diff` 列。`AuditEntry` 末尾追加 `diff`(raw JSON 文本 String,同 `meta` 贯穿模式)。`AuditRepository`/`AuditRecorder` 各加带-diff 重载(既有 6-arg `record` 变 `default` 委托 null diff → 14 调用点零改)。写端只改 `TaskController.update`:`taskDiff(existing, saved)` 遍历 15 可变字段、仅收录不同者 → `{field:[before,after]}`;`taskMeta` 补齐 3 漏发字段。读 API 经 `AuditEntry` 自动多出 `diff`。前端 `AuditEntry` 加 `diff`、审计页加「变更」列。

**Tech Stack:** Java 17、Spring Boot、PostgreSQL 16、Testcontainers、Flyway V11、既有 `AuditRepository`/`AuditRecorder`/`AuditEntry`/`Page` 归一化、React 审计页。无新增依赖。

**Spec:** `docs/superpowers/specs/2026-09-18-scheduler-audit-diff-design.md`(已核准)

## Global Constraints

- `diff` 仅 `task.update` 动作非 NULL;其余动作与既有行恒 NULL。`{}`(空对象)与 NULL 语义区分:无字段变化 → `{}`,未启用 diff 的动作 → NULL。
- `meta` 保持 post-state 语义,不重载。task.update 同时写 `taskMeta(saved)`(meta)和 `taskDiff(existing, saved)`(diff)。
- 持久层无 Jackson:diff 为 raw JSON 文本 String 贯穿,前端 `JSON.parse`。
- 审计非阻断:diff 构造/序列化失败只 log.warn,不影响操作与既有 meta 记录。
- diff 遍历用请求前读到的 `existing` 与更新后读回的 `saved`(与 `tasks.update` 落库语义一致)。
- `AuditEntry` 组件序列(末尾追加):`(id, occurredAt, operator, action, targetType, targetId, meta, source, diff)`;持久层 SELECT 显式列序、按此映射。
- 依赖顺序 T1 → T2 → T3 → T4;各提交自编译。全离线 `mvn -o`。

---

### Task 1: V11 迁移 + 核心 `AuditEntry.diff` + 持久层带-diff 重载

**Files:**
- Create: `scheduler-persistence/src/main/resources/db/migration/V11__audit_diff.sql`
- Modify: `scheduler-core/src/main/java/dev/scheduler/core/AuditEntry.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/AuditRepository.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcAuditRepository.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcAuditRepositoryTest.java`

**Interfaces:**
- Consumes: 无(第一个任务;沿用 subproject1 的 `app_audit` 表/`AuditEntry`/`AuditRepository`/`AbstractPostgresTest`)。
- Produces: `AuditEntry(..., source, diff)`(9 组件)、`AuditRepository.record(..., metaJson, diffJson, source)`(7-arg 抽象 + 6-arg default 委托 null)、`JdbcAuditRepository` INSERT 带 diff 列、findPage 读回 diff。Task 2 的 `AuditRecorder` 用新的 7-arg。

- [ ] **Step 1: 写失败测试(JdbcAuditRepositoryTest 加 3 断言)**

追加到 `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcAuditRepositoryTest.java`(既有 `clean()` @BeforeEach TRUNCATE 已清表):

```java
@Test
void record_withDiff_storesDiffColumn_andReadBackViaFindPage() {
  // 7-arg record 带 meta + diff(JSON 文本;diff 形如 {field:[before,after]})
  audits.record("alice", "task.update", TargetType.TASK, 5L,
      "{\"name\":\"after\",\"shardCount\":1}",
      "{\"cron\":[\"0 * * * * ?\",\"0 */5 * * * ?\"],\"retryCapMs\":[null,10000]}", "cli");
  // diff 列落库非 NULL 且为 JSON 文本
  assertEquals(1L, jdbc.queryForObject(
      "SELECT count(*) FROM app_audit WHERE id=? AND diff IS NOT NULL", Long.class, jdbc.queryForObject(
          "SELECT id FROM app_audit WHERE action='task.update'", Long.class)));
  // findPage 读回 diff 原样
  List<AuditEntry> rows = audits.findPage(null, "task.update", null, null, null, null, 10, 0);
  assertEquals(1, rows.size());
  AuditEntry e = rows.get(0);
  assertEquals("{\"cron\":[\"0 * * * * ?\",\"0 */5 * * * ?\"],\"retryCapMs\":[null,10000]}", e.diff());
  assertEquals("{\"name\":\"after\",\"shardCount\":1}", e.meta());
  assertEquals("cli", e.source());
}

@Test
void record_withoutDiff_diffColumnIsSqlNull() {
  // 既有 6-arg 走 default 委托 → diff 落 NULL(未启用 diff 的动作/旧行语义)
  audits.record("bob", "task.create", TargetType.TASK, 3L, null, null);
  assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE diff IS NULL", Long.class));
  assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE diff IS NOT NULL", Long.class));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-persistence -am test -Dtest=JdbcAuditRepositoryTest`
Expected: 编译/运行失败(`AuditEntry` 无 `diff()`、`record` 无 7-arg、缺列 `diff`)。

- [ ] **Step 3: V11 迁移 + AuditEntry + 持久层实现**

Create `scheduler-persistence/src/main/resources/db/migration/V11__audit_diff.sql`:
```sql
ALTER TABLE app_audit ADD COLUMN diff JSONB NULL;
```

Modify `scheduler-core/src/main/java/dev/scheduler/core/AuditEntry.java`(record 末尾追加 `diff`,更新 javadoc 提 diff):
```java
public record AuditEntry(
    long id,
    Instant occurredAt,
    String operator,
    String action,
    String targetType, // TargetType.db(),如 "task"
    long targetId,
    String meta,       // 操作后态 JSON 文本或 null
    String source,     // 请求方标识,通常 null
    String diff) {}
```

Modify `scheduler-persistence/src/main/java/dev/scheduler/persistence/AuditRepository.java`:
```java
/** 追加一条审计(带 diff JSON 文本);occurred_at 由 DB now() 回填。diffJson 为 before/after diff 的 JSON 文本或 null。 */
void record(String operator, String action, TargetType targetType, long targetId,
            String metaJson, String diffJson, String source);

/** 无 diff 的追加(委托带-diff 重载,diffJson=null)——14 个既有写端调用点与旧测试零改动。 */
default void record(String operator, String action, TargetType targetType, long targetId,
                    String metaJson, String source) {
  record(operator, action, targetType, targetId, metaJson, null, source);
}
```

Modify `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcAuditRepository.java`:
```java
@Override
public void record(String operator, String action, TargetType targetType,
                   long targetId, String metaJson, String diffJson, String source) {
  // ?::json 把文本转 JSONB;metaJson/diffJson 为 null 时 NULL::json = NULL。
  jdbc.update("""
      INSERT INTO app_audit (operator, action, target_type, target_id, meta, diff, source)
      VALUES (?, ?, ?, ?, ?::json, ?::json, ?)""",
      operator, action, targetType.db(), targetId, metaJson, diffJson, source);
}
```
并把 `findPage` 的 SELECT 与 mapper 加 `diff`:
```java
return jdbc.query("SELECT id, occurred_at, operator, action, target_type, target_id, meta, source, diff"
        + " FROM app_audit WHERE 1=1" + where(...)
        + " ORDER BY occurred_at DESC, id DESC LIMIT ? OFFSET ?",
    (rs, i) -> new AuditEntry(
        rs.getLong("id"), rs.getTimestamp("occurred_at").toInstant(),
        rs.getString("operator"), rs.getString("action"), rs.getString("target_type"),
        rs.getLong("target_id"), rs.getString("meta"), rs.getString("source"),
        rs.getString("diff")),
    a.toArray());
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-persistence -am test -Dtest=JdbcAuditRepositoryTest`
Expected: BUILD SUCCESS,新 2 用例 + 既有 3 用例全绿(V11 在 Testcontainers 库自动应用)。

- [ ] **Step 5: 提交**

```bash
cd /d/ai-project/scheduler && git add -A && git commit -m "feat(audit): V11 app_audit.diff 列 + AuditEntry.diff + 持久层带-diff record 重载(6-arg default 委托)"
```

---

### Task 2: server — `AuditRecorder` 带-diff 重载 + `taskDiff` + `taskMeta` 补齐 + `TaskController.update`

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/service/AuditRecorder.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: Task 1 的 `AuditRepository.record(..., metaJson, diffJson, source)` 与 `AuditEntry.diff`。
- Produces: `AuditRecorder.record(operator, action, target, id, Map meta, Map diff)`(public 5-arg 增 6-arg 重载);`TaskController.taskDiff(Task, Task) → Map<String,Object>`;`taskMeta` 补齐 `kind`/`retryableFailurePattern`/`maxActiveConcurrent`。Task 3 前端靠读 API 已呈 `diff` 字段。

- [ ] **Step 1: 改 AuditRecorder(加带-diff 重载,两既有 public 委托私有 7-arg)**

替换 `scheduler-server/src/main/java/dev/scheduler/server/service/AuditRecorder.java` 中 record 三方法为:

```java
/** source 为 null(保留列,未启用来源追踪)。 */
public void record(String operator, String action, TargetType target, long targetId, Map<String, Object> meta) {
  record(operator, action, target, targetId, meta, null, null);
}

public void record(String operator, String action, TargetType target, long targetId,
                   Map<String, Object> meta, String source) {
  record(operator, action, target, targetId, meta, null, source);
}

/** 带 before/after 字段级 diff(task.update 等);meta 为操作后态,diff 为 {field:[before,after]}。 */
public void record(String operator, String action, TargetType target, long targetId,
                   Map<String, Object> meta, Map<String, Object> diff) {
  record(operator, action, target, targetId, meta, diff, null);
}

private void record(String operator, String action, TargetType target, long targetId,
                    Map<String, Object> meta, Map<String, Object> diff, String source) {
  String who = (operator == null || operator.isBlank()) ? "anonymous" : operator;
  try {
    audits.record(who, action, target, targetId, serialize(meta), serialize(diff), source);
  } catch (RuntimeException e) {
    log.warn("audit record failed (non-blocking): {} {} target={} id={}", who, action, target.db(), targetId, e);
  }
}

/** meta/diff 序列化;null 或序列化失败 → null(失败只 warn,审计非阻断)。 */
private String serialize(Map<String, Object> meta) {
  if (meta == null) return null;
  try { return json.writeValueAsString(meta); }
  catch (JsonProcessingException e) { log.warn("audit meta serialization failed; recording without it", e); return null; }
}
```

- [ ] **Step 2: 改 TaskController — taskMeta 补齐 + taskDiff + update 接带-diff record**

`taskMeta(Task)` 内(在 `handlerRef` 之后)加三字段(create/update 后态完整化):
```java
m.put("kind", t.kind());
m.put("retryableFailurePattern", t.retryableFailurePattern());
m.put("maxActiveConcurrent", t.maxActiveConcurrent());
```

新增 `taskDiff` 与 `putDiff`(放在 `taskMeta` 之后;javadoc:`仅收录前后不同字段;无变化 → 空对象 {}`):
```java
/** task.update 的 before/after 字段级 diff:遍历 15 个可变字段,仅收录前后不同者 → {field:[before,after]}。 */
private Map<String, Object> taskDiff(Task before, Task after) {
  Map<String, Object> d = new LinkedHashMap<>();
  putDiff(d, "name", before.name(), after.name());
  putDiff(d, "kind", before.kind(), after.kind());
  putDiff(d, "handlerRef", before.handlerRef(), after.handlerRef());
  putDiff(d, "cron", before.cron(), after.cron());
  putDiff(d, "shardCount", before.shardCount(), after.shardCount());
  putDiff(d, "timeoutSeconds", before.timeoutSeconds(), after.timeoutSeconds());
  putDiff(d, "maxRetries", before.maxRetries(), after.maxRetries());
  putDiff(d, "backoffMs", before.backoffMs(), after.backoffMs());
  putDiff(d, "retryableFailurePattern", before.retryableFailurePattern(), after.retryableFailurePattern());
  putDiff(d, "maxActiveConcurrent", before.maxActiveConcurrent(), after.maxActiveConcurrent());
  putDiff(d, "enabled", before.enabled(), after.enabled());
  putDiff(d, "paused", before.paused(), after.paused());
  putDiff(d, "retryMode", before.retryMode(), after.retryMode());
  putDiff(d, "retryCapMs", before.retryCapMs(), after.retryCapMs());
  putDiff(d, "retryBudgetMs", before.retryBudgetMs(), after.retryBudgetMs());
  return d;
}

private void putDiff(Map<String, Object> d, String field, Object before, Object after) {
  if (!Objects.equals(before, after)) d.put(field, List.of(before, after));
}
```

在 `update` 方法的 record 调用(line ~120)改为带 diff:
```java
auditor.record(operator, "task.update", TargetType.TASK, saved.id(), taskMeta(saved), taskDiff(existing, saved));
```

确认 import:已有 `LinkedHashMap`/`List`/`Map`;`java.util.Objects` 需新增 import。

- [ ] **Step 3: 写失败测试(ApiIntegrationTest)**

(1) 在 `taskWriteActions_areAudited` 的 create 断言(line ~594)后追加 diff=nullAssert:
```java
.andExpect(jsonPath("$['items'][0].diff").value(org.hamcrest.Matchers.nullValue()));
```
(2) 在 update 断言(line ~602-605)追加 diff 内容断言(该 update 改了 `name` 与 `cron`,`shardCount` 未变 → 不应出现)。Jackson 冒号后空格,用容空格正则:
```java
.andExpect(jsonPath("$.total").value(1))
.andExpect(jsonPath("$['items'][0].meta").value(org.hamcrest.Matchers.containsString("renamed-audit")))
.andExpect(jsonPath("$['items'][0].diff").value(org.hamcrest.Matchers.containsString("renamed-audit")))
.andExpect(jsonPath("$['items'][0].diff").value(org.hamcrest.Matchers.matchesPattern(
    ".*\\\"name\\\":\\s*\\[\"audited-task\",\"renamed-audit\"\\].*")))
.andExpect(jsonPath("$['items'][0].diff").value(org.hamcrest.Matchers.not(
    org.hamcrest.Matchers.containsString("shardCount"))));
```
(3) 新增独立用例 `taskUpdate_identicalFields_diffIsEmptyObject`(同值 update → diff 为 `{}`,metdcs 仍全量后态):
```java
@Test
void taskUpdate_identicalFields_diffIsEmptyObject() throws Exception {
  long id = objectMapper.readTree(mvc.perform(post("/api/v1/tasks").header("X-Operator", "u")
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"name\":\"same-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
              + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
      .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
  // 完全相同的定义再 PUT 一次(不改任何字段)→ task.update,但 diff 应为空对象 {}
  mvc.perform(put("/api/v1/tasks/" + id).header("X-Operator", "u")
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"name\":\"same-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
              + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
      .andExpect(status().isOk());
  mvc.perform(get("/api/v1/audits").param("action", "task.update").param("targetId", String.valueOf(id)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.total").value(1))
      .andExpect(jsonPath("$['items'][0].diff").value("{}"));
}
```
(注意:若 `demo` 需存活 worker 才可建任务,沿用 `taskWriteActions_areAudited` 已落 `demo` 任务的既有既有;若挨个用例无 worker,见 Step 4 处置)

- [ ] **Step 4: 跑测试确认(含若 `demo` ref 需 worker 的处置)**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test -Dtest=ApiIntegrationTest`
Expected: 新断言/新用例绿,既有审计用例不回归。

若新 `taskUpdate_identicalFields_diffIsEmptyObject` 因 `demo` handlerRef 需存活 worker 而 400(可用 ref 集合为空),则把该用例的直接新建改为复用 `taskWriteActions_areAudited` 内部任务 id 不可行(跨方法无共享),改为:在 `taskWriteActions_areAudited` 的 update(Step 3.2 line 602)之后,BEFORE 的 update 实际已改字段;额外再发一次完全同值 PUT(复用同一 id/同一 auth alice)并断言第二条 diff=`{}`。此为 test-only 修正,忠于「同值→diff 空对象」意图,不改实现。

- [ ] **Step 5: 提交**

```bash
cd /d/ai-project/scheduler && git add -A && git commit -m "feat(audit): AuditRecorder 带-diff 重载 + taskDiff/taskMeta 补齐 + TaskController.update 记字段级 diff"
```

---

### Task 3: 前端 — `AuditEntry.diff` + 审计页「变更」列

**Files:**
- Modify: `web/src/api/types.ts`(AuditEntry 加 `diff`)
- Modify: `web/src/pages/AuditPage.tsx`(加 `diffSummary` + 「变更」列)

**Interfaces:**
- Consumes: Task 1/2 的读 API 契约(每行 `diff` 为 raw JSON 文本或 null);既有 `AuditEntry` 8 字段、`metaSummary`、表格结构。
- Produces: `AuditEntry.diff: string | null`;`diffSummary(d)` 渲染函数;表格第 5 列「变更」。

- [ ] **Step 1: types.ts 加 diff**

`web/src/api/types.ts` 的 `AuditEntry` 接口加字段(在 `source` 之后):
```ts
export interface AuditEntry {
  id: number;
  occurredAt: string;
  operator: string;
  action: string;
  targetType: string;
  targetId: number;
  meta: string | null;
  source: string | null;
  diff: string | null; // task.update 的 {field:[before,after]} JSON 文本;null = 未启用 diff 的动作
}
```

- [ ] **Step 2: AuditPage 加 diffSummary + 变更列**

在 `metaSummary` 之后加 `diffSummary`(复用同一 JSON.parse 容错模式):
```ts
/** diff 是 {field:[before,after]} JSON 文本;null / {} → 无变更;渲染 field: 旧 → 新。 */
const ESC = (v: unknown) => v === null || v === undefined ? '∅' : JSON.stringify(v);
const diffSummary = (d: string | null) => {
  if (!d) return '—'; // null → 未启用 diff;{} → 无字段变化
  try {
    const o = JSON.parse(d) as Record<string, [unknown, unknown]>;
    const keys = Object.keys(o);
    if (keys.length === 0) return '—';
    return keys.map((k) => `${k}: ${ESC(o[k][0])} → ${ESC(o[k][1])}`).join(' · ');
  } catch {
    return d;
  }
};
```

表头与行:在 `<th>目标</th>` 后加 `<th>变更</th>`,在对 `/meta` 的 `<td>` 前加:
```tsx
<td className="break-words font-mono text-xs text-slate-600">{diffSummary(r.diff)}</td>
```
表格列序:时间 | 操作者 | 动作 | 目标 | **变更** | meta。`meta` 的 td 保持现有 `{metaSummary(r.meta)}`。

- [ ] **Step 3: 类型检查**

Run: `cd /d/ai-project/scheduler/web && npx tsc --noEmit`
Expected: exit 0,无 TS 错误。

- [ ] **Step 4: 提交**

```bash
cd /d/ai-project/scheduler && git add -A && git commit -m "feat(web): 审计页新增「变更」列展示 task.update 字段级 before/after diff"
```

---

### Task 4: 全量构建 + 回归验证

**Files:**(无代码改动,仅验证)
- 验证:scheduler 根模块全量离线构建 + 全测试 + 前端构建。

- [ ] **Step 1: 离线安装(跳过测试,暴露编译断点)**

Run: `cd /d/ai-project/scheduler && mvn -o -T1C install -DskipTests`
Expected: BUILD SUCCESS(四模块全过,含 V11 迁移编译)。

- [ ] **Step 2: 全量测试(回归,含既有含 DAG/execution 的审计用例)**

Run: `cd /d/ai-project/scheduler && mvn -o -T1C test`
Expected: BUILD SUCCESS,全模块绿(V11 每模块 Testcontainers 库应用;append-only 表增列不影响既有行为)。

- [ ] **Step 3: 前端构建复核**

Run: `cd /d/ai-project/scheduler/web && npm run build`
Expected: 产物生成无错。

- [ ] **Step 4: 提交(若有未提交的回归修正)或确认工作树干净**

Run: `cd /d/ai-project/scheduler && git status`
Expected: 工作树干净(或仅含最后一条验证性提交)。

---

## Self-Review

**Spec 覆盖核对:** §1 数据(V11 diff 列)↔ T1;§2 capture/`taskDiff` 15 字段/`{}` vs NULL/`taskMeta` 补齐 ↔ T2;§3 读 API `AuditEntry.diff` 末尾 ↔ T1(+T2 走写端);§4 UI「变更」列取舍 ↔ T3;Global Constraints 贯穿。全部覆盖。

**Placeholder 扫描:** 无 TBD/TODO;每步骤给完整 SQL/方法体/断言,签名与既有实况(Task record 15 字段、AuditRecorder 既有两 public、JdbcAuditRepository INSERT/SELECT 列序、ApiIntegrationTest 既有 update 断言)对齐。`taskUpdate_identicalFields` 用例的 worker 依赖在 Step 4 给处置分支,非省略。

**类型一致性核对:** `taskDiff` 的 15 字段名与 Task record 组件一一对应;`AuditEntry` 9 组件序列在 T1(构造)与 read API(Jackson 序列化)与 T3 前端类型一致(meta/source/diff);`AuditRepository.record` 7-arg(diff 在 meta 后)与 `AuditRecorder` 私有 7-arg 调用一致;`diff` JSON 结构 `{field:[before,after]}` 在 T2 写端(TaskController)与 T3 前端 `diffSummary`(读 `o[k][0]/o[k][1]`)一致。既有 6-arg `record`(default)语义 = diff null 不变 → 14 调用点无回归。

**风险/已知取舍(供执行时 reference):**
- Jackson 序列化 `Map.of("name", List.of(old,new))` → `{"name":["old","new"]}`;实为紧凑(冒号后无空格的数组),但为避免与对象格式混淆,断言一律用容空格正则/containsString 而非精确串。
- `paused` 在 update 中若请求未带 paused 字段保留 existing → 不出现于 diff;只有显式改变才出现。符合「仅变化字段」定义。
- V11 只加列不重建索引;`app_audit` append-only 生产语义、测试 resetDb TRUNCATE 沿用。