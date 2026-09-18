# Scheduler 企业化治理 · 子项目2:操作审计 — 字段级 before/after diff

> **Goal:** 在既有操作审计日志的子项目1(_meta_ = 操作后紧凑后态)之上,为 **`task.update`** 增加**字段级 before/after diff**,记录操作者改动了哪些字段、每个字段从什么改成什么。其余动作(create/pause/resume/delete/trigger、execution.*、dag.*)保持现状不变。
>
> **Architecture:** 新增 `app_audit.diff` JSONB 列(V11,append-only,**向后全兼容**:既有行与其余动作同列 NULL)。只改 `TaskController.update` 一条写路径:控制器已同时持有更新前 `existing` 与更新后 `saved` 两个 Task,遍历可变字段、仅输出不同的字段为 `{ "field": [before, after] }`。`meta` 保持 post-state 语义不重载。读 API 的 `AuditEntry` 多出 `diff` 字段(raw JSON 文本,同 `meta` 模式);前端审计页新增「变更」列渲染。
>
> **Tech Stack:** Java 17、Spring Boot、PostgreSQL 16、Testcontainers、既有 `AuditRepository`/`AuditRecorder`/`AuditEntry`/`Page` 归一化、既有 React 审计页。无新增依赖。

## 关键前置事实(已核准,不猜测)

- 子项目1(已合入 main)的写端结构:`AuditRecorder.record(operator, action, TargetType, targetId, Map meta[, source])`,meta 经 ObjectMapper 序列化为 JSON 文本后由持久层落 `app_audit.meta` JSONB;**持久层无 Jackson**,diff 同为 raw JSON 文本贯穿。
- `TaskController.update`(line 87-122)已同时持有 `existing`(line 89,`tasks.findById`)与 `saved`(line 119,`tasks.findById` 上方更新后的读回),故 before/after 就地可得,无需新增查询。
- `taskMeta(Task)` 现有 12 字段 post-state;intent 为「后态关键字段」。Task record 共 16 组件(id + 15 可变),目前 taskMeta 漏发 `kind`/`retryableFailurePattern`/`maxActiveConcurrent`。
- 审计非阻断:diff 构造/序列化失败只 log.warn,不影响操作与既有 meta 记录。
- `ApiIntegrationTest.resetDb()` 已 TRUNCATE `app_audit`(每用例隔离)。子项目2 沿用,不改。

---

## §1 数据模型(V11 迁移,append-only)

```sql
ALTER TABLE app_audit ADD COLUMN diff JSONB NULL;
```

- 只加一列,不重建索引;既有两个索引 `(target_type, target_id, occurred_at DESC)`、`(operator, occurred_at DESC)` 不涉及 diff。
- diff 仅在 `task.update` 动作非 NULL;其余动作与既有行恒 NULL。

---

## §2 capture:diff 计算与落库

- 仅 `TaskController.update` 一处调用新增的带-diff `AuditRecorder` 重载:
  ```java
  auditor.record(operator, "task.update", TargetType.TASK, saved.id(), taskMeta(saved), taskDiff(existing, saved));
  ```
- `AuditRecorder` 新增重载 `record(operator, action, target, id, Map meta, Map diff)`(与既有 `record(..., Map meta)` / `record(..., Map meta, String source)` 并存),序列化逻辑与 meta 同一路径,序列化/落库失败同样非阻断。
- **持久层** `AuditRepository` 加带 diff 的重载;既有无 diff 重载委托 `diffJson=null`,14 个既有调用点与既有测试**零改动**。

### §2.1 `taskDiff(existing, saved)` 语义

遍历 15 个可变字段,返回 `LinkedHashMap`,**仅收录前后不同的字段**:
字段集 = `name, kind, handlerRef, cron, shardCount, timeoutSeconds, maxRetries, backoffMs, retryableFailurePattern, maxActiveConcurrent, enabled, paused, retryMode, retryCapMs, retryBudgetMs`。

格式:`{ "field": [before, after] }`,值均为参与序列化的对象:
- 标量:数值/布尔/String 按 `existing.<f>()` vs `saved.<f>()`。
- null↔值 变化:该字段收录为 `[null, 新值]` 或 `[旧值, null]`。
- 相等字段不出现。
- 无任何字段变化 → diff 为 `{}`(空对象,**仍非 NULL**,与「未启用 diff 的动作 NULL」语义区分)。

例:
```json
{ "cron": ["0 * * * * ?", "0 */5 * * * ?"], "retryCapMs": [null, 10000] }
```

- diff 计算用**请求前读到的 `existing`** 与**更新后读回的 `saved`**(与 `tasks.update` 落库语义一致)。
- 附带修正:`taskMeta(saved)` 补齐 `kind`/`retryableFailurePattern`/`maxActiveConcurrent` 三字段(post-state 完整化;test 同步更新)。

---

## §3 读 API

- `AuditEntry` record 核心加 `diff`(String,raw JSON 文本),**追加在末尾**:`(id, occurredAt, operator, action, targetType, targetId, meta, source, diff)`。JSON 字段顺序为 meta、source、diff;既有 `meta`/`source` 位置相对子项目1 不变。`GET /api/v1/audits` 的 JSON 多出 `"diff"` 字段。持久层 SELECT 明确列出列、按该序映射。
- **不加新过滤参数**(hasDiff 之类 YAGNI 不做)。
- findPage/count/排序(`occurred_at DESC, id DESC`)/既有过滤一律不变。

---

## §4 UI(审计页,第 6 入口)

- `web/src/api/types.ts` 的 `AuditEntry` 加 `diff: string | null`。
- `web/src/pages/AuditPage.tsx`:meta 列保留;新增「变更(diff)」列,渲染:
  - `diff == null` → `—`(子项目2 之前记录及其他动作)。
  - `{}` → 无变化(可显示 `—` 或 `{}`,前端约定显示 `—` 谓一致)。
  - 非空:`field: 旧 → 新`,多字段换行分隔;新值/旧值为字符串时直接显,对象/数组 `JSON.stringify`;null 显示 `∅`(或 `空`,前端约定);parse 失败回退 raw 文本。
- 既有过滤/分页/轮询/目标类型/`meta` 列不变。表头列序:时间 | 操作者 | 动作 | 目标 | 变更 | meta。

---

## 配置变更汇总

| 配置 | 位置 | 类型 |
|---|---|---|
| `app_audit.diff` JSONB 列 | V11 迁移 | 新增(单列,append-only) |
| `AuditEntry.diff`(core) | scheduler-core | 修改(加字段) |
| `AuditRepository.record` 带 diff 重载 + `JdbcAuditRepository` INSERT/mapper | scheduler-persistence | 修改(新增重载,既有不动) |
| `AuditRecorder.record` 带 diff 重载 + `taskDiff` + `taskMeta` 补齐 | scheduler-server | 修改 |
| `TaskController.update` 一处接 diff | scheduler-server | 修改 |
| 审计页「变更」列 + `AuditEntry.diff` | web | 修改 |

---

## Non-goals(YAGNI;本 spec 不做)

- 除 `task.update` 外任何动作的 diff(create 无 before;pause/resume 等 from action 名可派生)。
- DAG/execution 编辑的 diff(二者无 update 语义)。
- before 值的同时刻快照表、diff 检索过滤、`app_audit` 保留期清理、diff 批量导出。

## Spec 自审

- 占位符:无 TBD/TODO;表/列/重载签名/`taskDiff` 字段集与格式已核。
- 内部一致:diff 只落 `task.update`;meta 不重载;15 字段集合与 Task record 组件对齐;`{}` 与 NULL 语义区分;UI 渲染约定与后端 diff 格式匹配。
- 作用域聚焦单子项目(task.update diff),单一计划可控;14 既有调用点零改动保证回归面小。
- 歧义已裁决:diff 字段集含 taskMeta 漏发三字段;相等字段不输出;`{}` × NULL 语义;null 显示 `∅`;diff 序列化失败非阻断降级为无 diff(meta 仍记)。