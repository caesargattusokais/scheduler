# Scheduler 企业化治理 · 子项目3:操作审计 — before 内容快照(取证)

> **Goal:** 在既有操作审计日志之上(子项目1 `meta` 后态紧凑、子项目2 `diff` 字段级变差),新增**操作前完整 Task 状态快照** `app_audit.before_meta`,供取证/回溯任意历史时刻的任务原貌。**只记录 + 可读,不做还原写回**。
>
> **Architecture:** V12 在 `app_audit` 上**追加一个 `before_meta JSONB` 列**(append-only,与既有行/索引全兼容)。仅 5 个 `task.*` 动作(update/pause/resume/delete/trigger)在写 handler 内**就地捕获**操作前的全量 `Task`(旧态已由 `requireTask`/`existing` 取得,零新增查询),经 `AuditRecorder` 序列化为 JSON 文本落库。读 API 的 `AuditEntry` 追加 `before` 字段,`GET /audits` 直接暴露原始快照。核心价值:**task.delete 物理删除后,其定义只在 before_meta 留存**,可据此还原。
>
> **Tech Stack:** Java 17、Spring Boot、PostgreSQL 16、Testcontainers、既有 `AuditRepository`/`AuditRecorder`/`AuditEntry`/`Page` 归一化、既有 React 审计页。无新增依赖。

## 关键前置事实(已核准,不猜测)

- 子项目2 的写端结构:`AuditRecorder` 私有 7-arg `record(..., meta, diff, source)`,构造器注入 `ObjectMapper`,meta/diff 经 `serialize(Map)` 序列化;持久层无 Jackson,diff/before 均 raw JSON 文本贯穿。
- `TaskController` 各写 handler 的旧态就地可得:`update`(line 88)已持 `existing`(`tasks.findById`);`pause`(line 143)/`resume`(line 152)/`delete`(line 165)均先 `requireTask(id)`(拿到的 Task 现被丢弃 → 路由给 before 捕获);`trigger`(line 185)的 `requireTask` 在 `manualRun` 内,补一次取。
- Task record 共 16 组件(id + 15 可变):`id, name, kind, handlerRef, cron, shardCount, timeoutSeconds, maxRetries, backoffMs, retryableFailurePattern, maxActiveConcurrent, enabled, paused, retryMode, retryCapMs, retryBudgetMs`。
- `task.create` 无 before(操作前不存在)→ 排除;其余动作(dag/execution/shard)不在本子项目范围(其状态变更另有 `execution_shard_outcome` 等覆盖)。
- 审计非阻断:序列化/落库失败只 `log.warn`,不影响操作与既有 meta/diff。
- `ApiIntegrationTest.resetDb()` 已 TRUNCATE `app_audit`(每用例隔离)。V12 沿用。

---

## §1 数据模型(V12 迁移,append-only)

```sql
ALTER TABLE app_audit ADD COLUMN before_meta JSONB NULL;
```

- 只加一列,不重建索引;既有两个索引涉及 target_type/operator/occurred_at,diff 与 before_meta 均不参与。
- `before_meta` 仅在 5 个 `task.*` 动作非 NULL;其余动作与既有行恒 NULL。

**三列内容语义(互不重载):**

| 列 | 语义 | 覆盖 |
|---|---|---|
| `meta` | 操作后态紧凑字段 | 所有动作 |
| `diff` | 字段级变差 `{field:[before,after]}` | 仅 `task.update` |
| `before_meta` | **操作前全量 Task 定义** | 仅 `task.update`/`task.pause`/`task.resume`/`task.delete`/`task.trigger` |

---

## §2 capture:`taskBefore` 与落库

- 新增 `taskBefore(Task)` 产**全量 16 组件 LinkedHashMap**(含 delete 后只能靠它还原的完整定义),经 `AuditRecorder` 现有 serialize 落 JSON 文本。与 `taskMeta`(后态紧凑子集)不同,这是完整回放态。
- 各写 handler 捕获:
  - `update`:before = `existing`(更新前读回的 Task)。
  - `pause`/`resume`/`delete`:before = `requireTask(id)` 返回的 Task(现被丢弃,改为路由给 before)。
  - `trigger`:before = 触发前 `requireTask(id)`。
- **overload 链调整(镜像子项目2 dead-overload 裁决):** 加 before 后,带-diff 的 overload 唯一调用者 `task.update` 迁移到带-before 的新 overload(meta, diff, before);原先 `(meta, diff)` 的 overload 因此弃置 → **同批次删除**。既有 5-arg `(meta)` 保留(其余 9+ 调用点零改动)。持久层 `AuditRepository` 抽象 record 对应扩展 beforeJson(在 diff 后、source 前),既有默认委托 beforeJson=null → 其余调用点零改动。
- union 触发实例:update 记 `(meta=后态, diff=变差, before=前态)`;pause/resume/delete/trigger 记 `(meta, diff=null, before=前态)`。
- 审计非阻断不破:before 序列化/落库失败 `log.warn`,操作仍成功、meta/diff 仍记录。

---

## §3 读 API

- `AuditEntry` record 末尾累进 `before`(String,raw JSON 文本):`(id, occurredAt, operator, action, targetType, targetId, meta, source, diff, before)`。JSON 字段序 meta、source、diff、before;既有 meta/source/diff 位置相对子项目1/2 不变。`GET /api/v1/audits` 的 JSON 多出 `"before"`。
- **不加新过滤参数**(before 检索等 YAGNI 不做)。
- findPage/count/排序/既有过滤一律不变;持久层 SELECT 显式列序、按名映射 before 到末尾组件。

---

## §4 UI(审计页,最小展示)

- `web/src/api/types.ts` 的 `AuditEntry` 加 `before: string | null`。
- `web/src/pages/AuditPage.tsx`:新增「快照」列,紧邻 meta 列(_时间|操作者|动作|目标|变更|快照|meta_),渲染:
  - `before == null` → `—`(非 task.* 动作及旧行)。
  - 非 null → meta 同款 JSON 摘要(字段 = 值,` · ` 连接);parse 失败回退 raw 文本。
- 不加展开树/按快照过滤(YAGNI)。既有过滤/分页/轮询不变。

---

## 配置变更汇总

| 配置 | 位置 | 类型 |
|---|---|---|
| `app_audit.before_meta` JSONB 列 | V12 迁移 | 新增(单列,append-only) |
| `AuditEntry.before`(core 末尾) | scheduler-core | 修改(加字段) |
| `AuditRepository.record` 带 beforeJson 重载 + `JdbcAuditRepository` INSERT/mapper | scheduler-persistence | 修改(既有 default 委托 before=null,既有调用点不动) |
| `AuditRecorder` 带 before 重载 + `taskBefore` | scheduler-server | 修改(+删抛置的 (meta,diff) public) |
| `TaskController` 5 个写 handler 接 before | scheduler-server | 修改 |
| 审计页「快照」列 + `AuditEntry.before` | web | 修改 |

---

## Non-goals(YAGNI;本 spec 不做)

- 还原/回滚写通道(用户明定取证即可)。
- execution/shard/dag 的快照(其状态变更另有 `execution_shard_outcome` 覆盖)。
- before 过滤/按快照检索、before 批量导出、before 保留期清理(留后续能力点)。
- 快照做独立 side 表:快照与审计条目 1:1,追加为 `app_audit` 列原子读写、零 join,优于独立表。

## Spec 自审

- 占位符:无 TBD/TODO;列名/overload 链/捕获动作/字段集/读 API 组件序已核。
- 内部一致:before 只在 5 个 task.* 动作非 NULL;`create` 无 before 排除;delete 后只能靠 before_meta 还原(§1 table 与 §2 捕获一致);读 API/前端与三列语义对齐;overload 删除裁决与既有调用点零改动不冲突。
- 作用域聚焦单个能力点(task 前态快照),单一计划可控;9+ 既有调用点零改动保证回归面小。
- 歧义已裁决:列 vs side 表(列,原子零 join);`(meta,diff)` public 迁 8-arg 后弃置同批删;before 与 meta/diff 语义互不重载;delete 物理删除后仅快照留存定义。