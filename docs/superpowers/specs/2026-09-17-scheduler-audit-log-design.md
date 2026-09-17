# Scheduler 企业化治理 · 子项目 1:操作审计日志设计

> **Goal:** 记录**操作者主动动作**——谁(operator)、何时、对哪个 task/execution/shard/dag/dag_run、做了什么——写成 append-only 审计表,提供过滤分页读 API 与审计页。**系统驱动的状态迁移(认领/重试/回收/失败/父聚合)不重复记录**:已由 `execution_shard_outcome` 表覆盖。
>
> **Architecture:** 控制器 = 唯一的操作入口(operator 语义与 `X-Operator` 头在此),故在**控制器写 handler 内显式调用小 `AuditRecorder`** 追加审计(非 AOP、非仓储层 hook)。写操作成功落库后 best-effort 追加:审计 insert 失败只 log、不反操作(审计非阻断)。`occurred_at` 用 DB `now()`(权威时钟),append-only 不需幂等。
>
> **Tech Stack:** Java 17、Spring Boot、PostgreSQL 16(DB now 权威时钟)、Testcontainers、既有 `Page`/`Paging` 归一化、既有 React 页脚手架。无新增依赖。

## 关键前置事实(已核准,不猜测)

- 服务端**无认证/用户体系**(无 Principal、无安全链)。operator 身份采用**可选 `X-Operator` 请求头**,缺失记 `'anonymous'`;不阻断现有 curl/前端(缺省即可)。这是已核准裁决。
- 现状写端(待审计入口,全部在 controller,直接调仓储层,无中间服务层):`TaskController` create/update/pause/resume/delete/trigger;`ExecutionController` rerun/cancel/shard requeue;`DagController` create/pause/resume/trigger、dag_run cancel、dag_node rerun。
- 系统驱动迁移已由 `execution_shard_outcome`(status/detail)覆盖;审计**只记 operator 主动动作**(已核准裁决)。
- 读 API 复用既有 `Page`/`Paging`(TaskController.list 同款)。
- 前端已有 5 页脚手架 + 侧边栏导航;审计页为第 6 入口(已核准)。

---

## §1 数据模型(V10 迁移,append-only)

新表 `app_audit`:

| 列 | 类型 | 语义 |
|---|---|---|
| `id` | BIGSERIAL PRIMARY KEY | |
| `occurred_at` | TIMESTAMPTZ NOT NULL DEFAULT now() | DB 权威时钟 |
| `operator` | VARCHAR(64) NOT NULL | `X-Operator` 头,缺省 `'anonymous'` |
| `action` | VARCHAR(64) NOT NULL | 见 §4 清单,形如 `task.create` |
| `target_type` | VARCHAR(16) NOT NULL | `task` / `execution` / `shard` / `dag` / `dag_run` |
| `target_id` | BIGINT NOT NULL | 目标主键(创建类操作为新建行的 id) |
| `meta` | JSONB NULL | 紧凑结构化载荷(见 §4) |
| `source` | VARCHAR(255) NULL | 请求方标识(如客户端传入 `X-Audit-Source` 或空) |

索引(保证读 API 过滤路径):`(target_type, target_id, occurred_at DESC)`、`(operator, occurred_at DESC)`。

---

## §2 捕获点:显式 AuditRecorder(非 AOP)

- 新增 `AuditRecorder`(基于 `JdbcAuditRepository.record(...)`,一个 `INSERT`)。
- 每个写 handler 在操作**成功落库后、返回前**调:
  ```java
  auditor.record(operator, "task.update", TargetType.TASK, id, metaBuilder(...));
  ```
- `operator` 于控制器取 `@RequestHeader(value = "X-Operator", required = false) String operator`,`AuditRecorder` 内 `operator == null || isBlank ? "anonymous" : operator` 兜底。
- 写入失败:catch 并 `log.warn`,**不抛、不改变用户操作结果**(审计非阻断)。同请求内、操作后追加,不要求与操作同事务。
- 不采用 AOP `@Around` 切全部写方法:框架侵入大、难取 action 名与 targetId;本仓风格朴素,显式 helper 更贴 15 处写端的实况。

---

## §3 写入语义

- 时间戳用 DB `now()`(与 active/retry 判窗一致,权威时钟)。
- append-only,单条 INSERT,不需幂等键。
- 审计写入与操作解耦:操作先提交(成功为准),审计后追加;审计失败不回滚操作。
- `source` 列:记录请求方标识(可选,控制器可读 `X-Audit-Source` 头或忽略传 null);保留以支持审计追踪来源。

---

## §4 action 清单与 meta 载荷(紧凑后态,非 diff)

| action | target_type | target_id | meta(JSONB) |
|---|---|---|---|
| `task.create` | task | 新建 id | `{name, handlerRef, cron, shardCount, timeoutSeconds, maxRetries, backoffMs, retryMode, enabled, paused}`(后态关键字段) |
| `task.update` | task | id | 同上 update 后态关键字段 |
| `task.pause` | task | id | `{}` |
| `task.resume` | task | id | `{}` |
| `task.delete` | task | id | `{}` |
| `task.trigger` | task | id | `{}` |
| `execution.rerun` | execution | id | `{}` |
| `execution.cancel` | execution | id | `{}` |
| `shard.requeue` | shard | shardId | `{}` |
| `dag.create` | dag | 新建 id | `{name}` |
| `dag.pause` / `dag.resume` | dag | id | `{}` |
| `dag.trigger` | dag | id | `{}` |
| `dag_run.cancel` | dag_run | runId | `{}` |
| `dag_node.rerun` | dag_run | runId | `{nodeId}` |

- 不做 before/after 字段级 diff(YAGNI;更丰满的审计留作后续)。meta 存**操作后的关键后态字段**,供审计页速览。
- meta 具体字段写入**操作后已确认的实体状态**(create/update 用返回/形成的 Task/Dag 对象取字段;无对象时空对象 `{}`)。

---

## §5 读 API

`GET /api/v1/audits` — 过滤 + 分页,返回 `Page<AuditEntry>`,按 `occurred_at DESC`:

| 参数 | 类型 | 说明 |
|---|---|---|
| `operator` | String 可选 | **子串过滤(LIKE)**,与 `TaskController.list` 的 name 同款 |
| `action` | String 可选 | 等值过滤,如 `task.create` |
| `targetType` | String 可选 | 等值过滤 |
| `targetId` | Long 可选 | 等值过滤 |
| `from` / `to` | Instant/iso 可选 | 时间窗(`occurred_at >= from AND <= to`) |
| `limit` / `offset` | int 可选 | 沿用既有 `Paging.of(limit, offset)` 归一化 |

`AuditEntry` record:`(id, occurredAt, operator, action, targetType, targetId, meta, source)`。按 `(action,targetType,targetId,occurred_at)` 组合查询(有索引支撑)。

- `targetType`/`targetId` 与 `action` 组合即可精确定位某资源的在时间窗内操作史。

---

## §6 UI(审计页,第 6 入口)

- 侧边栏新增「审计」入口(第 6 项)。
- 只读表格:列 `occurred_at`(格式化) | `operator` | `action` | `target`(type:id) | `meta`(JSON 摘要)。
- 顶部过滤:operator 子串 / action 下拉或子串 / targetType / targetId / 时间起止。
- 分页 + 倒序;复用既有前端 `Page` 类型与页面脚手架、api client 扩展 `listAudits(filters)`。
- 调后端 `GET /api/v1/audits`。

---

## 配置变更汇总

| 配置 | 位置 | 类型 |
|---|---|---|
| `app_audit` 表 + 两索引 | V10 迁移 | 新增 |
| `AuditRecorder`/`JdbcAuditRepository`/`AuditEntry` | server+persistence | 新增 |
| `GET /api/v1/audits` | AuditController(新增) | 新增 |
| 15 处写 handler 接 operator + record 调用 | 三控制器 | 修改 |
| 前端审计页 + api client/types + 侧边栏 | web | 新增 |

---

## Non-goals(YAGNI;本 spec 不做)

- **before/after 字段级 diff**、`app_audit` 保留期清理、登录/用户体系、告警联动、审计批量导出。
- Worker/scheduler 内部自动状态迁移审计(已由 outcome 表覆盖)。

## Spec 自审

- 占位符:无 TBD/TODO;表名/列/action/`AuditRecorder.record` 签名/读 API 参数已核。
- 内部一致:捕获=控制器显式 helper;operator=可选头缺省 anonymous;meta=后态紧凑;读 API 复用 Page;UI 第 6 入口。
- 作用域聚焦单子项目(审计日志),单一计划可控;写端清单与既有控制器实况对齐。
- 歧义已裁决:非 AOP;审计非阻断;occurred_at=DB now;source 保留可选;meta 非 diff。