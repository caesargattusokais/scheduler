# M5 管理控制台 —— 实现级设计

- 日期：2026-09-09
- 上一级：`2026-09-01-distributed-task-scheduler-design.md` §6（高层已定，本文给出可落盘契约）
- 承载：`feat/m5-console`（自 main `1f56642` 起）
- 状态：草拟，待评审

## 0. 范围与纪律

M5 交付 v1 的最后一块：**管理控制台（独立 React SPA）**。M1–M4 已全部合并 main；本文定义控制台的后端 REST 增量、前端工程形态、指标契约，以及子里程碑切分与验收。

纪律（沿用 §6 与 §7，不重述推导）：
- **控制台只走 `/api/v1` REST，绝不直连执行库。** 人工查看/操作走 API，执行走 DB。
- 聚焦"能看卡在哪、能动 DLQ、能人工重跑/暂停、能看积压老化"，不做展示型 demo。
- **DagEngine 仍是 `dag_run`/`dag_run_node` 的唯一写者**（M4 纪律）：单节点重跑的节点态变更必须经引擎，不得在 controller/repository 里直接写节点终态。
- 明确不做（YAGNI / M4 §7 纪律）：自动回滚、整轮重试、DAG 超时。单节点重跑不做"下游自动复位"，下游仍按既有失败传播语义收敛。

## 1. 后端 REST 增量（4 处缺口 + 1 项指标源）

现状 `/api/v1`（全在 main）：tasks create/list/get/pause/resume/trigger；executions list(`taskId`,`status`)/get-detail(shards,args,result)/dlq/`shards/{id}/requeue`/`{id}/cancel`；dags create/list/get/pause/resume/trigger/runs/run-detail/run-cancel；metrics 走 `/actuator/prometheus`。

> 注：执行列表的 `status` 过滤已存在；缺口精确为**时间窗**与**分页**。

### 1.1 任务编辑 `PUT /api/v1/tasks/{id}`  （M5.1）

缺口：无编辑接口。

- 请求体（镜像 `CreateTaskRequest`，去除 creator-only 语义）：
  `name, kind, handlerRef, cron, shardCount, timeoutSeconds, maxRetries, backoffMs, retryableFailurePattern, maxActiveConcurrent, paused`
- **不动 `enabled`**：启停由既有 `pause/resume` 表达，PUT 不改 enabled。
- 校验与 create 一致（name/handlerRef/cron 非空、`validateCron` 6/7 字段、各数值下限）。
- 语义：定义即低频变更，运行头引用 `app_task` 仅存 id，定义字段可随时安全更新；`paused` 仅更新 pause 标志。
- 并发：不引入版本号（YAGNI——低频人工操作 + 单行更新）；约定"先 GET 再 PUT"。
- 成功 → `200 + Task(更新后现态)`；task 不存在 → 404。
- 数据层：`TaskRepository` 新增 `update(id, Task patch)`（SQL `UPDATE app_task SET ... WHERE id=?`，返回受影响行数；0 → 404）。

### 1.2 任务重跑 `POST /api/v1/tasks/{id}/rerun`  （M5.1）

缺口：无重跑端点。与既有入口分工：
- `POST /{id}/trigger` = 手动触发（无参）。
- `POST /{id}/rerun` = 复制新建一轮整体手动执行，可携带 `args`；与 DLQ 侧 `POST /shards/{id}/requeue`（失败分片重放）互补——本端点是对**整个任务**重跑。

- 请求体（可选）：`record RerunRequest(String args)`
- 语义：**无条件新建一轮 run**，不校验上次状态、不显式依赖上次失败（简单、幂等安全——每次新建独立 execution）。复用 trigger 底座 `shards.createParentWithShards(t.id(), key, t.shardCount())`，`key = "manual:"+taskId+":"+uuid`。
- 成功 → `201 + Execution(父 header)`；task 不存在 → 404。
- 验收：与 trigger 共用路径，新增一个 E2E 断言 rerun 产出一轮新 execution 且 args 正确落库。

### 1.3 执行列表时间窗 + 分页（补 `status` 之外）  （M5.2）

缺口：`GET /executions` 现只收 `taskId`/`status`，无法按时间窗看"卡住的老执行"。

- 请求（全部可选）：`taskId & status & from & to & limit & offset`
  - `from`/`to`：ISO-8601 → 按 `startedAt` 过滤。
  - 分页：`limit`（默认 100，上限 500）/ `offset`。
- **返回保持 JSON 数组不变**（向后兼容）；分页用 `limit+1` 探测末尾 `hasMore`，不引入 count(*) 总量（避免每页 counts 开销）。
  - 数组仍为 `List<Execution>`（父 header 列表，与现 `/list` 一致）。客户端用 `offset` 翻页。
- 语义限定：执行父 status 在 list 场景不派生 RUNNING（保持与现 list 一致——现 list 返回 raw header；详情才派生）。若要 list 也派生需另议，首版不派生。
- 数据层：`ExecutionQueryService.list` 重构为接受 filters（`taskId/status/from/to/limit/offset`），SQL 动态拼接 `WHERE` + `ORDER BY started_at DESC NULLS LAST` + `LIMIT/OFFSET`。

### 1.4 DAG 单节点重跑 `POST /api/v1/dags/runs/{runId}/nodes/{nodeId}/rerun`  （M5.3）

缺口：M4 §0 显式遗留至本里程碑。

- 前置条件：该 `dag_run_node` 处于**终态**（SUCCESS / FAILED / SKIPPED / CANCELED 均可）；非终态（PENDING/RUNNING/派生命中）→ 409。
- 语义：重跑该节点 = 对其关联 task 新建一轮 manual execution（沿用 `createParentWithShards`），把节点重新挂到新 `execution_id` 并置为该节点"重跑态"，随后由 **DagEngine** 正常推进至终态。**节点态写死走 DagEngine，不得在 controller/repository 直接改写。**
  - 具体机制：引擎新增"节点重跑"入口——对终态节点允许重置为新一轮运行头（重新 spawn 其上/中边），下游不再接上游终态的旧结果，按新运行头 derive 传播。
  - 边界：**不自动复位下游**（M4 §7 不做自动滚卷）；重跑节点的下游保持当前态，待该节点新运行头成功后按既有传播语义推进。
- 成功 → `200 + 该节点现态`（或 `202` 异步——首版同步返回新建 run 的节点行即可）。
- 需在 plan 收敛：重跑态如何表示（复用 PENDING→RUNNING 状态机还是引入 `node.rerun_token`）；是否允许"重跑已失败节点"（允许）与"同一 run 下重跑成功后再次重跑"（允许，每次新建）。
- 验收：E2E——终态 FAILED/SKIPPED 节点 rerun → 节点回到运行 → 终态被重新派生；未终态节点 rerun → 409；断言经 DagEngine（非直写）。

### 1.5 指标面板数据契约（补 §8 遗留）  （M5.3）

现状 gauges：`scheduler_active_runs`(per task)、`scheduler_due_queue_max_age_seconds`(per task)、`scheduler_dag_runs_active`(per dag)。按 §6.2 面板还需：
- **DLQ 深度**：新增 `scheduler_dlq_depth`（全局，或按 task 维度 tag 后聚合由面板求和；V1 全局 gauge）。
- **worker 存活**：信号源经探实现确认为 **PostgreSQL session advisory lock**（选主锁，见 `AdvisoryLockLeaderElection`）——**无独立心跳、无 per-worker 概念**。故暴露 `scheduler_worker_active`（0/1 = 当前节点是否持有选主锁，即调度进程存活）。这是明确边界：v1 不做 per-runner 心跳表。
- 面板数据契约 = 上表每个指标的类型/维度/demo 归一化方式，面板只读消费 `GET /actuator/prometheus`（或 actuator 的 JSON meter 端点），**不引入 Grafana 为默认**，不引入自建 K8s/编排。

## 2. 前端 React SPA（`scheduler/web/`）

- 脚手架：**React 19 + Vite + TypeScript**，独立 npm 工程 `scheduler/web/`（与 portfolio 同栈）。react-router 仅下一页路由，或多页各一 entry；首版以 3 路由 + 参数态。
- 依赖克制：**不引入 UI 组件库 / 状态库 / 图表库重依赖**（聚焦可运维）；指标面板用轻量内联 SVG/sparkline 手绘，避免重 dependency。
- 目录：`web/src/api/`（fetch + TS 类型镜像 §1 REST 契约的 client）、`web/src/pages/`（task / executions / dags）、`web/src/lib/`（共享小工具）。
- 数据流：页面本地 state + **轮询**刷新（可运维即刷新型；间隔可选 5s/10s 页面决定），无全局状态层。
- 联调：dev 用 vite proxy `/api` → `localhost:8080`；产物可由 Spring Boot 静态托管或独立部署（§6.1）。

页面映射 §6.2（五页 → 三路由 + 面板）：
1. **任务页**（M5.1）：列表/创建/编辑（触发/可靠性/分片/配额）/启用-暂停/手动触发/重跑。
2. **执行页**（M5.2）：列表（任务/状态/时间窗）+ 详情（shards、时间线、args、result）+ 取消。
3. **DLQ 页**（M5.2）：死信清单 + 查看详情 + 人工重放。
4. **DAG 页**（M5.3）：批次视图 + 批次状态 + 单节点重跑 + 批次终止。
5. **指标面板**（M5.3）：§1.5 六项核心指标只读展示。

## 3. 子里程碑与验收

沿用既有流程：每个子里程碑 = 独立 spec 增补（本规范内）+ plan(TODOs) + **TDD + subagent 实现 + 逐 Task 审查 + 分支末路审查(opus) + finishing 合并 main**；全 reactor 保持绿（现 140）。

| 子里程碑 | 后端 | 前端 | 验收锚点 |
|---|---|---|---|
| **M5.1 任务页** | §1.1 `PUT /tasks/{id}`、§1.2 `POST /tasks/{id}/rerun`、`TaskRepository.update` | `web/` 脚手架 + 任务页（列表/建/编/启停/触发/重跑） | 后端 PUT/rerun E2E 全绿；任务页对既有 + 新增端点可操作 |
| **M5.2 执行 + DLQ** | §1.3 执行时间窗+分页、`ExecutionQueryService` 重构 | 执行页（筛选/详情/取消）+ DLQ 页（清单/重放） | 执行筛选/详情/取消、DLQ 重放 E2E 全绿 |
| **M5.3 DAG + 指标** | §1.4 单节点重跑（经 DagEngine）、§1.5 `scheduler_dlq_depth` + `scheduler_worker_active` | DAG 页（批次/单节点重跑/终止）+ 指标面板 | 单节点重跑 E2E（含 409 分支）；面板渲染 6 指标 |

每个子里程碑先写 plan（writing-plans）再执行；文档（spec/plan）随分支提交。

## 4. 明确不做 / 边界

- 不做自动回滚、整轮重试、DAG 超时（M4 纪律）——单节点重跑不自动复位下游。
- 不引入 UI/状态/图表组件库重依赖；不引入 Grafana 为默认。
- 控制台绝不写库。
- 不做 per-runner worker 心跳；worker 存活以选主锁持有为信号并标注边界。
- 执行列表首版不派生父 RUNNING 展示（与现 list 一致）；如需一致派生留待后续。
- 执行列表不引入 count(*) 总量，用 hasMore 探测分页。

## 5. 开放项（进各子 plan 收敛）

1. 单节点重跑的"重跑态"表示：复用 PENDING→RUNNING 状态机 vs 引入 `rerun_token`。
2. rerun 是否带参即首版带 `args`（§1.2 已定），是否另需"重跑沿用上次参数"开关。
3. 时间窗 `from`/`to` 的时区语义（ISO-8601 带偏移 vs UTC）——首版取 ISO-8601 带偏移，按列时区求值。
4. metric hub：`scheduler_worker_active` 依赖选主锁持有判定，若未来引入多节点需重审。