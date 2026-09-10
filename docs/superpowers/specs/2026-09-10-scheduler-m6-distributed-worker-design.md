# M6 分布式 worker 面 —— 实现级设计

- 日期：2026-09-10
- 上一级：`2026-09-01-distributed-task-scheduler-design.md` §1（"任务调度器只负责何时跑/分片/重试/并发，业务 handler 属执行面"）；并**推翻** `2026-09-09-scheduler-m5-console-design.md` §4 的"不做 per-runner 心跳 / worker 存活以选主锁持有为信号"边界（M6 引入 per-worker 心跳注册表）。
- 承载：`feat/m6-distributed-worker`（自 main 起）
- 状态：草拟，待评审

## 0. 范围与纪律

M6 解决核心问题:**把「控制面(调度)」与「执行面(worker)」拆成进程**，使"加一种任务类型"不再需要改/重启调度器，并堵住三个既有加固缺口。这是 demo 项目走向成熟平台的第一步骨架。

核心纪律（沿用 §1 且 M6 强化）：
- **控制面 `scheduler-server` 进程中不再存在、不再 import 任何 `ExecutionHandler`**；它只负责调仓(trigger/分片/DAG)、路由决策、状态查询，以及校验。它绝不跑业务代码。
- **业务 handler 只存在于 `scheduler-worker` 进程中**，按 worker 各自的 `HandlerRegistry` 解析 `handlerRef` 并运行。
- **DB 同时充当 broker（DUE 队列 + 租约认领）与结果后端（outcome）与 worker 注册表**——沿用已有 lease-claim 模型（`ExecutorWorker` 原子认领 + ORPHANED 回收），不引入新中间件。
- 控制台(web SPA)只走 `/api/v1`，绝不直连 DB；worker 写 DB，控制面读 DB。
- worker 心跳/注册是**控制面的路由与校验数据源**，但 worker 存活不是"调度总在线"的前置——控制面与 worker 可独立启动/下线。

## 1. 模块拓扑：拆出 `scheduler-worker`

新增 Maven 模块 **`scheduler-worker`**（独立 Spring Boot app，入口 `dev.scheduler.worker.WorkerApplication`），自 `scheduler-server` **搬移**：
- `handle/`：`ExecutionHandler`、`HandlerContext`、`CancellationToken`、`HandlerRegistry`、`MapHandlerRegistry`、`DemoHandler`
- `execute/`：`ExecutorWorker`
- `retry/`：`FailureResolver`、`RetryPolicy`、相关

依赖 `scheduler-core` + `scheduler-persistence`（已拆好，零冲突——`ExecutorWorker` 已确认只碰 DB + registry，不依赖 web/trigger/leader/reconcile）。

**`scheduler-server` 保留并下沉**：web / trigger / leader / reconcile / dag / config。删除 `Beans.java` 中 `demoHandler`、`handlerRegistry` 两个 bean 及对 handler/executor 的装配；删 `execute/` `handle/` `retry/` 三个包。

控制面上下文彻底无 handler → 服务端不再有"按 handlerRef 取 handler"能力，改为读 worker 注册视图做校验（§3）。

worker 进程可选暴露 actuator（独立端口，如 8081）供 `scheduler_worker_active` 等 per-worker 指标；首版至少暴露 `/actuator/health` 供探活。

## 2. Worker 注册-心跳协议（DB 当注册表，零新增基础设施）

新表 `worker`（Flyway 迁移）：
```
worker (
  id           varchar  pk,          -- workerId
  refs         jsonb    not null,    -- 该 worker 注册的 handler refs
  last_seen    timestamptz not null, -- 心跳时间
  status       varchar  not null,    -- ALIVE / DEAD / DRAINING
)
```
- worker 启动时：注册本节点 refs（其 `HandlerRegistry.refs()`），置 status=ALIVE。
- worker 心跳循环 ~10s：upsert `last_seen`（可捎带 refs 变更）。
- 控制面判定：`last_seen` 超过阈值（如 30s）→ 视为下线（校验不再采信其 refs；其持有租约由既有 ORPHANED 回收休眠接管）。
- worker 认领 DUE shard → **按自身 registry** 解析 `handlerRef` → 运行 → 写 outcome → 释放租约。**从未向控制面请求任何运行时信息**（父 args/shardCount 与认领一样只读 DB）。

Flyway 迁移所有权（开放项 1）：当前迁移在 server 的 test-scope 下、且 server 与 worker 指向同一库。需在 plan 收敛——谁负责启动迁移（建议单一模块拥有，worker 启动做 baseline 校验，不重复建表）。

## 3. 服务端校验 + 冲突 fail-fast（堵住"拼错/孤儿 ref"）

- **`TaskController` create/update 校验**：要求 `handlerRef ∈ 存活 worker 的 refs`（控制面注册视图 = `refs` 并集，仅采信 `last_seen` 新鲜的 worker）。不满足 → `400` + 明确文案（如 "handler ref 'x' served by no live worker"）。**孤儿 ref 建不进去**，从"派发时 null 炸"变为"入口即拒绝"。
- **`MapHandlerRegistry` 改为 fail-fast**：构建时检测重复 `ref()` → 抛异常使 worker 启动失败。取代现 `toMap((a,b)->a)` 的静默覆盖。
- 控制面删除 `handlers.get(ref)` 路径；`handlerRef` 在 server 侧仅作字符串字段 + 校验。

## 4. 隔离与横向扩展

- 每 worker 一个 JVM/Spring app；handler 崩溃/OOM 只炸该 worker 的当次运行，其他 worker 与全部任务不受影响。
- 加任务类型 = 让某 worker 的 registry 注册新 `handlerRef`（改 worker 侧即可），控制面与前端零改动。
- 横向扩展 = 起更多 worker 进程，全部指向同一 DB，靠原子认领分摊 DUE shard。
- 回收续用既有机制：租约过期 + ORPHANED 状态 → 由存活 worker/控制面接管重跑。

## 5. 测试迁移

现状 `ApiIntegrationTest`（@Testcontainers + 完整 Boot 上下文）依赖 server 进程内跑 executor（触发 → 分片被执行 → 断言终态）。拆分后 server 上下文无 worker，该链会断。改造：
- **worker 集成测试**（新模块）：DUE→RUNNING→SUCCESS 全链 + 心跳注册 + 孤儿 ref 运行时语义。
- **server 集成测试**：触发后 shard 停在 DUE（无 worker 时）；`create/update` 拒绝孤儿 ref（400）。
- handler / registry / retry 单测搬入 `scheduler-worker`。
- 全 reactor 保持绿。

## 6. 实施顺序（子里程碑化）

沿用既有流程：独立 plan + TDD + subagent 实现 + 逐 Task 审查 + 分支末路审查(opus) + finishing + 合并 main；全 reactor 绿。

| 子 | 内容 | 验收锚点 |
|---|---|---|
| **M6.1 worker 模块骨架** | 建 `scheduler-worker` 模块 + `WorkerApplication` + 装配 execute/handler/retry beans + `worker` 表迁移 + worker 心跳注册 | worker 进程启动、注册表写入、`worker` 行 upsert；server 端能读出存活-refs 视图 |
| **M6.2 服务端校验 + fail-fast** | `TaskController` 存活-ref 校验；`MapHandlerRegistry` 重复 ref 抛错 | 孤儿 ref create/update → 400；重复 ref 起 app 失败；相关 E2E 全绿 |
| **M6.3 server 剔除 handler/executor** | 删 `execute/` `handle/` `retry/` 三包 + 装配；server 上下文无任何 handler | server 编译/启动无 handler 依赖；全 reactor 绿（含测试迁移） |
| **M6.4 端到端 + 真实 handler** | 1 控制 + N worker 实跑；新增第二个真实 handler 证明"不改控制面/前端加任务类型" | 多 worker 认领分摊；新 handler 建任务→执行→终态全链走通（前端下拉框自动多出该项） |

## 7. 明确不做 / 边界（YAGNI）

- 不做每执行子进程/jail（隔离粒度定为 worker 进程级，M6 范围）。
- 不做管理 API 鉴权、审计（后续安全里程碑）。
- 不换 broker（不加 Redis/Kafka）；DB-as-broker 对当前规模够用，换层是后续开关。
- 不做跨界语言 worker / 外部执行 SPI。
- 不引入 worker 分布式协调（认领依赖 DB 原子性，已够）。

## 8. 开放项（进各子 plan 收敛）

1. Flyway 迁移所有权：server vs worker 谁跑迁移；worker 启动的 baseline 策略。
2. `scheduler_worker_active` 指标语义：选主锁(控制面) vs per-worker 心跳，M6 是否在 worker 暴露独立指标端口。
3. worker 下线/灰度：status=DEAD/DRAINING 的写入时机与善后（DRAINING 停收新认领是否首版做）。
4. 前端 `/api/v1/handlers` 语义变为"存活 worker 的 refs 并集"——是否保留该端点（建议保留，前端下拉框零改动）。
5. worker 的 workerId 生成与持久（进程重启后换 id 对在途租约的影响）。