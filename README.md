# scheduler

分布式任务调度器 v1 — 核心调度 + Sharding + Workflow(DAG)+ Web 控制台。

多模块 Maven 工程 + React/TypeScript/Vite 前端。

## 模块

| 模块 | 职责 |
|------|------|
| `scheduler-core` | 领域模型、状态机、幂等键、leader 选举接口 |
| `scheduler-persistence` | JDBC 仓储 + Flyway 迁移（PostgreSQL） |
| `scheduler-server` | Spring Boot 应用、REST API、DAG Engine、Prometheus 指标 |
| `web` | 控制台前端（React + TS + Vite，vite 代理到后端） |

技术栈：Java 17、Spring Boot 3.2.5、PostgreSQL 16、Micrometer/Prometheus、React 18、Vite 6。

## 运行

### 后端（端口 `8080`）

```bash
mvn spring-boot:run -pl scheduler-server
```

配置（可经环境变量覆盖）：

| 环境变量 | 默认 | 说明 |
|----------|------|------|
| `DB_URL` | `jdbc:postgresql://localhost:5432/scheduler` | JDBC 连接串 |
| `DB_USER` / `DB_PASSWORD` | `scheduler` / `scheduler` | 数据库账号 |
| `SCHEDULER_WORKER_ID` | 空 | 节点 workerId；空则用 hostname 派生 |
| `SCHEDULER_LOOP_SCAN_DELAY_MS` | `5000` | 引擎扫描周期 |

Flyway 自动迁移 schema。多节点实例共享同一数据库，经 advisory lock 选主协调。

### 前端（端口 `5173`）

```bash
cd web && npm install && npm run build   # 构建
cd web && npm run dev                     # 开发，/api 与 /actuator 代理到 :8080
```

## Web 控制台路由

| 路由 | 页面 | 说明 |
|------|------|------|
| `/tasks` | 任务列表 | 任务 CRUD、编辑、暂停/恢复、手动触发、重跑；5s 轮询 |
| `/executions` | 执行列表 | 按 任务/状态/时间窗 过滤 + 分页；查看执行明细；取消执行 |
| `/dlq` | 死信队列 | 列出 `dead_letter` shard；单条 requeue 回队 |
| `/dags` | 工作流 (DAG) | DAG 列表、run 批次、run 详情、单节点重跑、终止批次；5s 轮询 |
| `/metrics` | 指标面板 | 5 核心指标卡 + 内联 SVG sparkline；5s 轮询 |

## REST API

统一前缀 `/api/v1`。错误统一走 `ApiExceptionHandler`：4xx 业务错误（含 404 不存在、409 并发/非法状态）、5xx 服务端异常。

### Tasks

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/tasks` | 列出任务 |
| `GET` | `/api/v1/tasks/{id}` | 单个任务 |
| `POST` | `/api/v1/tasks` | 创建任务 |
| `PUT` | `/api/v1/tasks/{id}` | 更新任务 |
| `POST` | `/api/v1/tasks/{id}/pause` | 暂停（不扫入新调度） |
| `POST` | `/api/v1/tasks/{id}/resume` | 恢复 |
| `POST` | `/api/v1/tasks/{id}/trigger` | 手动触发一次 → `200 Execution` |
| `POST` | `/api/v1/tasks/{id}/rerun` | 整任务重跑一次 → `200 Execution` |

任务字段：`name`、`kind`、`handlerRef`、`cron`、`shardCount`、`timeoutSeconds`、`maxRetries`、`backoffMs`、`retryableFailurePattern`、`maxActiveConcurrent`、`enabled`、`paused`。

### Executions

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/executions` | 执行列表；query：`taskId`、`status`、`from`、`to`（ISO-8601 带偏移）、`limit`、`offset` |
| `GET` | `/api/v1/executions/{id}` | 执行明细（含 shards） |
| `POST` | `/api/v1/executions/{id}/cancel` | 取消执行 → `200 Execution` |
| `GET` | `/api/v1/executions/dlq` | 死信 shard 列表 → `[Shard]` |
| `POST` | `/api/v1/executions/shards/{shardId}/requeue` | 死信出队重跑 → `200 Shard` |

### Dags（工作流，M5.3）

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/dags` | 列出 DAG |
| `GET` | `/api/v1/dags/{id}` | DAG 详情（含 nodes + edges） |
| `POST` | `/api/v1/dags` | 创建 DAG |
| `POST` | `/api/v1/dags/{id}/pause` / `.../resume` | 暂停 / 恢复 |
| `POST` | `/api/v1/dags/{id}/trigger` | 触发一次批次 → `200 DagRun` |
| `GET` | `/api/v1/dags/runs` | 批次列表；`?dagId=` 过滤 |
| `GET` | `/api/v1/dags/runs/{runId}` | run 详情（节点 + 各节点 shard） |
| `POST` | `/api/v1/dags/runs/{runId}/cancel` | 终止批次 → `200 RunDetail` |
| `POST` | `/api/v1/dags/runs/{runId}/nodes/{nodeId}/rerun` | **单节点重跑**：终态节点回 RUNNING 并重开批次（同事务置 run 为 PENDING）；非终态/竞态 → `409`；不存在 → `404`。→ `200 DagRunNode` |

> DAG 单节点重跑复用 RUNNING 状态机；每轮重跑生成新的 execution。终态→RUNNING 属 operator 回绕，经显式 CAS 保护；outcome 追加式幂等。

## 指标（Prometheus）

后端暴露 `/actuator/prometheus`（`management.endpoints.web.exposure.include: health,info,prometheus`）。控制台 `/metrics` 页聚合为 5 张卡：

| 指标 | 含义 | 标签 |
|------|------|------|
| `scheduler_active_runs` | 活跃执行数 | `task_id`,`task_name` |
| `scheduler_due_queue_max_age_seconds` | DUE 队列最深队龄（秒） | `task_id`,`task_name` |
| `scheduler_dag_runs_active` | 活跃 DAG 批次数 | `dag_id`,`dag_name` |
| `scheduler_dlq_depth` | 死信队列深度（全局，scrape 时实读 DB） | 无 |
| `scheduler_worker_active` | 当前节点是否持选主锁（调度主，`1`/`0`） | 无 |

## 开发方式

设计契约见 `docs/superpowers/specs/`（逐里程碑），实现计划见 `docs/superpowers/plans/`。每个里程碑经独立特性分支 + 逐任务实现/审查 + whole-branch 复审合并入 `main`。