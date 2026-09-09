# M5.2 执行页 + DLQ 页 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** M5 里程碑第二块——执行页（时间窗 + 分页筛选、详情、取消）与 DLQ 页（死信清单、人工重放）。

**Architecture:** 后端为 `GET /api/v1/executions` 补时间窗（`from`/`to`，按 `startedAt`）与分页（`limit` 默认 100 上限 500 / `offset`），`ExecutionQueryService.list` 重构接收 filters；`ExecutionController.list` 加 `from/to/limit/offset` 参数。DLQ/详情/取消/requeue 后端端点均已在 main 存在，仅前端新建页面消费。前端引入 react-router（spec §2 明确允许"react-router 仅下一页路由"）搭三路由导航，新增执行页与 DLQ 页。

**Tech Stack:** Java 21 / Spring Boot web-jdbc / Maven reactor（scheduler-server）· React 19 + Vite + TS（scheduler/web），新增依赖仅 `react-router-dom`。

**Spec:** `docs/superpowers/specs/2026-09-09-scheduler-m5-console-design.md`（本 plan 实现 §1.3 后端 + §2 执行页 / DLQ 页 / 路由）

## Global Constraints

（自 spec 逐字引用 + 现状核实，约束本 plan 全部 Task。）

- 控制台只走 `/api/v1` REST，绝不直连执行库。人工查看/操作走 API，执行走 DB。
- 执行列表 `GET /api/v1/executions` **返回保持 JSON 数组 `List<Execution>` 不变**（向后兼容）。
- 执行列表分页：`limit`（默认 100，上限 500）/ `offset`；**分页用 `limit+1` 探测末尾 `hasMore`，不引入 count(*) 总量**。
- 时间窗 `from`/`to`：ISO-8601 带偏移，按列时区求值（spec 开放项 §5.3 已收敛）。
- `ORDER BY started_at DESC NULLS LAST`（时间窗/分页需确定序）。
- 执行父 status 在 list 场景**不派生 RUNNING**（保持与现 list 一致；详情才派生）。首版不派生。
- DLQ 是 `FAILED` + `dead_letter` 布尔标记（status 枚举无 `dlq` 值）；重放 = `requeueShard`（CAS `FAILED→DUE`）。前端读 `shard.deadLetter`。
- 前端：无 UI 组件库 / 状态库 / 图表库重依赖（spec §2）；页面本地 state + 轮询；仅新增 `react-router-dom`。
- 前端 `Execution` 类型为结构子集——本次补上 `leaseUntil`/`nextRetryAt` 两字段（后端 record 有，前端类型须合法读取）。
- **不做**（YAGNI / spec §4）：执行列表 count(\*) 总量、父 list 派生 RUNNING、DAG 页（M5.3）、指标面板（M5.3）。

---

### 现状（主 `d759179`，已核实）

后端 `scheduler-server/.../service/ExecutionQueryService.java`：
```java
public List<Execution> list(Long taskId, String status) {
  StringBuilder sql = new StringBuilder("SELECT * FROM execution WHERE true");
  List<Object> args = new ArrayList<>();
  if (taskId != null) { sql.append(" AND task_id=?"); args.add(taskId); }
  if (status != null && !status.isBlank()) { sql.append(" AND status=?"); args.add(status); }
  sql.append(" ORDER BY id DESC");
  return jdbc.query(sql.toString(), ROW, args.toArray());
}
```
`Execution` 父 record（scheduler-core）：`(Long id, Long taskId, ExecutionStatus status, String idempotencyKey, String args, int shardIndex, int shardCount, int attempt, String workerId, Instant leaseUntil, Instant nextRetryAt, Instant startedAt, Instant finishedAt, String resultPayload)` —— 有 `startedAt`（列 `started_at timestamptz`）。

`ExecutionController.list`（`@RequestMapping("/api/v1/executions")`，第 41-45 行）：
```java
@GetMapping
public List<Execution> list(@RequestParam(required = false) Long taskId,
                            @RequestParam(required = false) String status) {
  return queryService.list(taskId, status);
}
```

DLQ / 详情 / 取消（main 已有，前端将消费）：
- `GET /executions/dlq` → `List<Shard>`（`findDeathLetterShards`：`SELECT * FROM execution_shard WHERE status='FAILED' AND dead_letter ORDER BY id`）。
- `GET /executions/{id}` → `ExecutionDetail(long id, long taskId, ExecutionStatus status, int shardCount, List<Shard> shards)`。
- `POST /executions/shards/{shardId}/requeue` → 404 缺失 / 409 非 FAILED / `200 Shard`。
- `POST /executions/{id}/cancel` → 200（已取消/直取消）/ 202（协作取消）/ 409（非 DUE）。

`Shard` record（scheduler-core）：`(Long id, Long executionId, int shardIndex, String shardData, ExecutionStatus status, int attempt, String workerId, Instant leaseUntil, Instant nextRetryAt, boolean cancelRequested, boolean deadLetter, Instant startedAt, Instant finishedAt, String resultPayload)`。

前端 `scheduler/web`：无 router（`App` 直接渲染 `<TasksPage/>`）。`client.ts` 有 `req<T>` + tasks 各方法。`types.ts` 有 `Task`/`Execution`（缺 `leaseUntil`/`nextRetryAt`）/`CreateTaskRequest`/`UpdateTaskRequest`；**无** `Shard`/`ExecutionDetail`。`useInterval(fn, ms)` 已存在。`vite.config.ts` 已代理 `/api → :8080`。

---

## Task 1: 后端执行列表时间窗 + 分页（service + controller + E2E）

后端唯一改动 Task。DLQ/详情/取消/requeue 端点均已在 main，不需后端改动。

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/service/ExecutionQueryService.java`（`list`）
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/ExecutionController.java`（`list` + `parseInstant` helper）
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`（补 E2E）

- [ ] **Step 1: 写失败测试（E2E —— 时间窗过滤 + 分页）**

在 `ApiIntegrationTest` 新增两个方法（复用既有 `postTask(String name)→long`；`resetDb()` 清库；trigger 用 HTTP）。先写测试、验证失败。

```java
@Test
void executionsTimeWindowFilters() throws Exception {
  resetDb();
  long id = postTask("tw");
  trigger(id); // POST /api/v1/tasks/{id}/trigger → 201, 建 1 个 execution 父
  String from = Instant.now().minusSeconds(60).toString();
  String to = Instant.now().plusSeconds(60).toString();
  mockMvc.perform(get("/api/v1/executions")
          .param("from", from).param("to", to))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$", hasSize(1)))
      .andExpect(jsonPath("$[0].taskId", is((int) id)));
  // 窗口排除：过去 1 小时之前的窗口 → 0 条
  String pastFrom = Instant.now().minusSeconds(7200).toString();
  String pastTo = Instant.now().minusSeconds(3600).toString();
  mockMvc.perform(get("/api/v1/executions")
          .param("from", pastFrom).param("to", pastTo))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$", hasSize(0)));
}

@Test
void executionsPagination() throws Exception {
  resetDb();
  long id = postTask("pg");
  trigger(id);
  trigger(id);
  trigger(id); // 3 个 execution 父
  mockMvc.perform(get("/api/v1/executions").param("limit", "2"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$", hasSize(2)));
  mockMvc.perform(get("/api/v1/executions")
          .param("limit", "2").param("offset", "2"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$", hasSize(1))); // 剩 1 条（3-2）
}
```
需 import：`java.time.Instant`、`org.hamcrest.Matchers.hasSize`/`is`（通常已 import）。`trigger(id)` 若已存在同签辅助则复用，否则按 Step 2 mockMvc 写法；`postTask` 返回 long。

运行（预期失败——service 无 from/to/limit/offset 参数 → 编译失败即 RED）：
```bash
mvn -pl scheduler-server -am test -Dtest=ApiIntegrationTest#executionsTimeWindowFilters+executionsPagination -Dsurefire.failIfNoSpecifiedTests=false
```
预期：编译错误 `list(java.lang.Long, java.lang.String) ... cannot be applied to (...)`。

- [ ] **Step 2: 实现 `ExecutionQueryService.list` 接收 filters**

替换 `list`（保留 `ROW`、`deriveStatus`、`getDetail` 不动）：

```java
package dev.scheduler.server.service; // 首部若缺，补 import
import java.sql.Timestamp;
import java.time.Instant;

// —— 在 ExecutionController 里加私有便捷，供 controller 用；service 直接处理 Instant ——

public List<Execution> list(Long taskId, String status, Instant from, Instant to,
                            Integer limit, Integer offset) {
  StringBuilder sql = new StringBuilder("SELECT * FROM execution WHERE true");
  List<Object> args = new ArrayList<>();
  if (taskId != null) { sql.append(" AND task_id=?"); args.add(taskId); }
  if (status != null && !status.isBlank()) { sql.append(" AND status=?"); args.add(status); }
  if (from != null) { sql.append(" AND started_at >= ?"); args.add(Timestamp.from(from)); }
  if (to != null) { sql.append(" AND started_at <= ?"); args.add(Timestamp.from(to)); }
  sql.append(" ORDER BY started_at DESC NULLS LAST");
  int lim = (limit == null) ? 100 : Math.min(Math.max(limit, 1), 500);
  int off = (offset == null) ? 0 : Math.max(offset, 0);
  sql.append(" LIMIT ? OFFSET ?");
  args.add(lim); args.add(off);
  return jdbc.query(sql.toString(), ROW, args.toArray());
}
```
要点：`Timestamp.from(instant)` 显式转 timestamptz 参数（Spring JdbcTemplate 对 java.sql 类型绑定零歧义）；`ORDER BY id DESC → started_at DESC NULLS LAST` 是有意变化（分页/时间窗的确定序）；`LIMIT/OFFSET` 恒追加（`where true` 保证串合法）。

>`verify`（若驱动/Spring 对 `Timestamp.from` 绑定报 `Method getXXX not found` 等，换 `args.add(from)` 直接绑 `Instant`——PostgreSQL 驱动支持 `setObject(Instant)`，两者等价）；`resolution`：以编译/实测为准，任一均可，保持测试绿。

- [ ] **Step 3: 实现 `ExecutionController.list` 加参数 + `parseInstant`**

在 `ExecutionController`：

```java
@GetMapping
public List<Execution> list(
    @RequestParam(required = false) Long taskId,
    @RequestParam(required = false) String status,
    @RequestParam(required = false) String from,
    @RequestParam(required = false) String to,
    @RequestParam(required = false) Integer limit,
    @RequestParam(required = false) Integer offset) {
  return queryService.list(taskId, status,
      parseInstant(from), parseInstant(to), limit, offset);
}

private static Instant parseInstant(String s) {
  if (s == null || s.isBlank()) return null;
  try {
    return Instant.parse(s); // ISO-8601 带偏移
  } catch (DateTimeParseException e) {
    throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
        "invalid `from`/`to` (expected ISO-8601 with offset): " + s);
  }
}
```
补 import：`java.time.Instant`、`java.time.format.DateTimeParseException`、`java.time.format.DateTimeParseException`（若缺 `org.springframework.http.HttpStatus`/`ResponseStatusException` 已 import）。

- [ ] **Step 4: 跑测试，验证 GREEN**

```bash
mvn -pl scheduler-server -am test -Dtest='ApiIntegrationTest#executionsTimeWindowFilters+executionsPagination' -Dsurefire.failIfNoSpecifiedTests=false
```
预期：2 新测试 PASS，且既有 `GET /executions` 相关测试不受影响。

- [ ] **Step 5: 全 reactor 回归**

```bash
mvn test   # Docker（Testcontainers）已启动
```
预期：BUILD SUCCESS，既有 71 tests + 2 新全绿，无回归。

- [ ] **Step 6: 提交**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/service/ExecutionQueryService.java \
        scheduler-server/src/main/java/dev/scheduler/server/web/ExecutionController.java \
        scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java
git commit -m "feat(server): GET /executions 时间窗+分页（from/to per startedAt, limit/offset, started_at DESC NULLS LAST）"
```

**Interfaces Produced（给前端 Task 2）:**
- `GET /api/v1/executions?taskId&status&from&to&limit&offset` → `List<Execution>`（数组不变）。`from/to` ISO-8601 带偏移按 `started_at` 过滤；`limit` 默认 100 上限 500，`offset` 默认 0；`ORDER BY started_at DESC NULLS LAST`。

---

## Task 2: 前端 API client + types 扩展

**Files:**
- Modify: `web/src/api/types.ts`
- Modify: `web/src/api/client.ts`

纯前端，验证 = `npm run build`（tsc -b + vite build）。**Uses Task 1 endpoint.**

- [ ] **Step 1: `types.ts` 加类型**

在现有 `Execution` interface 上加两字段，并新增：

```ts
export interface Execution { // 补字段，其余保留
  // ...现有全部字段...
  leaseUntil: string | null;
  nextRetryAt: string | null;
}

export interface Shard {
  id: number;
  executionId: number;
  shardIndex: number;
  shardData: string | null;
  status: string;
  attempt: number;
  workerId: string | null;
  leaseUntil: string | null;
  nextRetryAt: string | null;
  cancelRequested: boolean;
  deadLetter: boolean;
  startedAt: string | null;
  finishedAt: string | null;
  resultPayload: string | null;
}

export interface ExecutionDetail {
  id: number;
  taskId: number;
  status: string;
  shardCount: number;
  shards: Shard[];
}
```
`Shard`/`ExecutionDetail` 逐字段镜像后端 record（`deadLetter` 是 boolean——DLQ 标记）。`Execution` 补 `leaseUntil`/`nextRetryAt`（合法读取后端 record 全字段）。

- [ ] **Step 2: `client.ts` 加 executions/DLQ 方法**

追加（复用既有 `req<T>`）：

```ts
export interface ListExecutionsParams {
  taskId?: number;
  status?: string;
  from?: string; // ISO-8601 with offset
  to?: string;   // ISO-8601 with offset
  limit?: number;
  offset?: number;
}

export const listExecutions = (p: ListExecutionsParams = {}): Promise<Execution[]> => {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(p)) {
    if (v !== undefined && v !== null && v !== '') q.set(k, String(v));
  }
  const s = q.toString();
  return req<Execution[]>(`/api/v1/executions${s ? `?${s}` : ''}`);
};

export const getExecutionDetail = (id: number) =>
  req<ExecutionDetail>(`/api/v1/executions/${id}`);

export const cancelExecution = (id: number) =>
  req<Execution>(`/api/v1/executions/${id}/cancel`, { method: 'POST' });

export const getDlq = () => req<Shard[]>('/api/v1/executions/dlq');

export const requeueShard = (shardId: number) =>
  req<Shard>(`/api/v1/executions/shards/${shardId}/requeue`, { method: 'POST' });
```
补 import：`Shard`、`ExecutionDetail`（`Execution` 已在，`ListExecutionParams` 当接口本地声明）。空查询串时不带 `?`。

- [ ] **Step 3: build 验证**

```bash
cd scheduler/web && npm install && npm run build
```
预期：`tsc -b && vite build` 通过，无类型错误。

- [ ] **Step 4: 提交**

```bash
git add web/src/api/types.ts web/src/api/client.ts
git commit -m "feat(web): api client 扩展 executions/DLQ —— listExecutions/getExecutionDetail/cancelExecution/getDlq/requeueShard + Shard/ExecutionDetail 类型"
```

**Interfaces Produced（给 Task 3/4/5）:**
- `Execution`（含 `leaseUntil`/`nextRetryAt`）/`Shard`/`ExecutionDetail` 类型；`listExecutions(p)`/`getExecutionDetail(id)`/`cancelExecution(id)`/`getDlq()`/`requeueShard(shardId)`。`cancelExecution` 返回 `Promise<Execution>`（200 非 2xx 抛错；202 协作取消时 `req<T>` 仍 `res.json()` 解析 `Execution`，OK）。

---

## Task 3: 前端路由 + 三页导航（react-router）

给 SPA 加路由（spec §2「react-router 仅下一页路由」），把任务页移到 `/tasks`，预留 `/executions`、`/dlq`（页面体由 Task 4/5 填，此处先挂占位壳确保构建/导航可用）。**Uses Task 2.**

> 依赖收敛：仅新增 `react-router-dom`（^6 —— react19 稳定兼容、非 UI/状态库）。在 plan 执行时以 `npm install react-router-dom@^6` 落地。

**Files:**
- Modify: `web/package.json`（加 `react-router-dom` 依赖）
- Modify: `web/src/App.tsx`
- Create: `web/src/pages/AllPages.tsx`（壳：三路由 + 导航，执行/DLQ 先放空组件容器）

- [ ] **Step 1: 装依赖**

```bash
cd scheduler/web && npm install react-router-dom@^6
```
预期：package.json + package-lock.json 更新，react 19 无 peer 冲突。

- [ ] **Step 2: 改 `App.tsx`**

```tsx
import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import TasksPage from './pages/TasksPage';
import ExecutionsPage from './pages/ExecutionsPage';
import DlqPage from './pages/DlqPage';

export default function App() {
  return <RouterProvider router={router} />;
}
```
> 路由表与导航统一放此文件或单独 `main.tsx` 注册——实现选一处，任务页仍可访问、`/` 重定向到 `/tasks`。先写一个最小可编译版本（参见 Step 3），`ExecutionsPage`/`DlqPage` 占位由 Task 4/5 替换为真实实现（本 Task 先提供最简容器，保证 build 绿、路由可达）。

- [ ] **Step 3: 最小可编译路由 + 导航占位**

`App.tsx`（导航条含 任务 / 执行 / DLQ 三 link）：
```tsx
import { Link, Route, Routes } from 'react-router-dom';
import TasksPage from './pages/TasksPage';
/* 由 Task 4/5 填充真实页面；本 Task 先用占位容器保证链路 */
import ExecutionsPage from './pages/ExecutionsPage';
import DlqPage from './pages/DlqPage';

export default function App() {
  return (
    <div>
      <nav>
        <Link to="/tasks">任务</Link>{' '}
        <Link to="/executions">执行</Link>{' '}
        <Link to="/dlq">DLQ</Link>
      </nav>
      <Routes>
        <Route path="/" element={<Navigate to="/tasks" replace />} />
        <Route path="/tasks" element={<TasksPage />} />
        <Route path="/executions" element={<ExecutionsPage />} />
        <Route path="/dlq" element={<DlqPage />} />
      </Routes>
    </div>
  );
}
```
用 `createBrowserRouter` 或 `<BrowserRouter>` 包裹 `<Routes>`（实现择一并 `import { Navigate, BrowserRouter } from 'react-router-dom'` 补齐）。`executions/page` 的目标是 `web/src/pages/ExecutionsPage.tsx`/`DlqPage.tsx`，本 Task 先写两个极简 require（各 `export default () => <div>…（Task 4/5 实现）</div>`），使 `tsc` 可过。

- [ ] **Step 4: build 验证**

```bash
cd scheduler/web && npm run build
```
预期：通过，`tsc -b` 无未用/缺失 import 错误，`vite build` 出产物。

- [ ] **Step 5: 提交**

```bash
git add web/package.json web/package-lock.json web/src/App.tsx web/src/main.tsx web/src/pages/ExecutionsPage.tsx web/src/pages/DlqPage.tsx
git commit -m "feat(web): react-router 三路由导航（任务/执行/DLQ）+ 占位壳"
```
> 注：若实现选择把 router 注册放 `main.tsx`，则 `main.tsx` 一并纳入 commit。任务页行为保持（`/tasks` 可访问、5s 轮询照旧）。

---

## Task 4: 执行页（筛选 + 分页列表 + 详情 + 取消）

**Files:**
- Modify: `web/src/pages/ExecutionsPage.tsx`（替换 Task 3 占位）
- Uses: Task 1 endpoint + Task 2 client + `useInterval`（已有）

**页面行为（spec §6.2 页 2）：**
- 筛选：`taskId`（下拉，来自 `listTasks()`）、`status`（下拉：DUE/RUNNING/SUCCESS/FAILED/ORPHANED/CANCELED）、`from`/`to`（`datetime-local`）。
- 分页：底部「加载更多」；`PAGE_SIZE=10`，请求 `limit=PAGE_SIZE+1`（探测 hasMore），`arr.length > PAGE_SIZE ? arr.slice(0, PAGE_SIZE) : arr` 渲染，`hasMore = arr.length > PAGE_SIZE`；点击推 `offset += PAGE_SIZE`。
- 详情：点行 → `getExecutionDetail(id)` 展示父 + shards 表；「取消」按钮调 `cancelExecution(id)`（后端 200/202/409，错误经 err 呈现）。
- 5s 轮询刷新当前列表；本地 state，无全局状态层。

- [ ] **Step 1: 实现 `ExecutionsPage.tsx`**

```tsx
import { useCallback, useEffect, useState } from 'react';
import { listExecutions, getExecutionDetail, cancelExecution, listTasks } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { Execution, ExecutionDetail, Shard, Task } from '../api/types';

const PAGE_SIZE = 10;
const STATUSES = ['DUE', 'RUNNING', 'SUCCESS', 'FAILED', 'ORPHANED', 'CANCELED'];

export default function ExecutionsPage() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [taskId, setTaskId] = useState('');
  const [status, setStatus] = useState('');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [rows, setRows] = useState<Execution[]>([]);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [detail, setDetail] = useState<ExecutionDetail | null>(null);

  const load = useCallback(async () => {
    try {
      const q = { taskId: taskId ? Number(taskId) : undefined, status: status || undefined,
                  from: from ? new Date(from).toISOString() : undefined,
                  to: to ? new Date(to).toISOString() : undefined,
                  limit: PAGE_SIZE + 1, offset };
      const arr = await listExecutions(q);
      setRows(arr.length > PAGE_SIZE ? arr.slice(0, PAGE_SIZE) : arr);
      setHasMore(arr.length > PAGE_SIZE);
      setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [taskId, status, from, to, offset]);

  useEffect(() => { load(); }, [load]);
  useInterval(load, 5000);

  useEffect(() => { listTasks().then(setTasks).catch(() => {}); }, []);

  const openDetail = useCallback(async (id: number) => {
    try { setDetail(await getExecutionDetail(id)); setErr(null); }
    catch (e) { setErr(String(e)); }
  }, []);
  const doCancel = useCallback(async (id: number) => {
    try { await cancelExecution(id); setErr(null); await load(); setDetail(null); }
    catch (e) { setErr(String(e)); }
  }, [load]);
  const apply = () => { setOffset(0); load(); };

  return (
    <div>
      <h2>执行</h2>
      {err && <p style={{ color: 'red' }}>{err}</p>}
      <label>任务
        <select value={taskId} onChange={e => setTaskId(e.target.value)}>
          <option value="">全部</option>
          {tasks.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
        </select>
      </label>{' '}
      <label>状态
        <select value={status} onChange={e => setStatus(e.target.value)}>
          <option value="">全部</option>
          {STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </label>{' '}
      <label>从 <input type="datetime-local" value={from} onChange={e => setFrom(e.target.value)} /></label>{' '}
      <label>到 <input type="datetime-local" value={to} onChange={e => setTo(e.target.value)} /></label>{' '}
      <button onClick={apply}>应用</button>
      <table border={1} cellSpacing={0} cellPadding={4}>
        <thead><tr><th>id</th><th>taskId</th><th>状态</th><th>分片</th><th>attempt</th><th>startedAt</th></tr></thead>
        <tbody>
          {rows.map(r => (
            <tr key={r.id} onClick={() => openDetail(r.id)} style={{ cursor: 'pointer' }}>
              <td>{r.id}</td><td>{r.taskId}</td><td>{r.status}</td>
              <td>{r.shardCount}</td><td>{r.attempt}</td>
              <td>{r.startedAt ?? '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {hasMore && <button onClick={() => setOffset(o => o + PAGE_SIZE)}>加载更多</button>}
      {detail && (
        <div>
          <h3>详情 #${detail.id} · 状态 {detail.status}</h3>
          <button onClick={() => setDetail(null)}>关闭</button>{' '}
          <button onClick={() => doCancel(detail.id)}>取消执行</button>
          <table border={1} cellSpacing={0} cellPadding={4}>
            <thead><tr><th>shard</th><th>idx</th><th>状态</th><th>attempt</th><th>workerId</th><th>deadLetter</th></tr></thead>
            <tbody>
              {detail.shards.map(s => (
                <tr key={s.id}>
                  <td>{s.id}</td><td>{s.shardIndex}</td><td>{s.status}</td>
                  <td>{s.attempt}</td><td>{s.workerId ?? '—'}</td><td>{String(s.deadLetter)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
```
> 实现提示：`new Date(from)` 对 `datetime-local` 值（`YYYY-MM-DDTHH:mm`）按本地时区解析 → `.toISOString()` 转 UTC ISO（带偏移/`Z`），匹配后端 `Instant.parse`；`q` 中空串字段以 `undefined` 跳过（client 端 `listExecutions` 已过滤）。未使用变量（如 `Shard`）不 import——只 import 实际用到的（上方注释若 `Shard` 未直接用则不 import，避免 `tsc` unused 报错；`detail.shards` 类型由 `ExecutionDetail` 推导）。

- [ ] **Step 2: build 验证**

```bash
cd scheduler/web && npm run build
```
预期：通过，无 unused import / 类型错误。

- [ ] **Step 3: 提交**

```bash
git add web/src/pages/ExecutionsPage.tsx
git commit -m "feat(web): 执行页 —— 筛选(taskId/status/时间窗)+分页列表+详情+取消，5s 轮询"
```

---

## Task 5: DLQ 页（清单 + 重放）

**Files:**
- Modify: `web/src/pages/DlqPage.tsx`（替换 Task 3 占位）
- Uses: Task 2 client + `useInterval`

**页面行为（spec §6.2 页 3）：** 死信清单（`FAILED` + `deadLetter`）+ 看分片信息 + 人工重放（`requeueShard`）。5s 轮询。

- [ ] **Step 1: 实现 `DlqPage.tsx`**

```tsx
import { useCallback, useEffect, useState } from 'react';
import { getDlq, requeueShard } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { Shard } from '../api/types';

export default function DlqPage() {
  const [rows, setRows] = useState<Shard[]>([]);
  const [err, setErr] = useState<string | null>(null);

  const load = useCallback(async () => {
    try { setRows(await getDlq()); setErr(null); }
    catch (e) { setErr(String(e)); }
  }, []);

  useEffect(() => { load(); }, [load]);
  useInterval(load, 5000);

  const doRequeue = useCallback(async (id: number) => {
    try { await requeueShard(id); setErr(null); await load(); }
    catch (e) { setErr(String(e)); }
  }, [load]);

  return (
    <div>
      <h2>DLQ（死信）</h2>
      {err && <p style={{ color: 'red' }}>{err}</p>}
      {rows.length === 0
        ? <p>（空）</p>
        : (
          <table border={1} cellSpacing={0} cellPadding={4}>
            <thead><tr><th>shardId</th><th>execId</th><th>idx</th><th>状态</th><th>attempt</th><th>startedAt</th><th>操作</th></tr></thead>
            <tbody>
              {rows.map(s => (
                <tr key={s.id}>
                  <td>{s.id}</td><td>{s.executionId}</td><td>{s.shardIndex}</td>
                  <td>{s.status}</td><td>{s.attempt}</td><td>{s.startedAt ?? '—'}</td>
                  <td><button onClick={() => doRequeue(s.id)}>重放</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
    </div>
  );
}
```
> 重放语义：后端 `requeueShard` CAS `FAILED→DUE`，非 FAILED 时 409（后端已挡，页面随 409 呈现 err）。重放后该分片从 DLQ 列表消失（不再 `FAILED+dead_letter`）。

- [ ] **Step 2: build 验证**

```bash
cd scheduler/web && npm run build
```
预期：通过。

- [ ] **Step 3: 提交**

```bash
git add web/src/pages/DlqPage.tsx
git commit -m "feat(web): DLQ 页 —— 死信清单 + 人工重放，5s 轮询"
```

---

## Self-review（对照 spec）

**Spec coverage:**
- §1.3 后端时间窗+分页 → Task 1。✓
- §6.2 页 2 执行页（筛选/详情/取消）→ Task 4。✓
- §6.2 页 3 DLQ 页（清单/重放）→ Task 5（后端端点已存在，仅前端）。✓
- `limit+1` 探测 hasMore、不引入 count(\*)，list 不派生 RUNNING → Task 1/4。✓

**Placeholder scan:** 无 TBD/「类似 Task N」/「适当错误处理」；每个 Task 给可执行代码。Task 3 占位壳是**有意**的（路由骨架先可编译，页面体由 Task 4/5 替换）——已在文中标注。

**Type consistency:**
- 前端 `Execution` 补 `leaseUntil`/`nextRetryAt`（镜像后端 record）；`Shard`/`ExecutionDetail` 拼写与后端 record 字段一致。
- `listExecutions(p)` 参数名 = 后端 `@RequestParam`（taskId/status/from/to/limit/offset）。✓
- `cancelExecution` → `POST /{id}/cancel` → `Execution`；`requeueShard` → `POST /shards/{id}/requeue` → `Shard`。✓