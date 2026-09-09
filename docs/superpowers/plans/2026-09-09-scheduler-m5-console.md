# M5.1 任务页实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付 M5.1——任务的控制面：后端新增 `PUT /api/v1/tasks/{id}`（任务编辑）与 `POST /api/v1/tasks/{id}/rerun`（任务整体重跑），前端落地 `scheduler/web/` React SPA 脚手架 + 任务页（列表/创建/编辑/启停/触发/重跑）。

**Architecture:** 后端在现有 `TaskController`/`TaskRepository` 上加两个端点，复用 `createParentWithShards` 触发底座（`POST /{id}/trigger` 同路径）；`TaskRepository.update` 走单行 `UPDATE app_task ... WHERE id=?`。前端为独立 npm 工程 `scheduler/web/`（React 19 + Vite + TS），只消费 `/api/v1`，页面本地 state + 轮询，无 UI/状态/图表组件库。

**Tech Stack:** Java 21、Spring Boot web/jdbc、Flyway、PostgreSQL 16（Testcontainers）、JUnit 5 + MockMvc E2E；前端 React 19 + Vite + TypeScript。

**Spec:** `docs/superpowers/specs/2026-09-09-scheduler-m5-console-design.md`（§1.1、§1.2、§2、§3 的 M5.1 行）

## Global Constraints

- 控制台只走 `/api/v1` REST，**绝不直连执行库**；前端不新增跳库访问。
- 项目 cron 一律 **6 字段或 7 字段**（秒起）；`TaskController.validateCron` 强制，5 字段直接 400。
- Java 21 记录风格（record、不变式构造）；POST 返回 `201`、成功编辑返回 `200 + 现态`、不存在返回 `404`。
- 现有 `@RestController` / JdbcTemplate 直接注入风格，不引入 JPA/MyBatis。
- 仓储接口在 `scheduler-persistence`；实现类 `JdbcXxxRepository` 持 `JdbcTemplate` 私有构造。
- 测试：仓储层继承 `AbstractPostgresTest`（Testcontainers PG16）；E2E 在 `ApiIntegrationTest`（真 PG + MockMvc，同步驱动）。
- **本 spec §1.2 的 `rerun 可携带 args` 在 v1 收窄为无参**（带参需新增数据层 `args` 更新方法，YAGNI 后移）；rerun = 无条件新建一轮手动 run。
- `enabled` 不由 PUT 改动（启停走既有 `pause/resume`）；前端指向 `/api/v1/tasks`。
- 每个任务保持全 reactor 绿（现 140 测试）；勿改 V1–V4 迁移与既有运行时逻辑。

---

### Task 1: 后端——任务编辑 `PUT /api/v1/tasks/{id}` + `TaskRepository.update`

**Files:**
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/TaskRepository.java`
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcTaskRepository.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcTaskRepositoryTest.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: `Task` record（`name, kind, handlerRef, cron, shardCount, timeoutSeconds, maxRetries, backoffMs, retryableFailurePattern, maxActiveConcurrent, enabled, paused`）；`TaskController.requireTask/notFound/validateCron` 既有私有实现。
- Produces: `TaskRepository#boolean update(long id, Task t)`；`TaskController#PUT /{id}` + `record UpdateTaskRequest(...)`；共享静态校验 `static void checkDefinition(String name, String handlerRef, String cron, Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs)`。

- [ ] **Step 1: 写仓储层失败测试**（`JdbcTaskRepositoryTest` 追加）

```java
  @Test void update() {
    var repo = new JdbcTaskRepository(jdbc);
    var c = repo.create(new Task(null, "t1", "cron", "demo", "0 */5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    boolean ok = repo.update(c.id(), new Task(c.id(), "t2", "cron", "demo",
        "0 */6 * * * *", 3, 600, 2, 2000, ".*err.*", 4, true, true));
    assertTrue(ok);
    var cur = repo.findById(c.id()).orElseThrow();
    assertEquals("t2", cur.name());
    assertEquals("0 */6 * * * *", cur.cron());
    assertEquals(3, cur.shardCount());
    assertEquals(600, cur.timeoutSeconds());
    assertEquals(2, cur.maxRetries());
    assertEquals(2000L, cur.backoffMs());
    assertEquals(".*err.*", cur.retryableFailurePattern());
    assertEquals(4, cur.maxActiveConcurrent());
    assertTrue(cur.paused());
    assertFalse(repo.update(99999L, c)); // 不存在 → false
  }
```

- [ ] **Step 2: 运行确认失败**

Run: `mvn -pl scheduler-persistence -am test -Dtest=JdbcTaskRepositoryTest`
Expected: 编译失败（`TaskRepository.update` 未定义）。

- [ ] **Step 3: 实现 `TaskRepository.update`**

在 `TaskRepository` 接口加：

```java
  boolean update(long id, Task t);
```

在 `JdbcTaskRepository` 实现（镜像 `setPaused` 风格，`updated_at=now()`）：

```java
  @Override public boolean update(long id, Task t) {
    int rows = jdbc.update("""
        UPDATE app_task SET name=?, kind=?, handler_ref=?, cron=?, shard_count=?,
               timeout_seconds=?, max_retries=?, backoff_ms=?,
               retryable_failure_pattern=?, max_active_concurrent=?, paused=?, updated_at=now()
        WHERE id=?""",
        t.name(), t.kind(), t.handlerRef(), t.cron(), t.shardCount(), t.timeoutSeconds(),
        t.maxRetries(), t.backoffMs(), t.retryableFailurePattern(), t.maxActiveConcurrent(),
        t.paused(), id);
    return rows > 0;
  }
```

- [ ] **Step 4: 运行仓储测试，确认通过**

Run: `mvn -pl scheduler-persistence -am test -Dtest=JdbcTaskRepositoryTest`
Expected: 全绿（含原有 `roundTrip`）。

- [ ] **Step 5: 写 E2E 失败测试**（`ApiIntegrationTest` 追加 `import static ...RequestBuilders.put;`）

```java
  @Test void putEditsTask() throws Exception {
    String name = "put-edit";
    long id = createTask(name); // 复用本类既有建 task 辅助,若无则用下方 createWithName
    MvcResult r = mvc.perform(put("/api/v1/tasks/" + id)
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {"name":"renamed","kind":"cron","handlerRef":"demo","cron":"0 */6 * * * *",
                 "shardCount":3,"timeoutSeconds":600,"maxRetries":2,"backoffMs":2000,
                 "maxActiveConcurrent":4,"paused":true}"""))
        .andExpect(status().isOk()).andReturn();
    String body = r.getResponse().getContentAsString();
    assertTrue(body.contains("\"name\":\"renamed\""));
    assertTrue(body.contains("\"shardCount\":3"));
    assertTrue(body.contains("\"cron\":\"0 */6 * * * *\""));

    mvc.perform(put("/api/v1/tasks/999999999")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{}")).andExpect(status().isNotFound());
    mvc.perform(put("/api/v1/tasks/" + id)
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {"name":"","kind":"cron","handlerRef":"x","cron":"0 */6 * * * *",
                 "shardCount":1}"""))
        .andExpect(status().isBadRequest());
  }
```

> 若 `ApiIntegrationTest` 尚无 `createTask` 辅助方法，先加：`private long createTask(String name) throws Exception { ... POST /api/v1/tasks ... return parsed id; }`（镜像 `TaskController` 请求体；下同）。命名保持与现存测试一致。

- [ ] **Step 6: 运行 E2E 确认失败**

Run: `mvn -pl scheduler-server -am test -Dtest=ApiIntegrationTest#putEditsTask`
Expected: 失败（`PUT /api/v1/tasks/{id}` 未映射 → 404/405）。

- [ ] **Step 7: 实现控制器编辑端点**

在 `TaskController` 加请求记录 + 端点，并把 create 里重复的数值校验抽成共享静态方法（两处共用，直接服务于本需求）：

```java
  public record UpdateTaskRequest(String name, String kind, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs,
      String retryableFailurePattern, Integer maxActiveConcurrent, Boolean paused) {}

  @PutMapping("/{id}")
  public Task update(@PathVariable long id, @RequestBody UpdateTaskRequest req) {
    if (req == null || req.name() == null || req.name().isBlank()) {
      throw new IllegalArgumentException("name is required");
    }
    if (req.handlerRef() == null || req.handlerRef().isBlank()) {
      throw new IllegalArgumentException("handlerRef is required");
    }
    if (req.cron() == null || req.cron().isBlank()) {
      throw new IllegalArgumentException("cron is required");
    }
    checkDefinition(req.name(), req.handlerRef(), req.cron(), req.shardCount(),
        req.timeoutSeconds(), req.maxRetries(), req.backoffMs());
    Task existing = requireTask(id);
    Task updated = new Task(id, req.name(), req.kind() == null ? existing.kind() : req.kind(),
        req.handlerRef(), req.cron(),
        req.shardCount() == null ? existing.shardCount() : req.shardCount(),
        req.timeoutSeconds() == null ? existing.timeoutSeconds() : req.timeoutSeconds(),
        req.maxRetries() == null ? existing.maxRetries() : req.maxRetries(),
        req.backoffMs() == null ? existing.backoffMs() : req.backoffMs(),
        req.retryableFailurePattern(),
        req.maxActiveConcurrent() == null ? existing.maxActiveConcurrent() : req.maxActiveConcurrent(),
        existing.enabled(),
        req.paused() == null ? existing.paused() : req.paused());
    if (!tasks.update(id, updated)) {
      throw notFound("task " + id);
    }
    return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  }
```

新增共享校验（把 create 里 `timeoutSeconds/backoffMs/maxRetries/shardCount` 四段条件与 `validateCron` 调用收拢，`create` 也改调它）：

```java
  /** 定义域数值合法性:负值即 400;cron 必须 6/7 字段。 */
  static void checkDefinition(String name, String handlerRef, String cron,
      Integer shardCount, Integer timeoutSeconds, Integer maxRetries, Long backoffMs) {
    if (timeoutSeconds != null && timeoutSeconds < 0) throw new IllegalArgumentException("timeoutSeconds must be >= 0");
    if (backoffMs != null && backoffMs < 0) throw new IllegalArgumentException("backoffMs must be >= 0");
    if (maxRetries != null && maxRetries < 0) throw new IllegalArgumentException("maxRetries must be >= 0");
    if (shardCount != null && shardCount < 1) throw new IllegalArgumentException("shardCount must be >= 1");
    validateCron(cron);
  }
```

把 `create` 末尾的 `validateCron(req.cron())` 与四段数值校验替换为对 `checkDefinition(...)` 的调用（行为等价，仅位置收拢）。保留 `create` 内 `name/handlerRef/cron` 的必填判断。

- [ ] **Step 8: 运行 E2E 确认通过**

Run: `mvn -pl scheduler-server -am test -Dtest=ApiIntegrationTest#putEditsTask`
Expected: PASS。

- [ ] **Step 9: 跑全 reactor 回归**

Run: `mvn test`（需 Docker）
Expected: 全绿（原 140 + 新增）。

- [ ] **Step 10: Commit**

```bash
git add scheduler-persistence/src/main/java/dev/scheduler/persistence/TaskRepository.java \
        scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcTaskRepository.java \
        scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java \
        scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcTaskRepositoryTest.java \
        scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java
git commit -m "feat(server): PUT /api/v1/tasks/{id} 任务编辑 + TaskRepository.update
Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: 后端——任务整体重跑 `POST /api/v1/tasks/{id}/rerun`

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`

**Interfaces:**
- Consumes: `Task`/`Execution`、`shards.createParentWithShards(long taskId, String parentKey, int shardCount)`、`shards.findParent(long)`（既有）、`TaskController.requireTask`。
- Produces: `TaskController#POST /{id}/rerun` → `201 + Execution`；私有 `Execution manualRun(long taskId)`（trigger 与 rerun 共用）。

- [ ] **Step 1: 写失败测试**（`ApiIntegrationTest` 追加）

```java
  @Test void rerunCreatesFreshExecution() throws Exception {
    long id = createTask("rerun-me");
    // 先手动触发一轮 → executions 现有 1 条
    mvc.perform(post("/api/v1/tasks/" + id + "/trigger"))
        .andExpect(status().isCreated());
    // 重跑 → 再新建一轮（id 递增、父 DUE、taskId 匹配）
    MvcResult r = mvc.perform(post("/api/v1/tasks/" + id + "/rerun"))
        .andExpect(status().isCreated()).andReturn();
    long newExec = objectMapper.readTree(r.getResponse().getContentAsString())
        .path("id").asLong();
    assertTrue(newExec > 0);
    String body = mvc.perform(get("/api/v1/executions?taskId=" + id))
        .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
    assertEquals(2, objectMapper.readTree(body).size());
  }
```

- [ ] **Step 2: 运行确认失败**

Run: `mvn -pl scheduler-server -am test -Dtest=ApiIntegrationTest#rerunCreatesFreshExecution`
Expected: 失败（`/rerun` 未映射 → 404/405）。

- [ ] **Step 3: 实现 rerun 端点 + 抽 `manualRun` 共用**

```java
  /** 手动触发:建父 execution + 其 shard(与 trigger 共用底座)。 */
  private Execution manualRun(long taskId, String suffix) {
    Task t = requireTask(taskId);
    String key = "manual:" + t.id() + ":" + suffix;
    long pid = shards.createParentWithShards(t.id(), key, t.shardCount()).id();
    return shards.findParent(pid).orElseThrow(() -> notFound("execution " + pid));
  }
```

把 `trigger` 改为调它：

```java
  @PostMapping("/{id}/trigger")
  public ResponseEntity<Execution> trigger(@PathVariable long id) {
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(manualRun(id, UUID.randomUUID().toString()));
  }

  /** 任务整体重跑:无条件新建一轮手动 run(spec §1.2;v1 无参)。 */
  @PostMapping("/{id}/rerun")
  public ResponseEntity<Execution> rerun(@PathVariable long id) {
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(manualRun(id, "rerun-" + UUID.randomUUID()));
  }
```

（`import java.util.UUID;` 已在文件顶部 —— 既有 `trigger` 使用。保持 `manualRun` 复用，两条端点零逻辑分叉。）

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn -pl scheduler-server -am test -Dtest=ApiIntegrationTest#rerunCreatesFreshExecution`
Expected: PASS。

- [ ] **Step 5: 全 reactor 回归**

Run: `mvn test`
Expected: 全绿。

- [ ] **Step 6: Commit**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java \
        scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java
git commit -m "feat(server): POST /api/v1/tasks/{id}/rerun 任务整体重跑
Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: 前端——`scheduler/web/` React Vite TS 脚手架 + 任务 API client

**Files:**
- Create: `scheduler/web/package.json`
- Create: `scheduler/web/vite.config.ts`
- Create: `scheduler/web/tsconfig.json`
- Create: `scheduler/web/index.html`
- Create: `scheduler/web/src/main.tsx`
- Create: `scheduler/web/src/App.tsx`
- Create: `scheduler/web/src/api/types.ts`
- Create: `scheduler/web/src/api/client.ts`

**Interfaces:**
- Consumes: `Task`/`Execution` JSON 契约（§1.1/§1.2 + 既有 tasks/executions 端点）。
- Produces: `src/api/types.ts` 的 `interface Task {...}` 与 `execution` 类型；`src/api/client.ts` 的 `listTasks()/createTask()/updateTask()/pauseTask()/resumeTask()/triggerTask()/rerunTask()`；`App.tsx` 挂一个临时的"任务列表"占位路由（Task 4 替换为完整页）。

- [ ] **Step 1: 写脚手架文件**

`package.json`：

```json
{
  "name": "scheduler-web",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "react": "^19.0.0",
    "react-dom": "^19.0.0"
  },
  "devDependencies": {
    "@types/react": "^19.0.0",
    "@types/react-dom": "^19.0.0",
    "@vitejs/plugin-react": "^4.3.4",
    "typescript": "~5.6.0",
    "vite": "^6.0.0"
  }
}
```

`vite.config.ts`（dev 代理 `/api` → 后端，仅开发）：

```ts
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, proxy: { '/api': 'http://localhost:8080' } },
});
```

`tsconfig.json`：

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2022", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "skipLibCheck": true,
    "noEmit": true
  },
  "include": ["src", "vite.config.ts"]
}
```

`index.html`：

```html
<!doctype html>
<html lang="zh">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Scheduler 管理台</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

`src/main.tsx`：

```tsx
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
```

`src/api/types.ts`（镜像后端 `Task` record，字段名直用）：

```ts
export interface Task {
  id: number;
  name: string;
  kind: string;
  handlerRef: string;
  cron: string;
  shardCount: number;
  timeoutSeconds: number;
  maxRetries: number;
  backoffMs: number;
  retryableFailurePattern: string | null;
  maxActiveConcurrent: number;
  enabled: boolean;
  paused: boolean;
}

export interface Execution {
  id: number;
  taskId: number;
  status: string;
  idempotencyKey: string;
  args: string | null;
  shardIndex: number;
  shardCount: number;
  attempt: number;
  workerId: string | null;
  startedAt: string | null;
  finishedAt: string | null;
  resultPayload: string | null;
}

export interface CreateTaskRequest {
  name: string;
  kind?: string;
  handlerRef: string;
  cron: string;
  shardCount?: number;
  timeoutSeconds?: number;
  maxRetries?: number;
  backoffMs?: number;
  retryableFailurePattern?: string | null;
  maxActiveConcurrent?: number;
}

export interface UpdateTaskRequest extends CreateTaskRequest {
  paused?: boolean;
}
```

`src/api/client.ts`：

```ts
import type { CreateTaskRequest, Execution, Task, UpdateTaskRequest } from './types';

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: init?.body ? { 'Content-Type': 'application/json' } : undefined,
    ...init,
  });
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  return res.json() as Promise<T>;
}

export const listTasks = () => req<Task[]>('/api/v1/tasks');
export const createTask = (b: CreateTaskRequest) =>
  req<Task>('/api/v1/tasks', { method: 'POST', body: JSON.stringify(b) });
export const updateTask = (id: number, b: UpdateTaskRequest) =>
  req<Task>(`/api/v1/tasks/${id}`, { method: 'PUT', body: JSON.stringify(b) });
export const pauseTask = (id: number) => req<Task>(`/api/v1/tasks/${id}/pause`, { method: 'POST' });
export const resumeTask = (id: number) => req<Task>(`/api/v1/tasks/${id}/resume`, { method: 'POST' });
export const triggerTask = (id: number) => req<Execution>(`/api/v1/tasks/${id}/trigger`, { method: 'POST' });
export const rerunTask = (id: number) => req<Execution>(`/api/v1/tasks/${id}/rerun`, { method: 'POST' });
```

`src/App.tsx`（占位：拉一次任务列表，渲染 id+name；Task 4 替换为完整任务页与路由结构）：

```tsx
import { useEffect, useState } from 'react';
import { listTasks } from './api/client';
import type { Task } from './api/types';

export default function App() {
  const [tasks, setTasks] = useState<Task[]>([]);
  useEffect(() => {
    listTasks().then(setTasks).catch(() => setTasks([]));
  }, []);
  return (
    <div style={{ fontFamily: 'system-ui', padding: 16 }}>
      <h1>Scheduler 任务</h1>
      <ul>{tasks.map((t) => <li key={t.id}>{t.id} — {t.name}</li>)}</ul>
    </div>
  );
}
```

- [ ] **Step 2: 安装依赖**

Run: `cd scheduler/web && npm install`
Expected: `node_modules` 生成、无 peer 冲突报错。

- [ ] **Step 3: 类型检查 + 构建**

Run: `cd scheduler/web && npm run build`
Expected: `tsc -b && vite build` 均通过，产出 `dist/`。

- [ ] **Step 4: Commit**

```bash
cd scheduler/web
# 先确认 scheduler/.gitignore 是否忽略 node_modules/dist;若根 .gitignore 未含,补加
cd ../../.. 
git add scheduler/web
git commit -m "feat(web): scheduler/web React+Vite+TS 脚手架 + 任务 API client
Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: 前端——任务页（列表/创建/编辑/启停/触发/重跑）

**Files:**
- Create: `scheduler/web/src/pages/TasksPage.tsx`
- Create: `scheduler/web/src/lib/useInterval.ts`
- Modify: `scheduler/web/src/App.tsx`（渲染任务页，保留作为唯一页面入口）

**Interfaces:**
- Consumes: `src/api/client.ts` 全部函数、`src/api/types.ts` 的 `Task/CreateTaskRequest/UpdateTaskRequest`。
- Produces: `TasksPage` 默认导出组件（作为 App 唯一页面）；`useInterval(fn, ms)` hook（轻量轮询，5s）。

- [ ] **Step 1: 写 `useInterval` hook**

`src/lib/useInterval.ts`：

```ts
import { useEffect, useRef } from 'react';

export function useInterval(fn: () => void, ms: number | null) {
  const saved = useRef(fn);
  useEffect(() => { saved.current = fn; }, [fn]);
  useEffect(() => {
    if (ms === null) return;
    const id = setInterval(() => saved.current(), ms);
    return () => clearInterval(id);
  }, [ms]);
}
```

- [ ] **Step 2: 写 `TasksPage`**

`src/pages/TasksPage.tsx`（功能完整、无重依赖，页面本地 state + 5s 轮询）：

```tsx
import { FormEvent, useCallback, useEffect, useState } from 'react';
import {
  createTask, listTasks, pauseTask, rerunTask, resumeTask, triggerTask, updateTask,
} from '../api/client';
import type { CreateTaskRequest, Task } from '../api/types';
import { useInterval } from '../lib/useInterval';

const EMPTY: CreateTaskRequest = {
  name: '', handlerRef: '', cron: '0 */5 * * * *', shardCount: 1,
  timeoutSeconds: 300, maxRetries: 0, backoffMs: 1000, maxActiveConcurrent: 8,
};

export default function TasksPage() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [form, setForm] = useState<CreateTaskRequest>(EMPTY);
  const [editId, setEditId] = useState<number | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const refresh = useCallback(() => {
    listTasks().then(setTasks).catch((e) => setErr(String(e)));
  }, []);
  useEffect(() => { refresh(); }, [refresh]);
  useInterval(refresh, 5000);

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    try {
      if (editId === null) await createTask(form);
      else await updateTask(editId, { ...form, paused: tasks.find((t) => t.id === editId)?.paused });
      setForm(EMPTY); setEditId(null); setErr(null); refresh();
    } catch (x) { setErr(String(x)); }
  }
  function beginEdit(t: Task) {
    setEditId(t.id);
    setForm({ name: t.name, handlerRef: t.handlerRef, cron: t.cron, shardCount: t.shardCount,
      timeoutSeconds: t.timeoutSeconds, maxRetries: t.maxRetries, backoffMs: t.backoffMs,
      retryableFailurePattern: t.retryableFailurePattern ?? undefined, maxActiveConcurrent: t.maxActiveConcurrent });
  }
  async function act(fn: () => Promise<unknown>) {
    try { await fn(); setErr(null); refresh(); } catch (x) { setErr(String(x)); }
  }

  return (
    <div style={{ fontFamily: 'system-ui', padding: 16 }}>
      <h1>Scheduler 任务管理</h1>
      {err && <p style={{ color: 'brown' }}>{err}</p>}
      <form onSubmit={handleSubmit} style={{ display: 'grid', gap: 6, maxWidth: 520 }}>
        <h2>{editId === null ? '新建任务' : `编辑任务 #${editId}`}</h2>
        <label>名称<input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></label>
        <label>HandlerRef<input value={form.handlerRef} onChange={(e) => setForm({ ...form, handlerRef: e.target.value })} /></label>
        <label>Cron<input value={form.cron} onChange={(e) => setForm({ ...form, cron: e.target.value })} /></label>
        <label>分片数<input type="number" value={form.shardCount} onChange={(e) => setForm({ ...form, shardCount: +e.target.value })} /></label>
        <label>超时(s)<input type="number" value={form.timeoutSeconds} onChange={(e) => setForm({ ...form, timeoutSeconds: +e.target.value })} /></label>
        <label>重试次数<input type="number" value={form.maxRetries} onChange={(e) => setForm({ ...form, maxRetries: +e.target.value })} /></label>
        <button type="submit">{editId === null ? '创建' : '保存'}</button>
      </form>
      <table border={1} cellPadding={6} style={{ marginTop: 16 }}>
        <thead>
          <tr><th>ID</th><th>名称</th><th>Cron</th><th>分片</th><th>状态</th><th>操作</th></tr>
        </thead>
        <tbody>
          {tasks.map((t) => (
            <tr key={t.id}>
              <td>{t.id}</td><td>{t.name}</td><td>{t.cron}</td><td>{t.shardCount}</td>
              <td>{t.paused ? '暂停' : '启用'}</td>
              <td style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                <button onClick={() => act(() => (t.paused ? resumeTask(t.id) : pauseTask(t.id)))}>
                  {t.paused ? '启用' : '暂停'}
                </button>
                <button onClick={() => act(() => triggerTask(t.id))}>触发</button>
                <button onClick={() => act(() => rerunTask(t.id))}>重跑</button>
                <button onClick={() => beginEdit(t)}>编辑</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 3: `App.tsx` 渲染任务页、删占位**

```tsx
import TasksPage from './pages/TasksPage';

export default function App() {
  return <TasksPage />;
}
```

- [ ] **Step 4: 类型检查 + 构建**

Run: `cd scheduler/web && npm run build`
Expected: `tsc -b && vite build` 通过，无 TS 错误。

- [ ] **Step 5: 冒烟（可选，需要后端进程）**

Run: 后端 `mvn -pl scheduler-server spring-boot:run` 起后，另一终端 `cd scheduler/web && npm run dev`，浏览器打开 `http://localhost:5173`，确认任务列表/新建/编辑/启停/触发/重跑可用。若无后端则跳过，交由集成期验证。

- [ ] **Step 6: Commit**

```bash
cd scheduler/web && git add src/pages/TasksPage.tsx src/lib/useInterval.ts src/App.tsx && cd ../.. && git commit -m "feat(web): 任务页 —— 列表/创建/编辑/启停/触发/重跑(5s 轮询)
Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Self-Review

- **Spec coverage（§1.x / §2 / §3）**：§1.1 `PUT /tasks/{id}` → Task 1 ✓；§1.2 `rerun`（args 收窄）→ Task 2 ✓；§2 `scheduler/web/` 脚手架 → Task 3 ✓；任务页（§6.2 任务项：列表/建/编辑/启停/触发/重跑）→ Task 4 ✓。
- **Placeholder scan**：所有代码步骤均含实际代码；无 "TBD/TODO/类似上方"。test helper `createTask` 标注"若无则新增"并给出定位——按 `ApiIntegrationTest` 现状以 `createTask(name)` 为已存在辅助处理（Task 1 已注明兜底写法）。`web` gitignore 的 node_modules 是否忽略给出操作提示（Task 3 Step 4）。`scheduler-loop` 无关。
- **Type consistency**：`TaskRepository.update(long, Task) -> boolean` 在接口/实现/控制器三处一致；`TaskController.checkDefinition` 静态签名在 Task 1 Step 7 定义、Task 1 create 重构处调用一致；前端 `Task/CreateTaskRequest/UpdateTaskRequest/Execution`、`updateTask(id, UpdateTaskRequest)` 在 client/types/页面三处一致；`manualRun(taskId, suffix)` 在 trigger/rerun 两处对同一个 `Execution` 返回。
- **Arguments 收窄已声明**：spec §1.2 的 rerun 带参在 v1 后移（Global Constraints 说明；如需恢复→ Task 2 需新增 `args` 数据层更新方法）。