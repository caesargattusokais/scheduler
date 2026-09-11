import type {
  CreateTaskRequest,
  Dag,
  DagDetail,
  DagRun,
  DagRunNode,
  Execution,
  ExecutionDetail,
  Page,
  ParsedMetric,
  RunDetail,
  Shard,
  Task,
  UpdateTaskRequest,
} from './types';

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: init?.body ? { 'Content-Type': 'application/json' } : undefined,
    ...init,
  });
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  if (res.status === 204) return undefined as T; // 无响应体(如 DELETE)
  return res.json() as Promise<T>;
}

/** 把可选查询参数拼成查询串(空值/空串跳过)。 */
function qstr(p: object): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(p)) {
    if (v !== undefined && v !== null && v !== '') q.set(k, String(v));
  }
  const s = q.toString();
  return s ? `?${s}` : '';
}

// ---- 任务 ----
export interface ListTasksParams {
  name?: string;
  paused?: boolean;
  limit?: number;
  offset?: number;
}
export const listTasks = (p: ListTasksParams = {}): Promise<Page<Task>> =>
  req<Page<Task>>(`/api/v1/tasks${qstr(p)}`);
/** 已注册 handler 的 ref 列表,供任务表单下拉框枚举。 */
export const listHandlerRefs = () => req<string[]>('/api/v1/handlers');
export const createTask = (b: CreateTaskRequest) =>
  req<Task>('/api/v1/tasks', { method: 'POST', body: JSON.stringify(b) });
export const updateTask = (id: number, b: UpdateTaskRequest) =>
  req<Task>(`/api/v1/tasks/${id}`, { method: 'PUT', body: JSON.stringify(b) });
export const pauseTask = (id: number) => req<Task>(`/api/v1/tasks/${id}/pause`, { method: 'POST' });
export const resumeTask = (id: number) => req<Task>(`/api/v1/tasks/${id}/resume`, { method: 'POST' });
export const triggerTask = (id: number) => req<Execution>(`/api/v1/tasks/${id}/trigger`, { method: 'POST' });
/** 删除任务:仅当无执行记录且未被 DAG 引用(否则后端 409);成功 → 204。 */
export const deleteTask = (id: number) =>
  req<void>(`/api/v1/tasks/${id}`, { method: 'DELETE' });

export interface ListExecutionsParams {
  taskId?: number;
  status?: string;
  from?: string; // ISO-8601 with offset
  to?: string;   // ISO-8601 with offset
  limit?: number;
  offset?: number;
}

export const listExecutions = (p: ListExecutionsParams = {}): Promise<Page<Execution>> =>
  req<Page<Execution>>(`/api/v1/executions${qstr(p)}`);

export const getExecutionDetail = (id: number) =>
  req<ExecutionDetail>(`/api/v1/executions/${id}`);

export const cancelExecution = (id: number) =>
  req<Execution>(`/api/v1/executions/${id}/cancel`, { method: 'POST' });
/** M6.5 真重跑:引用某一轮终态执行,复制其 args 新建一轮并溯源(rerun_of)。源非终态 → 后端 409。 */
export const rerunExecution = (id: number) =>
  req<Execution>(`/api/v1/executions/${id}/rerun`, { method: 'POST' });

export interface ListDlqParams {
  taskId?: number;
  limit?: number;
  offset?: number;
}
export const getDlq = (p: ListDlqParams = {}): Promise<Page<Shard>> =>
  req<Page<Shard>>(`/api/v1/executions/dlq${qstr(p)}`);

export const requeueShard = (shardId: number) =>
  req<Shard>(`/api/v1/executions/shards/${shardId}/requeue`, { method: 'POST' });

// ---- DAG ----
export interface ListDagsParams {
  name?: string;
  limit?: number;
  offset?: number;
}
export const listDags = (p: ListDagsParams = {}): Promise<Page<Dag>> =>
  req<Page<Dag>>(`/api/v1/dags${qstr(p)}`);
export const getDag = (id: number) => req<DagDetail>(`/api/v1/dags/${id}`);
export const pauseDag = (id: number) => req<Dag>(`/api/v1/dags/${id}/pause`, { method: 'POST' });
export const resumeDag = (id: number) => req<Dag>(`/api/v1/dags/${id}/resume`, { method: 'POST' });
export const triggerDag = (id: number) => req<DagRun>(`/api/v1/dags/${id}/trigger`, { method: 'POST' });
export interface ListDagRunsParams {
  dagId?: number;
  status?: string;
  limit?: number;
  offset?: number;
}
export const listDagRuns = (p: ListDagRunsParams = {}): Promise<Page<DagRun>> =>
  req<Page<DagRun>>(`/api/v1/dags/runs${qstr(p)}`);
export const getRunDetail = (runId: number) => req<RunDetail>(`/api/v1/dags/runs/${runId}`);
export const cancelRun = (runId: number) =>
  req<RunDetail>(`/api/v1/dags/runs/${runId}/cancel`, { method: 'POST' });
export const rerunNode = (runId: number, nodeId: number) =>
  req<DagRunNode>(`/api/v1/dags/runs/${runId}/nodes/${nodeId}/rerun`, { method: 'POST' });

// ---- 指标(只读 /actuator/prometheus, spec §1.5) ----
export const parsePrometheus = (text: string): ParsedMetric[] => {
  const out: ParsedMetric[] = [];
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (!line || line.startsWith('#') || line === 'help' || !line.length) continue;
    const space = line.lastIndexOf(' ');
    if (space < 0) continue;
    const head = line.slice(0, space);
    const value = Number(line.slice(space + 1));
    if (Number.isNaN(value)) continue;
    const brace = head.indexOf('{');
    if (brace === -1) { out.push({ name: head, tags: {}, value }); continue; }
    const name = head.slice(0, brace);
    const tags: Record<string, string> = {};
    for (const pair of head.slice(brace + 1, -1).split(',')) {
      const eq = pair.indexOf('=');
      if (eq > 0) tags[pair.slice(0, eq).trim()] = pair.slice(eq + 1).replace(/^"|"$/g, '');
    }
    out.push({ name, tags, value });
  }
  return out;
};
export const fetchMetrics = async (): Promise<ParsedMetric[]> => {
  const res = await fetch('/actuator/prometheus');
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  return parsePrometheus(await res.text());
};