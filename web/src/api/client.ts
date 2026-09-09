import type {
  CreateTaskRequest,
  Dag,
  DagDetail,
  DagRun,
  DagRunNode,
  Execution,
  ExecutionDetail,
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

// ---- DAG ----
export const listDags = () => req<Dag[]>('/api/v1/dags');
export const getDag = (id: number) => req<DagDetail>(`/api/v1/dags/${id}`);
export const pauseDag = (id: number) => req<Dag>(`/api/v1/dags/${id}/pause`, { method: 'POST' });
export const resumeDag = (id: number) => req<Dag>(`/api/v1/dags/${id}/resume`, { method: 'POST' });
export const triggerDag = (id: number) => req<DagRun>(`/api/v1/dags/${id}/trigger`, { method: 'POST' });
export const listDagRuns = (dagId?: number) =>
  req<DagRun[]>(`/api/v1/dags/runs${dagId ? `?dagId=${dagId}` : ''}`);
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