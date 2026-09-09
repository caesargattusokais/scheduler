import type {
  CreateTaskRequest,
  Execution,
  ExecutionDetail,
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