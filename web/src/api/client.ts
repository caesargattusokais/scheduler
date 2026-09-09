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