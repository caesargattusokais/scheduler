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