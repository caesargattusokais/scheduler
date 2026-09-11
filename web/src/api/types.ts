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
  leaseUntil: string | null;
  nextRetryAt: string | null;
  startedAt: string | null;
  finishedAt: string | null;
  resultPayload: string | null;
  rerunOf: number | null;
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
  rerunOf: number | null;
  shards: Shard[];
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

export interface Dag {
  id: number; name: string;
  description: string | null; cron: string;
  enabled: boolean; paused: boolean;
  createdAt: string | null; updatedAt: string | null;
}
/** 镜像 DagController.DagDetail。 */
export interface DagDetail { dag: Dag; nodes: DagNode[]; edges: DagEdge[]; }
export interface DagNode { id: number; dagId: number; nodeKey: string; taskId: number; sortOrder: number; }
export interface DagEdge { id: number; dagId: number; fromNodeId: number; toNodeId: number; }
export interface DagRun {
  id: number; dagId: number; idempotencyKey: string;
  status: string; triggerReason: string; cancelRequested: boolean;
  finishedAt: string | null; createdAt: string | null;
}
export interface DagRunNode {
  id: number; dagRunId: number; nodeKey: string; taskId: number;
  executionId: number | null; status: string; sortOrder: number;
  detail: string | null; createdAt: string | null; finishedAt: string | null;
}
export interface NodeDetail { node: DagRunNode; shards: Shard[]; }
export interface RunDetail { run: DagRun; status: string; nodes: NodeDetail[]; }
/** /actuator/prometheus 解析后的单条系列。 */
export interface ParsedMetric { name: string; tags: Record<string, string>; value: number; }
/** 统一列表分页包裹(镜像后端 Page<T>):items 为当前页, total 为过滤后全量计数。 */
export interface Page<T> { items: T[]; total: number; offset: number; limit: number; }