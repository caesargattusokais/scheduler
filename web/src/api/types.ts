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
  /** 执行详情独有:该分片最近一次 FAILED outcome 的 detail(失败日志);列表/DLQ 中恒为 null。 */
  failureDetail?: string | null;
}

/** DLQ 行:Shard 全字段 + 所属任务名/handlerRef + 该分片最近一次失败日志(failureDetail)。 */
export interface DlqRow extends Shard {
  taskName: string | null;
  handlerRef: string | null;
  failureDetail: string | null;
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
/** 建工作流请求(镜像 DagController.CreateDagRequest)。 */
export interface CreateDagNode { nodeKey: string; taskId: number; sortOrder: number; }
export interface CreateDagEdge { from: string; to: string; }
export interface CreateDagRequest {
  name: string; description: string | null; cron: string;
  nodes: CreateDagNode[]; edges: CreateDagEdge[];
}
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

// 审计(meta 为后端原样透出的 JSON 文本,展示时 JSON.parse)
export interface AuditEntry {
  id: number;
  occurredAt: string;
  operator: string;
  action: string;
  targetType: string;
  targetId: number;
  meta: string | null;
  source: string | null;
  diff: string | null; // task.update 的 {field:[before,after]} JSON 文本;null = 未启用 diff 的动作
  before: string | null; // 操作前全量 Task 快照(task.* 动作);其余 null
}

/** /api/v1/audits/integrity — 审计取证链完整性(total 表全量,chained 已入链,firstTamperedId 首个被篡改行 id)。 */
export interface AuditIntegrity {
  totalRecords: number;
  chainedRecords: number;
  firstTamperedId: number | null;
  verified: boolean;
}