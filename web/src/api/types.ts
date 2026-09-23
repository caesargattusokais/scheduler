export interface Task {
  id: number;
  name: string;
  kind: string;
  handlerRef: string;
  cron: string | null;
  shardCount: number;
  timeoutSeconds: number;
  maxRetries: number;
  backoffMs: number;
  retryableFailurePattern: string | null;
  maxActiveConcurrent: number;
  enabled: boolean;
  paused: boolean;
  timezone: string;
  intervalSeconds: number | null;
  /** 事件触发时钟:非空数组 = 订阅这些路由 key,入站事件触发每事件一轮;与 cron/intervalSeconds 三选一互斥。 */
  eventRoutes: string[];
  /** 3c 部分成功策略:null/NONE=全成或全败;RATIO_PERCENT=成功占比%达标;MIN_SUCCESS=成功分片数达标;MAX_FAILURES=容忍失败分片数。 */
  successPolicyType: string | null;
  successPolicyValue: number | null;
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
  cron?: string | null;
  shardCount?: number;
  timeoutSeconds?: number;
  maxRetries?: number;
  backoffMs?: number;
  retryableFailurePattern?: string | null;
  maxActiveConcurrent?: number;
  timezone?: string;
  intervalSeconds?: number | null;
  /** 事件触发时钟:非空数组 = 事件触发(与 cron/intervalSeconds 三选一互斥)。 */
  eventRoutes?: string[];
  /** 3c 部分成功策略(缺省 NONE/null=全成或全败)。type 白名单四选一;RATIO_PERCENT 值∈[1,99]、MIN_SUCCESS≥1、MAX_FAILURES≥0。 */
  successPolicyType?: string;
  successPolicyValue?: number;
}

export interface UpdateTaskRequest extends CreateTaskRequest {
  paused?: boolean;
}

export interface Dag {
  id: number; name: string;
  description: string | null; cron: string | null;
  /** 1d 跨 DAG 依赖:依赖的上游 DAG id(非空时 cron 为 null,上游每成功一次 → 下游跑一次,事件链)。 */
  dependsOnDagId: number | null;
  enabled: boolean; paused: boolean;
  /** 1c 定义版本号:编辑(PUT)即 ++;run 封印它取自哪版。 */
  version: number;
  createdAt: string | null; updatedAt: string | null;
}
/** 镜像 DagController.DagDetail。 */
export interface DagDetail { dag: Dag; nodes: DagNode[]; edges: DagEdge[]; }
export interface DagNode { id: number; dagId: number; nodeKey: string; taskId: number; sortOrder: number; nodeMaxRetries: number; nodeBackoffMs: number; runIf: string; }
export interface DagEdge { id: number; dagId: number; fromNodeId: number; toNodeId: number; }
/** 建工作流请求(镜像 DagController.CreateDagRequest)。nodeMaxRetries 缺省 0=不重试;nodeBackoffMs 缺省 5000;runIf 缺省 'all_success'。 */
export interface CreateDagNode { nodeKey: string; taskId: number; sortOrder: number; nodeMaxRetries: number; nodeBackoffMs: number; runIf: string; }
export interface CreateDagEdge { from: string; to: string; }
export interface CreateDagRequest {
  name: string; description: string | null; cron: string | null;
  /** 1d 跨 DAG 依赖:cron 与 dependsOnDagId 恰其一(依赖取代定时,互斥)。 */
  dependsOnDagId?: number | null;
  nodes: CreateDagNode[]; edges: CreateDagEdge[];
}
export interface DagRun {
  id: number; dagId: number; idempotencyKey: string;
  status: string; triggerReason: string; cancelRequested: boolean;
  /** 1c 本批次封印的 DAG 定义版本号(旧 run 为 null)。 */
  dagVersion: number | null;
  finishedAt: string | null; createdAt: string | null;
}
export interface DagRunNode {
  id: number; dagRunId: number; nodeKey: string; taskId: number;
  executionId: number | null; status: string; sortOrder: number;
  detail: string | null; createdAt: string | null; finishedAt: string | null;
  attempt: number; nextRetryAt: string | null;
  /** 1c 运行时封印的节点行为(run 完全自包含,定义编辑不影响已封印批次)。 */
  runIf: string; nodeMaxRetries: number; nodeBackoffMs: number;
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

/** 操作者目录条目(镜像 OperatorController.list → OperatorEntry)。role 取 'OPERATOR'|'ADMIN'。 */
export interface OperatorEntry {
  name: string;
  role: 'OPERATOR' | 'ADMIN';
  active: boolean;
}
/** POST /api/v1/auth/login 响应:当前登录操作者与会话失效时刻(ISO)+必须改密标(共享默认口令引导置位)。 */
export interface LoginResponse {
  operator: OperatorEntry;
  expiresAt: string;
  /** true=口令仍为共享默认,须先自助改密才能进入主界面。 */
  mustChangePassword: boolean;
}
/** GET /api/v1/auth/me 响应:当前会话对应操作者 + 必须改密标。 */
export interface MeResponse {
  operator: OperatorEntry;
  mustChangePassword: boolean;
}
export interface UpsertOperatorRequest {
  name: string;
  role: 'OPERATOR' | 'ADMIN';
  active: boolean;
}
/** 操作者活动会话行(ADMIN 会话管理):tokenPrefix 为 token_hash 前 10 位展示键(非秘密),createdAt/expiresAt 由 DB 时钟。 */
export interface ActiveSession {
  tokenPrefix: string;
  createdAt: string;
  expiresAt: string;
}
/** POST /api/v1/operators/{name}/sessions/revoke 响应:本次撤销的活动会话数。 */
export interface SessionsRevokeResult { revoked: number; }
/** POST /api/v1/audits/archive 响应:{archived: 本次归档行数, olderThan: 截止时刻 ISO}。 */
export interface AuditArchiveResult { archived: number; olderThan: string; }

/** 3b 入站事件行(镜像后端 InboundEvent):PENDING=待分派 / DISPATCHED=已投递。taskId/executionId 分派后回填。 */
export interface InboundEvent {
  id: number;
  routeKey: string;
  payload: string | null; // JSON 文本(展示时 JSON.parse)
  dedupeKey: string;
  status: 'PENDING' | 'DISPATCHED';
  taskId: number | null;
  executionId: number | null;
  createdAt: string | null;
  dispatchedAt: string | null;
}
/** POST /api/v1/events 请求:routeKey/dedupeKey 必填;payload 为任意 JSON(dedupeKey 防重放,重放命中既有行)。 */
export interface CreateEventRequest {
  routeKey: string;
  payload?: unknown;
  dedupeKey: string;
}

// ---- 通知告警闭环(4-1):webhook 订阅 + 投递历史 ----
/** webhook 订阅行(镜像 persistence.Webhook):kinds 空数组 = 订阅全部 event kind。 */
export interface OutboundWebhook {
  id: number;
  url: string;
  secret: string | null;
  kinds: string[];
  enabled: boolean;
  maxAttempts: number;
  backoffMs: number;
  createdAt: string;
}
/** 建/改 webhook 请求体:url 必填;kinds 缺省空(订阅全部);enabled/maxAttempts/backoffMs 缺省由后端给默认 */
export interface WebhookRequest {
  url: string;
  secret?: string;
  kinds?: string[];
  enabled?: boolean;
  maxAttempts?: number;
  backoffMs?: number;
}
// ---- 4-2 执行 SLO 指标(DB 快照聚合) ----
/** GET /api/v1/metrics/executions 快照:近窗父延迟 p50/p95、分片均长、per-task 吞吐/成功率、近窗失败细分。 */
export interface ExecutionSlo {
  windowSeconds: number;
  parentLatencyP50Ms: number;
  parentLatencyP95Ms: number;
  shardAvgDurationMs: number;
  perTask: { taskId: number; taskName: string; throughput: number; successRate: number }[];
  recentFailures: { failed: number; deadLettered: number; timedOut: number };
}

/** 出站通知投递历史行(镜像 OutboundNotification):status ∈ PENDING/SENT/FAILED。 */
export interface OutboundNotification {
  id: number;
  kind: string;
  operator: string | null;
  targetType: string | null;
  targetId: number | null;
  payload: string | null; // JSON 文本(展示时 JSON.parse 前 120 字符)
  status: 'PENDING' | 'SENT' | 'FAILED';
  attempts: number;
  nextRetryAt: string | null;
  lastError: string | null;
  createdAt: string;
  sentAt: string | null;
}