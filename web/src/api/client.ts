import type {
  AuditArchiveResult,
  AuditEntry,
  AuditIntegrity,
  CreateTaskRequest,
  CreateDagRequest,
  Dag,
  DagDetail,
  DagRun,
  DagRunNode,
  DlqRow,
  Execution,
  ExecutionDetail,
  LoginResponse,
  MeResponse,
  OperatorEntry,
  Page,
  ParsedMetric,
  RunDetail,
  Shard,
  Task,
  UpdateTaskRequest,
  UpsertOperatorRequest,
} from './types';

// 清理旧版自报身份的残留 key(原 localStorage 'scheduler.operator');会话身份现由 HttpOnly cookie 承载。
localStorage.removeItem('scheduler.operator');

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    // 同源 fetch 自动携带 HttpOnly 会话 cookie(Path=/api),无需 credentials 标志。
    headers: {
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...(init?.headers as Record<string, string> | undefined),
    },
    ...init,
  });
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  if (res.status === 204) return undefined as T; // 无响应体(如 DELETE)
  return res.json() as Promise<T>;
}

// ---- 认证(强认证:HttpOnly 会话 cookie,浏览器自动存储/携带) ----
/** 登录:POST /auth/login。浏览器据 Set-Cookie 自动落 HttpOnly 会话 cookie(本函数不手动读 cookie)。失败抛错(含状态码与响应文本)。 */
export const login = async (name: string, password: string): Promise<LoginResponse> => {
  const res = await fetch('/api/v1/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, password }),
  });
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  return res.json() as Promise<LoginResponse>;
};
/** 当前会话身份:401 → 未认证(AuthGate 据此跳登录页)。 */
export const me = (): Promise<MeResponse> => req<MeResponse>('/api/v1/auth/me');
/** 登出:注销服务端会话并清除 cookie。 */
export const logout = (): Promise<void> => req<void>('/api/v1/auth/logout', { method: 'POST' });
/** 设密码(ADMIN 专属服务端收口):成功后该操作者的既有会话被撤销,需重新登录。 */
export const setPassword = (name: string, password: string): Promise<void> =>
  req<void>(`/api/v1/operators/${encodeURIComponent(name)}/password`, {
    method: 'POST',
    body: JSON.stringify({ password }),
  });

// ---- 自助改密(登录态自证)+ 首登强制改密 ----
/** 自助改密:验当前密 + 落新密 + 撤销旧会话 + 无缝签发新会话(cookie 自动更新)。成功必清服务端 must_change 标。
 *  当前密不符 → 后端 401;新密过短 → 400。返回 200 {operator, expiresAt}(同登录形)。 */
export const changePassword = (currentPassword: string, newPassword: string): Promise<LoginResponse> =>
  req<LoginResponse>('/api/v1/auth/change-password', { method: 'POST', body: JSON.stringify({ currentPassword, newPassword }) });

// 首登强制改密":登录时若 mustChangePassword=true,把本次提交的(共享默认)口令暂存,交 ForcePasswordChange 预填当前密,
// 免去重输默认口令。仅内存、一次性消费;页面刷新则失效(回到须自填当前密)。
let pendingCurrentPassword: string | null = null;
export function setPendingCurrentPassword(pw: string): void {
  pendingCurrentPassword = pw;
}
export function consumePendingCurrentPassword(): string | null {
  const p = pendingCurrentPassword;
  pendingCurrentPassword = null;
  return p;
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
export const getDlq = (p: ListDlqParams = {}): Promise<Page<DlqRow>> =>
  req<Page<DlqRow>>(`/api/v1/executions/dlq${qstr(p)}`);

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
/** 建工作流:DAG 定义是数据,仅存 dag/dag_node/dag_edge 三表,不改代码。 */
export const createDag = (b: CreateDagRequest) =>
  req<Dag>('/api/v1/dags', { method: 'POST', body: JSON.stringify(b) });
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

// ---- 审计(只读) ----
export interface ListAuditsParams {
  operator?: string;
  action?: string;
  targetType?: string;
  targetId?: number;
  from?: string; // ISO-8601 with offset
  to?: string;
  hasDiff?: boolean; // true → 仅真正改过字段的行(diff 非空对象)
  diffField?: string; // 顶层 JSONB 键:改过该字段的行(如 "cron")
  beforeField?: string; // 顶层 JSONB 键:操作前快照含该字段的行(如 "shardCount")
  metaField?: string; // 顶层 JSONB 键:meta 含该字段的行
  limit?: number;
  offset?: number;
}
export const listAudits = (p: ListAuditsParams = {}): Promise<Page<AuditEntry>> =>
  req<Page<AuditEntry>>(`/api/v1/audits${qstr(p)}`);
/** 审计取证链完整性:全量入链且无篡改 → verified。 */
export const getAuditIntegrity = () => req<AuditIntegrity>('/api/v1/audits/integrity');
/** 归档 occurred_at 早于 olderThan(ISO)的审计行,即删即重链(ADMIN 专属)。返回本次归档行数。 */
export const archiveAudits = (olderThan: string, limit?: number) =>
  req<AuditArchiveResult>(`/api/v1/audits/archive?olderThan=${encodeURIComponent(olderThan)}${limit ? `&limit=${limit}` : ''}`, { method: 'POST' });

// ---- 操作者目录(仅 ADMIN,OperatorInterceptor 收口) ----
export const listOperators = () => req<OperatorEntry[]>('/api/v1/operators');
export const upsertOperator = (b: UpsertOperatorRequest) =>
  req<OperatorEntry>('/api/v1/operators', { method: 'POST', body: JSON.stringify(b) });
export const deactivateOperator = (name: string) =>
  req<void>(`/api/v1/operators/${encodeURIComponent(name)}/deactivate`, { method: 'POST' });