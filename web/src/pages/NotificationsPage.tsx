// web/src/pages/NotificationsPage.tsx —— 通知告警闭环(4-1):webhook 订阅管理(写 ADMIN)+ 投递历史(读)。
import { useEffect, useState } from 'react';
import MultiSelect from '../components/MultiSelect';
import Pager from '../components/Pager';
import {
  createWebhook, deleteWebhook, listDags, listNotifications, listTasks, listWebhooks, updateWebhook,
} from '../api/client';
import { Dag, OutboundNotification, OutboundWebhook, Page, Task } from '../api/types';

const KINDS_SAMPLE = 'execution.completed, execution.failed';

function statusBadge(s: string): string {
  return s === 'SENT'
    ? 'rounded bg-emerald-100 px-1.5 py-0.5 text-xs font-medium text-emerald-700'
    : s === 'FAILED'
    ? 'rounded bg-red-100 px-1.5 py-0.5 text-xs font-medium text-red-700'
    : 'rounded bg-amber-100 px-1.5 py-0.5 text-xs font-medium text-amber-700';
}

/** 事件类型 → 人话(颜色随语义;未知类型回落为 kind 原文)。 */
const KIND_LABELS: Record<string, { text: string; cls: string }> = {
  'execution.completed': { text: '执行完成', cls: 'bg-emerald-100 text-emerald-700' },
  'execution.failed': { text: '执行失败', cls: 'bg-red-100 text-red-700' },
  'execution.timeout': { text: '执行超时', cls: 'bg-orange-100 text-orange-700' },
  'execution.dead_letter': { text: '落入死信', cls: 'bg-slate-200 text-slate-700' },
};
const TERMINAL_TEXT: Record<string, string> = {
  SUCCESS: '成功', FAILED: '失败', TIMEOUT: '超时', DEAD_LETTER: '死信',
};
const STATUS_TEXT: Record<string, string> = { SENT: '已送达', PENDING: '待投递', FAILED: '投递失败' };

/** 幂等解析通知 payload(payload JSON 文本 → 对象;解析失败返回 null)。 */
function parsePayload(s: string | null): Record<string, unknown> | null {
  if (!s) return null;
  try { return JSON.parse(s) as Record<string, unknown>; } catch { return null; }
}

/** 投递信封 JSON 缩进美化(悬浮看全文);原始文本则原样返回。 */
function prettyBody(s: string): string {
  try { return JSON.stringify(JSON.parse(s), null, 2); } catch { return s; }
}

export default function NotificationsPage() {
  const [hooks, setHooks] = useState<OutboundWebhook[]>([]);
  const [notifs, setNotifs] = useState<Page<OutboundNotification>>({ items: [], total: 0, offset: 0, limit: 20 });
  const [err, setErr] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  // 订阅编辑表单:id=null → 新建;否则更新。
  const [id, setId] = useState<number | null>(null);
  const [url, setUrl] = useState('');
  const [kinds, setKinds] = useState('');
  const [maxAttempts, setMaxAttempts] = useState(5);
  const [statusFilter, setStatusFilter] = useState('');
  // 任务维度过滤 scope:ALL=全部 / INCLUDE=白名单 / EXCLUDE=黑名单。
  const [scopeMode, setScopeMode] = useState<'ALL' | 'INCLUDE' | 'EXCLUDE'>('ALL');
  const [selTasks, setSelTasks] = useState<number[]>([]);
  const [selDags, setSelDags] = useState<number[]>([]);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [dags, setDags] = useState<Dag[]>([]);

  const loadHooks = async () => {
    try { setHooks(await listWebhooks()); setErr(null); }
    catch (e) { setErr(String(e)); }
  };
  const loadNotifs = async (offset = 0, limit = 20) => {
    try {
      setNotifs(await listNotifications({ status: statusFilter, limit, offset }));
      setErr(null);
    } catch (e) { setErr(String(e)); }
  };
  useEffect(() => {
    loadHooks();
    listTasks({ limit: 500 }).then((p) => setTasks(p.items)).catch(() => { /* 任务可选:失败不阻断页面 */ });
    listDags({ limit: 500 }).then((p) => setDags(p.items)).catch(() => { /* DAG 可选:失败不阻断页面 */ });
    /* eslint-disable-next-line react-hooks/exhaustive-deps */
  }, []);
  useEffect(() => { loadNotifs(0, 20); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [statusFilter]);

  const submit = async () => {
    if (!url.trim()) { setErr('订阅 URL 必填'); return; }
    if (scopeMode === 'INCLUDE' && selTasks.length === 0 && selDags.length === 0) {
      setErr('白名单(INCLUDE)至少选择一个任务或 DAG'); return;
    }
    const body = {
      url: url.trim(),
      ...(kinds.trim() ? { kinds: kinds.split(',').map((k) => k.trim()).filter(Boolean) } : {}),
      maxAttempts,
      ...(scopeMode !== 'ALL' ? { scopeMode } : {}),
      ...(selTasks.length > 0 ? { selectedTaskIds: selTasks } : {}),
      ...(selDags.length > 0 ? { selectedDagIds: selDags } : {}),
    };
    try {
      if (id == null) await createWebhook(body);
      else await updateWebhook(id, body);
      setNotice(id == null ? '已创建订阅' : '已更新订阅'); setErr(null);
      resetForm();
      await loadHooks();
    } catch (e) { setErr(String(e)); }
  };

  const toggle = async (h: OutboundWebhook) => {
    try {
      await updateWebhook(h.id, {
        url: h.url, secret: h.secret ?? undefined, kinds: h.kinds,
        enabled: !h.enabled, maxAttempts: h.maxAttempts, backoffMs: h.backoffMs,
        scopeMode: h.scopeMode, selectedTaskIds: h.selectedTaskIds, selectedDagIds: h.selectedDagIds,
      });
      await loadHooks();
    } catch (e) { setErr(String(e)); }
  };

  const remove = async (h: OutboundWebhook) => {
    if (!window.confirm(`删除订阅 ${h.url}?(已投递历史保留)`)) return;
    try { await deleteWebhook(h.id); await loadHooks(); }
    catch (e) { setErr(String(e)); }
  };

  const resetForm = () => {
    setId(null); setUrl(''); setKinds(''); setMaxAttempts(5);
    setScopeMode('ALL'); setSelTasks([]); setSelDags([]);
  };

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">通知</h1>
          <p className="page-sub">webhook 订阅(ADMIN 管理,DB 存储取代配置文件)与出站通知投递历史(PENDING/SENT/FAILED + 退避重试)</p>
        </div>
      </div>

      {notice && <div className="mb-4 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{notice}</div>}
      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      <div className="card mb-4 p-4">
        <div className="flex items-end gap-3">
          <label className="field grow">
            <span className="label">{id == null ? 'URL(新建订阅)' : `URL(更新 #${id})`}</span>
            <input className="input" placeholder="https://hooks.example.com/x" value={url}
              onChange={(e) => setUrl(e.target.value)} />
          </label>
          <label className="field grow">
            <span className="label">Kinds(逗号分隔,空 = 订阅全部)</span>
            <input className="input" placeholder={KINDS_SAMPLE} value={kinds}
              onChange={(e) => setKinds(e.target.value)} />
          </label>
          <label className="field" style={{ maxWidth: 110 }}>
            <span className="label">最大尝试</span>
            <input className="input" type="number" min={1} value={maxAttempts}
              onChange={(e) => setMaxAttempts(Number(e.target.value))} />
          </label>
          {id != null && <button className="btn btn-secondary" onClick={resetForm}>取消编辑</button>}
          <button className="btn btn-primary" onClick={submit} disabled={!url.trim()}>
            {id == null ? '创建' : '保存'}
          </button>
        </div>
        <div className="mt-3 border-t border-slate-100 pt-3">
          <span className="label">任务范围</span>
          <div className="mt-1 flex flex-wrap items-center gap-3">
            <label className="flex items-center gap-1 text-sm">
              <input type="radio" checked={scopeMode === 'ALL'} onChange={() => setScopeMode('ALL')} /> 全部(all)
            </label>
            <label className="flex items-center gap-1 text-sm">
              <input type="radio" checked={scopeMode === 'INCLUDE'} onChange={() => setScopeMode('INCLUDE')} /> 白名单(仅选中的任务/DAG)
            </label>
            <label className="flex items-center gap-1 text-sm">
              <input type="radio" checked={scopeMode === 'EXCLUDE'} onChange={() => setScopeMode('EXCLUDE')} /> 黑名单(排除选中的任务/DAG)
            </label>
          </div>
          {scopeMode !== 'ALL' && (
            <div className="mt-2 flex flex-wrap items-start gap-3">
              <MultiSelect
                label={scopeMode === 'INCLUDE' ? '白名单任务' : '黑名单任务'}
                options={tasks.map((t) => ({ id: t.id, name: t.name }))}
                selected={selTasks}
                onChange={setSelTasks}
                empty="暂无可选任务(创建任务后出现)"
              />
              <MultiSelect
                label={scopeMode === 'INCLUDE' ? '白名单 DAG' : '黑名单 DAG'}
                options={dags.map((d) => ({ id: d.id, name: d.name }))}
                selected={selDags}
                onChange={setSelDags}
                empty="暂无可选 DAG(创建工作流后出现)"
              />
            </div>
          )}
        </div>
      </div>

      <div className="card mb-4 p-4">
        <h2 className="mb-2 text-sm font-semibold text-slate-700">订阅 ({hooks.length})</h2>
        {hooks.length === 0 ? (
          <div className="py-6 text-center text-sm text-slate-400">尚无 webhook 订阅 —— 用上方表单登记端点后,通知将按其 kinds 订阅投递(HMAC-SHA256 签名)</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr><th>URL</th><th>Kinds</th><th>任务范围</th><th>状态</th><th>重试</th><th></th></tr>
              </thead>
              <tbody>
                {hooks.map((h) => (
                  <tr key={h.id}>
                    <td className="font-mono text-sm">{h.enabled ? h.url : <span className="text-slate-400">{h.url}</span>}</td>
                    <td className="text-sm">{h.kinds.length === 0 ? <span className="text-slate-400">全部</span> : h.kinds.join(', ')}</td>
                    <td className="text-sm">
                      {h.scopeMode === 'INCLUDE'
                        ? <span className="rounded bg-blue-100 px-1.5 py-0.5 text-xs font-medium text-blue-700">白名单{h.selectedTaskIds.length + h.selectedDagIds.length}项</span>
                        : h.scopeMode === 'EXCLUDE'
                        ? <span className="rounded bg-violet-100 px-1.5 py-0.5 text-xs font-medium text-violet-700">黑名单{h.selectedTaskIds.length + h.selectedDagIds.length}项</span>
                        : <span className="text-slate-400">全部</span>}
                    </td>
                    <td>{h.enabled
                      ? <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-xs font-medium text-emerald-700">启用</span>
                      : <span className="rounded bg-slate-200 px-1.5 py-0.5 text-xs font-medium text-slate-600">停用</span>}</td>
                    <td className="text-sm">{h.maxAttempts} 次 / {h.backoffMs} ms</td>
                    <td className="text-right">
                      <button className="btn btn-secondary" title={h.enabled ? '停用:不再向其投递' : '启用:恢复投递'}
                        onClick={() => toggle(h)}>{h.enabled ? '停用' : '启用'}</button>
                      <button className="btn btn-secondary" title="编辑该订阅"
                        onClick={() => {
                        setId(h.id); setUrl(h.url); setKinds(h.kinds.join(', ')); setMaxAttempts(h.maxAttempts);
                        setScopeMode(h.scopeMode); setSelTasks(h.selectedTaskIds); setSelDags(h.selectedDagIds);
                      }}>编辑</button>
                      <button className="btn btn-secondary" title="删除该订阅(历史保留)"
                        onClick={() => remove(h)}>删除</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="card p-4">
        <div className="mb-2 flex items-center gap-3">
          <h2 className="text-sm font-semibold text-slate-700">投递历史</h2>
          <select className="input" style={{ width: 'auto' }} value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}>
            <option value="">全部状态</option>
            <option value="PENDING">PENDING</option>
            <option value="SENT">SENT</option>
            <option value="FAILED">FAILED</option>
          </select>
        </div>
        {notifs.items.length === 0 ? (
          <div className="py-6 text-center text-sm text-slate-400">暂无投递记录(执行事件落库后由 leader 门控 dispatch 投喂)</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr><th>事件</th><th>任务 · 执行</th><th>结果</th><th>通知内容</th><th>投递给</th><th>状态</th><th>错误/说明</th><th>时间</th></tr>
              </thead>
              <tbody>
                {notifs.items.map((n) => {
                  const p = parsePayload(n.payload);
                  const terminal = p && typeof p.terminal === 'string'
                    ? p.terminal as string : null;
                  const failed = typeof p?.failedShards === 'number' ? p.failedShards as number : null;
                  const total = typeof p?.shardCount === 'number' ? p.shardCount as number : null;
                  const kind = KIND_LABELS[n.kind] ?? { text: n.kind, cls: 'bg-slate-100 text-slate-600' };
                  // 人话任务定位:先 controller join 的任务名,回落「targetType #id」。
                  const target =
                    n.taskName
                      ? <>{n.taskName} <span className="hidden text-slate-400 sm:inline">执行</span><span className="font-mono text-slate-500">#{n.targetId}</span></>
                      : n.targetType && n.targetId != null
                        ? <span className="text-slate-500">{n.targetType} #{n.targetId}</span>
                        : <span className="text-slate-400">—</span>;
                  return (
                    <tr key={n.id}>
                      <td><span className={`rounded px-1.5 py-0.5 text-xs font-medium ${kind.cls}`}>{kind.text}</span></td>
                      <td className="text-sm font-medium text-slate-700">{target}</td>
                      <td className="max-w-xs text-sm">
                        {terminal != null && (
                          <div>
                            <span className="font-medium text-slate-700">
                              {TERMINAL_TEXT[terminal] ?? terminal}
                              {failed != null && total != null && failed > 0 && (
                                <span className="ml-1 text-xs font-normal text-slate-500">(失败 {failed}/{total})</span>
                              )}
                            </span>
                            {n.resultPayload && (
                              <div className="mt-0.5 truncate font-mono text-xs text-slate-500"
                                title={`结果 payload:${n.resultPayload}`}>结果:{n.resultPayload}</div>
                            )}
                          </div>
                        )}
                        {terminal == null && <span className="text-slate-400">—</span>}
                      </td>
                      <td className="max-w-xs text-xs">
                        {n.deliveredBody ? (
                          <span className="block truncate font-mono text-slate-500"
                            title={`投递内容(HMAC-SHA256 签名即此字节):\n${prettyBody(n.deliveredBody)}`}>
                            {n.deliveredBody}
                          </span>
                        ) : (
                          <span className="text-slate-400">—</span>
                        )}
                      </td>
                      <td className="max-w-xs text-xs">
                        {n.deliveredTo.length > 0 ? (
                          <span className="block truncate font-mono text-slate-500" title={n.deliveredTo.join('、')}>
                            {n.deliveredTo.join('、')}
                          </span>
                        ) : (
                          <span className="text-slate-400">无订阅</span>
                        )}
                      </td>
                      <td className="max-w-xs truncate text-xs text-slate-500" title={n.lastError ?? ''}>{n.lastError || '—'}</td>
                      <td>
                        <span className={statusBadge(n.status)} title={n.status === 'PENDING' ? '待 leader 投递' : n.status === 'FAILED' ? '重试耗尽仍失败' : '接收端已回 200'}>
                          {STATUS_TEXT[n.status] ?? n.status}{n.attempts > 1 ? `(${n.attempts} 次)` : ''}
                        </span>
                      </td>
                      <td className="whitespace-nowrap text-xs text-slate-500">{n.createdAt ? new Date(n.createdAt).toLocaleString() : '—'}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        <Pager total={notifs.total} offset={notifs.offset} limit={notifs.limit}
          onPage={(o) => loadNotifs(o, notifs.limit)} />
      </div>
    </div>
  );
}