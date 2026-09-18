// web/src/pages/AuditPage.tsx
import { useCallback, useEffect, useState } from 'react';
import { listAudits } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { AuditEntry } from '../api/types';
import Pager from '../components/Pager';

const PAGE_SIZE = 20;
const ACTIONS = [
  'task.create', 'task.update', 'task.pause', 'task.resume', 'task.delete', 'task.trigger',
  'execution.rerun', 'execution.cancel', 'shard.requeue',
  'dag.create', 'dag.pause', 'dag.resume', 'dag.trigger', 'dag_run.cancel', 'dag_node.rerun',
];
const TARGET_TYPES = ['task', 'execution', 'shard', 'dag', 'dag_run'];

const fmt = (t: string) => (t ? new Date(t).toLocaleString() : '—');
/** meta 是后端透出的 JSON 文本,解析为紧凑摘要展示。 */
const metaSummary = (m: string | null) => {
  if (!m) return '—';
  try {
    const o = JSON.parse(m);
    const keys = Object.keys(o);
    return keys.length === 0
      ? '{}'
      : keys.map((k) => `${k}=${JSON.stringify(o[k])}`).join(' · ');
  } catch {
    return m;
  }
};
/** diff 是 {field:[before,after]} JSON 文本;null / {} → 无变更;渲染 field: 旧 → 新。 */
const ESC = (v: unknown) => v === null || v === undefined ? '∅' : JSON.stringify(v);
const diffSummary = (d: string | null) => {
  if (!d) return '—'; // null → 未启用 diff;{} → 无字段变化
  try {
    const o = JSON.parse(d) as Record<string, [unknown, unknown]>;
    const keys = Object.keys(o);
    if (keys.length === 0) return '—';
    return keys.map((k) => `${k}: ${ESC(o[k][0])} → ${ESC(o[k][1])}`).join(' · ');
  } catch {
    return d;
  }
};

export default function AuditPage() {
  const [rows, setRows] = useState<AuditEntry[]>([]);
  const [total, setTotal] = useState(0);
  const [operator, setOperator] = useState('');
  const [action, setAction] = useState('');
  const [targetType, setTargetType] = useState('');
  const [targetId, setTargetId] = useState('');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [offset, setOffset] = useState(0);
  const [err, setErr] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const nid = Number(targetId);
      const page = await listAudits({
        operator: operator === '' ? undefined : operator,
        action: action === '' ? undefined : action,
        targetType: targetType === '' ? undefined : targetType,
        targetId: targetId === '' || Number.isNaN(nid) ? undefined : nid,
        from: from === '' ? undefined : new Date(from).toISOString(),
        to:   to   === '' ? undefined : new Date(to).toISOString(),
        limit: PAGE_SIZE,
        offset,
      });
      setRows(page.items);
      setTotal(page.total);
      setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [operator, action, targetType, targetId, from, to, offset]);

  useEffect(() => { load(); }, [load]);
  useInterval(load, 5000);

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">审计</h1>
          <p className="page-sub">操作者主动动作的 append-only 审计;系统驱动的状态迁移不重复记录</p>
        </div>
        {total > 0 && <div className="text-sm text-slate-500">{total} 条</div>}
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      <div className="toolbar">
        <label className="field">
          <span className="label">操作者</span>
          <input className="input" placeholder="子串匹配" value={operator}
            onChange={(e) => { setOperator(e.target.value); setOffset(0); }} />
        </label>
        <label className="field">
          <span className="label">动作</span>
          <select className="input" value={action}
            onChange={(e) => { setAction(e.target.value); setOffset(0); }}>
            <option value="">全部</option>
            {ACTIONS.map((a) => <option key={a} value={a}>{a}</option>)}
          </select>
        </label>
        <label className="field">
          <span className="label">目标类型</span>
          <select className="input" value={targetType}
            onChange={(e) => { setTargetType(e.target.value); setOffset(0); }}>
            <option value="">全部</option>
            {TARGET_TYPES.map((t2) => <option key={t2} value={t2}>{t2}</option>)}
          </select>
        </label>
        <label className="field">
          <span className="label">目标 ID</span>
          <input className="input" inputMode="numeric" placeholder="资源主键" value={targetId}
            onChange={(e) => { setTargetId(e.target.value); setOffset(0); }} />
        </label>
        <label className="field">
          <span className="label">开始</span>
          <input className="input" type="datetime-local" value={from}
            onChange={(e) => { setFrom(e.target.value); setOffset(0); }} />
        </label>
        <label className="field">
          <span className="label">结束</span>
          <input className="input" type="datetime-local" value={to}
            onChange={(e) => { setTo(e.target.value); setOffset(0); }} />
        </label>
      </div>

      {rows.length === 0 ? (
        <div className="card p-10 text-center text-sm text-slate-400">暂无操作审计记录</div>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>时间</th>
                <th>操作者</th>
                <th>动作</th>
                <th>目标</th>
                <th>变更</th>
                <th>meta</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.id}>
                  <td className="text-xs text-slate-500">{fmt(r.occurredAt)}</td>
                  <td className="font-mono text-xs">{r.operator}</td>
                  <td><span className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700">{r.action}</span></td>
                  <td className="font-mono text-xs">{r.targetType}:{r.targetId}</td>
                  <td className="break-words font-mono text-xs text-slate-600">{diffSummary(r.diff)}</td>
                  <td className="break-words font-mono text-xs text-slate-600">{metaSummary(r.meta)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <Pager total={total} offset={offset} limit={PAGE_SIZE} onPage={setOffset} />
    </div>
  );
}