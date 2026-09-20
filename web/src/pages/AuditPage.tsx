// web/src/pages/AuditPage.tsx
import { useCallback, useEffect, useState } from 'react';
import { archiveAudits, getAuditIntegrity, listAudits } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { AuditEntry, AuditIntegrity } from '../api/types';
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
  const [hasDiff, setHasDiff] = useState(false);
  const [diffField, setDiffField] = useState('');
  const [beforeField, setBeforeField] = useState('');
  const [metaField, setMetaField] = useState('');
  const [offset, setOffset] = useState(0);
  const [err, setErr] = useState<string | null>(null);
  const [integrity, setIntegrity] = useState<AuditIntegrity | null>(null);
  const [archiveOlderThan, setArchiveOlderThan] = useState('');
  const [archiveMsg, setArchiveMsg] = useState<string | null>(null);

  /** 归档保留:把早于选定时点的审计行即删即重链(ADMIN 专属;后端权限收口)。归档后刷新列表 + 重查完整性。 */
  const doArchive = async () => {
    try {
      const r = await archiveAudits(new Date(archiveOlderThan).toISOString(), 1000);
      setArchiveMsg(`已归档 ${r.archived} 条(< ${r.olderThan})`);
      setErr(null);
      setOffset(0);
      await load();
      await checkIntegrity();
    } catch (e) { setErr(String(e)); }
  };

  /** 由当前过滤状态派生查询参数(不含分页,供列表与导出共用);字段过滤空置即剔除。 */
  const filters = useCallback(() => {
    const nid = Number(targetId);
    const f: { operator?: string; action?: string; targetType?: string; targetId?: number;
      from?: string; to?: string; hasDiff?: boolean; diffField?: string; beforeField?: string; metaField?: string } = {};
    if (operator !== '') f.operator = operator;
    if (action !== '') f.action = action;
    if (targetType !== '') f.targetType = targetType;
    if (targetId !== '' && !Number.isNaN(nid)) f.targetId = nid;
    if (from !== '') f.from = new Date(from).toISOString();
    if (to !== '') f.to = new Date(to).toISOString();
    if (hasDiff) f.hasDiff = true;
    if (diffField !== '') f.diffField = diffField;
    if (beforeField !== '') f.beforeField = beforeField;
    if (metaField !== '') f.metaField = metaField;
    return f;
  }, [operator, action, targetType, targetId, from, to, hasDiff, diffField, beforeField, metaField]);

  const load = useCallback(async () => {
    try {
      const fs = filters();
      const page = await listAudits({ ...fs, limit: PAGE_SIZE, offset });
      setRows(page.items);
      setTotal(page.total);
      setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [filters, offset]);

  /** 导出链接复用当前过滤(append-only 截断上限由后端恒 5 万行);构建查询串。 */
  const exportHref = useCallback(() => {
    const q = new URLSearchParams();
    for (const [k, v] of Object.entries(filters())) q.set(k, String(v));
    const s = q.toString();
    return `/api/v1/audits/export${s ? `?${s}` : ''}`;
  }, [filters]);

  /** 校验取证链完整性(挂载时自动一次,可手动重查)。 */
  const checkIntegrity = async () => {
    try { setIntegrity(await getAuditIntegrity()); setErr(null); }
    catch (e) { setErr(String(e)); }
  };
  useEffect(() => { load(); checkIntegrity(); }, [load]); // eslint-disable-line react-hooks/exhaustive-deps
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

        <div className="mb-4 flex items-center justify-end gap-3">
          {integrity && (
            integrity.verified
              ? <span className="rounded bg-emerald-100 px-2 py-0.5 text-xs font-medium text-emerald-700"
                  title={`${integrity.chainedRecords}/${integrity.totalRecords} 行已入链`}>取证链 ✓</span>
              : <span className="rounded bg-red-100 px-2 py-0.5 text-xs font-medium text-red-700"
                  title={`${integrity.chainedRecords}/${integrity.totalRecords} 行已入链`}>
                  链路已篡改{integrity.firstTamperedId != null && ` · 首个异常行 ${integrity.firstTamperedId}`} ✗</span>
          )}
          <button className="btn btn-secondary" onClick={() => checkIntegrity()}>校验</button>
          <a className="btn btn-secondary" download="audits.csv" href={exportHref()}>导出 CSV</a>
          <div className="flex items-center gap-2">
            <input className="input" type="datetime-local" title="归档早于该时点的审计行(ADMIN)"
              value={archiveOlderThan}
              onChange={(e) => setArchiveOlderThan(e.target.value)} />
            <button className="btn btn-secondary" onClick={doArchive} disabled={!archiveOlderThan}
              title="归档并删除早于该时点的审计行(ADMIN,即删即重链)">归档</button>
          </div>
          {archiveMsg && <span className="text-xs text-emerald-600">{archiveMsg}</span>}
        </div>

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
        <label className="field">
          <span className="label">有变更</span>
          <input className="input" type="checkbox" style={{ width: 'auto', margin: 'auto 0' }}
            checked={hasDiff}
            onChange={(e) => { setHasDiff(e.target.checked); setOffset(0); }} />
        </label>
        <label className="field">
          <span className="label">变更字段</span>
          <input className="input" placeholder="改过该字段,如 cron" value={diffField}
            onChange={(e) => { setDiffField(e.target.value); setOffset(0); }} />
        </label>
        <label className="field">
          <span className="label">快照字段</span>
          <input className="input" placeholder="前态含该字段,如 shardCount" value={beforeField}
            onChange={(e) => { setBeforeField(e.target.value); setOffset(0); }} />
        </label>
        <label className="field">
          <span className="label">meta 字段</span>
          <input className="input" placeholder="meta 含该字段,如 name" value={metaField}
            onChange={(e) => { setMetaField(e.target.value); setOffset(0); }} />
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
                <th>快照</th>
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
                  <td className="break-words font-mono text-xs text-slate-500">{metaSummary(r.before)}</td>
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