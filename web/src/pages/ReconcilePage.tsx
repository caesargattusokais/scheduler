// web/src/pages/ReconcilePage.tsx —— E3 对账可观测:待回收积压 + 决策流水。
import { useCallback, useEffect, useState } from 'react';
import { getReconcileActivity, getReconcileBacklog, listTasks } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { ReconcileDecision, ReclaimableShard, Task } from '../api/types';
import Pager from '../components/Pager';

const PAGE_SIZE = 20;

const PHASE_LABEL: Record<string, string> = {
  timeout_reclaim: '超时回收',
  orphan_reclaim: '孤儿回收',
  parent_aggregate: '父级汇聚',
  near_miss: '竞态跳过',
};

const OUTCOME_BADGE: Record<string, string> = {
  REQUEUED: 'bg-blue-100 text-blue-700',
  DEAD_LETTER: 'bg-red-100 text-red-700',
  SUCCESS: 'bg-emerald-100 text-emerald-700',
  FAILED: 'bg-red-100 text-red-700',
  CANCELED: 'bg-slate-100 text-slate-600',
  PARTIAL_SUCCESS: 'bg-amber-100 text-amber-700',
  SKIPPED: 'bg-slate-100 text-slate-500',
};

export default function ReconcilePage() {
  const [view, setView] = useState<'backlog' | 'activity'>('backlog');
  const [rows, setRows] = useState<ReclaimableShard[]>([]);
  const [total, setTotal] = useState(0);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [taskId, setTaskId] = useState('');
  const [offset, setOffset] = useState(0);
  const [acts, setActs] = useState<ReconcileDecision[]>([]);
  const [actTotal, setActTotal] = useState(0);
  const [actOffset, setActOffset] = useState(0);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => { listTasks({ limit: 100 }).then((p) => setTasks(p.items)).catch(() => {}); }, []);

  const loadBacklog = useCallback(async () => {
    try {
      const page = await getReconcileBacklog({ taskId: taskId === '' ? undefined : Number(taskId), limit: PAGE_SIZE, offset });
      setRows(page.items); setTotal(page.total); setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [taskId, offset]);

  const loadActivity = useCallback(async () => {
    try {
      const page = await getReconcileActivity({ limit: PAGE_SIZE, offset: actOffset });
      setActs(page.items); setActTotal(page.total); setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [actOffset]);

  useEffect(() => { view === 'backlog' ? loadBacklog() : loadActivity(); }, [view, loadBacklog, loadActivity]);
  useInterval(view === 'backlog' ? loadBacklog : loadActivity, 5000);

  const switchView = (v: 'backlog' | 'activity') => {
    setView(v);
    if (v === 'activity') setActOffset(0); else setOffset(0);
  };

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">对账</h1>
          <p className="page-sub">对账器决策留痕 —— 当前待回收积压明细与最近决策流水（append-only reconcile_decision）</p>
        </div>
        <div className="flex gap-2">
          <button type="button" className={`btn ${view === 'backlog' ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => switchView('backlog')}>积压</button>
          <button type="button" className={`btn ${view === 'activity' ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => switchView('activity')}>流水</button>
        </div>
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      {view === 'backlog' ? (
        <>
          <div className="toolbar">
            <label className="field">
              <span className="label">任务</span>
              <select className="input" value={taskId} onChange={(e) => { setTaskId(e.target.value); setOffset(0); }}>
                <option value="">全部</option>
                {tasks.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
              </select>
            </label>
            {total > 0 && <div className="text-sm text-slate-500">{total} 条待回收</div>}
          </div>

          {rows.length === 0 ? (
            <div className="card p-10 text-center text-sm text-slate-400">无待回收积压 —— 所有 RUNNING 分片的属主仍在线</div>
          ) : (
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th className="num">shard</th>
                    <th>任务</th>
                    <th>worker</th>
                    <th className="num">attempt</th>
                    <th className="num">积压龄</th>
                    <th className="num">exec</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((s) => (
                    <tr key={s.id}>
                      <td className="num">{s.id}</td>
                      <td><span className="font-mono text-xs text-slate-500">#{s.taskId}</span></td>
                      <td className="font-mono text-xs">{s.workerId ?? '—'}</td>
                      <td className="num">{s.attempt}</td>
                      <td className="num">{s.ageSeconds}s</td>
                      <td className="num">{s.executionId}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <Pager total={total} offset={offset} limit={PAGE_SIZE} onPage={setOffset} />
        </>
      ) : (
        <>
          {actTotal > 0 && <div className="mb-2 text-sm text-slate-500">{actTotal} 条决策</div>}
          {acts.length === 0 ? (
            <div className="card p-10 text-center text-sm text-slate-400">尚无决策流水 —— 对账器尚未产生任何回收/汇聚/竞态裁决</div>
          ) : (
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>阶段</th>
                    <th className="num">target</th>
                    <th>from → to</th>
                    <th>outcome</th>
                    <th>归因</th>
                    <th className="num">attempt</th>
                  </tr>
                </thead>
                <tbody>
                  {acts.map((a) => (
                    <tr key={a.id}>
                      <td className="text-xs font-mono">{a.createdAt ?? '—'}</td>
                      <td>{PHASE_LABEL[a.phase] ?? a.phase}</td>
                      <td className="num">{a.targetType === 'execution' ? `exec #${a.targetId}` : `shard #${a.targetId}`}</td>
                      <td className="text-xs font-mono">{a.fromStatus ?? '—'} → {a.toStatus ?? '—'}</td>
                      <td>{a.outcome ? (
                        <span className={`rounded px-1.5 py-0.5 text-xs font-medium ${OUTCOME_BADGE[a.outcome] ?? 'bg-slate-100 text-slate-600'}`}>{a.outcome}</span>
                      ) : '—'}</td>
                      <td className="text-xs text-slate-500">{a.reason}</td>
                      <td className="num">{a.attempt ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <Pager total={actTotal} offset={actOffset} limit={PAGE_SIZE} onPage={setActOffset} />
        </>
      )}
    </div>
  );
}