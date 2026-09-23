import { useCallback, useEffect, useState } from 'react';
import { getDlq, getTaskDlqReplays, listTasks, requeueShard, setTaskDlqReplays } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { DlqRow, Task } from '../api/types';
import StatusBadge from '../components/StatusBadge';
import Pager from '../components/Pager';

const PAGE_SIZE = 20;

const truncate = (s: string, n: number) => (s.length > n ? `${s.slice(0, n)}…` : s);

export default function DlqPage() {
  const [rows, setRows] = useState<DlqRow[]>([]);
  const [total, setTotal] = useState(0);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [taskId, setTaskId] = useState('');
  const [offset, setOffset] = useState(0);
  const [err, setErr] = useState<string | null>(null);
  // DLQ 治理:每任务自动重放上限(dlq_max_replays)。input 文本态便于未保存编辑;加载后填入当前值。
  const [caps, setCaps] = useState<Record<number, string>>({});

  useEffect(() => {
    listTasks({ limit: 100 }).then((p) => {
      setTasks(p.items);
      setCaps({});
      Promise.all(p.items.map((t) =>
        getTaskDlqReplays(t.id).then((v) => [t.id, String(v)] as const).catch(() => null)))
        .then((pairs) => {
          const next: Record<number, string> = {};
          for (const pair of pairs) if (pair) next[pair[0]] = pair[1];
          setCaps(next);
        }).catch(() => {});
    }).catch(() => {});
  }, []);

  const saveCap = useCallback(async (id: number) => {
    const raw = (caps[id] ?? '').trim();
    if (!/^\d+$/.test(raw)) { setErr('自动重放上限须为不小于 0 的整数'); return; }
    try {
      const v = await setTaskDlqReplays(id, Number(raw));
      setCaps((prev) => ({ ...prev, [id]: String(v) }));
      setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [caps]);

  const load = useCallback(async () => {
    try {
      const page = await getDlq({
        taskId: taskId === '' ? undefined : Number(taskId),
        limit: PAGE_SIZE,
        offset,
      });
      setRows(page.items);
      setTotal(page.total);
      setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [taskId, offset]);

  useEffect(() => { load(); }, [load]);
  useInterval(load, 5000);

  const doRequeue = useCallback(async (id: number) => {
    try { await requeueShard(id); setErr(null); await load(); }
    catch (e) { setErr(String(e)); }
  }, [load]);

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">DLQ（死信队列）</h1>
          <p className="page-sub">重试耗尽并标记死信的分片，可手动重放出队</p>
        </div>
        {total > 0 && <div className="text-sm text-slate-500">{total} 条死信</div>}
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      <div className="toolbar">
        <label className="field">
          <span className="label">任务</span>
          <select className="input" value={taskId} onChange={(e) => { setTaskId(e.target.value); setOffset(0); }}>
            <option value="">全部</option>
            {tasks.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
          </select>
        </label>
      </div>

      {tasks.length > 0 && (
        <div className="card mb-4">
          <div className="text-sm font-medium text-slate-500 mb-2">DLQ 治理 · 每任务自动重放上限（0 = 不自动重放，超限分片永久弃）</div>
          <div className="grid grid-cols-1 gap-2 md:grid-cols-2 xl:grid-cols-3">
            {tasks.map((t) => (
              <div key={t.id} className="flex items-center gap-2">
                <span className="flex-1 truncate text-sm">{t.name}</span>
                <input
                  className="input w-20 text-right"
                  type="number" min={0}
                  value={caps[t.id] ?? ''}
                  onChange={(e) => setCaps((p) => ({ ...p, [t.id]: e.target.value }))}
                  placeholder={caps[t.id] === undefined ? '…' : '0'}
                />
                <button className="btn-secondary" onClick={() => saveCap(t.id)}>保存</button>
              </div>
            ))}
          </div>
        </div>
      )}

      {rows.length === 0 ? (
        <div className="card p-10 text-center text-sm text-slate-400">死信队列为空 —— 暂无重试耗尽的失败分片</div>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>任务</th>
                <th className="num">idx</th>
                <th>状态</th>
                <th className="num">attempt</th>
                <th>失败原因</th>
                <th>入死信</th>
                <th className="text-right">操作</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((s) => (
                <tr key={s.id}>
                  <td>
                    <div className="font-medium">{s.taskName ?? `任务 #${s.executionId}`}</div>
                    <div className="text-xs text-slate-500">
                      {s.handlerRef ?? '—'} · exec #{s.executionId}
                    </div>
                  </td>
                  <td className="num">{s.shardIndex}</td>
                  <td><StatusBadge status={s.status} /></td>
                  <td className="num">{s.attempt}</td>
                  <td>
                    {s.failureDetail
                      ? <span className="break-words font-mono text-xs" title={s.failureDetail}>{truncate(s.failureDetail, 90)}</span>
                      : <span className="text-slate-300">—</span>}
                  </td>
                  <td className="text-xs text-slate-500">{s.finishedAt ?? '—'}</td>
                  <td className="text-right">
                    <button className="btn-danger" onClick={() => doRequeue(s.id)}>重放出队</button>
                  </td>
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