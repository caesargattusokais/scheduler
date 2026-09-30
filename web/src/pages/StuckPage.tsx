// web/src/pages/StuckPage.tsx —— E2-B 卡死视图:列出「属主仍 ALIVE 但已超运行预算」的 RUNNING 分片,
// 每行提供「强制放弃」按钮(破坏性:要求先停 worker,后端对仍 ALIVE 属主 409 兜底)。
import { useCallback, useEffect, useState } from 'react';
import { forceAbandonShard, getStuck, listTasks } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { StuckRow, Task } from '../api/types';
import StatusBadge from '../components/StatusBadge';
import Pager from '../components/Pager';

const PAGE_SIZE = 20;

export default function StuckPage() {
  const [rows, setRows] = useState<StuckRow[]>([]);
  const [total, setTotal] = useState(0);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [taskId, setTaskId] = useState('');
  const [offset, setOffset] = useState(0);
  const [err, setErr] = useState<string | null>(null);
  const [confirmId, setConfirmId] = useState<number | null>(null);

  useEffect(() => { listTasks({ limit: 100 }).then((p) => setTasks(p.items)).catch(() => {}); }, []);

  const load = useCallback(async () => {
    try {
      const page = await getStuck({ taskId: taskId === '' ? undefined : Number(taskId), limit: PAGE_SIZE, offset });
      setRows(page.items); setTotal(page.total); setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [taskId, offset]);

  useEffect(() => { load(); }, [load]);
  useInterval(load, 5000);

  const doAbandon = useCallback(async (id: number) => {
    try { await forceAbandonShard(id); setErr(null); setConfirmId(null); await load(); }
    catch (e) { setErr(String(e)); }
  }, [load]);

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">运行卡死</h1>
          <p className="page-sub">属主 worker 仍在线但已超过运行预算的 RUNNING 分片 —— 判断「慢而健康」还是「真卡住」;强制放弃须先停掉该 worker</p>
        </div>
        {total > 0 && <div className="text-sm text-slate-500">{total} 条卡死</div>}
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

      {rows.length === 0 ? (
        <div className="card p-10 text-center text-sm text-slate-400">无卡死分片 —— 所有 RUNNING 分片都在运行预算内或属主已离线</div>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>任务</th>
                <th className="num">idx</th>
                <th>状态</th>
                <th className="num">attempt</th>
                <th>worker</th>
                <th className="num">超龄</th>
                <th className="num">预算</th>
                <th className="text-right">操作</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((s) => (
                <tr key={s.shardId}>
                  <td>
                    <div className="font-medium">{s.taskName ?? `任务 #${s.executionId}`}</div>
                    <div className="text-xs text-slate-500">{s.handlerRef ?? '—'} · exec #{s.executionId}</div>
                  </td>
                  <td className="num">{s.shardIndex}</td>
                  <td><StatusBadge status="RUNNING" /></td>
                  <td className="num">{s.attempt}</td>
                  <td className="font-mono text-xs">{s.workerId ?? '—'}</td>
                  <td className="num">{s.ageSeconds}s</td>
                  <td className="num">{s.timeoutSeconds}s</td>
                  <td className="text-right">
                    {confirmId === s.shardId ? (
                      <>
                        <button className="btn-danger"
                          onClick={() => doAbandon(s.shardId)}>确认放弃</button>
                        <button className="btn-secondary ml-2"
                          onClick={() => setConfirmId(null)}>取消</button>
                      </>
                    ) : (
                      <button className="btn-danger" onClick={() => setConfirmId(s.shardId)}>强制放弃</button>
                    )}
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