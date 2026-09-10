import { useCallback, useEffect, useState } from 'react';
import { getDlq, requeueShard } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { Shard } from '../api/types';
import StatusBadge from '../components/StatusBadge';

export default function DlqPage() {
  const [rows, setRows] = useState<Shard[]>([]);
  const [err, setErr] = useState<string | null>(null);

  const load = useCallback(async () => {
    try { setRows(await getDlq()); setErr(null); }
    catch (e) { setErr(String(e)); }
  }, []);

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
        {rows.length > 0 && <div className="text-sm text-slate-500">{rows.length} 条死信</div>}
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      {rows.length === 0 ? (
        <div className="card p-10 text-center text-sm text-slate-400">死信队列为空 —— 暂无重试耗尽的失败分片</div>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th className="num">shardId</th>
                <th className="num">execId</th>
                <th className="num">idx</th>
                <th>状态</th>
                <th className="num">attempt</th>
                <th>startedAt</th>
                <th className="text-right">操作</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((s) => (
                <tr key={s.id}>
                  <td className="num">{s.id}</td>
                  <td className="num">{s.executionId}</td>
                  <td className="num">{s.shardIndex}</td>
                  <td><StatusBadge status={s.status} /></td>
                  <td className="num">{s.attempt}</td>
                  <td className="text-xs text-slate-500">{s.startedAt ?? '—'}</td>
                  <td className="text-right">
                    <button className="btn-danger" onClick={() => doRequeue(s.id)}>重放出队</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}