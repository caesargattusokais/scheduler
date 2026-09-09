import { useCallback, useEffect, useState } from 'react';
import { getDlq, requeueShard } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { Shard } from '../api/types';

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
      <h2>DLQ（死信）</h2>
      {err && <p style={{ color: 'red' }}>{err}</p>}
      {rows.length === 0
        ? <p>（空）</p>
        : (
          <table border={1} cellSpacing={0} cellPadding={4}>
            <thead><tr><th>shardId</th><th>execId</th><th>idx</th><th>状态</th><th>attempt</th><th>startedAt</th><th>操作</th></tr></thead>
            <tbody>
              {rows.map(s => (
                <tr key={s.id}>
                  <td>{s.id}</td><td>{s.executionId}</td><td>{s.shardIndex}</td>
                  <td>{s.status}</td><td>{s.attempt}</td><td>{s.startedAt ?? '—'}</td>
                  <td><button onClick={() => doRequeue(s.id)}>重放</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
    </div>
  );
}