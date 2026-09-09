import { useCallback, useEffect, useState } from 'react';
import { listExecutions, getExecutionDetail, cancelExecution, listTasks } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { Execution, ExecutionDetail, Task } from '../api/types';

const PAGE_SIZE = 10;
const STATUSES = ['DUE', 'RUNNING', 'SUCCESS', 'FAILED', 'ORPHANED', 'CANCELED'];

export default function ExecutionsPage() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [taskId, setTaskId] = useState('');
  const [status, setStatus] = useState('');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [rows, setRows] = useState<Execution[]>([]);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [detail, setDetail] = useState<ExecutionDetail | null>(null);

  const load = useCallback(async () => {
    try {
      const q = {
        taskId: taskId ? Number(taskId) : undefined,
        status: status || undefined,
        from: from ? new Date(from).toISOString() : undefined,
        to: to ? new Date(to).toISOString() : undefined,
        limit: PAGE_SIZE + 1,
        offset,
      };
      const arr = await listExecutions(q);
      setRows(arr.length > PAGE_SIZE ? arr.slice(0, PAGE_SIZE) : arr);
      setHasMore(arr.length > PAGE_SIZE);
      setErr(null);
    } catch (e) {
      setErr(String(e));
    }
  }, [taskId, status, from, to, offset]);

  useEffect(() => {
    load();
  }, [load]);
  useInterval(load, 5000);

  useEffect(() => {
    listTasks()
      .then(setTasks)
      .catch(() => {});
  }, []);

  const openDetail = useCallback(async (id: number) => {
    try {
      setDetail(await getExecutionDetail(id));
      setErr(null);
    } catch (e) {
      setErr(String(e));
    }
  }, []);
  const doCancel = useCallback(
    async (id: number) => {
      try {
        await cancelExecution(id);
        setErr(null);
        await load();
        setDetail(null);
      } catch (e) {
        setErr(String(e));
      }
    },
    [load]
  );
  const apply = () => {
    setOffset(0);
    load();
  };

  return (
    <div>
      <h2>执行</h2>
      {err && <p style={{ color: 'red' }}>{err}</p>}
      <label>
        任务
        <select value={taskId} onChange={e => setTaskId(e.target.value)}>
          <option value="">全部</option>
          {tasks.map(t => (
            <option key={t.id} value={t.id}>
              {t.name}
            </option>
          ))}
        </select>
      </label>{' '}
      <label>
        状态
        <select value={status} onChange={e => setStatus(e.target.value)}>
          <option value="">全部</option>
          {STATUSES.map(s => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </label>{' '}
      <label>
        从 <input type="datetime-local" value={from} onChange={e => setFrom(e.target.value)} />
      </label>{' '}
      <label>
        到 <input type="datetime-local" value={to} onChange={e => setTo(e.target.value)} />
      </label>{' '}
      <button onClick={apply}>应用</button>
      <table border={1} cellSpacing={0} cellPadding={4}>
        <thead>
          <tr>
            <th>id</th>
            <th>taskId</th>
            <th>状态</th>
            <th>分片</th>
            <th>attempt</th>
            <th>startedAt</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(r => (
            <tr key={r.id} onClick={() => openDetail(r.id)} style={{ cursor: 'pointer' }}>
              <td>{r.id}</td>
              <td>{r.taskId}</td>
              <td>{r.status}</td>
              <td>{r.shardCount}</td>
              <td>{r.attempt}</td>
              <td>{r.startedAt ?? '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {hasMore && <button onClick={() => setOffset(o => o + PAGE_SIZE)}>加载更多</button>}
      {detail && (
        <div>
          <h3>
            详情 #{detail.id} · 状态 {detail.status}
          </h3>
          <button onClick={() => setDetail(null)}>关闭</button>{' '}
          <button onClick={() => doCancel(detail.id)}>取消执行</button>
          <table border={1} cellSpacing={0} cellPadding={4}>
            <thead>
              <tr>
                <th>shard</th>
                <th>idx</th>
                <th>状态</th>
                <th>attempt</th>
                <th>workerId</th>
                <th>deadLetter</th>
              </tr>
            </thead>
            <tbody>
              {detail.shards.map(s => (
                <tr key={s.id}>
                  <td>{s.id}</td>
                  <td>{s.shardIndex}</td>
                  <td>{s.status}</td>
                  <td>{s.attempt}</td>
                  <td>{s.workerId ?? '—'}</td>
                  <td>{String(s.deadLetter)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}