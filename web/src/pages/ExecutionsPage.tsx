import { useCallback, useEffect, useState } from 'react';
import { listExecutions, getExecutionDetail, cancelExecution, listTasks } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { Execution, ExecutionDetail, Task } from '../api/types';
import StatusBadge from '../components/StatusBadge';

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
  };
  const reset = () => {
    setTaskId(''); setStatus(''); setFrom(''); setTo(''); setOffset(0);
  };

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">执行</h1>
          <p className="page-sub">按任务 / 状态 / 时间窗检索执行记录</p>
        </div>
        {rows.length > 0 && <div className="text-sm text-slate-500">显示 {offset + 1}–{offset + rows.length}</div>}
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
        <label className="field">
          <span className="label">状态</span>
          <select className="input" value={status} onChange={(e) => { setStatus(e.target.value); setOffset(0); }}>
            <option value="">全部</option>
            {STATUSES.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </label>
        <label className="field">
          <span className="label">从</span>
          <input className="input" type="datetime-local" value={from} onChange={(e) => { setFrom(e.target.value); setOffset(0); }} />
        </label>
        <label className="field">
          <span className="label">到</span>
          <input className="input" type="datetime-local" value={to} onChange={(e) => { setTo(e.target.value); setOffset(0); }} />
        </label>
        <div className="ml-auto flex items-center gap-2">
          <button className="btn-secondary" onClick={reset}>重置</button>
          <button className="btn-primary" onClick={apply}>应用</button>
        </div>
      </div>

      <div className="table-wrap">
        <table className="table">
          <thead>
            <tr>
              <th className="num">id</th>
              <th className="num">taskId</th>
              <th>状态</th>
              <th className="num">分片</th>
              <th className="num">attempt</th>
              <th>startedAt</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id} onClick={() => openDetail(r.id)} className="cursor-pointer">
                <td className="num">{r.id}</td>
                <td className="num">{r.taskId}</td>
                <td><StatusBadge status={r.status} /></td>
                <td className="num">{r.shardCount}</td>
                <td className="num">{r.attempt}</td>
                <td className="text-xs text-slate-500">{r.startedAt ?? '—'}</td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr><td colSpan={6} className="py-8 text-center text-slate-400">暂无执行记录</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {hasMore && (
        <div className="mt-3 text-center">
          <button className="btn-secondary" onClick={() => setOffset((o) => o + PAGE_SIZE)}>加载更多</button>
        </div>
      )}

      {detail && (
        <div className="card mt-5">
          <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
            <div className="flex items-center gap-2 text-sm font-semibold text-slate-900">
              执行详情 #{detail.id}
              <StatusBadge status={detail.status} />
            </div>
            <div className="flex items-center gap-2">
              <button className="btn-secondary" onClick={() => setDetail(null)}>关闭</button>
              <button className="btn-danger" onClick={() => doCancel(detail.id)}>取消执行</button>
            </div>
          </div>
          <div className="overflow-x-auto">
            <table className="table">
              <thead>
                <tr>
                  <th className="num">shard</th>
                  <th className="num">idx</th>
                  <th>状态</th>
                  <th className="num">attempt</th>
                  <th>workerId</th>
                  <th>deadLetter</th>
                </tr>
              </thead>
              <tbody>
                {detail.shards.map((s) => (
                  <tr key={s.id}>
                    <td className="num">{s.id}</td>
                    <td className="num">{s.shardIndex}</td>
                    <td><StatusBadge status={s.status} /></td>
                    <td className="num">{s.attempt}</td>
                    <td className="text-xs text-slate-500">{s.workerId ?? '—'}</td>
                    <td>{s.deadLetter ? <span className="badge badge-red">死信</span> : <span className="text-slate-300">—</span>}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}