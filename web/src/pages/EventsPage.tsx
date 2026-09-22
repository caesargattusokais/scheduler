import { FormEvent, useCallback, useEffect, useState } from 'react';
import { listEvents, postEvent } from '../api/client';
import { InboundEvent } from '../api/types';
import Pager from '../components/Pager';
import { useInterval } from '../lib/useInterval';

const PAGE_SIZE = 10;

/** 3b 事件触发器控制面:提交入站事件(落 PENDING 待 leader 事件引擎分派)+ 观察每条事件的路由分派状态。 */
export default function EventsPage() {
  const [events, setEvents] = useState<InboundEvent[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [routeKey, setRouteKey] = useState('order.created');
  const [dedupeKey, setDedupeKey] = useState('');
  const [payload, setPayload] = useState('{}');
  const [err, setErr] = useState<string | null>(null);
  const [ok, setOk] = useState<string | null>(null);

  const refresh = useCallback(() => {
    listEvents({ limit: PAGE_SIZE, offset }).then((page) => {
      setEvents(page.items); setTotal(page.total);
    }).catch((e) => setErr(String(e)));
  }, [offset]);
  useEffect(() => { refresh(); }, [refresh]);
  useInterval(refresh, 5000);

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setErr(null); setOk(null);
    let payloadObj: unknown = {};
    try { payloadObj = payload.trim() ? JSON.parse(payload) : {}; }
    catch { setErr('payload 不是合法 JSON'); return; }
    if (!dedupeKey.trim()) { setErr('dedupeKey 必填(防重放)'); return; }
    try {
      const evt = await postEvent({ routeKey: routeKey.trim(), payload: payloadObj, dedupeKey: dedupeKey.trim() });
      setOk(`已入站事件 #${evt.id}(${evt.status});分派由 leader 事件引擎异步完成。`);
      setDedupeKey('');
      refresh();
    } catch (x) { setErr(String(x)); }
  }

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">事件</h1>
          <p className="page-sub">入站事件触发:提交事件 → 匹配订阅任务的 route_key → 每事件一轮</p>
        </div>
        {total > 0 && (<div className="text-sm text-slate-500">共 {total} 条</div>)}
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}
      {ok && <div className="mb-4 rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">{ok}</div>}

      <form onSubmit={handleSubmit} className="card mb-5 p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-900">提交事件</h2>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <label className="field">
            <span className="label">RouteKey</span>
            <input className="input font-mono" value={routeKey}
              onChange={(e) => setRouteKey(e.target.value)} placeholder="order.created" />
          </label>
          <label className="field">
            <span className="label">DedupeKey (防重放,须唯一)</span>
            <input className="input font-mono" value={dedupeKey}
              onChange={(e) => setDedupeKey(e.target.value)} placeholder="evt-uuid" />
          </label>
          <label className="field col-span-1 sm:col-span-2">
            <span className="label">Payload (JSON)</span>
            <input className="input font-mono" value={payload}
              onChange={(e) => setPayload(e.target.value)} placeholder='{"oid":7}' />
          </label>
        </div>
        <div className="mt-3">
          <button type="submit" className="btn-primary">提交事件</button>
        </div>
      </form>

      <div className="table-wrap">
        <table className="table">
          <thead>
            <tr><th>ID</th><th>路由</th><th>状态</th><th>Payload</th><th>任务</th><th>执行</th><th>创建</th></tr>
          </thead>
          <tbody>
            {events.map((e) => (
              <tr key={e.id}>
                <td className="num">{e.id}</td>
                <td><code className="text-xs text-slate-600">{e.routeKey}</code></td>
                <td>{e.status === 'DISPATCHED'
                  ? <span className="badge badge-green">已分派</span>
                  : <span className="badge badge-slate">待分派</span>}</td>
                <td className="max-w-56"><code className="text-xs text-slate-500 break-all">{e.payload ?? '-'}</code></td>
                <td className="num">{e.taskId ?? '-'}</td>
                <td className="num">{e.executionId ?? '-'}</td>
                <td className="num whitespace-nowrap text-xs text-slate-500">{e.createdAt ? new Date(e.createdAt).toLocaleString() : '-'}</td>
              </tr>
            ))}
            {events.length === 0 && (
              <tr><td colSpan={7} className="py-6 text-center text-sm text-slate-400">暂无事件</td></tr>
            )}
          </tbody>
        </table>
      </div>

      <Pager total={total} offset={offset} limit={PAGE_SIZE} onPage={setOffset} />
    </div>
  );
}