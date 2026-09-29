// web/src/pages/SettingsPage.tsx —— 运行时设置(热更新):ADMIN 改 DB 热键,读侧全量展示。
import { useCallback, useEffect, useState } from 'react';
import { listRuntimeConfig, setRuntimeConfig } from '../api/client';
import { RuntimeConfigEntry } from '../api/types';

/** 各热键的用途说明(展示层;契约与默认值在 server RuntimeConfigKeys)。改动均热生效:delay 为下拍调度间隔,
 *   capacity/suspend 为 worker 与全循环下拍即读。 */
const KEY_DESC: Record<string, string> = {
  'loop.scan-delay-ms': '扫描入队循环的调度间隔(毫秒):每隔多久扫一次待调度任务并入队执行。',
  'reconcile.delay-ms': '对账循环间隔(毫秒):周期性校正执行状态(超时/活性回收/纠偏)。',
  'dag.delay-ms': 'DAG 派生循环间隔(毫秒):扫活跃运行、按依赖派生下游节点。',
  'event.scan-delay-ms': '事件入站扫描间隔(毫秒):轮询待处理的入站事件。',
  'notifications.dispatch-delay-ms': '通知投递轮询间隔(毫秒):outbox 出队并投递 webhook 的节奏。',
  'dlq.replay-delay-ms': 'DLQ 自动重放循环间隔(毫秒):扫超重放上限的死信分片并重新入队。',
  'audit.retention.delay-ms': '审计归档循环间隔(毫秒):周期性执行归档扫描(默认 1 小时)。',
  'audit.retention.days': '审计归档阈值(天):0=不归档;大于 0 时归档 N 天之前的审计(低优先,可调可启)。',
  'worker.capacity': '每个 worker 并发认领/执行的在途分片数上限。热调增/减即时生效,不重建线程池。',
  'worker.shutdown-grace-sec': 'worker 优雅停机等待在途分片排空的上限(秒);超时由 server 活性机制兜底回收。',
  suspend: '全局暂停开关:置 true 后所有 server 循环与 worker 认领都停新动作(排空在途);清位后下一拍恢复。',
};

export default function SettingsPage() {
  const [items, setItems] = useState<RuntimeConfigEntry[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [busyKey, setBusyKey] = useState<string | null>(null);

  const load = useCallback(async () => {
    try { setItems(await listRuntimeConfig()); setErr(null); }
    catch (e) { setErr(String(e)); }
  }, []);

  useEffect(() => { load(); }, [load]);

  const save = async (item: RuntimeConfigEntry, value: string) => {
    setBusyKey(item.key);
    try {
      await setRuntimeConfig(item.key, value);
      setNotice(`已更新 ${item.key}`); setErr(null);
      await load();
    } catch (e) { setErr(String(e)); }
    finally { setBusyKey(null); }
  };

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">运行时设置</h1>
          <p className="page-sub">DB 运行时热键:修改后下一调度拍生效,无需重启。ADMIN 可写,写入记审计。</p>
        </div>
      </div>
      {notice && <div className="mb-4 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{notice}</div>}
      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}
      <div className="card p-4">
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr><th>键</th><th>说明</th><th>当前值</th><th>来源</th><th>更新者</th><th>更新时间</th><th></th></tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <Row key={item.key} item={item} busy={busyKey === item.key} onSave={save} />
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function Row({ item, busy, onSave }: {
  item: RuntimeConfigEntry;
  busy: boolean;
  onSave: (item: RuntimeConfigEntry, value: string) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState(item.value);
  const isSuspend = item.key === 'suspend';
  return (
    <tr>
      <td className="font-mono text-sm">{item.key}</td>
      <td className="max-w-md text-left text-sm text-slate-600">{KEY_DESC[item.key] ?? '—'}</td>
      <td className="text-sm">
        {isSuspend
          ? (item.value === 'true'
              ? <span className="rounded bg-red-100 px-1.5 py-0.5 text-xs font-medium text-red-700">已暂停</span>
              : <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-xs font-medium text-emerald-700">运行中</span>)
          : <span className="font-mono text-sm">{item.value}</span>}
      </td>
      <td className="text-xs">{item.source === 'db' ? 'DB' : <span className="text-slate-400">默认</span>}</td>
      <td className="text-xs text-slate-500">{item.updatedBy ?? '—'}</td>
      <td className="whitespace-nowrap text-xs text-slate-500">{item.updatedAt ? new Date(item.updatedAt).toLocaleString() : '—'}</td>
      <td className="text-right">
        {editing ? (
          <>
            {isSuspend ? (
              <button className="btn btn-secondary"
                onClick={() => { onSave(item, item.value === 'true' ? 'false' : 'true'); setEditing(false); }}>
                {item.value === 'true' ? '恢复' : '暂停'}
              </button>
            ) : (
              <input className="input" style={{ width: 120 }} value={value}
                onChange={(e) => setValue(e.target.value)} />
            )}
            <button className="btn btn-primary" disabled={busy} onClick={() => onSave(item, isSuspend ? (item.value === 'true' ? 'false' : 'true') : value.trim())}>
              保存
            </button>
            <button className="btn btn-secondary" onClick={() => setEditing(false)}>取消</button>
          </>
        ) : (
          <button className="btn btn-secondary" onClick={() => { setValue(item.value); setEditing(true); }}>改</button>
        )}
      </td>
    </tr>
  );
}