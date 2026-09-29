// web/src/pages/SettingsPage.tsx —— 运行时设置(热更新):ADMIN 改 DB 热键,读侧全量展示。
import { useCallback, useEffect, useState } from 'react';
import { listRuntimeConfig, setRuntimeConfig } from '../api/client';
import { RuntimeConfigEntry } from '../api/types';

/** 各热键的用途说明(展示层;契约与默认值在 server RuntimeConfigKeys)。改动均热生效:delay 为下拍调度间隔,
 *   capacity/suspend 为 worker 与全循环下拍即读。 */
const KEY_DESC: Record<string, string> = {
  'loop.scan-delay-ms':
    '扫描入队循环(ScanLoop)的调度间隔(毫秒)。每隔这么久扫一次待调度任务,把到期的分片从 DUE 推进到可认领,交给 worker 执行。'
    + '调小→任务更及时入队,但扫库更频繁、DB 压力更高;调大→省开销,但入队延迟变大(批量任务可能在下次扫描才被看到)。'
    + '每次扫描开始才读取一次,改后下一拍生效,无需重启。',
  'reconcile.delay-ms':
    '对账循环(ReconcileLoop)的间隔(毫秒)。周期校正执行状态:处理超时、回收疑似失联 worker 的在途分片(活性/租约机制)、'
    + '补发执行完成/失败/超时的事件与通知等。调小→状态纠偏更及时,但对账更频繁;默认 15s 权衡及时性与开销。'
    + '仅 leader 节点执行;改后下一拍生效。',
  'dag.delay-ms':
    'DAG 派生循环(DagLoop)的间隔(毫秒)。周期扫仍在活跃的运行(run),按依赖关系把就绪的下游节点派生为新的执行。'
    + '仅上游 SUCCESS(非 FAILED/超时)才派生下游;调小→工作流推进更快;默认 5s。改后下一拍生效。',
  'event.scan-delay-ms':
    '事件入站扫描循环(EventLoop)的间隔(毫秒)。轮询待处理的入站事件(InboundEvent),命中事件规则后触发对应任务。'
    + '调小→事件响应更及时;默认 5s。改后下一拍生效。',
  'notifications.dispatch-delay-ms':
    '通知投递循环(NotificationLoop)的间隔(毫秒)。从 outbox 出队通知并投递 webhook(带 HMAC 签名、重试退避与幂等去重)。'
    + '默认 1s 较密;通知量大时可调大降频,避免打满 webhook 投递端点。仅 leader 执行;改后下一拍生效。',
  'dlq.replay-delay-ms':
    'DLQ 自动重放循环(DlqReplayLoop)的间隔(毫秒)。周期扫描被判为待重放/超上限的死信分片,重新入队再试一次。'
    + '默认 1s。若生产开了 DLQ 重放,用它控制重放节奏;调大可给下游恢复留时间。改后下一拍生效。',
  'audit.retention.delay-ms':
    '审计归档循环(AuditRetentionLoop)的间隔(毫秒)。周期执行归档扫描,把满足保留条件的审计记录搬到归档表。'
    + '默认 1 小时跑一次。仅 leader 执行;改后下一拍生效。',
  'audit.retention.days':
    '审计归档阈值(天)。0=不归档(默认);大于 0 时,归档 N 天前创建的审计到 app_audit_archive,并对取证链做「即删即重补链」'
    + '保证链完整性不断。低优先热键:可调可启;归档动作本身也会回写一条审计。设为很大值等价于近乎永不归档。',
  'worker.capacity':
    '每个 worker 进程并发认领/执行的在途分片数上限(worker 侧每拍热读)。增→立即补排执行线程提升并行;减→CAS 收窄并发,'
    + '多余执行线程让位但不重建线程池。有效范围钳制在 [1, max-capacity] 之间,其中 max-capacity 是重启级 @Value(默认 32)。'
    + '多 worker 分摊时此键决定单进程并行度;设 0 已不再允许(需用 suspend)暂停认领。改后 worker 下一拍生效。',
  'worker.shutdown-grace-sec':
    'worker 优雅停机时等待在途分片执行完(排空)的上限(秒,worker 侧每拍热读)。停机流程:停新认领→等 inflight 归零'
    + '(最多等这么久)→ 把 last_seen 置极早 deregister,让在途分片立即被其它 worker 认领 → 停心跳。'
    + '若 grace 到时仍在途未执行完,记警告并继续退出,剩余在途交给 server 活性机制(默认 60s 回收窗口)兜底二次认领。'
    + '对滚动发布:这是「给在途执行让渡时间」的预算,按最长单个 handler 时长设。',
  suspend:
    '全局暂停开关(所有 server 循环 + worker 认领环热读)。置 true:各 server 循环跳过执行、worker 停止新认领,但心跳照常、'
    + '已在途执行不受扰——用于运维期整体停顿/排空。清 false 后下一调度拍自动恢复。注意它不丢在途,只延迟新工作;'
    + '停拍的循环仍用约 5s 心跳兜底排拍,避免暂停期间忙轮询。',
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