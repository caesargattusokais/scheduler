// web/src/pages/NotificationsPage.tsx —— 通知告警闭环(4-1):webhook 订阅管理(写 ADMIN)+ 投递历史(读)。
import { useEffect, useState } from 'react';
import Pager from '../components/Pager';
import {
  createWebhook, deleteWebhook, listNotifications, listWebhooks, updateWebhook,
} from '../api/client';
import { OutboundNotification, OutboundWebhook, Page } from '../api/types';

const KINDS_SAMPLE = 'execution.completed, execution.failed';

function statusBadge(s: string): string {
  return s === 'SENT'
    ? 'rounded bg-emerald-100 px-1.5 py-0.5 text-xs font-medium text-emerald-700'
    : s === 'FAILED'
    ? 'rounded bg-red-100 px-1.5 py-0.5 text-xs font-medium text-red-700'
    : 'rounded bg-amber-100 px-1.5 py-0.5 text-xs font-medium text-amber-700';
}

export default function NotificationsPage() {
  const [hooks, setHooks] = useState<OutboundWebhook[]>([]);
  const [notifs, setNotifs] = useState<Page<OutboundNotification>>({ items: [], total: 0, offset: 0, limit: 20 });
  const [err, setErr] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  // 订阅编辑表单:id=null → 新建;否则更新。
  const [id, setId] = useState<number | null>(null);
  const [url, setUrl] = useState('');
  const [kinds, setKinds] = useState('');
  const [maxAttempts, setMaxAttempts] = useState(5);
  const [statusFilter, setStatusFilter] = useState('');

  const loadHooks = async () => {
    try { setHooks(await listWebhooks()); setErr(null); }
    catch (e) { setErr(String(e)); }
  };
  const loadNotifs = async (offset = 0, limit = 20) => {
    try {
      setNotifs(await listNotifications({ status: statusFilter, limit, offset }));
      setErr(null);
    } catch (e) { setErr(String(e)); }
  };
  useEffect(() => { loadHooks(); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, []);
  useEffect(() => { loadNotifs(0, 20); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [statusFilter]);

  const submit = async () => {
    if (!url.trim()) { setErr('订阅 URL 必填'); return; }
    const body = {
      url: url.trim(),
      ...(kinds.trim() ? { kinds: kinds.split(',').map((k) => k.trim()).filter(Boolean) } : {}),
      maxAttempts,
    };
    try {
      if (id == null) await createWebhook(body);
      else await updateWebhook(id, body);
      setNotice(id == null ? '已创建订阅' : '已更新订阅'); setErr(null);
      resetForm();
      await loadHooks();
    } catch (e) { setErr(String(e)); }
  };

  const toggle = async (h: OutboundWebhook) => {
    try {
      await updateWebhook(h.id, {
        url: h.url, secret: h.secret ?? undefined, kinds: h.kinds,
        enabled: !h.enabled, maxAttempts: h.maxAttempts, backoffMs: h.backoffMs,
      });
      await loadHooks();
    } catch (e) { setErr(String(e)); }
  };

  const remove = async (h: OutboundWebhook) => {
    if (!window.confirm(`删除订阅 ${h.url}?(已投递历史保留)`)) return;
    try { await deleteWebhook(h.id); await loadHooks(); }
    catch (e) { setErr(String(e)); }
  };

  const resetForm = () => { setId(null); setUrl(''); setKinds(''); setMaxAttempts(5); };

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">通知</h1>
          <p className="page-sub">webhook 订阅(ADMIN 管理,DB 存储取代配置文件)与出站通知投递历史(PENDING/SENT/FAILED + 退避重试)</p>
        </div>
      </div>

      {notice && <div className="mb-4 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{notice}</div>}
      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      <div className="card mb-4 p-4">
        <div className="flex items-end gap-3">
          <label className="field grow">
            <span className="label">{id == null ? 'URL(新建订阅)' : `URL(更新 #${id})`}</span>
            <input className="input" placeholder="https://hooks.example.com/x" value={url}
              onChange={(e) => setUrl(e.target.value)} />
          </label>
          <label className="field grow">
            <span className="label">Kinds(逗号分隔,空 = 订阅全部)</span>
            <input className="input" placeholder={KINDS_SAMPLE} value={kinds}
              onChange={(e) => setKinds(e.target.value)} />
          </label>
          <label className="field" style={{ maxWidth: 110 }}>
            <span className="label">最大尝试</span>
            <input className="input" type="number" min={1} value={maxAttempts}
              onChange={(e) => setMaxAttempts(Number(e.target.value))} />
          </label>
          {id != null && <button className="btn btn-secondary" onClick={resetForm}>取消编辑</button>}
          <button className="btn btn-primary" onClick={submit} disabled={!url.trim()}>
            {id == null ? '创建' : '保存'}
          </button>
        </div>
      </div>

      <div className="card mb-4 p-4">
        <h2 className="mb-2 text-sm font-semibold text-slate-700">订阅 ({hooks.length})</h2>
        {hooks.length === 0 ? (
          <div className="py-6 text-center text-sm text-slate-400">尚无 webhook 订阅 —— 用上方表单登记端点后,通知将按其 kinds 订阅投递(HMAC-SHA256 签名)</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr><th>URL</th><th>Kinds</th><th>状态</th><th>重试</th><th></th></tr>
              </thead>
              <tbody>
                {hooks.map((h) => (
                  <tr key={h.id}>
                    <td className="font-mono text-sm">{h.enabled ? h.url : <span className="text-slate-400">{h.url}</span>}</td>
                    <td className="text-sm">{h.kinds.length === 0 ? <span className="text-slate-400">全部</span> : h.kinds.join(', ')}</td>
                    <td>{h.enabled
                      ? <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-xs font-medium text-emerald-700">启用</span>
                      : <span className="rounded bg-slate-200 px-1.5 py-0.5 text-xs font-medium text-slate-600">停用</span>}</td>
                    <td className="text-sm">{h.maxAttempts} 次 / {h.backoffMs} ms</td>
                    <td className="text-right">
                      <button className="btn btn-secondary" title={h.enabled ? '停用:不再向其投递' : '启用:恢复投递'}
                        onClick={() => toggle(h)}>{h.enabled ? '停用' : '启用'}</button>
                      <button className="btn btn-secondary" title="编辑该订阅"
                        onClick={() => { setId(h.id); setUrl(h.url); setKinds(h.kinds.join(', ')); setMaxAttempts(h.maxAttempts); }}>编辑</button>
                      <button className="btn btn-secondary" title="删除该订阅(历史保留)"
                        onClick={() => remove(h)}>删除</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="card p-4">
        <div className="mb-2 flex items-center gap-3">
          <h2 className="text-sm font-semibold text-slate-700">投递历史</h2>
          <select className="input" style={{ width: 'auto' }} value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}>
            <option value="">全部状态</option>
            <option value="PENDING">PENDING</option>
            <option value="SENT">SENT</option>
            <option value="FAILED">FAILED</option>
          </select>
        </div>
        {notifs.items.length === 0 ? (
          <div className="py-6 text-center text-sm text-slate-400">暂无投递记录(执行事件落库后由 leader 门控 dispatch 投喂)</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr><th>Kind</th><th>目标</th><th>状态</th><th>尝试</th><th>错误</th><th>时间</th></tr>
              </thead>
              <tbody>
                {notifs.items.map((n) => (
                  <tr key={n.id}>
                    <td className="font-mono text-sm">{n.kind}</td>
                    <td className="text-sm">#{n.targetId ?? '—'}</td>
                    <td><span className={statusBadge(n.status)}>{n.status}</span></td>
                    <td className="text-sm">{n.attempts}</td>
                    <td className="max-w-xs truncate text-xs text-slate-500" title={n.lastError ?? ''}>{n.lastError ?? ''}</td>
                    <td className="whitespace-nowrap text-xs text-slate-500">{new Date(n.createdAt).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <Pager total={notifs.total} offset={notifs.offset} limit={notifs.limit}
          onPage={(o) => loadNotifs(o, notifs.limit)} />
      </div>
    </div>
  );
}