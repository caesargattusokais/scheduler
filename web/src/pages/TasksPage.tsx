import { FormEvent, useCallback, useEffect, useMemo, useState } from 'react';
import {
  createTask, deleteTask, listHandlerRefs, listTasks, pauseTask, resumeTask, triggerTask, updateTask,
} from '../api/client';
import type { CreateTaskRequest, Task } from '../api/types';
import CronEditor from '../components/CronEditor';
import Pager from '../components/Pager';
import { useInterval } from '../lib/useInterval';

const PAGE_SIZE = 10;

const EMPTY: CreateTaskRequest = {
  name: '', handlerRef: '', cron: '0 */5 * * * *', shardCount: 1,
  timeoutSeconds: 300, maxRetries: 0, backoffMs: 1000, maxActiveConcurrent: 8,
};

export default function TasksPage() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [total, setTotal] = useState(0);
  const [name, setName] = useState('');
  const [paused, setPaused] = useState('');
  const [offset, setOffset] = useState(0);
  const [handlerRefs, setHandlerRefs] = useState<string[]>([]);
  const [form, setForm] = useState<CreateTaskRequest>(EMPTY);
  const [editId, setEditId] = useState<number | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const refresh = useCallback(() => {
    listTasks({
      name: name || undefined,
      paused: paused === '' ? undefined : paused === 'true',
      limit: PAGE_SIZE,
      offset,
    }).then((page) => { setTasks(page.items); setTotal(page.total); })
      .catch((e) => setErr(String(e)));
  }, [name, paused, offset]);
  useEffect(() => { refresh(); }, [refresh]);
  useInterval(refresh, 5000);

  // 后台枚举已注册 handler,并给新建任务一个默认值(编辑保留任务自身 ref)。
  useEffect(() => {
    listHandlerRefs().then((refs) => {
      setHandlerRefs(refs);
      setForm((f) => (f.handlerRef === '' ? { ...f, handlerRef: refs[0] ?? '' } : f));
    }).catch((e) => setErr(String(e)));
  }, []);

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    try {
      if (editId === null) await createTask(form);
      else await updateTask(editId, { ...form, paused: tasks.find((t) => t.id === editId)?.paused });
      setForm(EMPTY); setEditId(null); setErr(null); refresh();
    } catch (x) { setErr(String(x)); }
  }
  function beginEdit(t: Task) {
    setEditId(t.id);
    setForm({ name: t.name, handlerRef: t.handlerRef, cron: t.cron, shardCount: t.shardCount,
      timeoutSeconds: t.timeoutSeconds, maxRetries: t.maxRetries, backoffMs: t.backoffMs,
      retryableFailurePattern: t.retryableFailurePattern ?? undefined, maxActiveConcurrent: t.maxActiveConcurrent });
  }
  async function act(fn: () => Promise<unknown>) {
    try { await fn(); setErr(null); refresh(); } catch (x) { setErr(String(x)); }
  }
  /** 删除任务(仅无子记录可删):确认后调 DELETE;后端 409(有执行/DAG 引用)会以 err 呈现。 */
  async function doDelete(t: Task) {
    if (!window.confirm(`删除任务「${t.name}」(#${t.id})? 该任务须无执行记录且未被任何 DAG 引用。`)) return;
    try {
      await deleteTask(t.id);
      if (editId === t.id) { setEditId(null); setForm(EMPTY); }
      setErr(null); refresh();
    } catch (x) { setErr(String(x)); }
  }

  const field = (key: keyof CreateTaskRequest, label: string, type = 'text') => (
    <label className="field">
      <span className="label">{label}</span>
      <input className="input" type={type} value={String(form[key] ?? '')}
        onChange={(e) => setForm({ ...form, [key]: type === 'number' ? +e.target.value : e.target.value })} />
    </label>
  );

  // HandlerRef 下拉选项 = 已注册 refs ∪ 当前值(旧任务若引用已注销的 ref,仍保留可提交,不强制改坏)。
  const handlerOptions = useMemo(
    () => [...new Set([...handlerRefs, ...(form.handlerRef ? [form.handlerRef] : [])])],
    [handlerRefs, form.handlerRef],
  );

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">任务</h1>
          <p className="page-sub">任务定义、手动触发与单任务重跑</p>
        </div>
        {total > 0 && (
          <div className="text-sm text-slate-500">共 {total} 个任务</div>
        )}
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      <div className="toolbar">
        <label className="field">
          <span className="label">名称</span>
          <input className="input" placeholder="搜索名称…" value={name}
            onChange={(e) => { setName(e.target.value); setOffset(0); }} />
        </label>
        <label className="field">
          <span className="label">状态</span>
          <select className="input" value={paused} onChange={(e) => { setPaused(e.target.value); setOffset(0); }}>
            <option value="">全部</option>
            <option value="true">暂停</option>
            <option value="false">启用</option>
          </select>
        </label>
      </div>

      <form onSubmit={handleSubmit} className="card mb-5 p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-900">
          {editId === null ? '新建任务' : `编辑任务 #${editId}`}
        </h2>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {field('name', '名称')}
          <label className="field">
            <span className="label">HandlerRef</span>
            <select className="input" value={form.handlerRef || ''}
              onChange={(e) => setForm({ ...form, handlerRef: e.target.value })}>
              {handlerOptions.length === 0 && <option value="">— 无已注册 handler —</option>}
              {handlerOptions.map((ref) => <option key={ref} value={ref}>{ref}</option>)}
            </select>
          </label>
          {field('shardCount', '分片数', 'number')}
          {field('timeoutSeconds', '超时 (s)', 'number')}
          {field('maxRetries', '重试次数', 'number')}
          {field('backoffMs', '退避 (ms)', 'number')}
          {field('maxActiveConcurrent', '最大并发', 'number')}
        </div>
        <div className="mt-3">
          <CronEditor value={form.cron} onChange={(v) => setForm({ ...form, cron: v })} />
        </div>
        <div className="mt-3 flex items-center gap-2">
          <button type="submit" className="btn-primary">{editId === null ? '创建任务' : '保存修改'}</button>
          {editId !== null && (
            <button type="button" className="btn-secondary" onClick={() => { setForm(EMPTY); setEditId(null); }}>取消编辑</button>
          )}
        </div>
      </form>

      <div className="table-wrap">
        <table className="table">
          <thead>
            <tr><th>ID</th><th>名称</th><th>Cron</th><th className="num">分片</th><th>状态</th><th>操作</th></tr>
          </thead>
          <tbody>
            {tasks.map((t) => (
              <tr key={t.id}>
                <td className="num">{t.id}</td>
                <td className="font-medium text-slate-900">{t.name}</td>
                <td><code className="text-xs text-slate-500">{t.cron}</code></td>
                <td className="num">{t.shardCount}</td>
                <td>{t.paused ? <span className="badge badge-slate">暂停</span> : <span className="badge badge-green">启用</span>}</td>
                <td className="whitespace-nowrap">
                  <div className="flex gap-1">
                    <button className="btn-secondary" onClick={() => act(() => (t.paused ? resumeTask(t.id) : pauseTask(t.id)))}>
                      {t.paused ? '启用' : '暂停'}
                    </button>
                    <button className="btn-ghost" onClick={() => act(() => triggerTask(t.id))}>触发</button>
                    <button className="btn-ghost" onClick={() => beginEdit(t)}>编辑</button>
                    <button className="btn-ghost text-red-600" onClick={() => doDelete(t)}>删除</button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <Pager total={total} offset={offset} limit={PAGE_SIZE} onPage={setOffset} />
    </div>
  );
}