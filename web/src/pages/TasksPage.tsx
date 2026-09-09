import { FormEvent, useCallback, useEffect, useState } from 'react';
import {
  createTask, listTasks, pauseTask, rerunTask, resumeTask, triggerTask, updateTask,
} from '../api/client';
import type { CreateTaskRequest, Task } from '../api/types';
import { useInterval } from '../lib/useInterval';

const EMPTY: CreateTaskRequest = {
  name: '', handlerRef: '', cron: '0 */5 * * * *', shardCount: 1,
  timeoutSeconds: 300, maxRetries: 0, backoffMs: 1000, maxActiveConcurrent: 8,
};

export default function TasksPage() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [form, setForm] = useState<CreateTaskRequest>(EMPTY);
  const [editId, setEditId] = useState<number | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const refresh = useCallback(() => {
    listTasks().then(setTasks).catch((e) => setErr(String(e)));
  }, []);
  useEffect(() => { refresh(); }, [refresh]);
  useInterval(refresh, 5000);

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

  return (
    <div style={{ fontFamily: 'system-ui', padding: 16 }}>
      <h1>Scheduler 任务管理</h1>
      {err && <p style={{ color: 'brown' }}>{err}</p>}
      <form onSubmit={handleSubmit} style={{ display: 'grid', gap: 6, maxWidth: 520 }}>
        <h2>{editId === null ? '新建任务' : `编辑任务 #${editId}`}</h2>
        <label>名称<input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></label>
        <label>HandlerRef<input value={form.handlerRef} onChange={(e) => setForm({ ...form, handlerRef: e.target.value })} /></label>
        <label>Cron<input value={form.cron} onChange={(e) => setForm({ ...form, cron: e.target.value })} /></label>
        <label>分片数<input type="number" value={form.shardCount} onChange={(e) => setForm({ ...form, shardCount: +e.target.value })} /></label>
        <label>超时(s)<input type="number" value={form.timeoutSeconds} onChange={(e) => setForm({ ...form, timeoutSeconds: +e.target.value })} /></label>
        <label>重试次数<input type="number" value={form.maxRetries} onChange={(e) => setForm({ ...form, maxRetries: +e.target.value })} /></label>
        <button type="submit">{editId === null ? '创建' : '保存'}</button>
      </form>
      <table border={1} cellPadding={6} style={{ marginTop: 16 }}>
        <thead>
          <tr><th>ID</th><th>名称</th><th>Cron</th><th>分片</th><th>状态</th><th>操作</th></tr>
        </thead>
        <tbody>
          {tasks.map((t) => (
            <tr key={t.id}>
              <td>{t.id}</td><td>{t.name}</td><td>{t.cron}</td><td>{t.shardCount}</td>
              <td>{t.paused ? '暂停' : '启用'}</td>
              <td style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                <button onClick={() => act(() => (t.paused ? resumeTask(t.id) : pauseTask(t.id)))}>
                  {t.paused ? '启用' : '暂停'}
                </button>
                <button onClick={() => act(() => triggerTask(t.id))}>触发</button>
                <button onClick={() => act(() => rerunTask(t.id))}>重跑</button>
                <button onClick={() => beginEdit(t)}>编辑</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}