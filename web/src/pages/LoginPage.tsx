// web/src/pages/LoginPage.tsx
import { useEffect, useState } from 'react';
import { login, me, setPendingCurrentPassword } from '../api/client';

/** onAuthed:登录成功后回调(由 App 的会话门桥接:重新 me() 判 mustChange → 强制改密门或主界面)。 */
export default function LoginPage({ onAuthed }: { onAuthed: () => void }) {
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // 挂载时先探当前会话:已登录(me 200)则直接进主界面,不闪烁登录表单。
  const [checking, setChecking] = useState(true);

  useEffect(() => {
    (async () => {
      try {
        await me();
        onAuthed(); // 实际会话门(App)在挂载时已判过;此处兜底:已有会话则重入网关心刷新身份。
      } catch {
        // 401 → 未登录,停留登录页。
      } finally {
        setChecking(false);
      }
    })();
  }, [onAuthed]);

  if (checking) return <div className="content flex items-center justify-center text-sm text-slate-400">校验会话…</div>;

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim() || !password) return;
    setBusy(true); setErr(null);
    try {
      const lr = await login(name.trim(), password);
      if (lr.mustChangePassword) setPendingCurrentPassword(password); // 共享默认口令需强制改密:暂存预填当前密
      onAuthed(); // 重新 me():must_change → 强制改密门;否则主界面。
    } catch (e2) {
      setErr(String(e2)); // 保留后端失败文本(如 401/403 原因)。
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="content flex min-h-screen items-center justify-center">
      <form className="card w-80 p-6" onSubmit={submit}>
        <h1 className="text-lg font-semibold text-slate-800">登录 Scheduler</h1>
        <p className="mt-1 mb-4 text-sm text-slate-500">使用操作者账号与密码登录(会话由 HttpOnly Cookie 标识)</p>
        {err && <div className="mb-3 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}
        <label className="field">
          <span className="label">操作者</span>
          <input className="input" placeholder="操作者名" value={name}
            onChange={(e) => setName(e.target.value)} />
        </label>
        <label className="field">
          <span className="label">密码</span>
          <input className="input" type="password" placeholder="密码" value={password}
            onChange={(e) => setPassword(e.target.value)} />
        </label>
        <button className="btn btn-primary mt-4 w-full" type="submit" disabled={busy || !name.trim() || !password}>
          {busy ? '登录中…' : '登录'}
        </button>
      </form>
    </div>
  );
}