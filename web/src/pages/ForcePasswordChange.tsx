// web/src/pages/ForcePasswordChange.tsx
// 自助/首登强制改密:验当前密 + 新密(≥8)+ 确认。成功 → 无缝新会话(Set-Cookie 自动续期),返回主界面。
// 两种用法:① 首登强制改密门(AuthGate 须改密时渲染,onDone 清 gate 的 mustChange 标);
//           ② 身份菜单「修改密码」账户路由(无 onDone,成功后返回任务页)。当前密由登录页暂存预填,免重输默认口令。
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { changePassword, consumePendingCurrentPassword } from '../api/client';

export default function ForcePasswordChange({ onDone }: { onDone?: () => void }) {
  const navigate = useNavigate();
  const [current, setCurrent] = useState(() => consumePendingCurrentPassword() ?? '');
  const [next, setNext] = useState('');
  const [confirm, setConfirm] = useState('');
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!current || next.length < 8 || next !== confirm) return;
    setBusy(true); setErr(null);
    try {
      await changePassword(current, next);
      onDone?.();                 // 强制门:清网关标 → 主界面(已登录,无需重登)
      navigate('/tasks', { replace: true });
    } catch (e2) {
      setErr(String(e2));         // 保留后端失败文本(如当前密不符 401)。
    } finally {
      setBusy(false);
    }
  };

  const valid = Boolean(current) && next.length >= 8 && next === confirm;

  return (
    <div className="content flex min-h-screen items-center justify-center">
      <form className="card w-80 p-6" onSubmit={submit}>
        <h1 className="text-lg font-semibold text-slate-800">修改密码</h1>
        <p className="mt-1 mb-4 text-sm text-slate-500">
          {onDone ? '首次登录:请设置你的专属密码后再进入系统。' : '更新你的登录密码(改密后旧会话一并失效)。'}
        </p>
        {err && <div className="mb-3 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}
        <label className="field">
          <span className="label">当前密码</span>
          <input className="input" type="password" placeholder="当前密码" value={current}
            onChange={(e) => setCurrent(e.target.value)} autoFocus={!current} />
        </label>
        <label className="field">
          <span className="label">新密码</span>
          <input className="input" type="password" placeholder="至少 8 位且含数字" value={next}
            onChange={(e) => setNext(e.target.value)} />
        </label>
        <label className="field">
          <span className="label">确认新密码</span>
          <input className="input" type="password" placeholder="再次输入新密码" value={confirm}
            onChange={(e) => setConfirm(e.target.value)} />
        </label>
        {next.length > 0 && next.length < 8 &&
          <p className="text-xs text-slate-500">新密码至少 8 位且含数字,不得与最近 5 次相同。</p>}
        {confirm.length > 0 && next !== confirm &&
          <p className="text-xs text-red-600">两次输入的新密码不一致。</p>}
        <button className="btn btn-primary mt-4 w-full" type="submit" disabled={busy || !valid}>
          {busy ? '保存中…' : '确认修改'}
        </button>
      </form>
    </div>
  );
}