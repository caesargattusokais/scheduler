// web/src/pages/OperatorsPage.tsx
import { useEffect, useState } from 'react';
import { deactivateOperator, listOperators, setPassword, upsertOperator } from '../api/client';
import { OperatorEntry } from '../api/types';

const ROLE_OPTS = ['OPERATOR', 'ADMIN'] as const;

export default function OperatorsPage() {
  const [rows, setRows] = useState<OperatorEntry[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [name, setName] = useState('');
  const [role, setRole] = useState<'OPERATOR' | 'ADMIN'>('OPERATOR');
  const [active, setActive] = useState(true);

  const load = async () => {
    try { setRows(await listOperators()); setErr(null); }
    catch (e) { setErr(String(e)); }
  };
  useEffect(() => { load(); }, []);

  const submit = async () => {
    try {
      await upsertOperator({ name, role, active });
      setName(''); setRole('OPERATOR'); setActive(true);
      await load();
    } catch (e) { setErr(String(e)); }
  };

  const deactivate = async (n: string) => {
    try { await deactivateOperator(n); await load(); }
    catch (e) { setErr(String(e)); }
  };

  // 「设密码」:ADMIN 专属;成功后该操作者的既有会话被服务端撤销,需重新登录。
  const setPwd = async (n: string) => {
    const pwd = window.prompt(`为操作者「${n}」设置新密码(成功后其现有会话将被撤销):`);
    if (pwd === null || pwd === '') return;
    try {
      await setPassword(n, pwd);
      setNotice(`已为 ${n} 设置新密码,其会话被撤销,需重新登录`); setErr(null);
    } catch (e) { setErr(String(e)); }
  };

  const badge = (r: string) => r === 'ADMIN'
    ? 'rounded bg-indigo-100 px-1.5 py-0.5 text-xs font-medium text-indigo-700'
    : 'rounded bg-slate-100 px-1.5 py-0.5 text-xs font-medium text-slate-700';

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">操作者</h1>
          <p className="page-sub">写端授权的白名单与角色目录(OPERATOR 可执行常规写,ADMIN 专属敏感写端;整体仅 ADMIN 可管理)</p>
        </div>
      </div>

      {notice && <div className="mb-4 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{notice}</div>}
      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      <div className="card mb-4 p-4">
        <div className="flex items-end gap-3">
          <label className="field">
            <span className="label">名称</span>
            <input className="input" placeholder="操作者名(白名单项)" value={name}
              onChange={(e) => setName(e.target.value)} />
          </label>
          <label className="field">
            <span className="label">角色</span>
            <select className="input" value={role}
              onChange={(e) => setRole(e.target.value as 'OPERATOR' | 'ADMIN')}>
              {ROLE_OPTS.map((r) => <option key={r} value={r}>{r}</option>)}
            </select>
          </label>
          <label className="field">
            <span className="label">启用</span>
            <input className="input" type="checkbox" style={{ width: 'auto', margin: 'auto 0' }}
              checked={active}
              onChange={(e) => setActive(e.target.checked)} />
          </label>
          <button className="btn btn-primary" onClick={submit} disabled={!name.trim()}>登记/更新</button>
        </div>
      </div>

      {rows.length === 0 ? (
        <div className="card p-10 text-center text-sm text-slate-400">目录为空,请用 ADMIN 身份登记首个操作者</div>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr><th>名称</th><th>角色</th><th>状态</th><th></th></tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.name}>
                  <td className="font-mono text-sm">{r.name}</td>
                  <td><span className={badge(r.role)}>{r.role}</span></td>
                  <td>{r.active
                    ? <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-xs font-medium text-emerald-700">启用</span>
                    : <span className="rounded bg-slate-200 px-1.5 py-0.5 text-xs font-medium text-slate-600">已停用</span>}</td>
                  <td className="text-right">
                    <button className="btn btn-secondary" title="设置该操作者的登录密码(ADMIN;成功后其会话被撤销,需重新登录)"
                      onClick={() => setPwd(r.name)}>设密码</button>
                    {r.active && (
                      <button className="btn btn-secondary" title="停用后该操作者不再能执行写操作"
                        onClick={() => deactivate(r.name)}>停用</button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}