import { useCallback, useEffect, useState } from 'react';
import { BrowserRouter, Link, NavLink, Navigate, Route, Routes } from 'react-router-dom';
import { logout, me } from './api/client';
import { OperatorEntry } from './api/types';
import LoginPage from './pages/LoginPage';
import ForcePasswordChange from './pages/ForcePasswordChange';
import TasksPage from './pages/TasksPage';
import ExecutionsPage from './pages/ExecutionsPage';
import DlqPage from './pages/DlqPage';
import DagsPage from './pages/DagsPage';
import MetricsPage from './pages/MetricsPage';
import AuditPage from './pages/AuditPage';
import OperatorsPage from './pages/OperatorsPage';
import EventsPage from './pages/EventsPage';

const NAV = [
  { to: '/tasks', label: '任务', icon: '▤' },
  { to: '/executions', label: '执行', icon: '↻' },
  { to: '/events', label: '事件', icon: '✉' }, // 3b 入口(事件触发入站)
  { to: '/dlq', label: 'DLQ', icon: '⚠' },
  { to: '/dags', label: '工作流', icon: '⌗' },
  { to: '/metrics', label: '指标', icon: '▦' },
  { to: '/audits', label: '审计', icon: '≡' }, // 第 6 入口
  { to: '/operators', label: '操作者', icon: '☺' }, // 第 7 入口(操作者目录,仅 ADMIN 可管理)
];

export default function App() {
  const [meOp, setMeOp] = useState<OperatorEntry | null>(null);
  /** true=当前会话仍在强制改密(must_change_password 引导置位)→ 硬门,须先自助改密才能进入主界面。 */
  const [mustChange, setMustChange] = useState(false);
  const [gatePending, setGatePending] = useState(true);

  /** 重探会话身份(登录/改密后调用):me() 返回 operator + must_change 标,据以在登录门/强制改密门/主界面间切换。 */
  const refresh = useCallback(async () => {
    setGatePending(true);
    try {
      const r = await me();
      setMeOp(r.operator);
      setMustChange(Boolean(r.mustChangePassword));
    } catch {
      setMeOp(null); // 401 → 未认证,渲染登录门。
      setMustChange(false);
    } finally {
      setGatePending(false);
    }
  }, []);

  useEffect(() => { void refresh(); }, [refresh]);

  // BrowserRouter 须包裹整棵被登录门保护的树:LoginPage 内部使用 useNavigate,必须在 Router 上下文内。
  return (
    <BrowserRouter>
      {gatePending ? <div className="content flex min-h-screen items-center justify-center text-sm text-slate-400">校验会话…</div> :
        // 未登录:渲染登录门。
        !meOp ? <LoginPage onAuthed={refresh} /> :
        // 共享默认口令仍须改密:硬门,先设置专属密码再进主界面。
        mustChange ? <ForcePasswordChange onDone={() => setMustChange(false)} /> :
      <div className="shell">
        <aside className="sidebar">
          <div className="sidebar-brand">
            <span className="flex h-6 w-6 items-center justify-center rounded bg-indigo-600 text-xs font-bold text-white">S</span>
            Scheduler
          </div>
          <nav className="sidebar-nav">
            {NAV.map((n) => (
              <NavLink key={n.to} to={n.to} end={n.to === '/tasks'} className={({ isActive }) => `navlink${isActive ? ' active' : ''}`}>
                <span className="w-5 text-center text-slate-400">{n.icon}</span>
                {n.label}
              </NavLink>
            ))}
          </nav>
          <div className="sidebar-operator">
            <span className="label">当前操作者</span>
            <div className="flex items-center gap-2">
              <span className="font-mono text-sm text-slate-700">{meOp.name}</span>
              <span className={meOp.role === 'ADMIN'
                ? 'rounded bg-indigo-100 px-1.5 py-0.5 text-xs font-medium text-indigo-700'
                : 'rounded bg-slate-100 px-1.5 py-0.5 text-xs font-medium text-slate-700'}>{meOp.role}</span>
            </div>
            <Link className="btn btn-secondary mt-2 w-full" to="/force-password"
              title="修改自己的登录密码(验当前密,成功后旧会话失效、新会话无缝接续)">
              修改密码
            </Link>
            <button className="btn btn-secondary mt-2 w-full" type="button"
              title="注销当前会话(清除 HttpOnly 会话 cookie)"
              onClick={async () => { await logout(); setMeOp(null); }}>
              登出
            </button>
          </div>
        </aside>
        <main className="content">
          <Routes>
            <Route path="/" element={<Navigate to="/tasks" replace />} />
            <Route path="/tasks" element={<TasksPage />} />
            <Route path="/executions" element={<ExecutionsPage />} />
            <Route path="/events" element={<EventsPage />} />
            <Route path="/dlq" element={<DlqPage />} />
            <Route path="/dags" element={<DagsPage />} />
            <Route path="/metrics" element={<MetricsPage />} />
            <Route path="/audits" element={<AuditPage />} />
            <Route path="/operators" element={<OperatorsPage />} />
            {/* 自助改密页(身份菜单入口;强制改密走上方硬门分支,不经此路由) */}
            <Route path="/force-password" element={<ForcePasswordChange />} />
          </Routes>
        </main>
      </div>}
    </BrowserRouter>
  );
}