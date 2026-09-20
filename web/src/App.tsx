import { useEffect, useState } from 'react';
import { BrowserRouter, NavLink, Navigate, Route, Routes } from 'react-router-dom';
import { logout, me } from './api/client';
import { OperatorEntry } from './api/types';
import LoginPage from './pages/LoginPage';
import TasksPage from './pages/TasksPage';
import ExecutionsPage from './pages/ExecutionsPage';
import DlqPage from './pages/DlqPage';
import DagsPage from './pages/DagsPage';
import MetricsPage from './pages/MetricsPage';
import AuditPage from './pages/AuditPage';
import OperatorsPage from './pages/OperatorsPage';

const NAV = [
  { to: '/tasks', label: '任务', icon: '▤' },
  { to: '/executions', label: '执行', icon: '↻' },
  { to: '/dlq', label: 'DLQ', icon: '⚠' },
  { to: '/dags', label: '工作流', icon: '⌗' },
  { to: '/metrics', label: '指标', icon: '▦' },
  { to: '/audits', label: '审计', icon: '≡' }, // 第 6 入口
  { to: '/operators', label: '操作者', icon: '☺' }, // 第 7 入口(操作者目录,仅 ADMIN 可管理)
];

let gateInflight: Promise<OperatorEntry> | null = null;

export default function App() {
  const [meOp, setMeOp] = useState<OperatorEntry | null>(null);
  const [gatePending, setGatePending] = useState(true);

  useEffect(() => {
    (async () => {
      try {
        const op = await (gateInflight ??= me().then((r) => r.operator));
        setMeOp(op);
      } catch {
        // 401 → 未认证,渲染登录门。
        setMeOp(null);
      } finally {
        setGatePending(false);
      }
    })();
  }, []);

  if (gatePending) return <div className="content flex min-h-screen items-center justify-center text-sm text-slate-400">校验会话…</div>;
  // 未登录:渲染登录门。
  if (!meOp) return <LoginPage />;

  return (
    <BrowserRouter>
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
            <Route path="/dlq" element={<DlqPage />} />
            <Route path="/dags" element={<DagsPage />} />
            <Route path="/metrics" element={<MetricsPage />} />
            <Route path="/audits" element={<AuditPage />} />
            <Route path="/operators" element={<OperatorsPage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}