import { BrowserRouter, NavLink, Navigate, Route, Routes } from 'react-router-dom';
import TasksPage from './pages/TasksPage';
import ExecutionsPage from './pages/ExecutionsPage';
import DlqPage from './pages/DlqPage';
import DagsPage from './pages/DagsPage';
import MetricsPage from './pages/MetricsPage';

const NAV = [
  { to: '/tasks', label: '任务', icon: '▤' },
  { to: '/executions', label: '执行', icon: '↻' },
  { to: '/dlq', label: 'DLQ', icon: '⚠' },
  { to: '/dags', label: '工作流', icon: '⌗' },
  { to: '/metrics', label: '指标', icon: '▦' },
];

export default function App() {
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
        </aside>
        <main className="content">
          <Routes>
            <Route path="/" element={<Navigate to="/tasks" replace />} />
            <Route path="/tasks" element={<TasksPage />} />
            <Route path="/executions" element={<ExecutionsPage />} />
            <Route path="/dlq" element={<DlqPage />} />
            <Route path="/dags" element={<DagsPage />} />
            <Route path="/metrics" element={<MetricsPage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}