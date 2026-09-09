import { BrowserRouter, Link, Navigate, Route, Routes } from 'react-router-dom';
import TasksPage from './pages/TasksPage';
import ExecutionsPage from './pages/ExecutionsPage';
import DlqPage from './pages/DlqPage';
import DagsPage from './pages/DagsPage';
import MetricsPage from './pages/MetricsPage';

export default function App() {
  return (
    <BrowserRouter>
      <div>
        <nav>
          <Link to="/tasks">任务</Link>{' '}
          <Link to="/executions">执行</Link>{' '}
          <Link to="/dlq">DLQ</Link>{' '}
          <Link to="/dags">工作流</Link>{' '}
          <Link to="/metrics">指标</Link>
        </nav>
        <Routes>
          <Route path="/" element={<Navigate to="/tasks" replace />} />
          <Route path="/tasks" element={<TasksPage />} />
          <Route path="/executions" element={<ExecutionsPage />} />
          <Route path="/dlq" element={<DlqPage />} />
          <Route path="/dags" element={<DagsPage />} />
          <Route path="/metrics" element={<MetricsPage />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}