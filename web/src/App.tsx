import { BrowserRouter, Link, Navigate, Route, Routes } from 'react-router-dom';
import TasksPage from './pages/TasksPage';
import ExecutionsPage from './pages/ExecutionsPage';
import DlqPage from './pages/DlqPage';

export default function App() {
  return (
    <BrowserRouter>
      <div>
        <nav>
          <Link to="/tasks">任务</Link>{' '}
          <Link to="/executions">执行</Link>{' '}
          <Link to="/dlq">DLQ</Link>
        </nav>
        <Routes>
          <Route path="/" element={<Navigate to="/tasks" replace />} />
          <Route path="/tasks" element={<TasksPage />} />
          <Route path="/executions" element={<ExecutionsPage />} />
          <Route path="/dlq" element={<DlqPage />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}