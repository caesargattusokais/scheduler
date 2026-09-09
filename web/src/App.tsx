import { useEffect, useState } from 'react';
import { listTasks } from './api/client';
import type { Task } from './api/types';

export default function App() {
  const [tasks, setTasks] = useState<Task[]>([]);
  useEffect(() => {
    listTasks().then(setTasks).catch(() => setTasks([]));
  }, []);
  return (
    <div style={{ fontFamily: 'system-ui', padding: 16 }}>
      <h1>Scheduler 任务</h1>
      <ul>{tasks.map((t) => <li key={t.id}>{t.id} — {t.name}</li>)}</ul>
    </div>
  );
}