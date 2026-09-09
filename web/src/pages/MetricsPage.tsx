import { useEffect, useState } from 'react';
import { fetchMetrics } from '../api/client';
import type { ParsedMetric } from '../api/types';
import { useInterval } from '../lib/useInterval';

/** 每卡聚合规则:计数类取系列和(DLQ/活跃),最老队龄取最大值。 */
interface CardDef { key: string; label: string; unit?: string; agg: 'sum' | 'max' | 'raw'; nice?: (v: number) => string; }

const CARDS: CardDef[] = [
  { key: 'scheduler_active_runs', label: '活跃执行', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_due_queue_max_age_seconds', label: 'DUE 最深队龄', unit: 's', agg: 'max', nice: (v) => `${v.toFixed(0)}s` },
  { key: 'scheduler_dag_runs_active', label: '活跃 DAG 批次', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_dlq_depth', label: 'DLQ 深度', agg: 'raw', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_worker_active', label: '调度进程存活', agg: 'raw', nice: (v) => (v >= 1 ? '存活' : '下线') },
];

const HISTORY = 20;

function aggregate(metrics: ParsedMetric[], def: CardDef): number {
  const hits = metrics.filter((m) => m.name === def.key);
  if (!hits.length) return 0;
  const vals = hits.map((m) => m.value);
  if (def.agg === 'max') return Math.max(...vals);
  if (def.agg === 'raw') return vals[0];
  return vals.reduce((a, b) => a + b, 0);
}

function drawPath(vals: number[]): string {
  if (!vals.length) return '';
  const w = 120, h = 36;
  const min = Math.min(...vals), max = Math.max(...vals);
  const span = max - min || 1;
  return vals.map((v, i) =>
    `${i === 0 ? 'M' : 'L'}${(i / (vals.length - 1)) * w},${h - ((v - min) / span) * (h - 4) - 2}`).join(' ');
}

export default function MetricsPage() {
  const [err, setErr] = useState<string | null>(null);
  // 面板数据源路径:每卡保留末 HISTORY 点画 sparkline(5s 轮询追加)
  const [hist, setHist] = useState<Record<string, number[]>>({});

  const tick = async () => {
    try {
      const metrics = await fetchMetrics();
      setHist((prev) => {
        const next: Record<string, number[]> = { ...prev };
        for (const c of CARDS) {
          const v = aggregate(metrics, c);
          next[c.key] = [...(prev[c.key] ?? []), v].slice(-HISTORY);
        }
        return next;
      });
      setErr(null);
    } catch (e) { setErr(String(e)); }
  };

  useEffect(() => { tick(); }, []);
  useInterval(tick, 5000);

  return (
    <div>
      <h2>指标面板</h2>
      {err && <p style={{ color: 'red' }}>{err}</p>}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px,1fr))', gap: 12 }}>
        {CARDS.map((c) => {
          const vals = hist[c.key] ?? [];
          const last = vals.length ? vals[vals.length - 1] : NaN;
          return (
            <div key={c.key} style={{ border: '1px solid #ccc', borderRadius: 6, padding: 10 }}>
              <div style={{ fontWeight: 600 }}>{c.label}</div>
              <div style={{ fontSize: 22 }}>{Number.isNaN(last) ? '—' : c.nice ? c.nice(last) : last}</div>
              <svg width={120} height={36}>
                <polyline points={drawPath(vals)} fill="none" stroke="currentColor" strokeWidth={1.5} />
              </svg>
            </div>
          );
        })}
      </div>
    </div>
  );
}