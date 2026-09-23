import { useEffect, useState } from 'react';
import { fetchMetrics, getExecutionSlo } from '../api/client';
import type { ExecutionSlo, ParsedMetric } from '../api/types';
import { useInterval } from '../lib/useInterval';

/** 每卡聚合规则:计数类取系列和(DLQ/活跃),最老队龄取最大值。 */
interface CardDef { key: string; label: string; unit?: string; agg: 'sum' | 'max' | 'raw'; nice?: (v: number) => string; }

const CARDS: CardDef[] = [
  { key: 'scheduler_active_runs', label: '活跃执行', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_due_queue_max_age_seconds', label: 'DUE 最深队龄', unit: 's', agg: 'max', nice: (v) => `${v.toFixed(0)}s` },
  { key: 'scheduler_dag_runs_active', label: '活跃 DAG 批次', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_dlq_depth', label: 'DLQ 深度', agg: 'raw', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_worker_active', label: '存活 worker', agg: 'raw', nice: (v) => `${Math.round(v)} 个` },
  { key: 'scheduler_execution_completed_1h', label: '1h 完成', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_execution_failed_1h', label: '1h 失败', agg: 'sum', nice: (v) => String(Math.round(v)) },
  { key: 'scheduler_execution_failure_rate_1h', label: '失败率', agg: 'raw', nice: (v) => `${(v * 100).toFixed(1)}%` },
  { key: 'scheduler_execution_latency_p95_ms', label: 'p95 延迟', unit: 'ms', agg: 'raw', nice: (v) => `${v >= 1000 ? (v / 1000).toFixed(2) + 's' : Math.round(v) + 'ms'}` },
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
  const denom = vals.length - 1 || 1; // 单样本时 0/0→NaN x,兜底为 1
  return vals.map((v, i) =>
    `${i === 0 ? 'M' : 'L'}${(i / denom) * w},${h - ((v - min) / span) * (h - 4) - 2}`).join(' ');
}

export default function MetricsPage() {
  const [err, setErr] = useState<string | null>(null);
  // 面板数据源路径:每卡保留末 HISTORY 点画 sparkline(5s 轮询追加)
  const [hist, setHist] = useState<Record<string, number[]>>({});
  // 4-2 执行 SLO 快照(DB 聚合,与 /actuator/prometheus 并行拉取)。
  const [slo, setSlo] = useState<ExecutionSlo | null>(null);

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
    try {
      setSlo(await getExecutionSlo());
    } catch (e) {
      if (err == null) setSlo(null);
    }
  };

  useEffect(() => { tick(); }, []);
  useInterval(tick, 5000);

  const fmtMs = (v: number) => (v >= 1000 ? `${(v / 1000).toFixed(2)}s` : `${Math.round(v)}ms`);

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">指标</h1>
          <p className="page-sub">9 个运维 + SLI gauge · 每 5s 刷新（数据源 /actuator/prometheus）+ 执行 SLO 快照（/api/v1/metrics/executions）</p>
        </div>
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-5">
        {CARDS.map((c) => {
          const vals = hist[c.key] ?? [];
          const last = vals.length ? vals[vals.length - 1] : NaN;
          return (
            <div key={c.key} className="metric-card">
              <div className="text-sm font-medium text-slate-500">{c.label}</div>
              <div className="metric-value">
                {Number.isNaN(last) ? '—' : c.nice ? c.nice(last) : last}
                {!Number.isNaN(last) && c.unit && <span className="metric-unit">{c.unit}</span>}
              </div>
              <div className="mt-2 h-9 text-indigo-500">
                {vals.length > 1 && (
                  <svg width={120} height={36} viewBox="0 0 120 36" preserveAspectRatio="none" className="h-full w-full">
                    <polyline points={drawPath(vals)} fill="none" stroke="currentColor" strokeWidth={1.5} strokeLinejoin="round" strokeLinecap="round" />
                  </svg>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {slo && (
        <div className="mt-6 grid grid-cols-2 gap-4 md:grid-cols-4">
          <div className="metric-card">
            <div className="text-sm font-medium text-slate-500">父延迟 p50 · p95</div>
            <div className="metric-value">{fmtMs(slo.parentLatencyP50Ms)} / {fmtMs(slo.parentLatencyP95Ms)}</div>
            <div className="mt-2 text-xs text-slate-400">近 {slo.windowSeconds / 3600}h 终态父执行(execution)</div>
          </div>
          <div className="metric-card">
            <div className="text-sm font-medium text-slate-500">分片平均时长</div>
            <div className="metric-value">{fmtMs(slo.shardAvgDurationMs)}</div>
            <div className="mt-2 text-xs text-slate-400">近窗分片 finished−started</div>
          </div>
          <div className="metric-card">
            <div className="text-sm font-medium text-slate-500">近窗失败</div>
            <div className="metric-value text-red-600">{slo.recentFailures.failed}</div>
            <div className="mt-2 text-xs text-slate-400">死信 {slo.recentFailures.deadLettered} · 超时 {slo.recentFailures.timedOut}</div>
          </div>
          <div className="metric-card">
            <div className="text-sm font-medium text-slate-500">任务吞吐 / 成功率</div>
            <div className="metric-value">
              {slo.perTask.length === 0 ? '—' : `${slo.perTask.reduce((a, t) => a + t.throughput, 0)} 次`}
            </div>
            <div className="mt-2 text-xs text-slate-400">
              {slo.perTask.length === 0 ? '近窗无终态任务' : slo.perTask.map((t) =>
                `#${t.taskId} ${t.taskName}:${t.throughput}次 ${(t.successRate * 100).toFixed(0)}%`).join(' · ')}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}