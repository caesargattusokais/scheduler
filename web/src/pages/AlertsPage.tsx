// web/src/pages/AlertsPage.tsx —— 治理·告警/健康 landing:把散在各页的"不健康"信号汇总为分级 triage 列表,
// 逐条可下钻。纯聚合,复用既有只读端点,零后端改动。5s 轮询。
import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { fetchMetrics, getAuditIntegrity, getDlq, listNotifications } from '../api/client';
import { AuditIntegrity, DlqRow, ParsedMetric } from '../api/types';
import { useInterval } from '../lib/useInterval';

/** 告警项:severity 决定优先级与配色;href 为下钻到所属页的路径。 */
interface Alert {
  id: string;
  severity: 'critical' | 'high' | 'medium';
  title: string;
  detail: string;
  href: string;
}

/** 触发阈值(运维经验值,常量即可;需要可配时再走运行时热键)。 */
const FAILURE_RATE_THRESHOLD = 0.05;  // 1h 失败率 > 5%
const P95_THRESHOLD_MS = 5000;        // 父执行 p95 延迟 > 5s

/** 三档严重度外观。 */
const SEV: Record<Alert['severity'], { dot: string; label: string; labelCls: string }> = {
  critical: { dot: 'bg-red-500', label: '严重', labelCls: 'bg-red-100 text-red-700' },
  high: { dot: 'bg-orange-500', label: '高', labelCls: 'bg-orange-100 text-orange-700' },
  medium: { dot: 'bg-amber-500', label: '中', labelCls: 'bg-amber-100 text-amber-700' },
};
const RANK: Record<Alert['severity'], number> = { critical: 3, high: 2, medium: 1 };

/** prometheus gauge 单值:同名系列取最大值(计数类已跨 series 和的在 MetricsPage;这里只读单值 gauge)。 */
function gauge(metrics: ParsedMetric[], name: string): number {
  const vals = metrics.filter((m) => m.name === name).map((m) => m.value);
  return vals.length ? Math.max(...vals) : 0;
}

const fmtMs = (v: number) => (v >= 1000 ? `${(v / 1000).toFixed(2)}s` : `${Math.round(v)}ms`);
const fmtPct = (v: number) => `${(v * 100).toFixed(1)}%`;

export default function AlertsPage() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [unavailable, setUnavailable] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [checkedAt, setCheckedAt] = useState<Date | null>(null);

  /** 每 tick:并行读各数据源(allSettled,单个失败只降级丢失那类告警,不阻断其余);
   *  全部失败才置 unavailable。据此聚合分层告警列表(按严重度降序)。 */
  const tick = async () => {
    const [iRes, dRes, nRes, mRes] = await Promise.allSettled([
      getAuditIntegrity(),
      getDlq({ limit: 1 }),
      listNotifications({ status: 'FAILED', limit: 1 }),
      fetchMetrics(),
    ]);
    const a: Alert[] = [];
    let anyOk = false;
    anyOk = anyOk || iRes.status === 'fulfilled';
    anyOk = anyOk || dRes.status === 'fulfilled';
    anyOk = anyOk || nRes.status === 'fulfilled';
    anyOk = anyOk || mRes.status === 'fulfilled';

    if (iRes.status === 'fulfilled') {
      const integrity = iRes.value as AuditIntegrity;
      if (!integrity.verified) {
        a.push({
          id: 'integrity', severity: 'critical', title: '审计取证链异常',
          detail: `${integrity.chainedRecords}/${integrity.totalRecords} 行已入链`
            + (integrity.firstTamperedId != null ? ` · 首个异常行 ${integrity.firstTamperedId}` : ''),
          href: '/audits',
        });
      }
    }
    if (dRes.status === 'fulfilled') {
      const dlqTotal = (dRes.value as { total: number }).total;
      if (dlqTotal > 0) a.push({ id: 'dlq', severity: 'high', title: 'DLQ 有积压', detail: `${dlqTotal} 条分片待治理`, href: '/dlq' });
    }
    if (nRes.status === 'fulfilled') {
      const failedTotal = (nRes.value as { total: number }).total;
      if (failedTotal > 0) a.push({ id: 'delivery', severity: 'high', title: '通知投递失败', detail: `${failedTotal} 条到达失败(重试已耗尽)`, href: '/notifications' });
    }
    if (mRes.status === 'fulfilled') {
      const metrics = mRes.value as ParsedMetric[];
      const rate = gauge(metrics, 'scheduler_execution_failure_rate_1h');
      if (rate > FAILURE_RATE_THRESHOLD) a.push({ id: 'failure-rate', severity: 'medium', title: '执行失败率偏高', detail: `1h 失败率 ${fmtPct(rate)}`, href: '/metrics' });
      const p95 = gauge(metrics, 'scheduler_execution_latency_p95_ms');
      if (p95 > P95_THRESHOLD_MS) a.push({ id: 'p95', severity: 'medium', title: '父执行延迟偏高', detail: `p95 ${fmtMs(p95)}`, href: '/metrics' });
      const workers = gauge(metrics, 'scheduler_worker_active');
      if (workers === 0) a.push({ id: 'workers', severity: 'medium', title: '无存活 worker', detail: '调度无法执行任务', href: '/metrics' });
    }

    a.sort((x, y) => RANK[y.severity] - RANK[x.severity]);
    setAlerts(a);
    setUnavailable(!anyOk);
    setErr(anyOk ? null : '无法读取任何告警数据源(后端/指标端点不可达)');
    setCheckedAt(new Date());
  };

  useEffect(() => { void tick(); }, []);
  useInterval(tick, 5000);

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">告警</h1>
          <p className="page-sub">治理·健康 landing —— 聚合各页不健康信号(DLQ/投递失败/失败率/延迟/worker/取证链)为分级 triage,可下钻</p>
        </div>
        {checkedAt && <div className="text-xs text-slate-400">检测于 {checkedAt.toLocaleTimeString()}</div>}
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      {/* 总览横幅 */}
      {alerts.length === 0 ? (
        <div className="mb-4 flex items-center gap-3 rounded-md border border-emerald-200 bg-emerald-50 px-4 py-3">
          <span className="h-2.5 w-2.5 rounded-full bg-emerald-500" />
          <div className="text-sm text-emerald-700">
            <span className="font-semibold">运行健康 · 全绿</span>
            <span className="ml-2">{unavailable ? '但指标源不可达,仅显示 DB 侧信号' : '无待处理告警'}</span>
          </div>
        </div>
      ) : (
        <div className="mb-4 flex items-center gap-3 rounded-md border border-red-200 bg-red-50 px-4 py-3">
          <span className="h-2.5 w-2.5 animate-pulse rounded-full bg-red-500" />
          <div className="text-sm text-red-700"><span className="font-semibold">{alerts.length} 项需要关注</span></div>
        </div>
      )}

      {alerts.length === 0 && !unavailable ? (
        <div className="card p-10 text-center text-sm text-slate-400">暂无告警,各信号源全部正常</div>
      ) : (
        <div className="card divide-y divide-slate-100">
          {alerts.map((al) => {
            const s = SEV[al.severity];
            return (
              <div key={al.id} className="flex items-start gap-3 px-4 py-3">
                <span className={`mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full ${s.dot}`} />
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-semibold text-slate-800">{al.title}</span>
                    <span className={`rounded px-1.5 py-0.5 text-xs font-medium ${s.labelCls}`}>{s.label}</span>
                  </div>
                  <div className="mt-0.5 text-xs text-slate-500">{al.detail}</div>
                </div>
                <Link to={al.href} className="shrink-0 text-sm font-medium text-indigo-600 hover:underline">处理</Link>
              </div>
            );
          })}
          {alerts.length === 0 && (
            <div className="px-4 py-3 text-xs text-slate-400">指标源不可达,仅 DB 侧检查无异常(取证链/DLQ/投递)。</div>
          )}
        </div>
      )}
    </div>
  );
}