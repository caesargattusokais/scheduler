// web/src/pages/AlertsPage.tsx —— 治理·告警/健康 landing:渲染后端 AlertEngine 迁移出的 episode
// (status='OPEN' 活动分诊 + status='RESOLVED' 历史页签)。前端不再做阈值/gauge 聚合,只读只渲染。5s 轮询活动列表。
import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import Pager from '../components/Pager';
import { listAlertEpisodes, listAlertHistory } from '../api/client';
import { AlertEpisodeView, Page } from '../api/types';
import { useInterval } from '../lib/useInterval';

/** 规则 id → 人话标题(镜像 NotificationsPage KIND_LABELS 常量映射;未知回落原文)。 */
const RULE_LABELS: Record<string, { text: string; cls: string }> = {
  'audit-tamper': { text: '审计取证链', cls: 'bg-violet-100 text-violet-700' },
  'dlq-depth': { text: 'DLQ 积压', cls: 'bg-orange-100 text-orange-700' },
  'delivery-failed': { text: '通知投递失败', cls: 'bg-red-100 text-red-700' },
  'failure-rate': { text: '执行失败率', cls: 'bg-sky-100 text-sky-700' },
  'p95-latency': { text: '父延迟偏高', cls: 'bg-amber-100 text-amber-700' },
  'worker-offline': { text: 'worker 全部离线', cls: 'bg-rose-100 text-rose-700' },
  'task-failure-rate': { text: '任务失败聚集', cls: 'bg-fuchsia-100 text-fuchsia-700' },
  'dag-run-failed': { text: 'DAG 批次失败', cls: 'bg-emerald-100 text-emerald-700' },
};

/** 规则 id → 下钻所属页(镜像旧 AlertsPage 的 href 映射;未知回落 /alerts)。 */
const RULE_DRILLDOWN: Record<string, string> = {
  'audit-tamper': '/audits',
  'dlq-depth': '/dlq',
  'delivery-failed': '/notifications',
  'failure-rate': '/metrics',
  'p95-latency': '/metrics',
  'worker-offline': '/metrics',
  'task-failure-rate': '/tasks',
  'dag-run-failed': '/dags',
};

/** 三档严重度外观(沿用旧页)。 */
const SEV: Record<AlertEpisodeView['severity'], { dot: string; label: string; labelCls: string }> = {
  critical: { dot: 'bg-red-500', label: '严重', labelCls: 'bg-red-100 text-red-700' },
  high: { dot: 'bg-orange-500', label: '高', labelCls: 'bg-orange-100 text-orange-700' },
  medium: { dot: 'bg-amber-500', label: '中', labelCls: 'bg-amber-100 text-amber-700' },
};

/** 告警 episode 目标定位:task/dag 名优先,回落「全局」。 */
const targetText = (e: AlertEpisodeView) => (e.targetName ?? '全局');

/** 时间串格式化:可空 ISO → toLocaleString;空则「—」。 */
const fmtAt = (s: string | null) => (s ? new Date(s).toLocaleString() : '—');

const HISTORY_LIMIT = 20;

export default function AlertsPage() {
  const [active, setActive] = useState<AlertEpisodeView[]>([]);
  const [history, setHistory] = useState<Page<AlertEpisodeView>>({ items: [], total: 0, offset: 0, limit: HISTORY_LIMIT });
  const [activeErr, setActiveErr] = useState<string | null>(null);
  const [historyErr, setHistoryErr] = useState<string | null>(null);
  const [checkedAt, setCheckedAt] = useState<Date | null>(null);
  const [activeTab, setActiveTab] = useState<'active' | 'history'>('active');

  /** 加载活动 episode(status='OPEN')。失败置 activeErr(仅展示降级,不阻断已渲染列表)。 */
  const loadActive = async () => {
    try {
      setActive(await listAlertEpisodes());
      setActiveErr(null);
    } catch (e) {
      setActiveErr(`读取活动告警失败:${String(e)}`);
    }
    setCheckedAt(new Date());
  };

  /** 加载已解决历史分页(status='RESOLVED')。 */
  const loadHistory = async (offset = 0, limit = HISTORY_LIMIT) => {
    try {
      setHistory(await listAlertHistory({ limit, offset }));
      setHistoryErr(null);
    } catch (e) {
      setHistoryErr(`读取告警历史失败:${String(e)}`);
    }
  };

  useEffect(() => { void loadActive(); void loadHistory(0, HISTORY_LIMIT); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, []);
  useInterval(() => { void loadActive(); }, 5000);

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">告警</h1>
          <p className="page-sub">治理·健康 landing —— 渲染后端 AlertEngine 推出的告警 episode(活动 triage + 已解决历史),逐条可下钻</p>
        </div>
        {checkedAt && <div className="text-xs text-slate-400">检测于 {checkedAt.toLocaleTimeString()}</div>}
      </div>

      {(activeErr || historyErr) && (
        <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{activeErr ?? historyErr}</div>
      )}

      <div className="mb-4 flex items-center gap-2">
        <button className={`rounded px-3 py-1.5 text-sm font-medium ${activeTab === 'active' ? 'bg-indigo-600 text-white' : 'bg-slate-100 text-slate-600'}`}
          onClick={() => setActiveTab('active')}>待处理 ({active.length})</button>
        <button className={`rounded px-3 py-1.5 text-sm font-medium ${activeTab === 'history' ? 'bg-indigo-600 text-white' : 'bg-slate-100 text-slate-600'}`}
          onClick={() => setActiveTab('history')}>历史</button>
      </div>

      {activeTab === 'active' ? (
        <>
          {/* 总览横幅:active 为空 → 全绿;否则「{n} 项需要关注」。
              加载失败时(activeErr 置位)抑制整个横幅——绝不出现「全绿」的假健康信号,仅留红色错误横幅 + 降级空态卡。 */}
          {!activeErr && (active.length === 0 ? (
            <div className="mb-4 flex items-center gap-3 rounded-md border border-emerald-200 bg-emerald-50 px-4 py-3">
              <span className="h-2.5 w-2.5 rounded-full bg-emerald-500" />
              <div className="text-sm text-emerald-700">
                <span className="font-semibold">运行健康 · 全绿</span>
                <span className="ml-2">无待处理告警</span>
              </div>
            </div>
          ) : (
            <div className="mb-4 flex items-center gap-3 rounded-md border border-red-200 bg-red-50 px-4 py-3">
              <span className="h-2.5 w-2.5 animate-pulse rounded-full bg-red-500" />
              <div className="text-sm text-red-700"><span className="font-semibold">{active.length} 项需要关注</span></div>
            </div>
          ))}

          {/* 分诊列表 */}
          {active.length === 0 ? (
            <div className="card p-10 text-center text-sm text-slate-400">
              {activeErr ? '活动告警读取失败,显示空态' : '暂无活动告警,各信号源后端检查正常'}
            </div>
          ) : (
            <div className="card divide-y divide-slate-100">
              {active.map((e) => {
                const s = SEV[e.severity];
                const rule = RULE_LABELS[e.rule] ?? { text: e.rule, cls: 'bg-slate-100 text-slate-600' };
                const href = RULE_DRILLDOWN[e.rule] ?? '/alerts';
                return (
                  <div key={e.id} className="flex items-start gap-3 px-4 py-3">
                    <span className={`mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full ${s.dot}`} />
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="text-sm font-semibold text-slate-800">{rule.text}</span>
                        <span className={`rounded px-1.5 py-0.5 text-xs font-medium ${s.labelCls}`}>{s.label}</span>
                        <span className={`rounded px-1.5 py-0.5 text-xs font-medium ${rule.cls}`}>{e.rule}</span>
                      </div>
                      <div className="mt-0.5 text-xs text-slate-500">
                        目标 <span className="font-medium text-slate-700">{targetText(e)}</span>
                        · 当前值 <span className="font-mono text-slate-700">{e.value}</span>
                        {e.openedAt && <> · 始于 <span className="text-slate-600">{fmtAt(e.openedAt)}</span></>}
                      </div>
                    </div>
                    <Link to={href} className="shrink-0 text-sm font-medium text-indigo-600 hover:underline">处理</Link>
                  </div>
                );
              })}
            </div>
          )}
        </>
      ) : (
        <div className="card p-4">
          <div className="mb-2 flex items-center justify-between">
            <h2 className="text-sm font-semibold text-slate-700">已解决告警历史</h2>
            <button className="btn btn-secondary" onClick={() => loadHistory(history.offset, history.limit)}>刷新</button>
          </div>
          {history.items.length === 0 ? (
            <div className="py-6 text-center text-sm text-slate-400">
              {historyErr ? '告警历史读取失败,显示空态' : '暂无已解决的告警(告警解决后在此沉淀)'}
            </div>
          ) : (
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr><th>规则</th><th>严重度</th><th>目标</th><th>活动值</th><th>始于</th><th>已解决值</th><th>解决于</th><th></th></tr>
                </thead>
                <tbody>
                  {history.items.map((e) => {
                    const s = SEV[e.severity];
                    const rule = RULE_LABELS[e.rule] ?? { text: e.rule, cls: 'bg-slate-100 text-slate-600' };
                    const href = RULE_DRILLDOWN[e.rule] ?? '/alerts';
                    return (
                      <tr key={e.id}>
                        <td><span className={`rounded px-1.5 py-0.5 text-xs font-medium ${rule.cls}`}>{rule.text}</span></td>
                        <td><span className={`rounded px-1.5 py-0.5 text-xs font-medium ${s.labelCls}`}>{s.label}</span></td>
                        <td className="text-sm text-slate-700">{targetText(e)}</td>
                        <td className="font-mono text-xs text-slate-600">{e.openedValue ?? e.value}</td>
                        <td className="whitespace-nowrap text-xs text-slate-500">{fmtAt(e.openedAt)}</td>
                        <td className="font-mono text-xs text-slate-600">{e.resolvedValue ?? '—'}</td>
                        <td className="whitespace-nowrap text-xs text-slate-500">{fmtAt(e.resolvedAt)}</td>
                        <td className="text-right"><Link to={href} className="text-sm font-medium text-indigo-600 hover:underline">处理</Link></td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
          <Pager total={history.total} offset={history.offset} limit={history.limit}
            onPage={(o) => loadHistory(o, history.limit)} />
        </div>
      )}
    </div>
  );
}