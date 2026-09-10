import { useEffect, useState } from 'react';
import { cancelRun, getRunDetail, listDagRuns, listDags, rerunNode, triggerDag } from '../api/client';
import type { Dag, DagRun, RunDetail } from '../api/types';
import { useInterval } from '../lib/useInterval';
import StatusBadge from '../components/StatusBadge';

const TERMINAL = new Set(['SUCCESS', 'FAILED', 'CANCELED', 'SKIPPED']);

export default function DagsPage() {
  const [err, setErr] = useState<string | null>(null);
  const [dags, setDags] = useState<Dag[]>([]);
  const [selected, setSelected] = useState<number | null>(null);
  const [runs, setRuns] = useState<DagRun[]>([]);
  const [detail, setDetail] = useState<RunDetail | null>(null);

  const loadDags = () => listDags().then(setDags).catch((e: unknown) => setErr(String(e)));
  const loadRuns = (dagId: number) => listDagRuns(dagId).then(setRuns).catch((e: unknown) => setErr(String(e)));
  const loadDetail = (runId: number) => getRunDetail(runId).then(setDetail).catch((e: unknown) => setErr(String(e)));

  useEffect(() => { loadDags(); }, []);
  useInterval(loadDags, 5000);
  useEffect(() => { if (selected != null) loadRuns(selected); }, [selected]);
  useInterval(() => { if (selected != null) loadRuns(selected); }, 5000);
  useInterval(() => { const runId = detail?.run?.id; if (runId != null) loadDetail(runId); }, 5000);

  async function onRerun(nodeId: number) {
    const runId = detail?.run?.id;
    if (runId == null) return;
    try { await rerunNode(runId, nodeId); await loadDetail(runId); } catch (e) { setErr(String(e)); }
  }
  async function onCancel() {
    const runId = detail?.run?.id;
    if (runId == null || selected == null) return;
    try { setDetail(await cancelRun(runId)); await loadRuns(selected); } catch (e) { setErr(String(e)); }
  }
  async function onTrigger() {
    if (selected == null) return;
    try { await triggerDag(selected); await loadRuns(selected); } catch (e) { setErr(String(e)); }
  }

  const nodes = detail?.nodes ?? [];
  // 选中 run 时为派生 status(detail.status)，否则用列表的 stored status
  const shownStatus = detail ? detail.status : null;

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">工作流 (DAG)</h1>
          <p className="page-sub">批次触发、详情、单节点重跑与批次终止</p>
        </div>
        <div className="flex items-center gap-2">
          <select
            className="input"
            value={selected ?? ''}
            onChange={(e) => { setSelected(e.target.value ? Number(e.target.value) : null); setDetail(null); }}
          >
            <option value="">选择 DAG…</option>
            {dags.map((d) => <option key={d.id} value={d.id}>{d.name} (id={d.id})</option>)}
          </select>
          <button className="btn-primary" disabled={selected == null} onClick={onTrigger}>手动触发</button>
        </div>
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      <div className="table-wrap">
        <table className="table">
          <thead>
            <tr><th className="num">批次号</th><th>状态</th><th className="text-right">操作</th></tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.id}>
                <td className="num font-medium text-slate-900">#{r.id}</td>
                <td><StatusBadge status={(shownStatus !== null && detail?.run?.id === r.id) ? shownStatus : r.status} /></td>
                <td className="text-right whitespace-nowrap">
                  <div className="flex justify-end gap-1">
                    <button className="btn-secondary" onClick={() => loadDetail(r.id)}>详情</button>
                    {detail?.run?.id === r.id && <button className="btn-danger" onClick={onCancel}>终止批次</button>}
                  </div>
                </td>
              </tr>
            ))}
            {runs.length === 0 && (
              <tr><td colSpan={3} className="py-8 text-center text-slate-400">还没有运行批次 —— 选一个 DAG 并触发</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {detail && (
        <div className="card mt-5">
          <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
            <div className="flex items-center gap-2 text-sm font-semibold text-slate-900">
              运行详情 #{detail.run.id}
              <StatusBadge status={detail.status} />
            </div>
            {selected != null && <button className="btn-danger" onClick={onCancel}>终止批次</button>}
          </div>
          <div className="divide-y divide-slate-100">
            {nodes.map((n) => (
              <div key={n.node.id} className="flex items-center justify-between px-4 py-3 hover:bg-slate-50">
                <div className="flex items-center gap-3">
                  <span className="text-sm font-medium text-slate-800">{n.node.nodeKey}</span>
                  <span className="text-xs text-slate-400">exec #{n.node.executionId ?? '—'}</span>
                </div>
                <div className="flex items-center gap-3">
                  <StatusBadge status={n.node.status} />
                  {TERMINAL.has(n.node.status) && (
                    <button className="btn-secondary" onClick={() => onRerun(n.node.id)}>重跑节点</button>
                  )}
                </div>
              </div>
            ))}
            {nodes.length === 0 && <div className="px-4 py-8 text-center text-sm text-slate-400">该批次暂无节点</div>}
          </div>
        </div>
      )}
    </div>
  );
}