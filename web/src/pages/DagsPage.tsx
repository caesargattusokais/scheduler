import { useEffect, useState } from 'react';
import { cancelRun, getRunDetail, listDagRuns, listDags, rerunNode, triggerDag } from '../api/client';
import type { Dag, DagRun, RunDetail } from '../api/types';
import { useInterval } from '../lib/useInterval';

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
      <h2>工作流 (DAG)</h2>
      {err && <p style={{ color: 'red' }}>{err}</p>}

      <h3>DAG</h3>
      <select
        value={selected ?? ''}
        onChange={(e) => { setSelected(e.target.value ? Number(e.target.value) : null); setDetail(null); }}
      >
        <option value="">选择 DAG…</option>
        {dags.map((d) => <option key={d.id} value={d.id}>{d.name} (id={d.id})</option>)}
      </select>{' '}
      <button onClick={onTrigger}>手动触发</button>

      <h3>运行批次</h3>
      <ul>
        {runs.map((r) => (
          <li key={r.id}>
            #{r.id} ·{(shownStatus !== null && detail?.run?.id === r.id) ? shownStatus : r.status}{' '}
            <button onClick={() => loadDetail(r.id)}>详情</button>
            {detail?.run?.id === r.id && <button onClick={onCancel}>终止批次</button>}
          </li>
        ))}
      </ul>

      {detail && (
        <div>
          <h3>运行详情 #{detail.run.id}</h3>
          {nodes.map((n) => (
            <div key={n.node.id}>
              <span>{n.node.nodeKey} · {n.node.status}</span>{' '}
              {TERMINAL.has(n.node.status) && <button onClick={() => onRerun(n.node.id)}>重跑节点</button>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}