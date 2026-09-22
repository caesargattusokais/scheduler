import { useEffect, useMemo, useState } from 'react';
import { cancelRun, createDag, getDag, getRunDetail, listDagRuns, listDags, listTasks, pauseDag, resumeDag, rerunNode, triggerDag } from '../api/client';
import type { CreateDagRequest, Dag, DagDetail, DagRun, NodeDetail, RunDetail, Task } from '../api/types';
import { useInterval } from '../lib/useInterval';
import Pager from '../components/Pager';

const TERMINAL = new Set(['SUCCESS', 'FAILED', 'CANCELED', 'SKIPPED']);
const RUNS_PAGE = 10;
const WF_PAGE = 10;

/** 状态 → 中文人话。给"看不懂英文状态"的用户。 */
const ZH_STATUS: Record<string, string> = {
  PENDING: '等待上游',
  DUE: '排队中',
  RUNNING: '运行中',
  SUCCESS: '成功',
  FAILED: '失败',
  CANCELED: '已取消',
  SKIPPED: '被跳过',
};
const ZH_TRIGGER: Record<string, string> = { manual: '手动触发', scheduled: '定时调度' };
/** 状态 → 徽章色类(复用全局 badge-* 语义色)。 */
const STAT_COLOR: Record<string, string> = {
  PENDING: 'badge-amber', DUE: 'badge-amber', RUNNING: 'badge-blue',
  SUCCESS: 'badge-green', FAILED: 'badge-red', CANCELED: 'badge-slate', SKIPPED: 'badge-slate',
};
const fmt = (iso: string | null) => (iso ? new Date(iso).toLocaleString() : '—');

/** 中文状态徽章:传入英文状态,显示中文、按英文取对语义色(避免共享 StatusBadge 不认识中文而全灰)。 */
function Chip({ status }: { status: string }) {
  return <span className={`badge ${STAT_COLOR[status] ?? 'badge-slate'}`}>{ZH_STATUS[status] ?? status}</span>;
}

/** 依赖图拓扑排序:入度 0 的节点先行 → 得到"先执行谁"的顺序(无环由后端 DFS 保证)。 */
function topoSort(keys: string[], edges: [string, string][]): string[] {
  const indeg = new Map<string, number>();
  const adj = new Map<string, string[]>();
  for (const k of keys) { indeg.set(k, 0); adj.set(k, []); }
  for (const [f, t] of edges) {
    if (!indeg.has(f) || !indeg.has(t)) continue;
    adj.get(f)!.push(t);
    indeg.set(t, (indeg.get(t) ?? 0) + 1);
  }
  const q = keys.filter((k) => (indeg.get(k) ?? 0) === 0);
  const out: string[] = [];
  while (q.length) {
    const k = q.shift()!;
    out.push(k);
    for (const m of adj.get(k)!) {
      const d = (indeg.get(m) ?? 0) - 1;
      indeg.set(m, d);
      if (d === 0) q.push(m);
    }
  }
  for (const k of keys) if (!out.includes(k)) out.push(k); // 兜底(正常不会发生)
  return out;
}

export default function DagsPage() {
  const [err, setErr] = useState<string | null>(null);
  const [dags, setDags] = useState<Dag[]>([]);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [dagDetail, setDagDetail] = useState<DagDetail | null>(null);
  const [selected, setSelected] = useState<number | null>(null);
  const [runs, setRuns] = useState<DagRun[]>([]);
  const [runsTotal, setRunsTotal] = useState(0);
  const [runsOffset, setRunsOffset] = useState(0);
  const [wfTotal, setWfTotal] = useState(0);
  const [wfOffset, setWfOffset] = useState(0);
  const [detail, setDetail] = useState<RunDetail | null>(null);
  /** 每个工作流的聚合信息:定义里的步骤数 + 最近一批次(列表卡片用;NDAG 不多,按需取) */
  const [wInfo, setWInfo] = useState<Record<number, { steps: number; last?: { id: number; status: string } }>>({});

  // 新建工作流编辑器
  const [builder, setBuilder] = useState(false);
  const [bName, setBName] = useState('');
  const [bDesc, setBDesc] = useState('');
  const [bCron, setBCron] = useState('0 */5 * * * *');
  const [bSteps, setBSteps] = useState<{ nodeKey: string; taskId: number | null; maxRetries: number; backoffMs: number; runIf: string }[]>([]);
  /** deps[i] = 第 i 步的上游步骤下标(只允许 j < i) */
  const [bDeps, setBDeps] = useState<number[][]>([]);
  const [bErr, setBErr] = useState<string | null>(null);
  const [bBusy, setBBusy] = useState(false);

  const taskById = useMemo(() => new Map(tasks.map((t) => [t.id, t])), [tasks]);

  const openBuilder = () => {
    setBName(''); setBDesc(''); setBCron('0 */5 * * * *'); setBErr(null); setBBusy(false);
    setBSteps([{ nodeKey: 'step1', taskId: null, maxRetries: 0, backoffMs: 5000, runIf: 'all_success' }, { nodeKey: 'step2', taskId: null, maxRetries: 0, backoffMs: 5000, runIf: 'all_success' }]);
    setBDeps([[], [0]]); // 第 2 步默认依赖第 1 步 = 线性
    setBuilder(true); // 关键:打开弹窗(此前漏掉,导致点了无反应)
  };
  const keyOf = (i: number) => bSteps[i].nodeKey.trim() || `step${i + 1}`;

  const addStep = () => {
    setBSteps((p) => [...p, { nodeKey: `step${p.length + 1}`, taskId: null, maxRetries: 0, backoffMs: 5000, runIf: 'all_success' }]);
    setBDeps((p) => [...p, []]); // 新步默认无上游,由用户勾选
  };
  const removeStep = (i: number) => {
    setBSteps((p) => p.filter((_, x) => x !== i));
    setBDeps((p) => {
      const next: number[][] = [];
      for (let k = 0; k < p.length; k++) {
        if (k === i) continue; // 删掉自身
        const mk = k > i ? k - 1 : k;
        next[mk] = (p[k] ?? [])
          .filter((u) => u !== i)                       // 去掉对被删步的依赖
          .map((u) => (u > i ? u - 1 : u))               // 之后的下标前移
          .filter((u) => u < mk);                        // 防自环/越界
      }
      return next;
    });
  };
  const toggleDep = (i: number, j: number) => {
    setBDeps((p) => (p[i] ?? []).includes(j)
      ? p.map((arr, k) => (k === i ? arr.filter((u) => u !== j) : arr))
      : p.map((arr, k) => (k === i ? [...arr, j].filter((u) => u < i) : arr)));
  };
  const onSaveDag = async () => {
    setBErr(null); setBBusy(true);
    try {
      if (!bName.trim()) throw new Error('请填写工作流名称');
      if (!bCron.trim()) throw new Error('请填写 cron 调度');
      if (bSteps.length === 0) throw new Error('至少需要一个步骤');
      if (bSteps.some((s) => s.taskId == null)) throw new Error('每个步骤都要选一个任务');
      const nodes = bSteps.map((s, i) => ({ nodeKey: s.nodeKey.trim() || `step${i + 1}`, taskId: s.taskId!, sortOrder: i + 1, nodeMaxRetries: s.maxRetries, nodeBackoffMs: s.backoffMs, runIf: s.runIf }));
      const nodeKeys = new Set(nodes.map((n) => n.nodeKey));
      if (nodeKeys.size !== nodes.length) throw new Error('步骤名重复,请改名');
      const edges = [];
      for (let i = 0; i < bSteps.length; i++) for (const j of bDeps[i] ?? []) {
        edges.push({ from: keyOf(j), to: keyOf(i) });
      }
      const req: CreateDagRequest = { name: bName.trim(), description: bDesc.trim() || null, cron: bCron.trim(), nodes, edges };
      const created = await createDag(req);
      setBuilder(false);
      setWfOffset(0);
      await loadDags(0); // 回到第一页刷新,新工作流通常排在后部,选中后可翻页定位
      setSelected(created.id);
    } catch (e) { setBErr(String(e instanceof Error ? e.message : e)); }
    finally { setBBusy(false); }
  };

  const loadDags = (offset: number) => listDags({ limit: WF_PAGE, offset }).then((p) => {
    setDags(p.items);
    setWfTotal(p.total);
    setSelected((cur) => cur ?? (p.items[0]?.id ?? null)); // 进页自动选中第一个 DAG,不再白屏
  }).catch((e: unknown) => setErr(String(e)));
  /** 列表「启停」:暂停/恢复工作流(pause/resume 只切 paused,enabled 恒为创建时值)。 */
  const toggleDagPause = (d: Dag) =>
    (d.paused ? resumeDag(d.id) : pauseDag(d.id))
      .then(() => loadDags(wfOffset))
      .catch((e: unknown) => setErr(String(e)));
  const loadRuns = (dagId: number, offset: number) =>
    listDagRuns({ dagId, limit: RUNS_PAGE, offset }).then((p) => { setRuns(p.items); setRunsTotal(p.total); })
      .catch((e: unknown) => setErr(String(e)));
  const loadDetail = (runId: number) => getRunDetail(runId).then(setDetail).catch((e: unknown) => setErr(String(e)));

  useEffect(() => { loadDags(wfOffset); listTasks({ limit: 100 }).then((p) => setTasks(p.items)).catch(() => {}); }, []);
  // 选中 DAG 时取它的定义(节点 + 依赖边),用来画流程图
  useEffect(() => {
    if (selected == null) return;
    getDag(selected).then(setDagDetail).catch((e: unknown) => setErr(String(e)));
  }, [selected]);
  useEffect(() => { if (selected != null) loadRuns(selected, runsOffset); }, [selected, runsOffset]);
  useInterval(() => loadDags(wfOffset), 5000);
  useInterval(() => { if (selected != null) loadRuns(selected, runsOffset); }, 5000);
  useInterval(() => { const runId = detail?.run?.id; if (runId != null) loadDetail(runId); }, 5000);

  // 工作流集合变化时,为每个工作流取步骤数 + 最近批次(填充列表卡片)
  const dagIdSet = useMemo(() => dags.map((d) => d.id).sort((a, b) => a - b).join(','), [dags]);
  // 每 10s 只刷新"最近批次"(列表卡片),比 5s 温柔些,避免 N 次请求狂刷
  useInterval(() => {
    for (const id of dags.map((d) => d.id)) {
      listDagRuns({ dagId: id, limit: 1 }).then((page) => {
        const last = page.items[0];
        if (last) setWInfo((p) => ({ ...p, [id]: { ...p[id], last: { id: last.id, status: last.status } } }));
      }).catch(() => {});
    }
  }, 10000);
  useEffect(() => {
    if (!dagIdSet) return;
    let alive = true;
    setWInfo((p) => { const clear = { ...p }; for (const d of dags) delete clear[d.id]; return clear; });
    for (const d of dags) {
      getDag(d.id).then((dg) => { if (alive) setWInfo((p) => ({ ...p, [d.id]: { ...p[d.id], steps: dg.nodes.length } })); }).catch(() => {});
      listDagRuns({ dagId: d.id, limit: 1 }).then((page) => {
        if (!alive) return;
        const last = page.items[0];
        setWInfo((p) => ({ ...p, [d.id]: { ...p[d.id], ...(last ? { last: { id: last.id, status: last.status } } : {}) } }));
      }).catch(() => {});
    }
    return () => { alive = false; };
  }, [dagIdSet]);

  async function onRerun(nodeId: number) {
    const runId = detail?.run?.id;
    if (runId == null) return;
    try { await rerunNode(runId, nodeId); await loadDetail(runId); } catch (e) { setErr(String(e)); }
  }
  async function onCancel() {
    const runId = detail?.run?.id;
    if (runId == null || selected == null) return;
    try { setDetail(await cancelRun(runId)); await loadRuns(selected, runsOffset); } catch (e) { setErr(String(e)); }
  }
  async function onTrigger() {
    if (selected == null) return;
    try {
      const run = await triggerDag(selected);
      await loadRuns(selected, runsOffset);
      await loadDetail(run.id); // 触发后自动打开最新批次,立刻能看到编排过程
    } catch (e) { setErr(String(e)); }
  }

  // ---- 流程图数据:按拓扑序摆放本批次节点 ----
  const nodeKeyById = useMemo(() => {
    const m = new Map<number, string>();
    for (const n of dagDetail?.nodes ?? []) m.set(n.id, n.nodeKey);
    return m;
  }, [dagDetail]);
  const edgePairs = useMemo(
    () => (dagDetail?.edges ?? []).map((e) => [nodeKeyById.get(e.fromNodeId), nodeKeyById.get(e.toNodeId)] as [string, string])
      .filter((e): e is [string, string] => Boolean(e[0] && e[1])),
    [dagDetail, nodeKeyById],
  );
  const orderedNodes = useMemo(() => {
    const nodes = detail?.nodes ?? [];
    const order = topoSort(nodes.map((n) => n.node.nodeKey), edgePairs);
    const byKey = new Map(nodes.map((n) => [n.node.nodeKey, n]));
    return order.map((k) => byKey.get(k)).filter((n): n is NodeDetail => Boolean(n));
  }, [detail, edgePairs]);

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">工作流 (DAG)</h1>
          <p className="page-sub">一个批次 = 一次运行本工作流;节点按依赖顺序依次执行,前一步成功才轮到下一步</p>
        </div>
        <div className="flex items-center gap-2">
          <button className="btn-primary" disabled={selected == null} onClick={onTrigger}>手动触发一次</button>
          <button className="btn-secondary" onClick={openBuilder}>新建工作流</button>
        </div>
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      {/* 工作流列表:一眼看清每个工作流是干嘛的 */}
      <div className="table-wrap mb-4">
        <div className="flex items-center justify-between border-b border-slate-200 bg-slate-50 px-4 py-2">
          <span className="text-sm font-semibold text-slate-700">工作流列表</span>
          <span className="text-xs text-slate-400">{dags.length} 个 — 点一行查看它的批次与流程</span>
        </div>
        <table className="table">
          <thead>
            <tr>
              <th>名称</th><th>描述</th><th>调度 cron</th><th>启停</th><th className="num">步骤</th><th>最近一批次</th>
            </tr>
          </thead>
          <tbody>
            {dags.map((d) => {
              const info = wInfo[d.id];
              const live = d.enabled && !d.paused;
              return (
                <tr
                  key={d.id}
                  onClick={() => { setSelected(d.id); setDetail(null); setRunsOffset(0); }}
                  className={`cursor-pointer transition-colors hover:bg-slate-50 ${selected === d.id ? 'bg-indigo-50/60' : ''}`}
                >
                  <td>
                    <div className="font-medium text-slate-900">{d.name}</div>
                    <div className="text-xs text-slate-400">id={d.id}</div>
                  </td>
                  <td className="text-xs text-slate-500 max-w-xs">{d.description || '—'}</td>
                  <td><code className="rounded bg-slate-100 px-1 font-mono text-xs">{d.cron}</code></td>
                  <td>
                    <div className="flex items-center gap-2">
                      <span className={`badge ${live ? 'badge-green' : d.paused ? 'badge-amber' : 'badge-slate'}`}>
                        {live ? '启用' : d.paused ? '已暂停' : '已停用'}
                      </span>
                      {d.enabled && (
                        <button className="btn-ghost" onClick={(e) => { e.stopPropagation(); toggleDagPause(d); }}>
                          {d.paused ? '启用' : '暂停'}
                        </button>
                      )}
                    </div>
                  </td>
                  <td className="num text-slate-700">{info?.steps ?? '…'}</td>
                  <td>
                    {info?.last
                      ? <span className="inline-flex items-center gap-1.5 text-xs"><span className="font-mono text-slate-500">#{info.last.id}</span><Chip status={info.last.status} /></span>
                      : <span className="text-xs text-slate-300">暂无批次</span>}
                  </td>
                </tr>
              );
            })}
            {dags.length === 0 && <tr><td colSpan={6} className="py-8 text-center text-slate-400">还没有工作流 —— 点右上「新建工作流」建一个</td></tr>}
          </tbody>
        </table>
        <Pager total={wfTotal} offset={wfOffset} limit={WF_PAGE} onPage={setWfOffset} />
      </div>

      {/* 批次列表 */}
      <div className="table-wrap">
        <table className="table">
          <thead>
            <tr><th className="num">批次号</th><th>触发方式</th><th>状态</th><th>发起时间</th><th className="text-right">操作</th></tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.id} className={detail?.run?.id === r.id ? 'bg-slate-50' : ''}>
                <td className="num font-medium text-slate-900">#{r.id}</td>
                <td className="text-xs text-slate-500">{ZH_TRIGGER[r.triggerReason] ?? r.triggerReason}</td>
                <td><Chip status={r.status} /></td>
                <td className="text-xs text-slate-500">{fmt(r.createdAt)}</td>
                <td className="text-right whitespace-nowrap">
                  <div className="flex justify-end gap-1">
                    <button className="btn-secondary" onClick={() => loadDetail(r.id)}>看流程</button>
                    {detail?.run?.id === r.id && <button className="btn-danger" onClick={onCancel}>终止</button>}
                  </div>
                </td>
              </tr>
            ))}
            {runs.length === 0 && (
              <tr><td colSpan={5} className="py-8 text-center text-slate-400">还没有运行批次 —— 选一个工作流并点「手动触发一次」</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {selected != null && (
        <Pager total={runsTotal} offset={runsOffset} limit={RUNS_PAGE} onPage={setRunsOffset} />
      )}

      {/* 选中批次的流程图(单个连贯视图,自解释) */}
      {detail && (
        <div className="card mt-5">
          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 px-4 py-3">
            <div className="flex items-center gap-2 text-sm font-semibold text-slate-900">
              批次 #{detail.run.id}
              <Chip status={detail.status} />
            </div>
            <div className="flex items-center gap-2 text-xs text-slate-500">
              <span>{ZH_TRIGGER[detail.run.triggerReason] ?? detail.run.triggerReason}</span>
              <span>·</span>
              <span>{orderedNodes.length} 个步骤</span>
              <span>·</span>
              <span>发起 {fmt(detail.run.createdAt)}</span>
              {selected != null && <button className="btn-danger" onClick={onCancel}>终止</button>}
            </div>
          </div>

          <div className="px-4 py-4">
            {orderedNodes.map((n, i) => {
              const task = taskById.get(n.node.taskId);
              const prev = orderedNodes[i - 1];
              const thereIsEdge = prev && edgePairs.some(([f, t]) => f === prev.node.nodeKey && t === n.node.nodeKey);
              return (
                <div key={n.node.id}>
                  {i > 0 && (
                    <div className="flex items-center gap-2 py-1 pl-1 text-xs text-slate-400">
                      <span className="text-slate-300">↓</span>
                      {thereIsEdge ? '上一节点成功后，本节点才执行' : '并行 / 等待其余上游'}
                    </div>
                  )}
                  <div className="flex items-start justify-between rounded-lg border border-slate-200 bg-white p-3">
                    <div className="min-w-0">
                      {/* 首行:第几步 + 节点名 + 状态徽章 */}
                      <div className="flex items-center gap-2">
                        <span className="text-xs font-semibold text-slate-400">第 {i + 1} 步</span>
                        <span className="font-mono font-semibold text-slate-900">{n.node.nodeKey}</span>
                        <Chip status={n.node.status} />
                      </div>
                      {/* 详情:每项一行,读起来不挤 */}
                      <dl className="mt-1.5 space-y-0.5 text-xs text-slate-500">
                        <div><span className="text-slate-400">任务</span> {task?.name ?? `任务 #${n.node.taskId}`}</div>
                        <div><span className="text-slate-400">调度器</span> handler={task?.handlerRef ?? '—'} · {task?.shardCount ?? '—'} 分片</div>
                        <div><span className="text-slate-400">执行批次</span> exec #{n.node.executionId ?? '—'}{n.node.attempt > 0 && <span className="ml-1 text-indigo-500">· 重试 #{n.node.attempt}</span>}</div>
                        {n.node.detail && <div><span className="text-slate-400">结果</span> {n.node.detail}</div>}
                        <div><span className="text-slate-400">结束</span> {n.node.finishedAt ? fmt(n.node.finishedAt) : '—'}</div>
                      </dl>
                    </div>
                    <div className="ml-3 flex shrink-0 items-center gap-2">
                      {TERMINAL.has(n.node.status) && (
                        <button className="btn-secondary" onClick={() => onRerun(n.node.id)}>重跑此节点</button>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
            {orderedNodes.length === 0 && <div className="py-8 text-center text-sm text-slate-400">该批次暂无步骤</div>}
          </div>

          {/* 状态图例:徽章(中文) + 这句状态真正意味着什么 */}
          <div className="flex flex-wrap items-center gap-x-5 gap-y-2 border-t border-slate-100 px-4 py-3 text-xs text-slate-500">
            {([
              ['PENDING', '前面还有步骤没跑完，等它们成功'],
              ['RUNNING', '正在执行'],
              ['SUCCESS', '已跑完，结果正常'],
              ['FAILED', '出错了'],
              ['SKIPPED', '上游失败了，这一步不再执行'],
              ['CANCELED', '被手动终止'],
            ] as [string, string][]).map(([s, def]) => (
              <span key={s} className="inline-flex items-center gap-1.5">
                <Chip status={s} /><span>{def}</span>
              </span>
            ))}
          </div>
        </div>
      )}

      {/* 新建工作流编辑器 */}
      {builder && (
        <div
          className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-slate-900/40 p-6"
          onClick={() => !bBusy && setBuilder(false)}
        >
          <div className="w-full max-w-2xl rounded-xl bg-white shadow-xl" onClick={(e) => e.stopPropagation()}>
            <div className="flex items-center justify-between border-b border-slate-200 px-5 py-3">
              <h2 className="text-sm font-semibold text-slate-900">新建工作流</h2>
              <button className="text-slate-400 hover:text-slate-600" onClick={() => setBuilder(false)} disabled={bBusy}>✕</button>
            </div>

            <div className="space-y-4 px-5 py-4">
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                <label className="field">
                  <span className="label">名称 *</span>
                  <input className="input" value={bName} onChange={(e) => setBName(e.target.value)} placeholder="如:每日报表流水线" />
                </label>
                <label className="field">
                  <span className="label">cron 调度 *</span>
                  <input className="input font-mono" value={bCron} onChange={(e) => setBCron(e.target.value)} />
                </label>
              </div>
              <label className="field">
                <span className="label">描述</span>
                <input className="input" value={bDesc} onChange={(e) => setBDesc(e.target.value)} placeholder="这个流水线做什么" />
              </label>
              <p className="text-xs text-slate-400">
                cron 需要 6 段(秒 分 时 日 月 周)。例:{' '}
                <code className="rounded bg-slate-100 px-1">0 */5 * * * *</code> 表示每 5 分钟触发一次。
              </p>

              {/* 步骤 + 依赖 */}
              <div>
                <div className="mb-1 flex items-center justify-between">
                  <span className="label">步骤(每步绑定一个任务)</span>
                  <button className="btn-secondary" onClick={addStep}>+ 加一步</button>
                </div>
                <div className="space-y-2">
                  {bSteps.map((s, i) => (
                    <div key={i} className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                      <div className="flex items-center gap-2">
                        <span className="shrink-0 text-xs font-semibold text-slate-400">第 {i + 1} 步</span>
                        <input
                          className="input w-28 font-mono"
                          value={s.nodeKey}
                          onChange={(e) => setBSteps((p) => p.map((x, k) => (k === i ? { ...x, nodeKey: e.target.value } : x)))}
                          placeholder="节点名"
                        />
                        <select
                          className="input flex-1"
                          value={s.taskId ?? ''}
                          onChange={(e) => setBSteps((p) => p.map((x, k) => (k === i ? { ...x, taskId: e.target.value ? Number(e.target.value) : null } : x)))}
                        >
                          <option value="">选择任务…</option>
                          {tasks.map((t) => <option key={t.id} value={t.id}>{t.name} ({t.handlerRef})</option>)}
                        </select>
                        <button className="btn-danger" onClick={() => removeStep(i)} disabled={bSteps.length <= 1}>删除</button>
                      </div>
                      {/* 1a 节点重试:失败在预算内自动重跑该节点(次数/间隔),0 次=不重试 */}
                      <div className="mt-2 flex items-center gap-3 text-xs text-slate-600">
                        <span className="shrink-0 text-slate-400">节点重试</span>
                        <label className="inline-flex items-center gap-1">
                          <span className="text-slate-400">次数</span>
                          <input type="number" min={0} className="input w-20"
                            value={s.maxRetries}
                            onChange={(e) => setBSteps((p) => p.map((x, k) => (k === i ? { ...x, maxRetries: Math.max(0, Number(e.target.value || 0)) } : x)))} />
                        </label>
                        <label className="inline-flex items-center gap-1">
                          <span className="text-slate-400">间隔 ms</span>
                          <input type="number" min={0} step={100} className="input w-24"
                            value={s.backoffMs}
                            onChange={(e) => setBSteps((p) => p.map((x, k) => (k === i ? { ...x, backoffMs: Math.max(0, Number(e.target.value || 0)) } : x)))} />
                        </label>
                        {/* 1b 边界分支:join 条件 —— all_success=全部上游成功才跑(默认);any_success=任一上游成功即跑(OR-join) */}
                        <label className="inline-flex items-center gap-1">
                          <span className="text-slate-400">满足条件</span>
                          <select className="input w-36" value={s.runIf}
                            onChange={(e) => setBSteps((p) => p.map((x, k) => (k === i ? { ...x, runIf: e.target.value } : x)))}>
                            <option value="all_success">全部成功(默认)</option>
                            <option value="any_success">任一成功</option>
                          </select>
                        </label>
                      </div>
                      {i >= 1 && (
                        <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-600">
                          <span className="text-slate-400">执行本步前,必须先完成:</span>
                          {Array.from({ length: i }, (_, j) => j).map((j) => (
                            <label key={j} className="inline-flex cursor-pointer items-center gap-1">
                              <input type="checkbox" checked={(bDeps[i] ?? []).includes(j)} onChange={() => toggleDep(i, j)} />
                              {keyOf(j)}
                            </label>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>

              {bErr && <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{bErr}</div>}
            </div>

            <div className="flex justify-end gap-2 border-t border-slate-200 px-5 py-3">
              <button className="btn-secondary" onClick={() => setBuilder(false)} disabled={bBusy}>取消</button>
              <button className="btn-primary" onClick={onSaveDag} disabled={bBusy}>{bBusy ? '创建中…' : '保存并创建'}</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}