/**
 * 状态徽章:把执行/DAG 状态映射为「浅色底 + 深色字」的 pill。
 * 色只用语义:运行中蓝、成功绿、排队/活跃琥珀、失败/死血红、终态灰。
 */
const COLORS: Record<string, string> = {
  RUNNING: 'badge-blue',
  PENDING: 'badge-amber',
  DUE: 'badge-amber',
  SUCCESS: 'badge-green',
  /** 3c:部分成功——达标但存在失败,琥珀(介于成功绿与失败红之间)。 */
  PARTIAL_SUCCESS: 'badge-amber',
  FAILED: 'badge-red',
  CANCELED: 'badge-slate',
  SKIPPED: 'badge-slate',
  ORPHANED: 'badge-violet',
};

export default function StatusBadge({ status }: { status: string }) {
  return <span className={`badge ${COLORS[status] ?? 'badge-slate'}`}>{status}</span>;
}