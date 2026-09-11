/** 统一列表分页:共 N 条 + 上一页/下一页,从 total 推导页数与边界禁用态。 */
export default function Pager({ total, offset, limit, onPage }: {
  total: number;
  offset: number;
  limit: number;
  onPage: (offset: number) => void;
}) {
  if (total === 0) return null;
  const page = Math.floor(offset / limit) + 1;
  const pages = Math.max(1, Math.ceil(total / limit));
  return (
    <div className="mt-3 flex items-center justify-between text-sm text-slate-500">
      <span>共 {total} 条</span>
      <div className="flex items-center gap-2">
        <button className="btn-secondary" disabled={page <= 1} onClick={() => onPage(offset - limit)}>上一页</button>
        <span>{page} / {pages}</span>
        <button className="btn-secondary" disabled={page >= pages} onClick={() => onPage(offset + limit)}>下一页</button>
      </div>
    </div>
  );
}