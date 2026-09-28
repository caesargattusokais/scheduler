// web/src/components/MultiSelect.tsx —— 下拉多选:点开带复选框的候选面板(含关键词过滤),点外部收起。
import { useEffect, useRef, useState } from 'react';

/** 下拉多选;选项 id 为 number,已选项在闭合态以徽章呈现。由 NotificationsPage(任务/DAG)复用。 */
export default function MultiSelect<T extends string | number>({ label, options, selected, onChange, empty }: {
  label: string;
  options: { id: T; name: string }[];
  selected: T[];
  onChange: (ids: T[]) => void;
  empty: string;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const boxRef = useRef<HTMLDivElement>(null);

  // 点击面板外部 → 收起;切换打开时清空搜索词。
  useEffect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      if (boxRef.current && !boxRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, [open]);

  const q = query.trim().toLowerCase();
  const visible = options.filter((o) => !q || o.name.toLowerCase().includes(q));
  const toggle = (id: T) =>
    onChange(selected.includes(id) ? selected.filter((x) => x !== id) : [...selected, id]);

  return (
    <div className="field grow" ref={boxRef}>
      <span className="label">{label}({selected.length})</span>
      <div className="relative">
        <button type="button" className="input text-left" onClick={() => { setOpen(!open); setQuery(''); }}>
          {selected.length === 0
            ? <span className="text-slate-400">{empty}</span>
            : (
              <span className="flex flex-wrap gap-1">
                {options.filter((o) => selected.includes(o.id)).map((o) => (
                  <span key={o.id} className="rounded bg-blue-100 px-1.5 py-0.5 text-xs font-medium text-blue-700">
                    {o.name}{typeof o.id === 'number' && <span className="ml-0.5 text-blue-400">#{o.id}</span>}
                  </span>
                ))}
              </span>
            )}
          <span className="float-right translate-y-1 text-slate-400">{open ? '▲' : '▼'}</span>
        </button>
        {open && (
          <div className="absolute z-20 mt-1 w-full overflow-hidden rounded-md border border-slate-200 bg-white shadow-lg">
            <input autoFocus className="input rounded-none border-0 border-b border-slate-100" placeholder="过滤..."
              value={query} onChange={(e) => setQuery(e.target.value)} />
            <div className="max-h-56 overflow-y-auto p-1">
              {options.length === 0 && <div className="px-2 py-3 text-center text-sm text-slate-400">{empty}</div>}
              {visible.length === 0 && options.length > 0 && (
                <div className="px-2 py-3 text-center text-sm text-slate-400">无匹配</div>
              )}
              {visible.map((o) => (
                <label key={o.id} className="flex cursor-pointer items-center gap-2 px-2 py-1 text-sm hover:bg-slate-50">
                  <input type="checkbox" checked={selected.includes(o.id)}
                    onChange={() => toggle(o.id)} />
                  <span className="flex-1 truncate">{o.name}</span>
                  {typeof o.id === 'number' && <span className="font-mono text-xs text-slate-400">#{o.id}</span>}
                </label>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}