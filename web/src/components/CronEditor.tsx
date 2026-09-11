import { buildCron, FIELDS, isSimpleToken, optionsFor, parseCron, tokenError } from '../lib/cron';

const PRESETS: { name: string; cron: string }[] = [
  { name: '每秒', cron: '* * * * * *' },
  { name: '每分钟', cron: '0 * * * * *' },
  { name: '每5分钟', cron: '0 */5 * * * *' },
  { name: '每小时', cron: '0 0 * * * *' },
  { name: '每天00:00', cron: '0 0 0 * * *' },
  { name: '工作日09:00', cron: '0 0 9 * * 1-5' },
  { name: '周一09:00', cron: '0 0 9 * * 1' },
];

const CUSTOM = '__custom__';

export default function CronEditor({ value, onChange }: {
  value: string;
  onChange: (v: string) => void;
}) {
  const toks = parseCron(value);
  const segs = toks ? toks.slice(0, 6) : null;
  const hasYear = toks !== null && toks.length === 7;

  // 段级刷新:改某个下拉即重建 6 字段表达式(丢弃年)。
  function setSeg(i: number, tok: string) {
    if (!segs) return;
    const next = segs.slice();
    next[i] = tok;
    onChange(buildCron(next));
  }

  let status: { kind: 'ok' | 'err'; text: string } = { kind: 'ok', text: '' };
  if (segs === null) {
    status = { kind: 'err', text: '非 6/7 字段表达式,请输入形如「秒 分 时 日 月 周」的 6 段 cron' };
  } else {
    const bad = segs.findIndex((t, i) => tokenError(t, i) !== null);
    if (bad >= 0) {
      status = { kind: 'err', text: `第 ${bad + 1} 段「${FIELDS[bad].everyLabel}」非法:${tokenError(segs[bad], bad)}` };
    } else if (hasYear) {
      status = { kind: 'ok', text: '7 字段(带年)可识别;可视化编辑会重置为 6 字段' };
    } else {
      status = { kind: 'ok', text: '6 字段合法(秒 分 时 日 月 周)' };
    }
  }

  return (
    <div className="flex flex-col gap-2">
      {/* 常用预设 */}
      <div className="flex flex-wrap gap-1">
        {PRESETS.map((p) => (
          <button key={p.cron} type="button"
            className="rounded border border-slate-200 px-1.5 py-0.5 text-xs text-slate-500 hover:border-indigo-300 hover:text-indigo-600"
            onClick={() => onChange(p.cron)}>
            {p.name}
          </button>
        ))}
      </div>

      {/* 6 段下拉 */}
      <div className="grid grid-cols-3 gap-2 sm:grid-cols-6">
        {FIELDS.map((f, i) => {
          const raw = segs ? segs[i] : '';
          const simple = segs !== null && isSimpleToken(raw);
          const opts = optionsFor(i);
          const known = simple && opts.some((o) => o.value === raw);
          return (
            <label key={f.key} className="field">
              <span className="label">{f.everyLabel}</span>
              <select
                className="input"
                disabled={!segs}
                value={known ? raw : CUSTOM}
                onChange={(e) => { if (e.target.value !== CUSTOM) setSeg(i, e.target.value); }}
                data-testid={`cron-seg-${f.key}`}
              >
                {opts.map((o) => (
                  <option key={o.value} value={o.value}>{o.label}</option>
                ))}
                <option value={CUSTOM}>{segs && !simple ? `自定义 ${raw}` : '自定义…'}</option>
              </select>
            </label>
          );
        })}
      </div>

      {/* 原样文本(高级表达式:区间/列表 直接用文本改) */}
      <input className="input font-mono text-xs" value={value} spellCheck={false}
        onChange={(e) => onChange(e.target.value)} data-testid="cron-raw"
        placeholder="秒 分 时 日 月 周,如 0 */5 * * * *" />

      <p className={status.kind === 'ok' ? 'text-xs text-emerald-600' : 'text-xs text-red-600'}
        data-testid="cron-status">
        {status.text}
      </p>
    </div>
  );
}