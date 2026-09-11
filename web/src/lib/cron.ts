// 纯函数:6/7 字段 Spring Cron 表达式的解析/拼接/可视化下拉选项与轻量校验。
// 字段序:秒 分 时 日 月 周(第 7 段=年,仅识别不编辑)。

export interface CronField { key: string; min: number; max: number; unit: string; everyLabel: string; }

export const FIELDS: readonly CronField[] = [
  { key: 'second',  min: 0,  max: 59, unit: '秒', everyLabel: '每秒' },
  { key: 'minute',  min: 0,  max: 59, unit: '分', everyLabel: '每分钟' },
  { key: 'hour',    min: 0,  max: 23, unit: '时', everyLabel: '每小时' },
  { key: 'day',     min: 1,  max: 31, unit: '日', everyLabel: '每日' },
  { key: 'month',   min: 1,  max: 12, unit: '月', everyLabel: '每月' },
  { key: 'weekday', min: 0,  max: 7,  unit: '周', everyLabel: '每周' },
];

// 每字段可视化提供的“每 N 单位”步进;秒/分为主,其余按需精简。
const STEPS: readonly number[][] = [
  [1, 5, 10, 15, 20, 30],
  [1, 5, 10, 15, 20, 30],
  [2, 3, 4, 6, 12],
  [2],
  [],
  [],
];

const WEEKDAY: Record<number, string> =
  { 0: '周日', 1: '周一', 2: '周二', 3: '周三', 4: '周四', 5: '周五', 6: '周六', 7: '周日' };

export interface CronOption { value: string; label: string; }

export function optionsFor(i: number): CronOption[] {
  const f = FIELDS[i];
  const opts: CronOption[] = [{ value: '*', label: f.everyLabel }];
  for (const s of STEPS[i]) {
    if (s >= f.min && s <= f.max) opts.push({ value: `*/${s}`, label: `每${s}${f.unit}` });
  }
  for (let v = f.min; v <= f.max; v++) opts.push({ value: String(v), label: valueLabel(i, v) });
  return opts;
}

function valueLabel(i: number, v: number): string {
  if (i === 4) return `${v}月`;
  if (i === 5) return `${v}·${WEEKDAY[v]}`;
  return String(v);
}

/** 拆分 cron 为 tokens;仅接受 6 或 7 字段。非法返回 null。 */
export function parseCron(value: string): string[] | null {
  const toks = value.trim().split(/\s+/).filter(Boolean);
  if (toks.length < 6 || toks.length > 7) return null;
  return toks;
}

/** 由前 6 段重建 6 字段表达式(丢弃第 7 段年)。 */
export function buildCron(segs: string[]): string {
  return segs.slice(0, 6).join(' ');
}

/** 某 token 是否属于可视化可直选的形式(每种可复用文本,否则回退“自定义”)。 */
export function isSimpleToken(tok: string): boolean {
  return /^\d+$/.test(tok) || tok === '*' || /^\*\/\d+$/.test(tok);
}

/** 轻量段校验:数字是否越界;步进是否合法;范围/列表类约定为合法交由后端判定。 */
export function tokenError(tok: string, i: number): string | null {
  const f = FIELDS[i];
  if (tok === '*') return null;
  const m = /^(\d+)$/.exec(tok);
  if (m) {
    const v = +m[1];
    return v >= f.min && v <= f.max ? null : `${f.everyLabel}取值 ${f.min}-${f.max}`;
  }
  const st = /^\*\/(\d+)$/.exec(tok);
  if (st) {
    const s = +st[1];
    return s >= 1 && s <= f.max ? null : `步进 1-${f.max}`;
  }
  return null; // `?`、区间、列表:交由后端 Spring 解析器判定
}