# Scheduler 告警子系统 · Episode-based 设计(治理告警前端升级)

> **规划路径**:v3 迭代子项;本 spec 实现「治理/告警/审计前端」的真升级 —— 把告警从「前端每 5s 快照打一次请求」升级为「后端持续判定 + episode(事件)生命周期 + 历史」。
> **前置**:web 已有 `AlertsPage`(纯前端聚合,清单式,瞬时抖动/无状态)——本 spec 是其后端化与强化。
> **更新日期**:2026-09-29

---

## 1. 背景与问题

现状 `AlertsPage` 只是把若干只读端点打一遍并在浏览器里实时合计:
- **瞬时快照会抖动**:每 5s 读一次当前值,一次短暂毛刺立刻翻红再翻绿,没有「持续期」判定。
- **无状态、每次轮询重算**:同一排告警反复重生,没有「何时开始/恢复」,刷新即失忆。
- **只有聚合不可下钻**:「DLQ 345 条」不说是哪些任务;「失败率偏高」不说哪个任务。
- **阈值写死**:5% / 5s 是常量,改一次要重部署。
- **无历史**:看不到「这个 DLQ 是从几点涨起来的」。

**核心矛盾**:告警的本质是**事件**(何时起、何时消、持续多久),而现状是**状态**(current values)。只有后端把一个信号判定为「已持续超阈值一段时间」并落成一条可回溯的 episode,才能回答「哪个任务、何时开始、现在值多少」。

**目标(可验收)**:
1. **Episode 生命周期**:信号持续超阈值 → episode 从 PENDING 确认到 OPEN;持续回落 → RESOLVED(终态历史)。PENDING/OPEN 同 (rule,target) 单一活跃行。
2. **可下钻粒度**:基础设施规则(取证链/DLQ/投递/失败率/p95/worker)全局;按任务失败聚集(per-task episode)、按 DAG 批次失败(per-dag episode)。
3. **热键可调**:`scheduler.alert.*` 白名单热键(阈值/open-recover 窗口/轮询间隔/历史保留),运行的下一拍生效。
4. **历史 + 清理**:resolved episode 可查分页;按 `alert.history-keep-days` 热键清理(镜像审计归档思路)。
5. **前端改渲染**:`AlertsPage` 从「算告警」改为「渲染 active episodes + 历史页签」。

**非目标(明确排除)**:
- 不做 **ack/静默/维护窗口**(降噪是独立能力,本期只做真实开已关的 episode;避免 scope 爆炸)。
- 不做**降级通知渠道**(escalation / 把告警经 webhook 再外发 —— 通知底座已能投递,本期不在告警侧复用)。
- 不做**多集群/多 region**(C 子项,架构级,另行推进)。
- 不做历史回滚/恢复操作;episode 是**观测记录**,不驱动任何系统动作。

---

## 2. 数据模型(V31 迁移)

单表 `alert_episode`,状态机单行承载体(不另设 counter 表)。

```sql
CREATE TABLE alert_episode (
  id           bigserial PRIMARY KEY,
  key          text NOT NULL,                    -- 确定性去重键,见下
  rule         text NOT NULL,                    -- 规则 id,见 §3 目录
  target_type  text NOT NULL DEFAULT 'none',     -- none | task | dag
  target_id    bigint,                           -- task/dag 时非空;全局为 NULL
  severity     text NOT NULL,                    -- critical | high | medium
  status       text NOT NULL DEFAULT 'PENDING',  -- PENDING | OPEN | RESOLVED
  sample_count int NOT NULL,                     -- 带符号连续拍数:负=连续低于阈值(见 §4 状态机)
  value        text NOT NULL,                    -- 最近一次采样值展示串
  opened_at    timestamptz,
  opened_value text,
  resolved_at  timestamptz,
  resolved_value text,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- 同 (rule,target) 同时只允许一个非终态 episode:PENDING/OPEN 时 key 唯一,RESOLVED 释放后同 key 可再起新 episode(历史叠加)。
CREATE UNIQUE INDEX alert_episode_active_key_uq
  ON alert_episode (key) WHERE status IN ('PENDING','OPEN');
CREATE INDEX alert_episode_status_idx ON alert_episode (status, updated_at);
```

### key 构造
- 全局规则:`rule`(如 `dlq-depth`)。
- per-task:`rule + ':task:' + taskId`(如 `task-failure-rate:task:42`)。
- per-dag:`rule + ':dag:' + dagId`。
partial unique 保证任意时刻每个 key 至多 1 个活跃 episode;RESOLVED 释放约束后,再次触发即新行(重复事故 → 多条历史)。

---

## 3. 规则目录(8 条)

信号统一在服务端评价,与 SLO 端同源。

| 规则 id | target | 严重度 | 信号(评价) | 阈值热键(默认) | 增值展示 |
|---|---|---|---|---|---|
| `audit-tamper` | none | critical | `AuditRepository.integrity().verified == false` | `alert.audit-tamper`(1) | `chained/total` |
| `dlq-depth` | none | high | 死信分片存量(清洗态) | `alert.dlq-depth`(1) | 深度 |
| `delivery-failed` | none | high | 终态 FAILED 出站通知数 | `alert.delivery-failed`(1) | 计数 |
| `failure-rate` | none | medium | 近窗 1h 失败比(bad/(ok+bad)),且 total≥MIN_TOTAL | `alert.failure-rate`(0.05) | 百分比 |
| `p95-latency` | none | medium | `ExecutionMetrics` parentLatencyP95Ms | `alert.p95-latency-ms`(5000) | ms |
| `worker-offline` | none | medium | 存活 worker 数==0 | `alert.worker-offline`(1) | 计数 |
| `task-failure-rate` | task | high | 每任务近窗失败比,且 throughput≥MIN_TOTAL | `alert.task-failure-rate`(0.2) | episode/任务 |
| `dag-run-failed` | dag | high | 每 DAG 近窗失败批次计数 | `alert.dag-run-failed`(1) | episode/DAG |

- **阈值热键 = 0 → 该规则禁用**(评价直接跳过)。
- **严重度规则内静态**,不做热配(避免 8×3 键爆炸)。
- **MIN_TOTAL**(失败率类规则的样本下限,默认 3)为引擎常量,避免近零流量下一两次失败即报警;不在热键(注释注明未来可热键化)。
- 全局规则总是产出 target=None、targetId=NULL 的单一信号;per-task/per-dag 规则对每个任务/每个 DAG 产出独立信号 → 各自成独立 episode。

---

## 4. 评价与状态机

### 4.1 `AlertEngine`(server service)
依赖注入:`ExecutionMetrics`(抽取,见 §6)、`ShardRepository`(DLQ 深度)、`NotificationRepository`(FAILED 计数)、`AuditRepository`(integrity)、`WorkerRepository`(存活)、`RuntimeConfigService`(热键)、`AlertRepository`(episode 读写)。

每拍 `evaluateOnce()`:
1. 读热键(open-samples / recover-samples / 各 rule 阈值)。
2. 对每条**启用**规则评价 → 信号集:`Signal(rule, targetType, targetId, severity, valueText, active)`。
3. 逐信号与现行 episode 行对比,迁移状态(见 4.2)。批量 upsert/update。
4. 历史清理:`DELETE FROM alert_episode WHERE status='RESOLVED' AND resolved_at < now() - alert.history-keep-days`。

### 4.2 状态机(单行,PENDING → OPEN → RESOLVED)
`sample_count` **带符号**:正值=连续高于阈值拍数;负值=连续低于阈值拍数。方向翻转时重置为 ±1。

对每个信号:
- **无现有活跃行**:
  - active → `INSERT (status=PENDING, sample_count=1, value, opened_at=now)`。
  - else → 无动作。
- **有 PENDING/OPEN 行**:
  - active:`sample_count = (已>0 ? sc+1 : 1)`;若 `status==PENDING && sc >= open_samples` → `status='OPEN', opened_at=now, opened_value=value`。
  - else:`sample_count = (已<0 ? sc-1 : -1)`;若 `sc <= -recover_samples` → `status==OPEN ? RESOLVED(resolved_at=now, resolved_value=value) : DELETE(未确认之苦,不成历史)`。
  - 每拍刷新 `value` / `updated_at`。
- **容忍单拍抖动**:一次毛刺不足 `open_samples` 拍到 PENDING 即回落 → DELETE,不成 OPEN、无历史噪音。

### 4.3 leader 门控与循环
- `Beans.AlertLoop extends ConfigurableLoop`:delay 走 `alert.delay-ms`(默认 15000),`loopOnce()` = `if (!leader.isLeader()) return; engine.evaluateOnce();`(镜像 NotificationLoop)。
- `@ConditionalOnProperty(name="scheduler.alert.enabled", havingValue="true", matchIfMissing=true)`,避免测试/开发态后台竞态(ApiIntegrationTest 关)。

---

## 5. 热键白名单(`RuntimeConfigKeys` 追加)

| key | 默认 | 语义 |
|---|---|---|
| `alert.delay-ms` | 15000 | AlertLoop 轮询间隔(ConfigurableLoop delayKey) |
| `alert.open-samples` | 3 | PENDING→OPEN 所需连续高于拍数 |
| `alert.recover-samples` | 3 | OPEN→RESOLVED 所需连续低于拍数 |
| `alert.history-keep-days` | 7 | resolved episode 保留天数,清理由 §4.1(4) 做 |
| `alert.audit-tamper` | 1 | 0=禁用 |
| `alert.dlq-depth` | 1 | 阈值(深度) |
| `alert.delivery-failed` | 1 | 阈值(计数) |
| `alert.failure-rate` | 0.05 | 阈值(比率) |
| `alert.p95-latency-ms` | 5000 | 阈值(ms) |
| `alert.worker-offline` | 1 | 0=禁用(watch) |
| `alert.task-failure-rate` | 0.2 | 阈值(比率);每任务评价 |
| `alert.dag-run-failed` | 1 | 阈值(每 DAG 近窗失败批次数) |

全部走既有 `RuntimeConfigService` 白名单写(ADMIN 收口 + 记 `runtime-config.update` 审计)与「运行时设置」页展示 —— 与 `dlq.replay-delay-ms`/`audit.retention.days` 同机制,零新基础设施。

---

## 6. 信号源抽取(`ExecutionMetrics`)

`MetricsController` 现直接持 `JdbcTemplate` 算 SLO(近窗失败/每任务成功率/p95)。告警评价须用同一组查询,否则两端漂移。

- 新增 `scheduler-server .../service/ExecutionMetrics`,封装现 MetricsController 的四类查询(父 p50/p95、分片均长、per-task 吞吐+成功率、近窗失败细分),以 `ExecutionSlo`/`TaskMetric`/`RecentFailures` 返回(把现 controller 里的 record 移到 service 包)。
- `MetricsController` 改为委托 `ExecutionMetrics`(接口不变对外)。
- `AlertEngine` 注入同一 `ExecutionMetrics` → **评价端与 SLO 端同源**。

需补的小方法(现 Repo 若有同名即免):
- DLQ 深度:`ShardRepository` 死信存量计数。
- FAILED 通知数:`NotificationRepository` 按 status 计数。
- per-DAG 近窗失败:`DagRepository` 或 AlertRepository join `dag_run` 按 dag_id 近窗 `status='FAILED'` 分组。
- `AlertRepository`:**findActiveByKey / findActiveAll / upsert(迁移) / deleteKey / pruneResolved(cutoff)** + 供 controller 的视图查询(join task/dag 名)。

---

## 7. API(读开放)

### `GET /api/v1/alerts/active`
非分页(活跃集小)。返回 `List<AlertEpisodeView>`:
```json
[{
  "id":1, "rule":"task-failure-rate", "targetType":"task", "targetId":42,
  "targetName":"daily-export", "severity":"high", "status":"OPEN",
  "value":"0.33", "openedAt":"2026-09-29T08:00:00Z", "samples":5
}]
```

### `GET /api/v1/alerts/history?limit&offset`
`Page<AlertEpisodeView>`(status=RESOLVED,分页信封同 TaskController),多 `resolvedAt`/`resolvedValue`(OPEN 字段可空)。

### 控制器
`AlertsController`(`/api/v1/alerts`,`RequestParam limit/offset` + Paging 信封);读开放(与审计读一致,`CurrentOperator` 拦截器仅收口写端点)。查询 left-join `app_task`/`app_dag` 补 `targetName`,缺名回落 `targetType:targetId`。

---

## 8. 前端重做

### `AlertsPage`
- 删掉前端「算告警」逻辑(阈值常量、gauge 聚合),改为 **渲染 `GET /api/v1/alerts/active`**(5s 轮询):
  - 顶部绿/红横幅:**active 为空 → 全绿**;否则「N 项需要关注」。
  - triage 列表:severity 点/标签、规则人话标题、`targetName`(task/dag)或全局、当前值 `value`、`openedAt`「始于」、samples;行尾下钻链接同现(规则→所属页)。
- 新增**「历史」页签**:打 `GET /api/v1/alerts/history?limit&offset`,Pager 化,只读。
- `client.ts` 加 `listAlertEpisodes()`/`listAlertHistory()`;`types` 加 `AlertEpisodeView`。
- `MetricsPage` / `NotificationsPage` / 其余不动。

### 人话标题映射
规则 id → 标题(未知回落原文,镜像 NotificationsPage 的 KIND_LABELS 手法):
`audit-tamper`→审计取证链 / `dlq-depth`→DLQ 积压 / `delivery-failed`→通知投递失败 / `failure-rate`→执行失败率 / `p95-latency`→父延迟 / `worker-offline`→worker 全部离线 / `task-failure-rate`→任务失败聚集 / `dag-run-failed`→DAG 批次失败。

---

## 9. 测试

### `AlertEngineTest`(单测,自足)
- 状态机:单拍不足 open-samples 回落 → 不留行与历史;持续 open 拍到 OPEN;持续低于 recover 拍到 RESOLVED;PENDING 未确认回落 → DELETE。
- per-task/per-dag:多个超阈任务 → 各成独立 episode(key 分型)。
- 阈值热键读取;某规则阈=0 → 禁用。
- pruneResolved:只清 status=RESOLVED 且超 keep-days 者。
- 构造 `new AlertEngine(metrics, shards, notifs, audits, workers, settings, alertsRepo)`:settings 用 in-memory RuntimeConfigService 假仓储镜像 LoopWiringTest 手法。

### `ApiIntegrationTest`
- `alertsHappen` 类:预设一条已 OPEN 的 episode + 一条 RESOLVED → `GET /active`、`GET /history` 信封/字段断言(resetDb 并存到 alert_episode,镜像 notification 测试)。
- 空态:active 空数组、history total=0。
- leader 门控:AlertLoop 存在性与 auditRetentionDays_hotKey_drivesLoopArchival 同手法(bean 注入 + 反射 loopOnce)可选;npm run build 常绿。

---

## 10. 自审对照(写 plan 时收敛)

- §1 五条缺陷 → §1 目标 1-5 一条不漏落位 → 全部有对应 Task。
- 粒度:全局 + per-task + per-dag 由 §3 目录保证(两条扩展规则)。
- 热键化由 §5 白名单 + §4.1(1) 评价时读保证。
- 历史由 §4.1(4) 清理 + §7 history 端点 + §8 历史页签保证。
- 状态机 §4.2 与 §5 open/recover-samples 热键一致;`sample_count` 带符号语义与 key partial unique 覆盖并发/重复事故。
- **与既有模式零冲突**:ConfigurableLoop / RuntimeConfigKeys 白名单 / @ConditionalOnProperty / Page 信封 / CurrentOperator 收口 —— 全镜像既有实现,无新基建。
- 组合一致性:`AlertEpisodeView` 字段在 §7 定义、§8 前端消费、§9 断言 —— 一致。