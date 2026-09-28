# Scheduler v2 — 四方向验收清单

> **性质**:v2(四方向路线 + 企业化治理)的验收依据。四方向没有单一总纲 plan,拆散在多个自洽子项目 spec/plan 中;本文档把它们汇总成逐条可勾的清单。
> **验收基准**:全部模块测试绿(`mvn -o test` → reactor BUILD SUCCESS)、无遗留断编译、工作区干净。
> **更新日期**:2026-09-28

## 勾选规则
- **[✓]** = 已实现 + 对应测试绿 + 已提交
- 每个子项目附:`规划`(spec/plan 路径)、`落地`(主要 feat commit)、`验收要点`(该子项目声称解决的核心问题)

---

## 方向 1 · 编排(DAG)

| # | 子项目 | 规划 | 落地 | 状态 | 验收要点 |
|---|---|---|---|---|---|
| 1a | DAG node 级重试预算 | plan `2026-09-08-scheduler-m4-workflow-dag` | `29b0c45` | **[✓]** | DagEngine 每节点独立重试(per-node retry budget) |
| 1b | DAG 条件分支 OR-join(any_success) | spec M4 `2026-09-08` | `b2fae16` | **[✓]** | run_if 支持 any_success 汇聚 |
| 1c | DAG 版本化(run 封印 + 编辑) | spec `2026-09-22-scheduler-dag-versioning-design` | `cd64d87` | **[✓]** | createRun 封印整份定义;引擎只读 run 封印;PUT 编辑 version++ |
| 1d | 跨 DAG 依赖(事件链) | spec M4 `2026-09-08` | `21a9e29` | **[✓]** | depends_on_dag_id 取代 cron 互斥;上游成功一次→下游幂等派发一次 |

## 方向 2 · 可观测

| # | 子项目 | 规划 | 落地 | 状态 | 验收要点 |
|---|---|---|---|---|---|
| 2b | 执行 SLI 指标(吞吐/失败率/p95) | plan `2026-09-09-scheduler-m5-3-dag-metrics` | `a0aec7a` | **[✓]** | per-DAG/全量吞吐、失败率、p95 延迟指标 |

## 方向 3 · 可靠性

| # | 子项目 | 规划 | 落地 | 状态 | 验收要点 |
|---|---|---|---|---|---|
| 3a | 时区感知 cron + 秒级间隔触发 | spec `2026-09-16-runtime-retry-timeout` | `b3d9463` | **[✓]** | cron 按 task 时区;intervalSeconds 秒级 |
| 3b | 入站事件触发器(事件 outbox + 路由订阅) | spec `2026-09-16` | `7dc78ec` | **[✓]** | app_task_event outbox;按 route key 路由触发 |
| 3c | 分片聚合与部分成功(成功策略) | spec `2026-09-16` | `520094e` | **[✓]** | PARTIAL_SUCCESS 状态 + SuccessPolicy(四阈值) |
| 3d | DLQ 治理强化(自动重放上限 + 领袖门控) | 子项目 · 方向3 | `8f8adb0` | **[✓]** | 每任务 dlq_max_replays;leader 门控重放循环 |

## 方向 4 · 分发(通知 / API / worker)

| # | 子项目 | 规划 | 落地 | 状态 | 验收要点 |
|---|---|---|---|---|---|
| 4a | 通知底座(outbox + leader 门控投递 webhook) | 子项目 · block1/2a/4a | `6606860` `df7a6c0` | **[✓]** | app_notification outbox;HMAC-SHA256/指数退避/幂等;NotificationFirer 点火终态事件 |
| 4-1 | 通知告警闭环(webhook 订阅入 DB + 投递历史) | 子项目 · 4-1 | `095a788` | **[✓]** | webhook DB 化;投递历史 PENDING/SENT/FAILED + 退避 |
| — | 投递历史可读性增强(任务名/结果/信封/投递给) | 本会话 | `4aa6386` `2f84bff` `4d6a775` | **[✓]** | 每行回答「哪个执行完成 + 结果 + 发的啥 + 发给谁」 |
| — | 通知任务维度过滤(白/黑名单 scope) | 本会话 | `6662d3a` | **[✓]** | scope_mode ALL/INCLUDE/EXCLUDE + task/DAG 选中集;真实投递验证通过 |
| 4-2 | 执行 SLO 指标(DB 快照聚合) | 子项目 · 4-2 | `c164563` | **[✓]** | 近窗父延迟 p50/p95、分片均长、per-task 吞吐/成功率 |
| 4b | OpenAPI 文档与 Swagger UI | 子项目 · 4b | `9b36513` | **[✓]** | springdoc 生成 + 会话安全声明 |
| — | OpenAPI 契约守卫 + api-docs 快照工件 | 子项目 · 4 | `9565df0` | **[✓]** | 关键路径 API 契约测试 + 快照工件 |
| 4c | worker 扩容(选片摊开 + per-worker 并发容量) | 子项目 · 4c | `ea43d6d` | **[✓]** | findCandidate 按 workerId 摊开;per-worker capacity |

---

## 企业化治理(横向)

| # | 子项目 | 规划 | 落地 | 状态 | 验收要点 |
|---|---|---|---|---|---|
| G1 | 操作审计完整链路 | spec `2026-09-17/18` | `93be75e` `194b4cb` `079aa50` | **[✓]** | V10 app_audit;diff(V11);before 快照(V12);task.* 全覆盖 |
| G2 | 审计取证链(hash-chain + integrity) | 子项目5 | `01a9108` | **[✓]** | prev_hash/chunk_hash;GET /audits/integrity 双校验 |
| G3 | 审计联合检索 + CSV 导出 | 子项目4 | `fa63d5e` | **[✓]** | beforeField/metaField 过滤;CSV(BOM+公式注入防护) |
| G4 | 审计访问控制(白名单 + 角色收口写端点) | 子项目6 | `f8faa7c` | **[✓]** | 非 ADMIN 写操作收口;审计读开放 |
| G5 | 审计保留(归档 + 即删即重链) | 子项目7 | `7bdf7c6` | **[✓]** | 归档原子不破链;周期 loop |
| G6 | 操作者强认证(口令/会话/退避) | spec `2026-09-20-operator-strong-auth` | `343c291` `786cc47` `aa5ae25` | **[✓]** | BCrypt 登录 + HttpOnly cookie + DB 指数退避;V17 自助改密/首登强制改密 |
| G7 | 会话管理 + 强制登出 | 子项目·会话 | `48129de` `6469b1b` | **[✓]** | ADMIN 活动会话视图 + revoke |
| G8 | 口令策略增强(长度/复杂度/防重) | 子项目·口令 | `bb1c9dc` | **[✓]** | minLength8/digit/historySize5防重 |
| G9 | 认证全量审计(含登出/事件审计) | 子项目·认证审计 | `64fbc20` `4cf8c23` | **[✓]** | auth.* 入 app_audit;access.denied;event.submit |

---

## v2 附加完成项(本次会话)

- **断编译修复**:`JdbcEventRepositoryTest` 对齐新 `enqueue()→EnqueueResult`;`JdbcWebhookRepositoryTest` 按新 scope 签名修正并补 scope 往返断言 — commit `0e5077e`
- **前端下拉组件**:`MultiSelect` 泛型化(string/number 通用),Kinds/任务/DAG 统一下拉多选 — `92b469d` `c2ab525`

## 测试基准(2026-09-28 实测)

| 模块 | 结果 |
|---|---|
| scheduler-core | ✅ |
| scheduler-persistence | 202 例 ✅ |
| scheduler-server | 225 例 ✅(含通知 matcher/dispatcher/firer、集成) |
| scheduler-worker | 32 例 ✅ |
| web 前端构建 | ✅ |
| 全量 reactor `mvn -o test` | ✅ BUILD SUCCESS |

## 仍未纳入/explicit 排除项
- 无统一四方向 plan 文档(本清单即替代);各子项目如需展开,见其 spec/plan 路径。