# Scheduler E3 复审发现收尾 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 收掉 `fix/e3-review-findings` 分支第二轮 whole-branch 复审的遗留发现（文档/规格/测试缺口），使分支可合入 `main`。

**Architecture:** 本轮**无生产行为变更**——除 Task 2 新增一个加载 Spring 上下文的守卫测试外，其余均为文档、规格与测试一致性修正。分支的实质改动（活性窗同源、点火隔离、聚合 gauge 删除）已在第一轮完成并通过全量回归。

**Tech Stack:** Java 21、Spring Boot 3.2.5、Spring JDBC(手写 SQL,无 JPA)、Flyway、Micrometer/Prometheus;React 19 + TypeScript + Vite 6 + Tailwind 4。

**Spec:** docs/superpowers/specs/2026-10-12-scheduler-e3-reconciler-observability-design.md、docs/superpowers/specs/2026-09-30-scheduler-e2b-stuck-alive-observability-design.md

**分支:** `fix/e3-review-findings`（基于 `main`，e3 已合入 `main`）

---

## 背景

第一轮复审的 6 项发现已修复（7 个提交，`e5d771c..02af387`）。第二轮 whole-branch 复审以 5 个独立视角（CLAUDE.md 合规 / 浅层缺陷扫描 / git 历史 / 规格文档 / 测试有效性）并行执行，每条发现经置信度打分（0–100）。

**结论：未发现存活的生产缺陷。** 分支的核心主张（活性窗同源、点火隔离、聚合 gauge 可导出）经独立复核成立，其中两处由复审者独立复现：

- `3b501f3` 对 `d140cb4` 的持久层回退经机械验证为**逐字节等价**（`git diff 75a4f4f HEAD -- <3 files>` 为空）——无半回退、无遗留缓存。
- 新增的死信兄弟隔离测试经独立变异验证（绿 → 变异为"整循环包裹"形状 → 恰好一处失败 `Wanted 3 times`）。

**同时必须记录的一条诚实结论：** `Reconciler.fireQuietly`（第一轮发现 #1 的修复）在当前生产路径上是**不可达的防御性代码**——`NotificationHub.fire` 已自行 `catch (Exception)` 并只记日志（`NotificationHub.java:33-40`），故真实 firer 不会抛出。该包裹仅在该接缝未来被替换为会抛出的实现时生效。测试（含本轮新增的死信测试）用 mock 抛出证明包裹有效，而 mock 抛出是生产当前无法产生的场景。**不得**把"修复了点火中止扫描的 bug"作为已修缺陷对外表述。

## 复审发现与处置

| # | 发现 | 置信度 | 处置 |
|---|------|--------|------|
| 1 | e3 spec §6.1 仍列已删除的 `scheduler_reconcile_reclaimed_1h`；分支未触碰任何 `docs/` 文件 | 60/45/35 | 待办 Task 1 |
| 2 | **"同源"不变量未被测试钉住**：4 个守卫测试均以构造器字面量注入窗口，从不加载属性 → 5 处 `@Value` 键漂移（改 `:60`、键名拼错）整套测试仍绿 | 50 | 待办 Task 2 |
| 3 | `SettingsPage.tsx:42` 对运维称回收窗口"默认 60s"，代码默认 30 | 50 | 待办 Task 3 |
| 4 | `CLAUDE.md` 活性契约段未记载"3 处读者有意保留 30s"，该理由仅存在于提交信息中 | 40/25 | 待办 Task 3 |
| 5 | e2b spec §4.3 把防御性回拨写死为 `now - 600s`，代码已为 `max(600, 2*stale)` | 25 | 待办 Task 1 |
| 6 | 强制放弃的防御性回拨用 JVM `Instant.now()`，而所有读侧比对 DB `now()`（"单一时钟"纪律） | 30/25 | 待定 Task 4（前置存在，需持久层改动） |
| 7 | README "实际有效上限 60s" 是无条件的，但仅在**有 worker 心跳**时成立（清理由 worker 驱动） | 自审 | 待办 Task 3 |

### 已核实为误报 / 不予处置

- **"死信循环仍会中止兄弟"**（第一轮复审）——**误报**。`fireQuietly` 的 `catch` 位于循环体内，逐片独立隔离。已双向验证：现码通过、变异为所述形状后失败。回归测试 `firerThrows_onDeadLetter_everySiblingStillFires` 已保留。
- **"`cbcdc8f` 与 `3b20361` 两条提交信息互相矛盾（窗 ≥600s 不可达）"**——**表述过强**。该结论仅在"有活体 worker 存在"时成立（清理会删行 → `markOffline` 更新 0 行）。当**全体 worker 均已停止心跳 >600s** 时无人清理，行存活且 `last_seen < now-600` → 门放行 → 回拨把 `last_seen` 前移入窗（`isAlive` 为闭区间）→ 复活。故 `cbcdc8f` 修的场景可达（窄）。该观察的正当产出是发现 #7（README 上限声明需加限定）。
- **"门的窗口可配且无下界校验"**——Reconciler 早已有同样暴露面，本分支只是多一个调用点。
- **"`telemetry.record` 未加保护"**——前置存在；异常只使恢复延后一拍，不丢状态。
- **"`scheduler_reconcile_scan_*` 两个 meter 不在 `Beans.java`"**——前置存在；CLAUDE.md "Meters are defined once in Beans.java" 本就不精确，边际。
- **新增回拨测试的挂钟依赖**（置信度 18）——真实脆弱性但余量约 1000×（已实测今日为红，报 `实际回拨 600s`）。随 Task 2 一并处理更划算。

---

## Task 1 — 规格对齐（e3 §6.1 + e2b §4.3）

**依据：** 仓库既有先例 `6747421`（`chore(alert): governance wrap-up — cleanups + spec aligned to impl`）——规格在实现之后对齐是既有治理动作，非改写历史。

- [ ] **Step 1: 删除 e3 spec §6.1 的聚合 gauge 条目**
  `docs/superpowers/specs/2026-10-12-scheduler-e3-reconciler-observability-design.md:101` 删除 `scheduler_reconcile_reclaimed_1h` 行，并注明它 = `timeout_reclaim + orphan_reclaim`，由 PromQL 导出（与控制台卡片数 16→15 一并说明）。

- [ ] **Step 2: 更新 e2b spec §4.3 的回拨描述**
  `docs/superpowers/specs/2026-09-30-scheduler-e2b-stuck-alive-observability-design.md:87` 把 `markOffline(workerId, now - 600s)` 改为 `max(600s, 2×staleAfterSeconds)`，并说明理由（回拨必须严格超出配置窗，否则摘活即复活）。默认 30s 时取值不变。

- [ ] **Step 3: 验证**
  Run: `grep -rn "scheduler_reconcile_reclaimed_1h" docs/`
  Expected: 仅 plan 文档（历史实现计划）残留，spec 内无。

---

## Task 2 — 钉住"同源"属性键（测试，唯一新增测试）

**动机：** 该里程碑的整个主张是"5 处读同一属性键"。现有 4 个守卫测试只证明"类把构造器实参透传"，不证明"它们读同一个键"。键漂移可静默回归。

- [ ] **Step 1: 先写失败测试**
  在 `scheduler-server/src/test/java/dev/scheduler/server/config/` 新增上下文加载测试（如 `LivenessWindowWiringTest`），以 `scheduler.worker.stale-after-seconds=60`（非默认值）启动最小 Spring 上下文，断言 Reconciler、`scheduler_reconcile_backlog` gauge、`ReconcileController`、`AlertEngine`、`ExecutionController` 全部看到 60。

- [ ] **Step 2: 验证 RED**
  把 `Beans.java:363`（reconciler bean）的 `@Value("${scheduler.worker.stale-after-seconds:30}")` 改成 `:60`（或把键名拼错），Run 该测试，确认失败。
  Expected: 失败，且失败原因指向"各处看到的窗口不一致"。

- [ ] **Step 3: 还原键并验证 GREEN**
  Expected: 通过。

- [ ] **Step 4: 顺带加固回拨测试的挂钟依赖**
  `ExecutionControllerAbandonWindowTest:72-74` 改为对捕获时间戳断言"距 `Instant.now()` 至少 601s"之外的更强形式，或直接断言生产回拨量为 `2×stale`（消除 `600 + ε` 的边界）。

---

## Task 3 — 运维面文档一致性

- [ ] **Step 1: 修正控制台文案**
  `web/src/pages/SettingsPage.tsx:42` 的"默认 60s 回收窗口"改为 30s（代码默认；无 `application.yml`/`start-dev.sh` 覆盖）。

- [ ] **Step 2: 补 `CLAUDE.md` 活性契约段**
  在 `D:\ai-project\scheduler\CLAUDE.md` 的 worker liveness 段补一句：`scheduler_worker_active`、worker-offline 告警、`AvailableHandlerRefs` 三处**有意**保留 30s（观测/准入面；窗口漂移只误导看板，不造成双跑）。

- [ ] **Step 3: 给 README 的 60s 上限加限定**
  `README.md:35` 的"实际有效上限 60s"改为"**有 worker 在心跳时**有效上限 60s"——清理由 worker 心跳驱动，全体 worker 停止心跳时无人清理，窗按配置值生效。

---

## Task 4 —（待定）回拨改用 DB 时钟

**前置存在，非本分支引入。** 两个复审者均评 25–30：需 10 分钟级 JVM/DB 偏斜才会咬合。

- [ ] **Step 1: 决策** — 是否本轮处理。
- [ ] 若处理：新增 DB 时钟锚定的 `markOffline`（`last_seen = now() - make_interval(secs => ?)`），与门的读侧"单一时钟"一致，并去掉 `2×stale` 的算术。

---

## 验证要求

- [ ] `mvn test`（Docker 需在跑）全绿：core / persistence / server / worker 零失败。
- [ ] `cd web && npm run build` 成功退出。
- [ ] 逐项提交，不合并 `main`（合并需用户明确授权）。

## 备注

- `CLAUDE.md` 目前**未被 git 跟踪**；本轮对其的编辑仍保持未跟踪状态，除非用户另行要求纳入。
- `docs/openapi/api-docs.json` 无需改动（本轮未增删端点或环境变量）。
