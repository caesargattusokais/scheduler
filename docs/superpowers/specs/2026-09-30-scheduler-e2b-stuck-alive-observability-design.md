# Scheduler E2-B — stuck-alive 卡死分片:可观测 + 强制放弃

> **规划路径**:E(调度纵深)子项第二件,E1(调度幂等/Exactly-once)之后的可观测性补位。
> **前置**:E1 统一活性门控回收(B1)、告警子系统(V31)、DLQ 治理(V28)。
> **状态**:设计中(2026-09-30)。
> **更新日期**:2026-09-30

---

## 1. 背景与问题

E1 把调度端回收统一成 **活性门控**(B1):调度器只在属主 worker **非 ALIVE** 时回收一条 RUNNING 分片。由此主动接受了一个代价——

> E1 ·§4.4:活但 handler 冻结(worker 进程活着、heartbeat 正常、handler 卡死)时,分片**不可回收**,滞留到 worker 停/被标死;活体不因「慢/超时」被杀。

这个代价今天**完全不可见**:一条 RUNNING 分片由 ALIVE 属主持有、已超其运行预算、却卡住不动——没有任何信号、没有任何列表、没有任何处置手段。运维只有靠经验翻 `execution_shard` 才能撞见。

**本设计的缺口(对照探查已核实源码):**
- `findOverRuntime`(JdbcShardRepository:245-258)用 `now()-started_at > timeout` 判「超时」,但**故意排除**活体属主(`NOT EXISTS(ALIVE)`)——活体超预算分片被它跳过。
- `findExpiredRunning`(:230-243)纯活性门,不回收活体。
- `execution_shard` 无「最近推进时间」字段;`started_at` 每次 claim 重置,是唯一进度锚点。
- `worker.status` 只有 ALIVE/DEAD/DRAINING 三态;`markOffline` 只把 `last_seen` 拨旧,不置 OFFLINE 列;无服务器侧强制杀 worker。

**照映出的空白**:对「属主 ALIVE 且超预算、卡住的 RUNNING」既无告警信号,也无滞留视图,更无显式处置路径。

---

## 2. 设计原则与边界(先说破)

值得放大的边界问题是:**一个「卡死但 ALIVE」的 worker 在心跳层面是真活的**(handler 冻结不中断心跳线程)。它一旦恢复就会继续跑那条分片——若调度器自动回收,就犯了 B1 最想消灭的**双跑副作用**。

**因此本设计分层承诺:**
- **可观测(本设计覆盖)**:stuck-alive 分片被检测、被记入告警、可列出。这是 E1 §7 点名要补、当时没做的部分。
- **处置(本设计覆盖,显式运维动作)**:强制放弃端点是**显式、人工发起**的管理动作,不自动回收活体。
- **双跑安全(契约,不做)**:自动回收活体被**明确禁止**。强制放弃只对「属主已非 ALIVE」的分片生效,且要求运维先真正停掉 worker。放弃后若旧 handler 恢复,靠 E1 的 attempt 栅栏 + requeue 归零后的新 claim 兜底(见 §4.3)。

**本设计选择**:stuck-alive = 复用 task 运行超时(零新配置)+ 属主必须非 ALIVE 才可放弃(强制两步序)。全部派生,零迁移。

---

## 3. stuck-alive 判据(复用现成,零新字段)

**一条 RUNNING 分片被定义为 stuck-alive,当且仅当:**

```
status='RUNNING'
AND worker_id IS NOT NULL            -- 必属主(claim 恒置)
AND now() - started_at > <其 execution→task.timeout_seconds>
AND EXISTS (SELECT 1 FROM worker w
            WHERE w.id = worker_id
              AND w.status='ALIVE'
              AND w.last_seen >= now() - staleAfterSeconds)
```

- **判定复用**:前两条同 `findOverRuntime`;超时取自**每个 task 自己的** `timeout_seconds`(经 execution→task 关联);活性用 E1 同一 NOT EXISTS/EXISTS ALIVE 门的**取反**——`findOverRuntime` 要死属主,stuck-alive 要活属主。二者是同一超时判据的互补两边。
- **零新字段/零迁移**:stuck 完全由 `started_at` + `task.timeout_seconds` + `worker.last_seen` 派生。无 V32。
- **`timeout_seconds>0` 才参与**:与 `findOverRuntime` 一致,无超时预算的 task 不判卡死(Reconciler 已按 `task.timeoutSeconds()>0` 门控)。
- **局限(如实接受)**:无「最近推进」字段,只能看 `started_at` 年龄。**慢而健康的超长任务**会被标记为 stuck——这是决策①(复用 task 超时)的既定代价,靠运维对警告的解读区分。后续若要区分「慢 vs 真卡」,需加 `last_progress_at`,不在本设计范围。

---

## 4. 组件与数据流

### 4.1 滞留检测查询(persistence)

镜像 `findOverRuntime` 的参数形状与 per-task 结构,新增:

- `ShardRepository.findStuckAliveRunning(long taskId, int timeoutSeconds, int staleAfterSeconds)` → 该 task 下 stuck-alive 的 RUNNING 分片列表(驱动滞留视图)。
- `ShardRepository.countStuckAliveRunning(int staleAfterSeconds)` → 全部 task 的 stuck-alive 分片总数(驱动告警信号;以「所有 timeout>0 的 task」求和实现,或单查询 JOIN)。

`JdbcShardRepository` 实现复用 `findOverRuntime` 初版的谓词,仅把 `NOT EXISTS(ALIVE)` 换成 `EXISTS(ALIVE)`。所有权/状态读取仍是普通只读查询,不加锁。

### 4.2 告警信号(接入 V31 引擎)

- `RuntimeConfigKeys` 白名单加阈值键(如 `stuckAliveThreshold`,默认 1)+ `DEFAULTS` 登记(不登记则写入被拒)。
- `AlertEngine.collectSignals` 加一条**全局**规则 `stuck-alive`:producer 查询 `countStuckAliveRunning(...)` → 超过阈值 → `Signal(rule="stuck-alive", targetType=null, active, value=<count>)`。对齐 worker-offline 的全局型规则;episode 状态机(PENDING→OPEN、OPEN→RESOLVED、部分唯一索引)全部复用。
- leader 门控的 `AlertLoop` 已就位,引擎纯函数可测。

### 4.3 滞留视图 + 强制放弃端点(server)

**视图** `GET /api/v1/executions/shards/stuck`(可分页/按 task 过滤):列出 stuck-alive 分片,含 shardId / executionId / taskId / workerId / attempt / started_at / 超龄秒 / timeoutSeconds。驱动前端卡死页。只读,审计读开放同既有。

**强制放弃** `POST /api/v1/executions/shards/{shardId}/abandon`(镜像既有 `POST .../shards/{shardId}/requeue`,ExecutionController:139 全套模式):

1. load 分片;守卫 `status=RUNNING` 且 `worker_id` 非空;
2. **安全前门**:查属主是否仍 ALIVE——**是则 409 拒绝**(`Conflict`,金融消息「worker 仍 ALIVE:请先停掉该 worker 再放弃」),强制「先停 worker,再放弃」两步序;
3. 防御性 `markOffline(workerId, now - 600s)`(把属主拨出 ALIVE 视图,即便守卫已过仍兜底);
4. `markStatus(RUNNING → FAILED, "operator force-abandon")`——用非 attempt 守卫的恢复者 CAS(同 Reconciler 语义);
5. `requeueShard`(FAILED → DUE,attempt 归 0,next_retry_at 清,replay_count++)→ 换干净 attempt 交新 claim;
6. 审计 `auditor.record(current, "shard.force-abandon", TargetType.SHARD, ...)`;
7. 返回分片。

**双跑/失败语义(残留,接受)**:放弃仅对「属主已非 ALIVE」的分片放行,活体前门杜绝漏停双跑。极端时序——旧 handler 恰在新 claim 前恢复写回——被 E1 attempt 栅栏 + 新 claim attempt=1 拒绝,不落 outcome/不双完成;唯一残余是新 claim 就绪前旧 handler 的一次外副作用,靠「先停 worker」的运维契约兜底,与本设计边界一致。

---

## 5. 迁移

**零新表/零新列**:stuck 由 `started_at`/`worker.last_seen`/`task.timeout_seconds` 派生;放弃复用 `markStatus` + `requeueShard`;处置经 `app_audit`(append-only)留痕。无 V32。

---

## 6. API 变更

- **新增** `GET /api/v1/executions/shards/stuck`(只读,审计读开放)。
- **新增** `POST /api/v1/executions/shards/{shardId}/abandon`(写,OperatorInterceptor 收口)。
- 无既有端点/字段/信封变更。OpenAPI 快照需加这两条(additive 重导)。

---

## 7. 可观测

- V31 `stuck-alive` 告警:全局「存在 stuck-alive RUNNING 分片」→ 命中可告警;episode 打开/恢复可见。
- `shard.force-abandon` 审计事件:谁、何时、对哪条分片做过强制放弃。
- 卡死页给出滞留明细(超龄、属主、attempt),运维可据此判断「慢 vs 真卡」。

---

## 8. 测试

**检测查询:**
- stuck-alive:活属主 + 超预算 → 被返回;活属主 + 未超预算 → 不返回;死属主(即便超预算、方向同 findOverRuntime)→ **不**入 stuck(那是普通回收路径,避免重复宣称)。
- 无 timeout 预算 task → 不计。

**告警:**
- count 超阈值 → Signal active;episode 状态机 PENDING→OPEN→RESOLVED 全链路(镜像既有告警测试)。
- `alert.enabled=false` 集成测试下无后台竞态(镜像 V31 习惯)。

**端点:**
- `abandon`:属主 ALIVE → 409;属主非 ALIVE → markOffline + RUNNING→FAILED("operator force-abandon") + requeue(→DUE attempt=0)+ `shard.force-abandon` 审计落一条;返回分片。
- 守卫:非 RUNNING / 无属主 → 拒绝。
- `stuck` 视图:分页/过滤正确,只列 stuck-alive。

**回归:** 既有认领/回收/告警/DLQ 测试保持绿;离线 Maven + 前端 build 常绿。

---

## 9. 自审对照

- 已核实:findOverRuntime 排除活体、无进度字段、markOffline 语义、V31 规则接法、requeue/审计端点模式 —— 全对齐源码。
- E1 §7 点名未做的「活体滞留可见」在本设计兑现。
- 不自动回收活体,B1 不变量不违反。
- 零迁移、零新依赖;全部复用现有列/机制/端点模式。
- 兼容:纯新增端点 + 新告警键,无既有行为变更,既有测试应保持绿。

---

## 10. 全局约束

- 版本/依赖:零新增依赖、零迁移(V32 无);仅 Java/Spring 既有栈。
- 命名:新仓库方法对齐 `findOverRuntime`/`countDeadLetter` 既有命名;告警键 `stuck-alive` 对齐 `worker-offline` 惯例。
- 平台:单一时钟(DB 持时);stuck-alive 复用 E1 的活体判据(stale 窗与 Reconciler 同源)。
- 只读端点审计读开放;写端点(abandon)经 OperatorInterceptor + audit 留痕。
- 测试门:离线 Maven `-o`;server→`-pl scheduler-server -am`,persistence→`-pl scheduler-persistence -am`,`-Dsurefire.failIfNoSpecifiedTests=false`;前端 `npm run build` 常绿。
```