# Scheduler v2 执行运行时深化 · 子项目 1:重试策略化 + 执行超时设计

> **Goal:** 把执行失败的「重试」从单一固定指数退避升级为**可配置策略**(fixed/linear/exponential + 退避封顶可调 +
> 整轮重试总时限 W),并引入**单分片运行时限(max_runtime)**:超过即分布式判定超时、走统一失败路径(重试/死信/取消兄弟)。
> 零永久 RUNNING 继续成立;worker 并发模型与上次心跳隔离修复不动。
>
> **Architecture:** 重试决策仍在纯类 `RetryPolicy`(无时钟、更按 Task 携带的 mode/cap);**新增退避模式与封顶**都在决策内;
> **整轮重试时限 W** 是"按次数重试"之外的第二个终止条件,需要跨调用持久状态 → 用分片新列 `retry_budget_until`(首次进重试时置
> `now()+W`),每次失败先判 `budget <= DB now()` 耗尽则转死信。**执行超时**采用**分布式判定**:server Reconciler(同一 15s 对账循环)
> 对设了 `max_runtime` 的任务额外查 `RUNNING AND now() - started_at > max_runtime`,沿既有 markStatus(FAILED) + `FailureResolver.handle`
> 回收——worker 迟到写回被 owner 归属守卫拒,不改 worker 线程。超时/预算判窗统一用 DB `now()`(与 reliability 活性判据一致)。
>
> **Tech Stack:** Java 17、Spring Boot、PostgreSQL 16(DB now 权威时钟)、Testcontainers 集成测试。无新增依赖。

## 关键前置事实(已核准,不猜测)

- 分片 `started_at` 在 claim 时写入;现为 `COALESCE(started_at, now())` → 重试后重新认领会**沿用首次认领时间**(不反映本轮运行起点)。
- `ExecutionQueryService`/`ExecutionController` 的执行**时间窗过滤与分页正按 `shard.started_at`** 读——重置语义对其是修正(显示本轮起点而非首次)。
- Reconciler.scanOnce 逐任务循环:`findExpiredRunning`(租约/心跳双信号)→ markStatus(FAILED)+ `failureResolver.handle`;失败单行 `IllegalStateException` 跳过不中止整批。
- `RetryPolicy` 现:指数退避封顶硬编码 1h、`shouldRetry` 按 `maxRetries` + 可选 `retryableFailurePattern`。降级判定在 `FailureResolver.handle`(有 shards+clock)。

---

## §1 任务配置模型(新增 4 列)

`app_task` 加列(V9 迁移),默认值均与现状等价(老任务零行为变化);全部可空/有缺省,只对新语义显式设。

| 列 | 类型 | 默认 | 语义 |
|---|---|---|---|
| `retry_mode` | VARCHAR(16) | `'exponential'` | 退避模式 `fixed` \| `linear` \| `exponential` |
| `retry_cap_ms` | BIGINT NULL | NULL(=全局 1h 兜底) | 退避封顶,任务级覆盖 |
| `retry_budget_ms` | BIGINT NULL | NULL(无限,按次数重试) | 整轮重试总时限 W |
| `max_runtime_ms` | BIGINT NULL | NULL(无限,不超时) | 单分片运行时限 |

- `core.Task` record:加 `retryMode`(String)、`retryCapMs`/`retryBudgetMs`/`maxRuntimeMs`(均 `Long`,JSON `null`=未设)。
- `TaskRepository`/`JdbcTaskRepository` MAP 与 insert/update 同步。**REST 任务 DTO**(TaskController create/update + 读)加这 4 字段,后端可设。
- **React 表单不加**(非目标;列为后续小步——否则治理/运维面扩大,超出本子项目运行时纵深)。

## §2 重试引擎:模式化 + 封顶 + 整轮时限

### 2.1 退避三模式 + 统一封顶(`RetryPolicy.delayMs` 重构)

签名由 `delayMs(long backoffMs, int attempt)` 改为 **`delayMs(Task task, int attempt)`**,内部读 `task.backoffMs()` / `task.retryMode()` / `task.retryCapMs()`:

- `fixed` → `min(backoffMs, cap)`
- `linear` → `min(backoffMs * attempt, cap)`
- `exponential` → 现公式 `min(backoffMs * 2^(attempt-1), cap)`(现状即此)
- `cap` = 任务设了 `retryCapMs` 用它,否则兜底全局 `CAP=1h`;`CAP` 常量保留为缺省兜底。
- `shift` 溢出守卫保留(> cap/2 → 直接返回 cap)。

`shouldRetry(task, attempt, detail)` **签名不变**(仍纯按次数 + pattern)。

### 2.2 整轮重试时限 W(第二终止条件)

- 新列 `execution_shard.retry_budget_until TIMESTAMPTZ NULL`(V9)。
- `scheduleRetry`(现有重试→DUE 路径)增强:**当 task.retryBudgetMs != null 且该 shard `retry_budget_until` 尚未置**(首次进重试),置 `retry_budget_until = now() + W`;已置则保持(窗口从首次失败起算,重试间不续)。
- `FailureResolver.handle` 在 `shouldRetry` **之前**先判**窗口耗尽**:**`retry_budget_until IS NOT NULL AND retry_budget_until <= DB now()` → 即使还有 maxRetries 余量也直接死信**(跳过重试)。加 `ShardRepository.retryBudgetExhausted(shardId)`(单 SQL,DB now 判窗;与活性判据同源)。
- 语义:W = 「自首次失败起,整个重试努力不得超过 W」;到期即止,转死信(并 `cancelSiblings` fail-fast,与死信分支既有行为一致)。
- 未设 W 的任务:预算判窗恒 false,行为=现状(仅按次数)。

## §3 分布式执行超时(max_runtime)

### 3.1 裁定:认领重置 `started_at=now()`(每轮运行起点)

`claim` 语句由 `started_at=COALESCE(started_at, now())` 改为无条件 **`started_at=now()`**。理由:超时须从**本轮**认领起算;重试后若沿用旧时间会含队列等待/重试间隔,超时误判。副作用是修正执行视图时间窗(见前置事实)——是正向修正。已核无其它语义依赖旧「首认领」解读。

### 3.2 server 侧超时分支(复用回收机制,不动 worker 线程)

在 `Reconciler.scanOnce` 逐任务循环中,**对 `task.maxRuntimeMs()!=null` 的任务**额外跑:

```sql
SELECT s.id, s.attempt FROM execution_shard s JOIN execution e ON e.id = s.execution_id
WHERE e.task_id=?
  AND s.status='RUNNING'
  AND s.worker_id IS NOT NULL            -- 已认领才算运行(未认领的异常 RUNNING 行不参与超时)
  AND now() - s.started_at > make_interval(secs => ?)
```

`made_interval` 参数 = `maxRuntimeMs/1000.0`(double)。含 `worker_id IS NOT NULL` 守卫(未认领的 DUE/异常 RUNNING 行不参与超时判定,只有真正在跑的分片)——与 reliability 的 worker_id-null 裁决同姿态。返回 `ExpiredShard(id, attempt)`(复用该 record)。

- **回收动作复用现有回收路径**:`markStatus(run.id(), FAILED, workerId, "runtime timeout")` 命中 → `failureResolver.handle(task, id, attempt, "runtime timeout")`(→ scheduleRetry 或 死信+fail-fast)。detail 统一 `"runtime timeout"`。
- worker 迟到写回由 owner 归属守卫拒(worker_id 已被对账方改写)→ 结果丢弃 = 硬超时语义(太晚)。
- 心跳仍新的活 worker 超预算(还在跑但超时)与死 worker(心跳陈旧)两分支互不干扰,都汇入同一 fail 路径。
- leader 门控沿用:ReconcileLoop.tick 在 scan 前把关,Reconciler 本身不新增门控。

## 防双跑与一致

- 超时/预算都走既有 markStatus CAS + FailureResolver 判定 → 复用 worker_id 归属守卫,不产生双写;被抢 shard 由 DUE 幂等重跑、不重。
- 预算/超时判窗均用 DB now()(与 reliability「DB 权威时钟」一致);RetryPolicy 退避仍用 JVM clock(FailureResolver 现有语义)。
- attempt 语义:超时回收沿用 shard 当前 attempt(与对账现有传入一致)。

## 配置变更汇总

| 配置 | 位置 | 类型 |
|---|---|---|
| Task `retry_mode` / `retry_cap_ms` / `retry_budget_ms` | task 行(V9)+ DTO | 新增 |
| Task `max_runtime_ms` | task 行(V9)+ DTO | 新增 |
| shard `retry_budget_until` | shard 行(V9) | 新增 |
| claim 重置 `started_at=now()` | JdbcShardRepository.claim | 行为修正 |

## 测试计划(Testcontainers + DB now)

1. **RetryPolicy(纯单元)**:三模式退避值、任务级 cap、无 cap 兜底 1h、linear/exponential 溢出守卫;`delayMs(Task, attempt)` 新签名各分支。
2. **持久层**:`retryBudgetExhausted` 预算未设/未到/已耗尽;claim 重置 started_at(首次认领 → now;重试重新认领 → 更新的 now,非旧值)。
3. **ReconcilerTest**:设了 `max_runtime` 的 RUNNING shard(started_at 拨旧)→ 被铁 FAILED + outcome(runtime timeout);对照组没设 max_runtime 不被铁;worker 活但超时也被铁(CHECK 心跳仍新鲜的场景);未认领(worker_id null)的异常 RUNNING 行不被超时分支误收。
4. **FailureResolverTest**:W 耗尽(预算到点)即便 maxRetries 有余也转死信;未设 W 按次数重试(现状回归)。
5. **配置回归**:老任务默认值下行为不变(exponential + 1h + 仅次数),既有测试应全绿。

## Non-goals(YAGNI;本 spec 不做)

- **React 任务表单加这 4 字段**:列后端 DTO 后续 UI 小步。
- **线程级中止超时(B 方案)**:选分布式 A,不碰 worker 并发/心跳模型。
- **预算窗口「自成功延期/自最后失败起算」**:定为自**首次失败**起算(护栏简化;若需更复杂语义另行评估)。
- **告警/通知、审计 UI**:独立子项目,不在本 spec。

## Spec 自审

- 占位符:无 TBD/TODO;SQL/列名/签名已核准(`retry_mode`/`retry_cap_ms`/`retry_budget_ms`/`max_runtime_ms`/`retry_budget_until`;`delayMs(Task,int)`/`retryBudgetExhausted`/`findOverRuntime`→ 复用 `ExpiredShard`)。
- 内部一致:超时与预算均 DB now;重试决策仍无时钟(时钟侧上移 FailureResolver/scheduleRetry);防双跑复用归属守卫,不新增机制。
- 作用域:聚焦重试策略化 + 执行超时,单一实现计划可控;旧默认全兼容、既有测试应全绿。
- 歧义已裁决:started_at 每轮重置;超时/预算 detail 统一文案(`runtime timeout`);W 自首次失败起算;max_runtime 判超时仅当 worker_id 非空。