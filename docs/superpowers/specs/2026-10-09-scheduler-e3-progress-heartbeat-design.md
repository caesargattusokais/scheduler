# Scheduler E3 — progress-heartbeat:区分「慢而健康」vs「真卡住」

> **规划路径**:E(调度纵深)子项第三件,E2B(stuck-alive 可观测)之后的分辨力补位。
> **前置**:E2B stuck-alive 视图/告警/强制放弃、E1 统一活性门 + attempt 栅栏、worker 续租线程(renewLease 所有权守卫)。
> **状态**:设计中
> **更新日期**:2026-10-09

---

## 1. 背景与问题

E2B 的 stuck-alive 判据复用了 task 的 `timeout_seconds` + `started_at`,并如实记录了一条已知局限(E2B spec §3 末):

> 无「最近推进」字段,只能看 `started_at` 年龄。**慢而健康的超长任务**会被标记为 stuck——靠运维解读区分。后续区分「慢 vs 真卡」需加 `last_progress_at`,不在本设计范围。

即:E2B 把一个「仍在你一格一格往前推、只是单轮预算很长」的分片,和一个「handler 已经冻结、永远不再动」的分片,**混成了一个信号**。运维被逼着靠经验猜。

**本设计的缺口(对照已核实源码):**
- `HandlerContext` 是不可变 `record`,无任何可供 handler 上报"我仍在推进"的回调。
- `execution_shard` 无"最近推进时间"列;`started_at` 每次 claim 重置(started_at=now(),见 JdbcShardRepository:179),是唯一进度锚点。
- worker 的续租线程(ExecutorWorker 的 `renewer`,scheduleWithFixedDelay @renewSeconds)已经周期性、所有权守卫地续租,是现成的"运行期按时写库"通道,可承载进度落库。
- `ShardRepository` 已有 `recordResultPayload(shardId, ownerWorkerId, payload)` 这种独立写回先例,可镜像出 `recordProgress`。

**照映出的解法**:给 handler 一个显式 `touch()` 心跳,落成 `last_progress_at`;stuck-alive 判定改按"距最近一次推进多久"算,而非"距启动多久"。有推进的分片不再被误标,冻结的分片照旧浮现。

---

## 2. 设计原则与边界(先说破)

**进度心跳是纯观测信号,不是第二根活性权威。** 这直承项目「租约与超时分离、单一权威」的一等纪律:

- **不碰活性**:`touch()` 不改 `lease_until`,不改 `worker.status/last_seen`,不触发任何回收/重排/死信。并发与回收仍由 E1 的「owner ALIVE 活性门 + attempt 栅栏」独家判定。
- **不碰超时(时长)**:`touch()` 不影响 `timeout_seconds` 的协作取消——CancellationToken 照旧按 run 总时长到点请求取消,"慢而健康"的分片仍会被协作取消拦住,不会因为一直 touch 就无限跑。
- **不自动回收**:进度陈旧只让分片**进入 stuck 视图/告警**;任何回收仍走既有的显式运维动作(RUNNING 兜底)或活性门(owner 非 ALIVE)。
- **单一时钟(DB)**:`last_progress_at` 由 DB `now()` 在落库时盖章,JVM 只传"有没有推进"这个布尔语义,不传时间戳——避免 JVM/DB 时钟偏斜污染"进度年龄"(同 e2b 的 abandon 活性门纪律)。

**本设计选择**:新字段 `last_progress_at` + 新任务阈值 `progress_timeout_seconds`,二者都**可空**,缺省即退化到 E2B 现状——对既有任务/handler 零行为变化。

---

## 3. 判定语义(渐进式)

`progress_timeout_seconds` 就是一个"进度陈旧预算":一次推进之后,若超过该时长再无推进,判定分片卡住。

**一条 RUNNING 分片被定义为 stuck-alive,当且仅当:**

```
status='RUNNING'
AND worker_id IS NOT NULL
AND 属主 ALIVE(EXISTS worker ALIVE + last_seen >= now()-staleAfterSeconds)
AND (
  -- 分支 A:未 opt-in progress 监控 → 沿用 E2B(复用 timeout_seconds + started_at)
  (t.progress_timeout_seconds IS NULL OR t.progress_timeout_seconds = 0)
    AND t.timeout_seconds > 0
    AND now() - s.started_at > make_interval(secs => t.timeout_seconds::double precision)
  OR
  -- 分支 B:已 opt-in → 改按"距最近推进"算;COALESCE 兜底:从不 touch 也至少从 started_at 起算
  (t.progress_timeout_seconds > 0)
    AND now() - COALESCE(s.last_progress_at, s.started_at) > make_interval(secs => t.progress_timeout_seconds::double precision)
)
```

- **分支 A = 现状**,`progress_timeout_seconds` 为 NULL/0 的任务完全不受影响。
- **分支 B** 用 `COALESCE(last_progress_at, started_at)`:老 handler 若被配上 progress 阈值但从不 `touch()`,也不会"永久健康"——至少从 `started_at` 起算会被标 stuck。
- **`timeout_seconds` 与 `progress_timeout_seconds` 解耦**:分支 B 不再要求 `timeout_seconds>0`。一个"无 run 总时长上限、但有进度陈旧预算"的任务是合法组合。

> 语义边界:进度落库新鲜度滞后续租周期(≤`renewSeconds`,默认 15s)。`progress_timeout_seconds` 应在分钟级,远大于该抖动;这是"合并 flush"写回方式的既定代价(见 §5),如实接受。

---

## 4. 数据模型(迁移 V32)

```sql
ALTER TABLE execution_shard ADD COLUMN last_progress_at timestamptz;  -- NULL = handler 从未 touch
ALTER TABLE app_task       ADD COLUMN progress_timeout_seconds int;   -- NULL/0 = 不启用 progress 监控
```

- `last_progress_at` 每次推进落库即置 `now()`;`started_at` 仍每次 claim 重置(leave unchanged)。
- `progress_timeout_seconds` 进 task 领域模型、repository upsert、`/tasks` create/update 字段与校验、web 表单与类型。

---

## 5. Handler 契约与数据流

### 5.1 上报:`HandlerContext.touch()`

- worker/handler 新增 **`ProgressSignal`**(可变 holder):`AtomicBoolean progressed`;`void touch()` 置位;`boolean consume()` 读并清零。**纯布尔**——只表达"刚有过推进",不携带时间戳/百分比。
- `HandlerContext` record 增一组件 `ProgressSignal progress`;增实例方法 `void touch()`(`progress == null` 时 no-op,否则 `progress.touch()`,兼容 `of(...)` 便捷构造与既有测试)。
- `ExecutionHandler` 契约不变;handler 在**自己的批处理循环内周期性调用 `ctx.touch()`**。既有 4 个 handler(`echo`/`demo`/`myjob`/`sync-demo`)各自在 per-chunk / per-loop 边界补调用点,作为示范。

### 5.2 落库:`ShardRepository.recordProgress`(写)

镜像 `recordResultPayload` 的独立写回,但带 renewLease 同款 attempt 栅栏:

```java
/** E3 进度落库:DB now() 盖章,所有权 + attempt 栅栏(镜像 renewLease);0 行 = 已被接管/重认领,静默。 */
boolean recordProgress(long shardId, String ownerWorkerId, int expectedAttempt);
```

```sql
UPDATE execution_shard SET last_progress_at = now()
WHERE id=? AND status='RUNNING' AND worker_id=? AND attempt=?
```

- **DB 盖章**:不传 JVM 时间戳(单一时钟纪律)。
- **attempt 栅栏**:与 renewLease 一致,旧 attempt 的迟触发不会覆盖已被重认领分片的进度。

### 5.3 合并 flush(worker)

`ExecutorWorker.workOne()` 内,handler 运行前已建立 `ProgressSignal`,并注入 `HandlerContext`。既有周期续租 `renewer`(scheduleWithFixedDelay @renewSeconds)在**每次 `renewLease` 成功后**,若 `progress.consume()` 返回 true,调用 `shards.recordProgress(shard.id(), workerId, shard.attempt()+1)`。

- 复用现成续租线程/所有权守卫,**零新线程、零新定时器、handler 紧循环任意调用 touch() 也不产生 DB 写放大**(至多每续租周期一次)。
- 短运行 handler(一 run 即返回 SUCCESS)天然不需要进度:它要么已终态,要么还没超过任何预算。

### 5.4 读取(stuck 视图/告警)

- `ShardRepository.findStuckAliveRunning(Long taskId, int staleAfterSeconds)` / `countStuckAliveRunning(int staleAfterSeconds)` 的 SQL 谓词按 §3 重写,`StuckShard` record 增 `Instant lastProgressAt`。
- `ExecutionController.StuckView` 增 `lastProgressAt` + `progressAgeSeconds`(`now - COALESCE(last_progress_at, started_at)`),与既有 `ageSeconds`(`now - started_at`)并列,让运维一眼区分「慢而健康」vs「真卡住」。
- **告警规则不改**:`stuck-alive` 仍消费 `countStuckAliveRunning`,阈值热键 `alert.stuck-alive` 照旧——谓词已含新语义。

---

## 6. API / UI 面

- 任务字段:`progress_timeout_seconds`(可空 int)进 `POST/PUT /api/v1/tasks`、`GET` 序列化、`TaskController` 校验(与 `timeout_seconds` 同级:非负;NULL/0 语义等价"未启用")。
- web:`Task` 类型 + 任务表单加"进度预算"输入;`/stuck` 页 StuckRow 加 `lastProgressAt` + `progressAgeSeconds` 两列。
- OpenAPI 快照 additive 重导(`GET /executions/stuck` 响应列 + task 字段)。

---

## 7. 测试策略

- **persistence**:V32 迁移列存在(SchemaSmokeTest);`findStuckAliveRunning`/`countStuckAliveRunning` 的分叉用例——(a) opt-in 且 `touch` 后未超预算 → 不列;(b) opt-in 且超预算未 touch(COALESCE 兜底)→ 列;(c) 未 opt-in → 沿用 timeout/started_at 旧谓词;(d) 死属主 → 不列(走普通回收);(e) 慢而健康(touch 间歇小于预算)→ 始终不列。
- **worker**:`ProgressSignal` 单元;`ExecutorWorker` 合并 flush 时序(handler touch → renew tick 落 `last_progress_at`)与 attempt 栅栏(stale flush 0 行)。
- **server**:`ApiIntegrationTest` 对 `/executions/stuck` 显示进度两列 + abandon 不受影响;`AlertEngineTest` 计数一致性。
- **web**:`npm run build` 类型绿。

---

## 8. 边界 / 明确不做

- **不做**自动回收/重排/死信——进度陈旧只是观测 + 显式运维(force-abandon 门控仍只看 owner ALIVE,与 progress 无关)。
- **不做**值语义进度(百分比/计数/阶段)——`last_progress_at` 是纯时间戳,回答"多久没动了"。
- **不做**覆盖协作取消:`timeout_seconds` 与 CancellationToken 原样独立,`touch()` 不给 run 续命。
- **不做**新告警规则——`stuck-alive` 规则与阈值不变。
- **不做**对 `started_at` 语义的任何改动。

---

## 9. 遗留 / 后续

- 若未来需要更细粒度的"进度证据"(如已处理行数),再评估 `last_progress_at` 旁挂 `progress_payload`;本期不展开。
- E 系列后续两件(handler 侧幂等去重契约、对账行为可观测化)另立 spec,不在本设计范围。