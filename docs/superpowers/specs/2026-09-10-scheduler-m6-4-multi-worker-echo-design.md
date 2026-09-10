# M6.4 多 worker 实跑 + 第二个真实 handler —— 实现级设计

- 日期：2026-09-10
- 上一级：`2026-09-10-scheduler-m6-distributed-worker-design.md` §6 里程碑 **M6.4**（"1 控制 + N worker 实跑；新增第二个真实 handler 证明'不改控制面/前端加任务类型'"）；并**收敛**该 spec 开放项 **§8.5**（worker 的 workerId 生成与持久）。
- 载体：`feat/m6-4-multi-worker`
- 状态：草拟，待评审

## 0. 范围与纪律

M6.3 已把控制面与执行面拆成进程并**剔除 server 侧全部 handler**。M6.4 把"横向扩展"与"加任务类型零改动"这两条 M6 承诺**坐实到可运行**：同一控制面下多个 worker 进程并行分摊同一任务的分片；新增第二种真实 handler 只动 worker 侧，server 与前端零改动。

沿用并强化 M6/M6.3 纪律：
- **server 与 DB schema 零改动**。M6.4 的全部主代码落在 `scheduler-worker`（workerId 唯一、echo handler、result_payload 写通路）与一行可选的前端展示列。
- worker 各自注册并解析 `handlerRef`；加任务类型 = 在某个 worker 的 `HandlerRegistry` 注册新 `ref()`，控制面与前端自动可见。
- 横向扩展 = 更多 worker 进程指向同一 DB，靠原子认领分摊 DUE shard，无协调、无新中间件。
- 不引入每执行子进程 / jail、不改 broker、不加跨语言 SPI（沿 M6 spec §7 边界）。

## 1. 目标与验收锚点

| # | 目标 | 验收锚点 |
|---|------|---------|
| G1 | 1 控制 + N worker 实跑 | 同一任务分片被 ≥2 个 worker 进程认领分摊执行；执行详情中每个 shard 的 `worker_id` 各异 |
| G2 | 不改控制面/前端加任务类型 | 仅 worker 侧新增 `echo` handler；`/api/v1/handlers` 自动变为 `["demo","echo"]`，前端创建任务下拉框自动多出 `echo`（下拉框本就读 `/handlers`，零前端改动） |
| G3 | 新 handler 全链走通 | `echo` 任务建表→手动触发→各 shard 执行→父终态全链成功；result_payload 写入并有值 |

## 2. workerId 进程唯一（收敛 §8.5）

**问题**：`WorkerConfig.defaultWorkerId()` 现返回 `worker@<hostname>`。同一主机上启动两个 worker 进程会得到相同 id，`upsertHeartbeat` 相互覆盖同一 `worker` 行 → 注册表只认一个，无法支撑 G1。

**改动**：默认 id 追加进程号。

```
worker@<hostname>:<pid>
```

- 用 Java 17 `ProcessHandle.current().pid()`，同机并发进程必不同 → 注册表各自成行，`worker_id` 归属可区分。
- PID 在崩溃后**可能复用**：新进程以相同 id 接续在途租约（保留原"重启后不换 id、接续租约"意图）；即使不复用，新 id 自成一帧，旧行因 `last_seen` 超 30s 窗口自然淡出存活视图——自愈，无陈旧 ALIVE 累积。
- `SCHEDULER_WORKER_ID` 显式覆盖仍优先（demo/E2E 用 `worker-b` 等手钉 id）。

**验证**：worker 模块测试断言默认 id 含 hostname + pid 后缀；E2E 起两个进程看两条存活行。

## 3. 第二个真实 handler `echo` + result_payload 写通路

`result_payload`（`execution_shard`，V3）、`worker_id` 列、API 字段（`ExecutionDetail.shards[].workerId`/`resultPayload`）、web 类型都已存在，但**没有任何写通路**：`ExecutionHandler.handle()` 返回 void，SUCCESS 的 detail 恒为常量 `"ok"`。让 `echo` 达成"带执行归属"的最小通路如下（全部在 worker 模块）：

1. **`HandlerContext` 记录增加 `workerId` 字段**：`ExecutorWorker` 本就有该 id；`HandlerContext.of(...)` 便捷构造赋空默认，既有测试不改。
2. **`ExecutionHandler` 增加 `default String resultPayload(HandlerContext ctx){ return ""; }`**：非破坏性默认；`DemoHandler` 不覆盖（返回空）。
3. **`ExecutorWorker`**：SUCCESS 路径取 `h.resultPayload(ctx)`；非空则经新仓储方法写入 `execution_shard.result_payload`，带 `worker_id` 归属守卫（镜像 `markStatusOwned`）——回写若已被他方接管则静默丢弃。
4. **`EchoHandler`（ref `echo`）**：协作取消感知（镜像 `DemoHandler` 的 token 检查）；`resultPayload` 返回
   ```
   {"handler":"echo","workerId":"<id>","shardIndex":N,"echoed":"<args>","at":"<ts>"}
   ```

依赖：`echo` 仅依赖 `core`/`persistence`（同 handler 类纪律），不碰控制面。

## 4. 前端

- **"加任务类型零前端改动"成立**：下拉框数据源 `/handlers` 与 `handlersEndpoint` 均不变；`echo` 因 worker 注册而自动出现（G2）。M6 spec §8.4（保留 `/handlers`）就此定案。
- **可选增列展示**：执行详情 shard 表加一列渲染 `result_payload`（让"带执行归属"的 payload 在控制台直接可见）。约 3 行纯展示，与 handler 注册机制无关；不做也不影响 G2/G3，仅影响"payload 是否肉眼可见"。

## 5. 测试

- `EchoHandlerTest`：协作取消抛 `CancellationException`；`resultPayload` 形状含 `workerId`/`shardIndex`/`echoed`。
- **承重 N-worker 分摊测试**（worker 模块集成测试）：构造 2 个不同 workerId 的 `ExecutorWorker` 实例，共享同一已播种 N 个 `echo` 散片的 pool，交替 `workOne()` 至全终态；断言：① 每个 shard 归属非空；② 两实例各自取到的 shard 集**互斥**；③ 合起来 = 全部分片。从单元层证明多 worker 认领分摊（G1）。
- 默认 workerId suffix 单测（G2 前提）。
- 全 reactor 保持绿。

## 6. 实施顺序（bounded，独立 plan）

在 `scheduler-worker`、`scheduler-persistence`（新仓储方法）、`web`（可选列）三个模块内的小型改动，自成一个 plan、TDD、逐 Task 审查、全 reactor 绿：

| 步 | 内容 |
|----|------|
| T1 | workerId 进程唯一 + 单测 |
| T2 | `HandlerContext.workerId` + `ExecutionHandler.resultPayload` 默认 + `ExecutorWorker` 回写通路 + `result_payload` 仓储写方法（归属守卫） |
| T3 | `EchoHandler` + 注册 bean + `EchoHandlerTest` |
| T4 | N-worker 分摊集成测试（承重）+ 全 reactor 验证 |
| T5 | E2E 实跑 smoke + README 增补 |

## 7. 明确不做 / 边界（YAGNI）

- 不做每执行子进程 / jail（隔离粒度=worker 进程级，沿 M6 spec §7）。
- 不做 worker 间协调（认领依赖 DB 原子性）。
- 不做 `DRAINING`/`DEAD` 灰度下线写入时机（开放项 §8.3 留后续）。
- 不改 broker、不加鉴权/审计。
- 不把 result_payload 做成分片级"数据回传平台"——只落字符串、供展示/排查，不做结构化大对象。

## 8. 开放项收敛

1. **§8.5 workerId 生成**：本 design 收敛为 `worker@<hostname>:<pid>` 默认 + `SCHEDULER_WORKER_ID` 覆盖（§2）。
2. **§8.4 `/api/v1/handlers` 保留**：本 milestones 继续保留并作为"加任务类型零前端改动"的机制（§4）。
3. **§8.3 下线/灰度**：不收敛，留后续里程碑。
4. **§8.1 Flyway 所有权**：不收敛（沿用现状：server 跑迁移，worker `spring.flyway.enabled=false`）。