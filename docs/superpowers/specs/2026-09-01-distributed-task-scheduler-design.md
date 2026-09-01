# 分布式业务任务调度平台 — 设计文档

> 日期:2026-09-01
> 目录:独立新项目 `D:\ai-project\scheduler`
> 目标:**做得对**,不是快速拿到结果。本项目优先正确性与架构质量,不为赶进度降配。

---

## 0. 项目要旨

自研一个"阿里级"分布式业务任务调度系统的可运行代码库。调度能力对齐 SchedulerX 一类产品的核心面,但技术选型自定、可控、可演进。

**设计取向**:诚实反映一次执行在分布式下的真实会话(session)语义,而非玩具式"定时跑一下"。所有可靠性问题(重复派发、孤儿、重试、并发)以设计的一等公民处理,不事后打补丁。

### 已锁定的决策(本会话确认)

| 维度 | 决策 |
|------|------|
| 交付目标 | 真实可运行的代码库 |
| 项目归属 | 独立新项目,自选栈(不受 zfj / usql 约束) |
| v1 能力 | 四项齐全:核心调度循环 + 可靠性三件套 + 分片并行 + 工作流 DAG |
| 分布形态 | 调度器集群 + 执行器集群,**DB 认领派发 + 对账器**(方案①,否决 MQ/直推) |
| 存储 | PostgreSQL |
| 语言 | Java 21 LTS |
| 框架 | Spring Boot 3.x |
| 数据访问 | **Spring JDBC + 手写 SQL**,不使用 JPA/Hibernate(认领/租约/对账的原子与锁语义要裸写才直白可控) |
| 测试 | JUnit 5 + Testcontainers(PostgreSQL)集成测试;核心状态机纯单元测试不碰库 |
| 构建 | Maven 多模块 |

### 演进顺序(里程碑,每个里程碑均端到端可运行)

1. **M1** 核心调度循环(定时触发、触发/执行解耦、task/execution 两层、并发配额)
2. **M2** 可靠性三件套(超时、心跳/租约防孤儿、幂等重试 + 死信 DLQ)
3. **M3** 分片并行执行
4. **M4** 工作流依赖编排(DAG)
5. **M5** 管理控制台(独立 React SPA,消费 M1 起的稳定 REST API)

> 里程碑在同一代码库里演进,并非一次到位。每步都能用、能验证;后补极贵的可靠性在 M2 就位,不最后补。控制台 M5 亦可自 M2 后以最小运维页起步逐步叠加。

---

## 1. 第一区段(已确认):顶层架构 + 领域模型 + 执行状态机

### 1.1 顶层架构

```
                      ┌──────────────────────────────────────┐
                      │          Scheduler 集群 (多节点)        │
                      │  ┌────────┐  ┌────────┐  ┌────────┐   │
                      │  │  Leader │  │ standby│  │ standby│   │
                      │  └────────┘  └────────┘  └────────┘   │
                      │    触发引擎  工作流引擎  分片编排         │
                      │         对账器 (reconciler)            │
                      └───────────────────┬──────────────────┘
                                          │  读/写 执行记录(写 DUE 派发)
                                          ▼
                              ┌────────────────────────┐
                              │  PostgreSQL              │
                              │ task / execution / ...    │
                              └────────────────────────┘
                                          ▲  认领/心跳/回写
                      ┌───────────────────┴──────────────────┐
                      │          Executor 集群 (N 台)          │
                      │  认领循环/心跳   → 执行处理器(任务插件)   │
                      └────────────────────────────────────┘
          REST API(创建/暂停/触发/重跑/查询)  ──→  Scheduler 集群
```

- **Leader 选举**:PostgreSQL **advisory lock**。仅 Leader 执行全局扫描(触发引擎、工作流引擎、分片编排、对账器)。Standby 不干活,职责简单、无分治、无脑裂事务交叠。
- **派发不靠 push**:调度器只负责把"该跑的执行记录"写成 `DUE` 落库;**认领由执行器从库里主动拉** —— DB 认领派发天然防重复派发。

### 1.2 领域模型(核心:task/execution 两层彻底分离)

#### `task` — 任务定义(低频变更)
- `id`, `name`, `kind`, `handlerRef`(指向执行处理器)
- 触发配置:`cron`(定时) / `dependency`(依赖) / `manual`
- `shardCount`(=1 表示不分片)
- 可靠性配置:`timeoutSeconds`、`maxRetries`、`backoff`、**`retryableFailurePattern`**(区分可重试/不可重试失败——重试不打在不会好的失败上)
- `maxActiveConcurrent`(并发配额)
- `enabled` / `paused`

#### `execution` — 执行实例(高频写入,一次运行的完整快照)
- `id`(全局,贯穿日志/指标/追踪)、`taskId`、**`idempotencyKey`**(task + 触发时刻 + shardIndex)
- `status`(见 1.3)、`args`、`shardIndex` / `shardCount`
- `attempt`(第几次尝试)、`workerId`、`leaseUntil`(心跳租约基准)
- `startedAt` / `finishedAt` / `resultPayload`
- **触发信息与结果随 execution 走,不在 task 上** —— 任务定义与一次运行解耦的落点。

#### `task_dependency` — DAG 边
- `(taskId, dependsOn, condition)`。留给工作流引擎(里程碑 4)使用。

#### `execution_outcome` — 执行事件/变更日志(追加不覆盖)
- execution 每次状态迁移只追加一行,支撑**审计 / 追踪 / 人工追溯**。
- **注意**:它不是对账器的状态权威 —— 对账器走 **§2.2 的声明式 CAS 推导**(应然 vs 实然),不靠重放结果事件推进状态。outcome 只承担"发生了什么"的记录角色,两者分工明确、不冲突。

### 1.3 执行状态机(核心,后面一切围绕它)

```
                       ┌────────── retry(退避+抖动) ──────────┐
                       ▼                                     │
  NOT_CREATED → DUE ──认领──► RUNNING ──成功──► SUCCESS
                    (到期)    │  ▲                            │
                             │  │ 心跳续租 leaseUntil          │
                             │  └────────────────            │
                             │  失败(可重试) → FAILED(attempt)
                             │  失败(不可重试) → FAILED → DLQ(死信)
                             │  超时/租约过期 → ORPHANED → 对账器回收
                             ▼
                         CANCELED
                        (取消 → 回 DUE 可被重新认领,或终止)
```

**关键语义(写进状态机,而非散落 if):**
- **认领原子恰好一次**:执行器 `UPDATE … SET status='RUNNING', workerId=?, leaseUntil=? WHERE id=? AND status='DUE'`,影响行数 ×0 → 已被抢走,跳过。幂等由 `idempotencyKey` 兜底。
- **孤儿判定**:租约过期(`leaseUntil < now`)且状态 = RUNNING → 对账器标 ORPHANED →(尽力下发取消后)重排回 DUE 被重新认领。**宁可悬着等人处理,不允两个它抢同一份活。**
- **可重试失败 vs 不可重试失败**由 `retryableFailurePattern` 判定 → 走重试还是 DLQ,不无脑重试。

---

## 2. 第二区段(已确认):可靠性三件套 + 并发配额/背压

### 2.1 租约(活性)与超时(时长)分离

两个易混、必须分开的守护:

- **租约(lease)——回答"worker 还活着吗、还在有效占有这份活吗"**,处理进程崩溃。
  - 执行器每 `heartbeatInterval` 续租:`leaseUntil = 当前心跳时间 + leaseGrace`。
  - worker 猝死后,最多 `leaseGrace` 内 `leaseUntil` 过期 → 判孤儿回收。**这是"活性"的唯一权威来源。**
- **超时(timeout)——回答"run 是否超过允许业务时长"**,处理"活着但卡死/太慢"。
  - 主路径**协作式取消**:框架把 deadline/`CancellationToken` 交给处理器,到点请求取消、处理器协作停止。
  - 框架层**不**对"租约还新鲜但超时"的 run 强行收回 —— 否则与仍活着的 worker 抢同一份活,制造重复。
- **结论**:超时是"建议 + 协作",租约是"孤儿回收的权威",不混用。宁可让"超时但还活着"的 run 协作取消,也不在它还占租约时再派一份。

### 2.2 对账器(reconciler)

- 仅 **Leader** 跑,周期扫描(秒级,批量 + LIMIT 分页,耐心自愈,**绝不在一次遍历里承诺清完**)。
- **声明式对账**:从触发器/租约/重试计划推导"应然",用**原子条件更新(CAS)**把库拉向应然(非事件重放,不引入 event-sourcing)。
- 四类检查:
  1. **孤儿回收(租约过期)**:`UPDATE … SET status='ORPHANED' WHERE status='RUNNING' AND leaseUntil < now`(`leaseUntil` 也进 WHERE 构成 CAS)。影响 0 行 → 已接管,跳过;命中 → 尽力下发取消 → 有重试额度则重排回 `DUE`,否则 `FAILED → DLQ`。CAS 使其在选主切换、并发认领间不双重回收。
  2. **触发补发(scheduler 崩溃恢复)**:触发生成幂等 —— 按"截至 `now-lookback` 应有哪些执行"重算,靠 `idempotencyKey` 唯一约束 UPSErt 缺失的 `DUE`。Leader 崩溃恢复后自然补漏,不需独立故障恢复流程,调度器崩溃安全。
  3. **重试到点重排**:`FAILED` 且 `nextRetryAt` 到 → 原子翻回 `DUE`(attempt++)。
  4. **陈旧状态兜底扫描**:整理遗留异常态。
- **核心纪律**:每次更动都是一条"带旧态条件 + 结果"的**原子 SQL,影响行数即并发裁决**,没有"读了判断再写"的非原子窗口。

### 2.3 幂等重试 + 死信 DLQ

- **重试**:可重试失败 → attempt++,`nextRetryAt = 当前 + backoff(attempt) + jitter`,状态 → `FAILED`(非立刻成功)。到点由对账器(检查 3)重排 `DUE`。`attempt >= maxRetries` → 终结。
- **死信 DLQ**:不可重试失败或重试耗尽 → `FAILED`,并把 execution + 失败详情 + args 快照写入 `dlq` 表,**永不自动重试**,供人工查看/重放 —— 兜底要有人看得见、做得动。
- **幂等**:`idempotencyKey` + 唯一约束兜底 —— 同一 (task, 触发时刻, shardIndex) 不可能产生两个 execution,双触发/恢复补发都过不了。**框架保证"同一 execution 至多一个 RUNNING"**;最终效果的幂等是**处理器契约**(至少一次 vs 恰好一次的最终差别落在 handler),框架提供 key,业务方用 key 去重。

### 2.4 并发配额 + 背压

- **准入在认领时原子裁决**:认领 `DUE` 前仅当该 task 当前活跃数(租约新鲜 + RUNNING)< `maxActiveConcurrent` 才允许认领。语义定死:**没有 run 准入能越过配额**。
- **触发侧也受配额约束**:定时到但已在配额顶 → 新触发**等待(积压 DUE)**,不跳过、不无限叠;除非 `allowOverlap`。
- **背压发生在派发前**:调度器不为注定跑不了的量写 DUE;堆淤表现为 `DUE` 积压 + **队列老龄化指标**(最老 DUE 延时),而非无限膨胀的 RUNNING。数据完整优先,积压可度量、可告警。
- **可观测基线**:每 task 活跃数、DUE 队列最深年龄、重试率、超时/孤儿率、DLQ 深度。

---

## 3. 第三区段(已确认):分片并行执行

**核心判断**:分片是"一个 execution 的展开 + 一个汇聚父态",**不是 N 个独立任务**;父态只能由对账器原子推导,不能让 shard worker 抢着写。

### 3.1 数据模型:分片是同一触发的展开,不发明新实体

- 一次触发 → **一个 `execution`(父,shardCount=N)**,不拆成 N 个独立 execution。
- 真正跑的原子单元是 **`execution_shard`**:`executionId, shardIndex(0..N-1), shardData, status, workerId, leaseUntil, attempt, …`。
- 父 `execution` 持总体状态(自各 shard 汇聚);每个 shard 复用完整生命周期(认领/租约/重试/孤儿/超时/死信)。
- **分片边界上做完整可靠性,复用而非另起炉灶**:一个 shard 即一个 mini-execution,`idempotencyKey = executionId + shardIndex`,天然唯一、天然幂等。

### 3.2 分片如何产生(`shardData` 两种情况,统一 `Partitioner` 接口)

- **A. 路由器式**(数据来源决定分片):从业务表读一批主键,按 `uid % N` 等切分;shardData 是键区间/查询谓词。
- **B. 静态分桶式**:预定义 N 个均匀 bucket(如分省);shardData 是每个 bucket 的枚举。
- 框架只认 `Partitioner` 输出 `List<shardData>`;分片方式由 handler 决定,框架负责并行与汇聚的正确性。
- **坑规避**:分片数据源与执行数据源分叉后各 shard 又重读全量 —— `Partitioner` 产出"轻量定位符"(键/谓词),不把数据搬进 shard 表。

### 3.3 并行与汇聚语义

- **并行性由 worker 数决定,不由 shardCount 决定**:N 个 shard 分布到可调度执行器;**每个 shard 仍受 task 并发配额约束**(与单一 run 一致),分片不等于放开配额。
- **v1 不做跨 shard 依赖**(如"省份依赖前序"):每个 shard 独立、无顺序假设,汇聚只认"全部完成"。分片内顺序留给工作流引擎(M4),不在分片里发明第二套排序。
- **汇聚(Sink)**:父在**所有 shard 达终态**后才进终态:
  - 全部 SUCCESS → 父 SUCCESS;
  - 任一 shard 失败且耗尽重试 → 父 FAILED,保留部分成功细节;
  - **`shardFailPolicy` 可配置**:**`FAIL_FAST`**(任一失败 → 尝试取消其余,默认,更省资源、语义更明确)或 `COMPLETE_ALL`(其余照跑,结果汇总标部分失败)。
- **部分结果不丢**:父 FAILED 时已完成 shard 证据保留于 execution_outcome,可人工重放**单个失败 shard**(非整个 execution)。

### 3.4 分片边界上的幂等与一致性

- **shard 级 idempotencyKey** = executionId + shardIndex:单 shard 重试/孤儿回收可安全重放。
- **父状态由 shard 状态推导、不靠"最后一个写者"覆盖**:父 completion 由 Leader 对账器在"全部 shard 达终态"时**一次原子**推进,不靠 shard worker 并发叠加终结父 —— 避免两个 shard 同时想终结父产生竞态/覆盖部分结果。

### 3.5 第三区段边界定型

- 支持路由器式与静态分桶式 `Partitioner`,统一接口。
- 不做 shard 间顺序/依赖(留给 M4 工作流)。
- 汇聚走对账器原子推进,`shardFailPolicy` 可配置,默认 FAIL_FAST。
- 单 shard 重放、部分结果保留。

---

## 4. 第四区段(已确认):工作流依赖编排(DAG)

**核心判断**:工作流的强大恰恰来自克制(scoping)——压在"批量触发 + 依赖推导 + 成功才推进 + 失败挂起人工 + 单节点重跑"这一小层,其余(自动回滚、整轮重试、DAG 超时)全部显式宣布不做。想要更多,要付"第二套执行引擎"的代价。

### 4.1 建模:复用 execution 状态机,不发明第二套执行引擎

- 工作流 = 一组 `task`,由 `task_dependency` 表表达 DAG 边连起来。
- **没有独立"工作流执行器"**:每个 task 的 run 仍是普通 `execution`,走同一套认领/租约/重试/超时。
- **触发单元是 DAG 的一个"批次(batch)"**:一次下游触发,把批次内可启动 task 各自生成 `DUE` execution;批次推进由工作流引擎处理。

### 4.2 拓扑与依赖解析

- 依赖解析按**入度 + 就绪**推导:当且仅当**依赖的祖先全部 SUCCESS**(且自身 cron/manual 条件满足)时,生成该节点 execution 为 `DUE`。
- **声明式推导、不用每节点回调**:DAG 每时刻"可启动节点集合"是 `nodes ⊆ DAG` 且入度 0 的闭包,由对账器按当前批次状态**周期推导**,不强推、不靠成功后 push 下一节点。

### 4.3 分支与下一个 batch 的语义(最难,必须钉死)

- **DAG 不是只跑一轮**:下游触发后,批次内"上次全部成功"才算一次成功批次。
- **"本轮批次何时结束"**:某节点失败 → **默认整轮挂起(pause)等人工**,不做自动回滚/整轮自动重试。自动回滚 DAG 是另一个深渊。
- 人工二选一续跑:
  - **A. 重跑失败节点**(仅 re-run 该节点,SUCCESS 祖先不动)→ 继续到该轮自然完成;
  - **B. 显式终止/标记批次失败** → 记录批次为 FAILED。
- **失败分支不作为、不重试整轮**:失败即挂起,不 throw、不 retry-DAG。

### 4.4 单批次幂等与人工界面

- 批次 = `workflow_run`(一个 batch 实例),持状态、开始时间;**停在同批次内使"只重跑一个失败节点"可行,且不触发新批次**。
- 同批次节点 idempotencyKey 带批次序号 → 重跑失败节点不产生"新版本",而是原 batch 语义下的新 attempt / 新 execution。
- 人工重跑单节点 = 原 `execution`(已 FAILED)→ 重新 `DUE` 被重新认领,不重新触发其余成功节点。

### 4.5 第四区段边界定型(本设计**不做**)

- **不做自动回滚**;成**卖不做整轮自动重试**;**不做超时整轮判定**。
- 语义可完成"依赖 + 失败分支 + 人工重跑单节点",但对账器推进 batch 用了 `workflow_run` 状态推导,粒度较大。
- 这是四区段中最需实现时再压缩的一段:设计坚持"极简 + 显式不做";实现若遇超界需求,先回来改设计,不硬塞。

---

## 5. 第五区段(已确认):REST API 面 + 可观测

### 5.1 控制面与执行面分离(Api 走控制面,不进执行热路径)

- 创建/暂停/重跑/查询是低频人工操作(REST);高频的执行认领/心跳不是 REST —— 执行器直接走 DB。**别把 DB 认领和 HTTP API 混成一个入口**。
- Spring Boot 应用,一个 HTTP 层无脑转领域服务;领域逻辑在 service,不入 controller。

控制面 API(`/api/v1` 前缀):

| 方法 | 路径 | 用途 |
|------|------|------|
| POST | `/api/v1/tasks` | 创建任务(触发/可靠性/分片配置) |
| GET | `/api/v1/tasks` | 列出/筛选任务 |
| GET | `/api/v1/tasks/{id}` | 任务详情 |
| PUT | `/api/v1/tasks/{id}` | 更新配置(触发/可靠性/配额) |
| POST | `/api/v1/tasks/{id}/pause` · `/resume` | 暂停 / 恢复 |
| POST | `/api/v1/tasks/{id}/trigger` | 手动触发一次(生成 execution) |
| POST | `/api/v1/tasks/{id}/rerun` | 重跑(指定 execution / 批次) |
| GET | `/api/v1/executions` | 列表/筛选(任务、状态、时间窗) |
| GET | `/api/v1/executions/{id}` | 执行详情(含 shards、时间线) |
| POST | `/api/v1/executions/{id}/retry` | 重跑单个 run 节点 |
| POST | `/api/v1/executions/{id}/cancel` | 取消 run(尽力下发取消) |
| GET | `/api/v1/dlq` | 死信清单(人工查看/重放) |

**执行处理器注册(第二个接口,非 REST)**:`handlerRef` 字符串 → handler 实例的**自动发现/注册表**,`task.handlerRef` 路由到实现 —— 它是"worker 知道跑哪段代码"的关键。建议取向:**注解扫描 + 配置映射**(落到实现计划定稿)。

### 5.2 可观测:三根支柱 + 兜底界面

单一可追踪性,REST 请求 → 每个 execution/shard → 日志/指标,贯穿 `executionId` / `shardId`。

- **Metrics(Micrometer / Prometheus)**:每 task 活跃 RUNNING、DUE 队列最深年龄(背压核心指标)、重试率、超时率、孤儿率、DLQ 深度、Worker 数/存活、调度与执行吞吐、认领竞态率。
- **Logs**:结构化 log,每条带 `executionId`/`taskId`/`shardId`/`workerId`,统一 trace 前缀。
- **Traces**:一次 run 全链路 trace(触发 → 认领 → 处理器 → 回写)。
- **兜底界面**:看板(卡在哪、最老积压多久、哪些进 DLQ)+ DLQ **可操作**(重跑/暂停),**别只进日志**。

### 5.3 第五区段边界

- 控制面 REST 只做低频人工操作,不进热路径。
- 处理器 `handlerRef` 未找到时**启动失败即摸排(不静默)**。
- 三支柱 + DLQ 兜底界面;**不引入自建 K8s/编排**;看板轻量选型(VictoriaMetrics + Grafana 或原地起步),马到实现计划定具体。

---

## 6. 第六区段(已确认):管理控制台(独立 React SPA)

> 核心纪律:**控制台是控制面,只走 `/api/v1` REST,绝不直连执行库。** 与执行器认领走 DB 是两回事——执行走 DB、人工操作/查看走 API,不撬开"控制面与执行面分离"。

### 6.1 技术栈与工程形态

- React 19 + Vite + TypeScript(与 portfolio 同栈),独立 npm 工程 `scheduler/web/`。
- 只消费 `/api/v1` REST;构建产物可由 Spring Boot 静态托管或独立部署。

### 6.2 页面面(聚焦可运维,非展示型 demo)

- **任务**:列表/创建/编辑(触发/可靠性/分片/配额)、启用/暂停、手动触发、重跑。
- **执行**:实时列表(任务/状态/时间窗筛选)、详情(含 shards、时间线、args、result)、取消。
- **DLQ**:死信清单、查看详情、人工重放。
- **工作流**:DAG 批次视图、批次状态、单节点重跑、批次终止。
- **指标面板**:核心运维指标只读展示 —— 每 task 活跃数、DUE 队列最深年龄、重试/超时/孤儿率、DLQ 深度、worker 存活。

### 6.3 指标来源与边界

- Metrics 经 Prometheus 端点暴露,控制台面板做只读展示;**不引入自建 K8s/编排**;Grafana 作为可选深挖,控制台内嵌核心面板为默认。

### 6.4 里程碑与边界

- 控制台归 **M5**(后端 M1–M4 能力稳定后,消费稳定 REST API 构建);也可在 M2 后以最小可用运维页起步(M1 起的 REST API 已就绪)逐步叠加,具体排进实现计划。
- 界面聚焦"能看卡在哪、能动 DLQ、能人工重跑/暂停、能看积压老化",不作展示型 demo。

---

## 7. 整体设计判断(复述)

把"阿里级"翻译成**六条可执行的纪律**:
1. **认领即原子**(DB CAS,恰好一次);
2. **租约与超时分离**(活性 vs 时长,不混用);
3. **对账器每次更动一条 CAS SQL**;
4. **分片父态只由对账器推导**(不靠 shard worker 抢写);
5. **工作流极简 + 显式不做**(不做自动回滚/整轮重试/DAG 超时);
6. **控制面与执行面分开**(API 低频人工,执行走 DB)。

没有一条靠魔法,都是"语义钉死 + 边界显式"。

---

## 8. 遗留待定(马到实现计划时收敛)

- 处理器注册的具体机制(注解扫描 + 配置映射)。
- 指标暴露端点与控制台面板的数据契约。
- scheduler 根目录名(暂用 `scheduler`)/ git 初始化(**已建**,`main` 分支,spec 已提交)。
- Maven 模块切分与每个里程碑的验收定义(见实现计划)。
- **工作流 DAG**:依赖触发、失败分支、人工重跑单节点、条件执行。
- **REST API 面**与**可观测**(metrics / 日志 / 追踪 / 看板兜底)。
- **演进路径**:从老板式 DB 认领到 MQ(若未来需要)的抽取边界。

---

## 3. 明确否决(避免返工)

- 未引入分布式一致性协调器(ZooKeeper 等)——用 PostgreSQL advisory lock 承载选主,保持派发语义落在真库上。
- 不在 v1 用 **JPA/Hibernate**。
- 不在 v1 用 **MQ 作主派发通道**(至少一次语义 + 仍需对账器兜底,MQ 不省事反更重)。
- 不做单进程内嵌执行器(方案 A)作为目标形态 —— 偏离"做好"的取向。