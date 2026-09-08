# Scheduler M4 — Workflow DAG (任务依赖编排) 设计

> 工作流的里程碑细化。主 spec（`2026-09-01-distributed-task-scheduler-design.md` §4）已锁定本里程碑的形状；本文档把 §4 落到可实现的确定性规格。Governing：**正确性优先 + 数据完整优先**（用户全局指令），每项设计无歧义、可被 Plan 逐字引用。本 spec 建立在 M1-M3 已落地并在 `main` 之上（parent-`execution` 为汇聚头、`execution_shard` 为运行单元、Reconciler 对账、FAIL_FAST、取消级联、幂等、带锁选主）—— M4 **不改 worker 运行时**,只在其上叠 DAG 调度层。

## 0. 方针与 v1 裁剪（已获用户裁定）

- **依赖 DAG,无图算子**:一个工作流只表达「已注册任务↔已注册任务」的先后依赖,不引入条件分支/动态扇出/汇聚器等图执行算子。节点 = 引用一个已注册 `app_task`(该任务自带 shardCount 并行)。条件等需求以多个 lock 任务表达,YAGNI。
- **节点引用已注册任务,不内联定义**:`app_dag_node.task_id` → `app_task`。运行时该节点即「用该任务的 `createParentWithShards` 生成一次执行(父+shards),由现有 worker 照常运行」。全部经 M3 打磨的底子(幂等/对账/取消/FAIL_FAST)原样复用。
- **DagEngine 是 dag 表的唯一写者,worker 无感知**:worker 只认领/运行 `execution_shard`,完全不感知 DAG。DagEngine 从 spawned 节点 execution 的 shards **只读**派生节点终态(镜像 Reconciler 对父执行的做法,但写到 `dag_run_node`)。Reconciler 保持 execution/shard 层不变,互不触碰对方表。
- **节点 execution 惰性生成**(用户裁定):节点 PENDING → 其所有上游全 SUCCESS 才调 `createParentWithShards` 生成 execution(spawn)。上游失败/取消的下游**不白跑**(永不 spawn→SKIPPED/CANCELED)。
- **有向无环在创建时强制**(用户裁定):`POST /dags` 建 DAG 时 DFS 拒绝成环与自环 → 400。运行时假定 DAG。
- **SKIPPED 与 CANCELED 区分**(用户裁定):上游自然失败 → 下游 `SKIPPED`;操作者取消 → `CANCELED`。审计诚实;终态推导多一条优先级规则(见 §3.3)。
- **失败/取消传播跨周期收敛(镜像 M3 FAIL_FAST 跨周期哲学)**:节点只在其上游全终态时被判定就绪/跳过,同一 scan 内不风暴式连锁;下游 SKIPPED/终态由后续 scan 收敛。确定性、无竞态含糊。
- 沿主 spec:v1 DAG 无跨 dag_run 的依赖或时序(各自独立执行),无重跑单节点端点(defer 到管理后台里程碑,节点历史由 `dag_run_node_outcome` 保留证据)。

## 1. 数据模型(V4 迁移)

### 1.1 定义表(建 DAG 时写,运行期只读)

```sql
CREATE TABLE app_dag (
  id          BIGSERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  description TEXT,
  cron        TEXT,                          -- 6/7 字段(同 app_task 校验)
  enabled     BOOLEAN NOT NULL DEFAULT true,
  paused      BOOLEAN NOT NULL DEFAULT false,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE app_dag_node (
  id          BIGSERIAL PRIMARY KEY,
  dag_id      BIGINT NOT NULL REFERENCES app_dag(id) ON DELETE CASCADE,
  node_key    TEXT NOT NULL,                 -- DAG 内唯一别名(A/B/C)
  task_id     BIGINT NOT NULL REFERENCES app_task(id),
  sort_order  INT NOT NULL DEFAULT 0,
  UNIQUE (dag_id, node_key)
);

CREATE TABLE dag_edge (
  id          BIGSERIAL PRIMARY KEY,
  dag_id      BIGINT NOT NULL REFERENCES app_dag(id) ON DELETE CASCADE,
  from_node_id BIGINT NOT NULL REFERENCES app_dag_node(id) ON DELETE CASCADE,
  to_node_id   BIGINT NOT NULL REFERENCES app_dag_node(id) ON DELETE CASCADE,
  UNIQUE (dag_id, from_node_id, to_node_id),
  CHECK (from_node_id <> to_node_id)          -- 无自环(底层兜底;创建时亦校验)
);
```

- 节点幂等 = `(dag_id, node_key)` 唯一;边幂等 = `(dag_id, from, to)` 唯一(重复边创建时 400 拒绝而不是重复插)。
- **建 DAG 原子**:dag + 节点 + 边同一事务插入(镜像 `createParentWithShards` 单事务原则)。创建时服务层 DFS 从每个入口节点校验无有向环;存在环 → `IllegalArgumentException`(400)且整个事务回滚。
- 每个节点引用已存在 task(存在性校验,否则 400)。

### 1.2 运行表(触发时建,由 DagEngine 写)

```sql
CREATE TABLE dag_run (
  id              BIGSERIAL PRIMARY KEY,
  dag_id          BIGINT NOT NULL REFERENCES app_dag(id),
  idempotency_key TEXT NOT NULL UNIQUE,       -- 调度:dag:{id}:{triggerAtEpochMs};手动:dag:{id}:manual:{uuid}
  status          TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING | SUCCESS | FAILED | CANCELED(见 1.3)
  trigger_reason  TEXT NOT NULL,              -- scheduled | manual
  cancel_requested BOOLEAN NOT NULL DEFAULT false,
  finished_at     TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_dag_run_dag_status ON dag_run (dag_id, status);

CREATE TABLE dag_run_node (
  id            BIGSERIAL PRIMARY KEY,
  dag_run_id    BIGINT NOT NULL REFERENCES dag_run(id) ON DELETE CASCADE,
  node_key      TEXT NOT NULL,
  task_id       BIGINT NOT NULL REFERENCES app_task(id),   -- 触发时快照
  execution_id  BIGINT REFERENCES execution(id),           -- 惰性 spawn 后填(=该节点生成的父 execution)
  status        TEXT NOT NULL DEFAULT 'PENDING',           -- PENDING | RUNNING | SUCCESS | FAILED | CANCELED | SKIPPED
  sort_order    INT NOT NULL DEFAULT 0,
  detail        TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at   TIMESTAMPTZ,
  UNIQUE (dag_run_id, node_key)
);
CREATE INDEX idx_dag_run_node_run_status ON dag_run_node (dag_run_id, status);

CREATE TABLE dag_run_outcome (
  id          BIGSERIAL PRIMARY KEY,
  dag_run_id  BIGINT NOT NULL REFERENCES dag_run(id) ON DELETE CASCADE,
  status      TEXT NOT NULL,
  detail      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE dag_run_node_outcome (
  id          BIGSERIAL PRIMARY KEY,
  node_id     BIGINT NOT NULL REFERENCES dag_run_node(id) ON DELETE CASCADE,
  status      TEXT NOT NULL,
  detail      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- 运行节点幂等 = `(dag_run_id, node_key)` 唯一;`dag_run.idempotency_key` 唯一(调度重放不重复建 run)。
- `dag_run_node.task_id` 在触发时**快照**(镜像 M3 父 execution 快照 task 语义)——即便任务此后被改,该 run 的执行定义不变。`execution_id` 惰性填,Lazy-spawn 后链到该节点生成的父 `execution`。
- 新枚举:`DagRunStatus {PENDING, SUCCESS, FAILED, CANCELED}`;`DagRunNodeStatus {PENDING, RUNNING, SUCCESS, FAILED, CANCELED, SKIPPED}`。独立于 `ExecutionStatus`,不复用。

### 1.3 存储状态语义(镜像 M3 父头不落中间态)

- `dag_run.status` 存储仅 `PENDING`(创建)→ 终态(`SUCCESS/FAILED/CANCELED`);**不落 RUNNING**。有任一非终态节点的 dag_run,读侧显示 `RUNNING`(派生,只读映射,不写库)。
- `dag_run_node.status` 为真实运行单元状态机:`PENDING → RUNNING → 终态(SUCCESS|FAILED|CANCELED|SKIPPED)`,全部落库(RUNNING = 该节点已 spawn、执行在途)。
- **节点终态经 `dag_run_node_outcome` 落审计;dag_run 终态经 `dag_run_outcome` 落审计**;SKIPPED/CANCELED 传播均带 `detail` 说明。

## 2. 触发与创建 run

`DagEngine.scanOnce()`(单 scan,镜像 TriggerEngine + Reconciler 合一的节奏)分两阶段:

### 2.1 触发(DAG cron tick)

- 对每个 `enabled` 且未 `paused` 的 `app_dag`,检查其 `cron` 在固定 Clock 的当前 tick 是否命中(复用 6/7 字段解析与 tick 边界判定,与 `TriggerEngine` 同逻辑)。
- 命中 → 同事务:插 `dag_run(status=PENDING, key=dag:{id}:{tickEpochMs}, trigger_reason=scheduled)` + 按 `sort_order` 插全部 `dag_run_node(status=PENDING, task_id=快照)`;幂等键冲突 → 已存在则跳过(重放自愈,镜像 M3 cron 路径)。
- 手动触发布 (trigger endpoint §5)、幂等键 `dag:{id}:manual:{uuid}`(镜像 TaskController.trigger)。

### 2.2 传播(核心:推进已有 dag_run)

对每个 `status=PENDING`(未终态)的 `dag_run`,对其每个非终态 `dag_run_node` 应用 §3 规则。单次 `scanOnce` 做一遍;收敛允许多周期(跨周期哲学,§0)。

## 3. 传播与终态派生规则(确定性,无歧义)

### 3.1 节点就绪/跳过/取消判定(PENDING 节点)

设 `D` = 该节点的直接上游节点集合(读 `dag_edge` 到本节点)。对 PENDING 节点 N:

1. 若 `D` 中**任一**为终态 `FAILED` 或 `SKIPPED` → N → **SKIPPED**;`detail="upstream failed"`,落 `dag_run_node_outcome(SKIPPED)`。**绝不 spawn**。
2. 否则,若 `D` 中任一为终态 `CANCELED`(且无 FAILED/SKIPPED)→ N → **CANCELED**;`detail="upstream cancelled"`,落 outcome。(操作者取消向 PENDING 下游传导,见 §4 与本处的 read-side 收敛一致。)
3. 否则(其余情形——含「D 非全终态」与「D 全 SUCCESS」):
   - 若 `D` **全 SUCCESS** → N **就绪**:调 `createParentWithShards(N.task_id, "dag:{dag_id}:run:{run_id}:node:{node_key}", task.shardCount())` 生成父 execution+shards;回填 `N.execution_id`;N → **RUNNING**(`detail="spawned"`);压入 **执行队列**让 worker 认领(worker 循环照常,无新机制)。
   - 若 `D` 未全终态 → N 保持 PENDING,本周期不动作(等上游再扫描收敛)。

> 根节点(无上游)在 run 创建后的首次传播即满足「全 SUCCESS 的空真」→ 立即 spawn RUNNING。

### 3.2 RUNNING 节点终态派生(只读看 shards,镜像 Reconciler 对父执行)

对 RUNNING 节点 N(已 spawn,`execution_id` 非空):
- 读 `execution_id` 对应父 execution 的全部 `execution_shard`;若**全终态**(SUCCESS/FAILED/CANCELED):
  - 任一 shard `FAILED` → N → **FAILED**;`detail="shards failed"`。
  - 否则若任一 shard `CANCELED` → N → **CANCELED**;`detail="shards cancelled"`(该节点自带 FAIL_FAST/取消已把兄弟 shard 收 Terminal)。
  - 否则(全 SUCCESS)→ N → **SUCCESS**;`detail="all shards ok"`。
  - 落一条 `dag_run_node_outcome`。
- shards 未全终态 → 保持 RUNNING。
- 节点 FAILED/CANCELED 后,**不向兄弟节点风暴取消**(跨周期 SKIPPED 收敛,§0);该节点 execution 内部的兄弟 shard FAIL_FAST/取消由现有执行链路处理,与 DagEngine 无关。

### 3.3 dag_run 终态派生(CAS,仅写一次)

当一次 dag_run 的**全部节点终态**:
- 任一节点 `FAILED` 或 `SKIPPED` → dag_run → **FAILED**;`detail="node failed"`。
- 否则若任一节点 `CANCELED` → dag_run → **CANCELED**;`detail="node cancelled"`。
- 否则(全节点 SUCCESS)→ dag_run → **SUCCESS**;`detail="all nodes ok"`。
- CAS 写 `dag_run.status` 从非终态→终态(`WHERE status='PENDING'`,同 M3 `finalizeParent` 的 CAS-0→幂等跳过)落 `finished_at` + 一条 `dag_run_outcome`;CAS-0 → 已终态,幂等跳过。

> 优先级镜像 M3 父汇聚:FAILED/SKIPPED 先于 CANCELED 先于 SUCCESS。

## 4. 取消级联(dag → node → execution → shard)

`POST /dags/runs/{runId}/cancel`(镜像 ExecutionController.cancel 父级联的 dag 版):

- `dag_run = findById`;不存在 → 404;已 `CANCELED` → 幂等 200;终态 `SUCCESS|FAILED` → 409(不可取消)。
- 置 `dag_run.cancel_requested=true`。
- 对该 run 每个非终态节点:
  - **PENDING**(未 spawn)→ 节点 → `CANCELED`,`detail="cascade cancel"`,落 outcome;**不 spawn**。
  - **RUNNING**(spawned)→ 节点 → `CANCELED` + outcome;并取消其 execution:该父 execution 有 RUNNING shard → `requestCancelParent(executionId)`(协作);否则 → `cancelParentImmediate(executionId)`(硬取消全 shard)。**完全复用 M3 的父取消仓储方法**,把取消逐级传导到 shards。取消把该 run 全部节点一次性置终态 → 下一周期扫描(§3.3)本 run 收 `CANCELED`。
- 返回 200 + 派生当前态。
- 取消是最终语义:被取消节点(或 dag_run)不因上游/重试翻回——CAS 终态不可逆。
- **确定性**:对 RUNNING 节点,即使其 execution 的 shards 已全部 SUCCESS 但 DagEngine 尚未派生出节点 SUCCESS 时操作者 cancel,取消仍胜出(节点 → CANCELED)。此为镜像 M3 已裁定语义(「cancel-after-complete-but-before-reconcile, cancel wins,可辩护」)的 dag 层同级表述,确定性一致、可辩护;同 M3 记为 deferred-Minor 而非缺陷。

## 5. 控制面(`/api/v1/dags`)

- `POST /dags` 建 DAG(§1.1:task 存在 + 无自环 + DFS 无环 + 节点/边唯一)。请求体 `{name, description?, cron, nodes:[{nodeKey, taskId, sortOrder?}], edges:[{from, to}]}`;校验失败 → 400(IllegalArgumentException)。
- `GET /dags` 列表;`GET /dags/{id}` 详情(dag + nodes + edges)。
- `POST /dags/{id}/pause|resume`(镜像 task pause/resume):暂停后该 dag 不再被 cron 触发;已进行的 dag_run 不受影响。
- `POST /dags/{id}/trigger` 手动 → 建 dag_run + 全 PENDING 节点,返回 dag_run。
- `GET /dags/runs` 列表(可 `dagId` 过滤);`GET /dags/runs/{runId}` 详情:dag_run(派生 RUNNING)+ 全节点(每节点带其 execution_id 及该 execution 的 shards)+ 派生终态路径。
- `POST /dags/runs/{runId}/cancel`(§4)。
- 沿用既有 HTTP 惯用法(`ResponseEntity`/`ResponseStatusException`、403/404/409/200/202),注入 `DagRepository`/`TaskRepository`/`ShardRepository`。

## 6. 指标(prometheus,镜像 per-task 系列)

`Beans` 增一个 `MeterBinder dagMetrics(TaskRepository…)→DagRepository`:对每个已注册 `app_dag` 注册 per-dag 系列(带 `dag_id`/`dag_name` 标签,镜像既有 per-task 系列与「上下文绑定时刻注册、新 DAG 重启后出现」的已知重启边界):
- `scheduler_dag_runs_active` = 该 dag 非终态 dag_run 计数(gauge)。
- `scheduler_dag_run_nodes_pending` = 该 dag 全部 dag_run 中 PENDING 节点计数(gauge)。
- `scheduler_dag_runs_total` = 该 dag 累计 dag_run 数(gauge,读 `dag_run` 计数)。v1 至少两类 active+total;pending 可 defer。

## 7. 测试计划(确定性,镜像既有 harness)

- `SchemaSmokeTest`:V4 迁移应用(V1-V4 校验通过,3→4)。
- `JdbcDagRepositoryTest`(persistence):建 DAG+节点+边单事务;重复节点 key/重复边 → 拒绝;**成环/自环 → 建 DAG 失败(400);幂等 dag_run(调度 key 重放 → 一条);就绪节点查询;节点终态派生;dag_run 终态派生 + CAS-0 幂等;取消级联;SKIPPED 传播**。TRUNCATE 增 `app_dag, app_dag_node, dag_edge, dag_run, dag_run_node, dag_run_outcome, dag_run_node_outcome`。
- `DagEngineTest`(server,镜像 ReconcilerTest fixed-CLOCK + 同步 scanOnce harness):**线性 A→B→C 全成功**(逐次 scanOnce 驱动 spawn,worker 跑 shard,Reconciler 聚父,scanOnce 派生节点终态,末次 dag_run SUCCESS);**上游 FAILED → 下游 SKIPPED → dag_run FAILED**(上游不 spawn 下游,证明惰性);**并行扇入**(A→B 且 A→C:B、C 均待 A SUCCESS 才 spawn);**手动触发 + 取消级联**(dag PENDING 节点 CANCELED、RUNNING 节点其 execution 的 shards 被级联 CANCELED、终态 CANCELED)。
- `ApiIntegrationTest` 新 E2E + 既有全绿:建 DAG(含成环 400)→ 手动 trigger → 驱动 `DagEngine.scanOnce` + `executorWorker.workOne` + `reconciler.scanOnce` → dag_run SUCCESS 链路;FAILED 变体;cancel 级联端点。既有 57 server 测试不回归。
- 全模块绿:`scheduler-persistence`(58 现有 + 新增)、`scheduler-server`(57 现有 + 新增),根 reactor `mvn -q test` exit 0。

## 8. 对 Plan 的确定性承诺

- **明确定义、逐字可引用**:§1 建/运行表、§2 触发、§3 传播/终态规则、§4 取消、§5 端点、§6 指标、§7 测试,均给出无歧义语义与优先序。
- **我司不变量(务必保住)**:父 `execution` 与 `execution_shard` 的既有迁移/约束 `V1/V2/V3` **不动**;Reconciler 不改;`ExecutionStatus`/`ExecutionTransitions`/`Shard` 复用;只新增 dag 表 + 新枚举 + DagEngine + 控制面 + 指标,与 M3 各层对称、不交叉写对方表。
- **Ruling 记录**:本 spec 已把用户三项裁定(惰性 spawn、创建时机 400 防环、SKIPPED/CANCELED 区分)固化为 §0/§3 可执行规则;实现期出现的偏差一律记 `Ruling:` 进 ledger。