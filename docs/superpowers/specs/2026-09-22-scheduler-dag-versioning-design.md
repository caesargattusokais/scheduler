# Scheduler 1c DAG 版本化 — 设计

**日期**:2026-09-22
**路线**:四方向 · 1c(跨 DAG 依赖 1d 之后)

## 目标
「一个 run 用它创建那一刻的定义;之后定义怎么改,在飞/历史的 run 都不受影响。」
做法:createRun 时把整份定义**封印进 run 自身**,DagEngine 推进只读 run 封印快照,永不回头读定义表。同时引入 DAG 编辑(PUT)使"编辑即 version++"真正启用版本化。

## 核心不变量
- 一个 run 的 **拓扑(边)、节点行为(node_max_retries/node_backoff_ms/run_if)、归属定义版本(dag_version)** 全部在 createRun 物化时冻结进 run。
- 定义表(`app_dag/app_dag_node/dag_edge`)保持**单版原地改**(编辑重写节点/边 + version++)。因为 run 已封印,原地重写对在飞 run 零影响。
- DagEngine 推进**只读跑封印快照**(run 节点 + `dag_run_edge`),不读当前定义。此前的裂缝点在 V25 前是"节点身份读 run 快照、拓扑与重试每轮重读当前定义"——当定义唯一且不可变时两边恰好一致;引入编辑后若仍重读会套新拓扑,故必须封印。

## 已拍板的设计决策
1. **交付范围** = 运行封印 + 引入编辑(不引版本号于 run 外的东西;无历史浏览/回滚)。
2. **封印机制** = run 级快照(能力列进 `dag_run_node` + 新 `dag_run_edge` 存边;定义行进**不带** `dag_version`,编辑原地改 + version++)。

## V25 迁移
- `ALTER TABLE app_dag ADD COLUMN version INT NOT NULL DEFAULT 1;`
- `ALTER TABLE dag_run ADD COLUMN dag_version INT;`(这版 run 用的定义版本,溯源)
- `ALTER TABLE dag_run_node ADD COLUMN run_if TEXT, ADD COLUMN node_max_retries INT NOT NULL DEFAULT 0, ADD COLUMN node_backoff_ms BIGINT NOT NULL DEFAULT 5000;`
- 新表 `dag_run_edge(id, dag_run_id FK→dag_run, from_node_id FK→dag_run_node, to_node_id FK→dag_run_node)`
- **历史回填**(版本化前定义不可变 → 当前定义值即旧 run 实际所用,回填保历史语义):
  - 行为列:按 (dag_run.dag_id, node_key) 从 `app_dag_node` 回填 run_if/node_max_retries/node_backoff_ms(COALESCE 缺省 'all_success'/0/5000)。
  - 边:按 (dag.dag_id, node_key) 把 `dag_edge` 归位到 `dag_run_edge`(from/to 各落到 run 内对应 node)。

## createRun 封印
物化游(幂等 by idempotency_key)时:
- `dag_run.dag_version` = `app_dag.version`(重放时以当次读到的 version 覆盖无妨——同 key 重放不重插节点/边,只返回既有 run)。
- 节点快照补齐:从 `app_dag_node` 拷 **run_if/node_max_retries/node_backoff_ms** 进 `dag_run_node`(现已快照 task_id/sort_order/node_key)。
- 边封印:从 `app_dag_edge` 按 node_key 归位到新建 run 的 node id,插入 `dag_run_edge`。
- 重放命中既有 run → 不重插节点/边(existing==0 判断不变)。

## DagEngine 改读封印定义
- `DagRunNode` 记录 += `runIf, nodeMaxRetries, nodeBackoffMs`(封印值),新增便捷 13-参旧构造保既有调用方零改动。
- 新 `DagRunEdge` 记录(`id, dagRunId, fromNodeId, toNodeId`)。
- `propagateRun` 删除 `findNodes(run.dagId())`/`findEdges(dagId)`(不再构建 defByKey/keyByNodeId 自当前定义):
  - 上游拓扑读 `dags.findRunEdges(run.id())`,node_id→node_key 映射用 run 节点自身。
  - `stepPendingNode/stepRunningNode` 的重试/join 判断改用 run 节点上的封印值(RUNNING/PENDING 节点的 runIf/nodeMaxRetries/nodeBackoffMs)。
- 定义表只在 create/edit 用;运行侧完全解耦。历史 run(若无回填)按封印列处理。

## 编辑端点(PUT)
- `PUT /api/v1/dags/{id}`:改 name/description/cron∥dependsOnDagId/节点/边(含每节点重试与 run_if)。
- 校验与 create 完全一致(task 存在、nodeKey 唯一、边引用已有键、无自环、DFS 无环、密钥唯一、依赖存在 + 链无环、cron 与 dependsOn 恰其一)。
- 实现(事务):删旧 `dag_edge` + `app_dag_node` → 重插节点/边(共享 `insertDefinition` 助手,create/update 复用避免校验两处漂移)→ 更新 `app_dag` 头(`paused/enabled` 不动)+ `version = version + 1`。
- 审计 `dag.update`:before 全量快照 + field diff(name/description/cron/dependsOnDagId;镜像 TaskController taskDiff)。节点/边变更在 diff 的范围外(以 before 快照可溯源)。
- 单节点重跑/cancel/pause/resume 全部作用在 run 封印快照上 → 不受影响。

## 前端
- DagsPage:选中 DAG 加「编辑」入口,新建弹窗复用为编辑态;从 `dagDetail` **预填**(名称/描述/触发源 cron∥依赖 / 步骤含重试与 run_if / 上游勾选),保存走 `updateDag(id, req)`。
- `Dag` 类型 += `version`;client += `updateDag`;`UpdateDagRequest` 复用 `CreateDagRequest`(`dependsOnDagId` 可空、`version` 由服务端自增,前端不送)。

## 测试(边界 TDD)
- 持久层:updateDag → version++、节点/边改写回读;createRun 封印 version + 节点行为 + 边;`findRunEdges` 回读;历史回填正确性(走 migration,靠边界语义测试兜底)。
- 引擎(最关键):建 A→B 触发 run1 → 编辑为 A→C → **run1 仍按 A→B 推进(封印)**,新建 run2 按 A→C;改节点重试预算,在飞 run 用旧值。
- 集成:PUT 200 + version++ 回显;校验 400(缺名 / 恰其一 / 坏环 / 不存在 404);编辑后原 run detail 仍一致;pause/resume/cancel/rerun 不受影响。
- 前端 build 绿。

## 范围外(YAGNI)
- 版本历史浏览 / 任意版本回滚 / 发布(草稿/生产)管理;定义行进带 `dag_version`。定义表保持单版。