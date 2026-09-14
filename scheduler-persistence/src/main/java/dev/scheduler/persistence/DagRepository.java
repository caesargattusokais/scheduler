package dev.scheduler.persistence;

import dev.scheduler.core.Dag;
import dev.scheduler.core.DagEdge;
import dev.scheduler.core.DagNode;
import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunNodeStatus;
import dev.scheduler.core.DagRunStatus;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** 工作流 DAG 接口(spec §1)。定义表建 DAG 时写;运行表由 DagEngine 写;DagEngine 是运行表唯一写者。 */
public interface DagRepository {

  // ---- 定义 ----
  record NodeInput(String nodeKey, long taskId, int sortOrder) {}
  record EdgeInput(String from, String to) {}

  /** 建 DAG:dag + 节点 + 边同事务。校验:引用的 task 存在、nodeKey 唯一、边引用已有 nodeKey、无自环、
   *  DFS 无有向环、边唯一。任一违反抛 IllegalArgumentException(400)且整事务回滚。返回建出的 dag 头。 */
  Dag createDag(String name, String description, String cron,
                List<NodeInput> nodes, List<EdgeInput> edges);

  Optional<Dag> findDag(long id);
  List<Dag> findAllDags();
  /** DAG 列表:name 子串(ILIKE)过滤 + limit/offset 分页(ORDER BY id)。 */
  List<Dag> findDagsPage(String name, int limit, int offset);
  /** 与上同过滤条件(不含分页)的 DAG 全量计数。 */
  long countDags(String name);
  /** enabled 且非 paused 且 cron 非空(镜像 TaskRepository.findCronEnabled)。 */
  List<Dag> findCronEnabledDags();
  List<DagNode> findNodes(long dagId);
  List<DagEdge> findEdges(long dagId);
  void setPaused(long dagId, boolean paused);

  // ---- 运行 ----
  /** 调度触发:单事务建 dag_run(PENDING)+ 全部 dag_run_node(PENDING, task_id 快照)。幂等 by key(重放自愈)。 */
  DagRun createScheduledRun(long dagId, Instant triggerAt);
  /** 手动触发:同 createScheduledRun,幂等键 = manual:{uuid}。 */
  DagRun createManualRun(long dagId);
  Optional<DagRun> findRun(long runId);
  Optional<DagRunNode> findNode(long nodeId);
  /** run 列表;dagId 空则全部,ORDER BY id DESC。 */
  List<DagRun> findRuns(Long dagId);
  /** run 列表分页:dagId 空则全部 + status 过滤 + limit/offset,ORDER BY id DESC。 */
  List<DagRun> findRunsPage(Long dagId, String status, int limit, int offset);
  /** 与上同过滤条件(不含分页)的 run 全量计数。 */
  long countRuns(Long dagId, String status);
  /** 未终态 run(stored status='PENDING'),由 DagEngine 每周期推进。 */
  List<DagRun> findActiveRuns();
  /** 某 run 的全部节点,ORDER BY sort_order, id。 */
  List<DagRunNode> findNodesOfRun(long runId);
  /** 某 run 的非终态节点(status IN (PENDING, RUNNING)),取消级联用。 */
  List<DagRunNode> findNonTerminalNodes(long runId);

  /** 惰性 spawn:置 execution_id + RUNNING,落 node_outcome(RUNNING,'spawned')。CAS on status='PENDING':
   *  0 行=已推进 → 返回 false。 */
  boolean markNodeSpawned(long nodeId, long executionId);

  /** 节点状态迁移:read-validate 后 CAS on 读取到的当前态 + 落 node_outcome;非法迁移抛
   *  IllegalStateException;CAS 0 行=行已被他方改走 → 返回 false,不落误导性 outcome。 */
  boolean markNodeStatus(long nodeId, DagRunNodeStatus to, String detail);

  /** dag_run 终态:CAS on status='PENDING'(stored 只 PENDING→终态),落 run_outcome + finished_at。
   *  CAS 0 行=已终态 → 幂等返回 false,不落误导性 outcome。 */
  boolean finalizeRun(long runId, DagRunStatus terminal, String detail);

  /** 单节点重跑(仅 operator):把终态节点回绕到新一轮运行——CAS on status IN (SUCCESS,FAILED,SKIPPED,CANCELED)
   *  置 status='RUNNING'、execution_id 重挂新 execution、finished_at 清 NULL;并把 dag_run 重开为 'PENDING'
   *  (finished_at=NULL)使 findActiveRuns 重新纳入、引擎随后重派生。同事务落 dag_run_node_outcome(RUNNING,'node rerun')。
   *  CAS 0 行=节点已非终态/竞态 → false,不落 outcome。调用方必须是 DagEngine(引擎是运行表唯一写者)。
   */
  boolean rerunNodeToExecution(long runId, long nodeId, long newExecutionId);

  /** 置 run 取消请求标志(cancel_requested=true);仅 PENDING run,静默跳过已终态。 */
  void requestCancelRun(long runId);

  /** 某 dag 下未终态 run 计数(metrics/active gauge)。 */
  long countActiveRuns(long dagId);

  /** 全局未终态 run 计数(跨所有 dag;metrics 全局 gauge,懒查 → 新建 DAG 无需重启即计入)。 */
  long countActiveRuns();
}
