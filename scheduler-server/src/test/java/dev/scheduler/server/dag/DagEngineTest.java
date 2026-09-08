package dev.scheduler.server.dag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.scheduler.core.Dag;
import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunNodeStatus;
import dev.scheduler.core.DagRunStatus;
import dev.scheduler.core.IdempotencyKeys;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.DagRepository.EdgeInput;
import dev.scheduler.persistence.DagRepository.NodeInput;
import dev.scheduler.persistence.JdbcDagRepository;
import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.leader.AdvisoryLockLeaderElection;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * 真 PG + 固定时钟:确定性校验 DagEngine(spec §2 触发 + §3 传播/终态派生)。
 * 每 scanOnce 仅推进一层(跨周期收敛:单 scan 内下游仍见上游 scan-start 态),故逐层扫描驱动。
 */
@Testcontainers
class DagEngineTest {
  @Container static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>("postgres:16-alpine");
  private static final String CRON_EVERY_5 = "0 */5 * * * *";
  private static final Instant FIRED = Instant.parse("2026-01-01T10:05:00Z");
  private static final Clock CLOCK = Clock.fixed(FIRED, ZoneOffset.UTC);

  private static JdbcTemplate jdbc;
  private TaskRepository tasks;
  private ShardRepository shards;
  private DagRepository dags;

  @BeforeAll static void setUp() {
    Flyway.configure()
        .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
        .load().migrate();
    jdbc = new JdbcTemplate(new DriverManagerDataSource(
        PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()));
  }

  @BeforeEach void clearTables() {
    jdbc.execute("TRUNCATE app_dag, app_dag_node, dag_edge, dag_run, dag_run_node,"
        + " dag_run_outcome, dag_run_node_outcome,"
        + " execution, execution_shard, execution_shard_outcome, execution_outcome,"
        + " app_task RESTART IDENTITY CASCADE");
    tasks = new JdbcTaskRepository(jdbc);
    shards = new JdbcShardRepository(jdbc);
    dags = new JdbcDagRepository(jdbc);
  }

  // ---- 场景助手 ----

  /** 建一个按 key→task 映射定义节点、edges 定义边的 DAG;cron 非空才能被调度触发。 */
  private Dag newDag(String cron, Map<String, Long> byKey, List<String[]> edges) {
    Map<String, Long> keys = new LinkedHashMap<>(byKey);
    List<NodeInput> nodes = keys.entrySet().stream()
        .map(e -> new NodeInput(e.getKey(), e.getValue(), 0)).toList();
    List<EdgeInput> edgeInputs = edges.stream()
        .map(e -> new EdgeInput(e[0], e[1])).toList();
    return dags.createDag("d", null, cron, nodes, edgeInputs);
  }

  /** 边列表助手:以 varargs 形式给出 String[] 形如 {from, to}(避免 List.of(String[][]) 的泛型收窄歧义)。 */
  private static List<String[]> edges(String[]... es) {
    return List.of(es);
  }

  private long newTask(int shardCount) {
    return tasks.create(new Task(null, "t", "cron", "demo", null,
        shardCount, 300, 0, 1000, null, 5, true, false)).id();
  }

  private DagEngine engine(AdvisoryLockLeaderElection leader) {
    return new DagEngine(dags, tasks, shards, leader, CLOCK);
  }

  private DagRunNode node(long runId, String key) {
    return dags.findNodesOfRun(runId).stream()
        .filter(n -> n.nodeKey().equals(key)).findFirst().orElseThrow();
  }

  /** 直接以 SQL 把某节点 spawned execution 的全部 shard 置为终态(模拟 worker 跑完,让 DagEngine 派生节点终态)。 */
  private void setShardStatus(long runId, String nodeKey, String status) {
    jdbc.update("UPDATE execution_shard SET status=?, finished_at=now()"
            + " WHERE execution_id=(SELECT execution_id FROM dag_run_node"
            + " WHERE dag_run_id=? AND node_key=?)",
        status, runId, nodeKey);
  }

  private long runOutcomeCount(long runId, String status) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM dag_run_outcome WHERE dag_run_id=? AND status=?",
        Long.class, runId, status);
  }

  private long nodeOutcomeCount(long nodeId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM dag_run_node_outcome WHERE node_id=?", Long.class, nodeId);
  }

  // ---- 测试 ----

  /** cron tick:命中 → 建一条 scheduled dag_run,根节点在首次传播即被 spawn(spec §3.1 空真假)→ RUNNING;
   *  再扫幂等不加 run。 */
  @Test void trigger_cronTick_createsRunWithPendingNodes() {
    long taskId = newTask(1);
    Dag dag = newDag(CRON_EVERY_5, Map.of("A", taskId), List.of());
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      var engine = engine(leader);
      engine.scanOnce(); // 触发创建 run + 首次传播随即 spawn 根节点

      List<DagRun> runs = dags.findRuns(null);
      assertEquals(1, runs.size());
      DagRun run = runs.get(0);
      assertEquals(DagRunStatus.PENDING, run.status());
      assertEquals("scheduled", run.triggerReason());
      assertEquals(IdempotencyKeys.forDagTrigger(dag.id(), FIRED), run.idempotencyKey());
      List<DagRunNode> nodes = dags.findNodesOfRun(run.id());
      assertEquals(1, nodes.size());
      assertEquals(DagRunNodeStatus.RUNNING, nodes.get(0).status(),
          "根节点(无上游)在首个传播即满足全 SUCCESS 空真 → 立即 spawn RUNNING(spec §3.1)");
      assertNotNull(nodes.get(0).executionId(), "根节点已惰性 spawn，execution_id 已回填");

      engine.scanOnce(); // 幂等:同 tick 重放不再新建 run/执行
      assertEquals(1, dags.findRuns(null).size());
      assertEquals(1, dags.findNodesOfRun(run.id()).size());
    } finally { leader.close(); }
  }

  /** 线性 A→B→C:逐层 scan 驱动 spawn → worker 跑完 shard → scan 派生节点终态;末次全终态 → dag_run SUCCESS。
   *  跨周期:下游在 A 派生 SUCCESS 的同一 scan 仍见 A 的 scan-start RUNNING → 保持 PENDING,下一 scan 才 spawn。 */
  @Test void linearAtoBtoC_allSuccess_dagRunSuccess() {
    long t1 = newTask(1), t2 = newTask(1), t3 = newTask(1);
    Dag dag = newDag(null, Map.of("A", t1, "B", t2, "C", t3),
        edges(new String[]{"A", "B"}, new String[]{"B", "C"}));
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      var engine = engine(leader);
      DagRun run = dags.createManualRun(dag.id());

      engine.scanOnce(); // S1:A 根节点 spawn
      assertEquals(DagRunNodeStatus.RUNNING, node(run.id(), "A").status());
      assertNotNull(node(run.id(), "A").executionId());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "B").status());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "C").status());

      setShardStatus(run.id(), "A", "SUCCESS");
      engine.scanOnce(); // S2:A 派生 SUCCESS;B 本 scan 仍见 A scan-start RUNNING → PENDING
      assertEquals(DagRunNodeStatus.SUCCESS, node(run.id(), "A").status());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "B").status());

      engine.scanOnce(); // S3:B 上游 A 已 SUCCESS → spawn RUNNING
      assertEquals(DagRunNodeStatus.RUNNING, node(run.id(), "B").status());

      setShardStatus(run.id(), "B", "SUCCESS");
      engine.scanOnce(); // S4:B 派生 SUCCESS;C 跨周期保持 PENDING
      assertEquals(DagRunNodeStatus.SUCCESS, node(run.id(), "B").status());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "C").status());

      engine.scanOnce(); // S5:C spawn
      assertEquals(DagRunNodeStatus.RUNNING, node(run.id(), "C").status());

      setShardStatus(run.id(), "C", "SUCCESS");
      engine.scanOnce(); // S6:C 派生 SUCCESS;++ 全节点终态 → run SUCCESS
      assertEquals(DagRunNodeStatus.SUCCESS, node(run.id(), "C").status());
      assertEquals(DagRunStatus.SUCCESS, dags.findRun(run.id()).orElseThrow().status());
      assertEquals(1L, runOutcomeCount(run.id(), "SUCCESS"), "dag_run 落一条 SUCCESS outcome");
      for (DagRunNode n : dags.findNodesOfRun(run.id())) {
        assertEquals(2L, nodeOutcomeCount(n.id()),
            "每个节点至少 RUNNING(spawned) 与终态两行 outcome 落审计");
      }
    } finally { leader.close(); }
  }

  /** 上游 FAILED → 下游 SKIPPED(绝不 spawn,execution_id 保持 NULL)→ dag_run FAILED。 */
  @Test void upstreamFailed_downstreamSkipped_dagRunFailed() {
    long t1 = newTask(1), t2 = newTask(1);
    Dag dag = newDag(null, Map.of("A", t1, "B", t2), edges(new String[]{"A", "B"}));
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      var engine = engine(leader);
      DagRun run = dags.createManualRun(dag.id());

      engine.scanOnce(); // S1:A spawn
      assertEquals(DagRunNodeStatus.RUNNING, node(run.id(), "A").status());

      setShardStatus(run.id(), "A", "FAILED");
      engine.scanOnce(); // S2:A 派生 FAILED;B 跨周期保持 PENDING
      assertEquals(DagRunNodeStatus.FAILED, node(run.id(), "A").status());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "B").status());

      engine.scanOnce(); // S3:A 终态 FAILED → B SKIPPED(惰性,绝不 spawn);全节点终态 → run FAILED
      assertEquals(DagRunNodeStatus.SKIPPED, node(run.id(), "B").status());
      assertNull(node(run.id(), "B").executionId(), "下游 SKIPPED 未被 spawn(惰性证明)");
      assertEquals(DagRunStatus.FAILED, dags.findRun(run.id()).orElseThrow().status());
    } finally { leader.close(); }
  }

  /** 扇入:A 是 B、C 共享上游;B、C 均待 A SUCCESS 才 spawn,且同 scan 一并 spawn;全成 → run SUCCESS。 */
  @Test void fanIn_bothDownstreamsWaitForSharedUpstream_Success() {
    long t1 = newTask(1), t2 = newTask(1), t3 = newTask(1);
    Dag dag = newDag(null, Map.of("A", t1, "B", t2, "C", t3),
        edges(new String[]{"A", "B"}, new String[]{"A", "C"}));
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      var engine = engine(leader);
      DagRun run = dags.createManualRun(dag.id());

      engine.scanOnce(); // S1:A spawn RUNNING;B、C 均 PENDING(共享上游未终态→等)
      assertEquals(DagRunNodeStatus.RUNNING, node(run.id(), "A").status());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "B").status());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "C").status());

      setShardStatus(run.id(), "A", "SUCCESS");
      engine.scanOnce(); // S2:A 派生 SUCCESS;B、C 本 scan 仍见 A scan-start RUNNING → PENDING(跨周期)
      assertEquals(DagRunNodeStatus.SUCCESS, node(run.id(), "A").status());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "B").status());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "C").status());

      engine.scanOnce(); // S3:A 已 SUCCESS → B、C 同 scan 一并 spawn
      assertEquals(DagRunNodeStatus.RUNNING, node(run.id(), "B").status());
      assertEquals(DagRunNodeStatus.RUNNING, node(run.id(), "C").status());

      setShardStatus(run.id(), "B", "SUCCESS");
      setShardStatus(run.id(), "C", "SUCCESS");
      engine.scanOnce(); // S4:B、C 派生 SUCCESS;++ 全节点终态 → run SUCCESS
      assertEquals(DagRunNodeStatus.SUCCESS, node(run.id(), "B").status());
      assertEquals(DagRunNodeStatus.SUCCESS, node(run.id(), "C").status());
      assertEquals(DagRunStatus.SUCCESS, dags.findRun(run.id()).orElseThrow().status());
    } finally { leader.close(); }
  }

  /** 手动触发 + 取消级联:即便 spawned 节点 A 的 shards 已全 SUCCESS,DagEngine 尚未来得及派生 SUCCESS 就打 cancel,
   *  取消仍胜出(A 节点 CANCELED);PENDING 下游 B 一并 CANCELED(不 spawn);run → CANCELED。 */
  @Test void manualTrigger_cancelCascade_dagRunCancelled() {
    long t1 = newTask(1), t2 = newTask(1);
    Dag dag = newDag(null, Map.of("A", t1, "B", t2), edges(new String[]{"A", "B"}));
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      var engine = engine(leader);
      DagRun run = dags.createManualRun(dag.id());

      engine.scanOnce(); // S1:A spawn RUNNING;B PENDING
      assertEquals(DagRunNodeStatus.RUNNING, node(run.id(), "A").status());
      assertNotNull(node(run.id(), "A").executionId());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "B").status());

      // 模拟 worker 已把 A 的 shards 跑成 SUCCESS,但 DagEngine 尚未派生 A 的 SUCCESS
      setShardStatus(run.id(), "A", "SUCCESS");

      // 操作者取消 + Task 3 级联(此处直接用 repo 调用镜像该端点语义;HTTP 端点在 Task 3)
      dags.requestCancelRun(run.id());
      for (DagRunNode n : dags.findNonTerminalNodes(run.id())) {
        dags.markNodeStatus(n.id(), DagRunNodeStatus.CANCELED, "cascade cancel");
        if (n.executionId() != null) shards.cancelParentImmediate(n.executionId());
      }

      // 取消是最终语义:即便 shards 已 SUCCESS,A 节点仍 CANCELED(cancel-wins)
      assertEquals(DagRunNodeStatus.CANCELED, node(run.id(), "A").status());
      assertEquals(DagRunNodeStatus.CANCELED, node(run.id(), "B").status());
      assertNull(node(run.id(), "B").executionId(), "PENDING 下游级联取消绝不 spawn");

      engine.scanOnce(); // 全节点终态 → run 收敛为 CANCELED
      assertEquals(DagRunStatus.CANCELED, dags.findRun(run.id()).orElseThrow().status());
      assertEquals(1L, runOutcomeCount(run.id(), "CANCELED"));
    } finally { leader.close(); }
  }

  /** 读侧取消收敛(spec §3.1 规则 2):A 的 shards 被取消 → DagEngine 派生 A CANCELED;PENDING 下游 B 不以手工
   *  置 CANCELED,而是下一 scan 由引擎据上游 CANCELED 自行派生 CANCELED(绝不 spawn)→ run CANCELED。
   *  覆盖引擎 anyCancelled → CANCELED 分支(区别于 test 5 的手工级联)。 */
  @Test void upstreamCancelled_downstreamCancelled_readSideDerived() {
    long t1 = newTask(1), t2 = newTask(1);
    Dag dag = newDag(null, Map.of("A", t1, "B", t2), edges(new String[]{"A", "B"}));
    var leader = new AdvisoryLockLeaderElection(jdbc);
    try {
      var engine = engine(leader);
      DagRun run = dags.createManualRun(dag.id());

      engine.scanOnce(); // S1:A spawn RUNNING;B PENDING
      assertEquals(DagRunNodeStatus.RUNNING, node(run.id(), "A").status());
      assertNotNull(node(run.id(), "A").executionId());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "B").status());

      setShardStatus(run.id(), "A", "CANCELED"); // 模拟 worker/取消把 A 的 shards 收为 CANCELED
      engine.scanOnce(); // S2:A 据 shards 派生 CANCELED;B 本 scan 仍见 A scan-start RUNNING → PENDING(跨周期)
      assertEquals(DagRunNodeStatus.CANCELED, node(run.id(), "A").status());
      assertEquals(DagRunNodeStatus.PENDING, node(run.id(), "B").status());

      engine.scanOnce(); // S3:B 上游 A 终态 CANCELED → 引擎读侧派生 B CANCELED(非手工置),绝不 spawn
      assertEquals(DagRunNodeStatus.CANCELED, node(run.id(), "B").status());
      assertNull(node(run.id(), "B").executionId(), "读侧取消收敛的 PENDING 下游绝不 spawn");
      assertEquals(DagRunStatus.CANCELED, dags.findRun(run.id()).orElseThrow().status());
    } finally { leader.close(); }
  }
}
