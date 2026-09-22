package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.*;
import dev.scheduler.core.Dag;
import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunNodeStatus;
import dev.scheduler.core.DagRunStatus;
import dev.scheduler.core.IdempotencyKeys;
import dev.scheduler.core.Task;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcDagRepositoryTest extends AbstractPostgresTest {
  private final JdbcTaskRepository taskRepo = new JdbcTaskRepository(jdbc);
  private final JdbcShardRepository shardRepo = new JdbcShardRepository(jdbc);
  private final DagRepository dagRepo = new JdbcDagRepository(jdbc);

  @BeforeEach void clean() {
    jdbc.update("TRUNCATE app_dag, app_dag_node, dag_edge, dag_run, dag_run_node,"
        + " dag_run_outcome, dag_run_node_outcome, execution, execution_shard,"
        + " execution_shard_outcome, execution_outcome, app_task RESTART IDENTITY CASCADE");
  }

  private long newTask(String cron) {
    Task t = taskRepo.create(new Task(null, "t"+System.nanoTime(), "demo", "demo", cron,
        1, 300, 0, 1000, null, 1, true, false));
    return t.id();
  }

  /** 建一个双节点 A→B 的 DAG,返回 dag id。 */
  private long newDag() {
    long t = newTask("0 */5 * * * *");
    Dag d = dagRepo.createDag("d"+System.nanoTime(), "desc", "0 */5 * * * *",
        List.of(new DagRepository.NodeInput("A", t, 0), new DagRepository.NodeInput("B", t, 1)),
        List.of(new DagRepository.EdgeInput("A", "B")));
    return d.id();
  }

  @Test void createDag_persistsNodesAndEdges() {
    long t1 = newTask("0 */5 * * * *");
    long t2 = newTask("0 */5 * * * *");
    var dag = dagRepo.createDag("d", "desc", "0 */5 * * * *",
        List.of(new DagRepository.NodeInput("A", t1, 0), new DagRepository.NodeInput("B", t2, 1)),
        List.of(new DagRepository.EdgeInput("A", "B")));

    assertTrue(dagRepo.findDag(dag.id()).isPresent());
    assertEquals("d", dagRepo.findDag(dag.id()).get().name());

    var nodes = dagRepo.findNodes(dag.id());
    assertEquals(2, nodes.size());
    assertEquals("A", nodes.get(0).nodeKey());
    assertEquals(t1, nodes.get(0).taskId());
    assertEquals(0, nodes.get(0).sortOrder());
    assertEquals("B", nodes.get(1).nodeKey());
    assertEquals(t2, nodes.get(1).taskId());
    assertEquals(1, nodes.get(1).sortOrder());

    var edges = dagRepo.findEdges(dag.id());
    assertEquals(1, edges.size());
    long aId = nodes.get(0).id();
    long bId = nodes.get(1).id();
    assertEquals(aId, edges.get(0).fromNodeId());
    assertEquals(bId, edges.get(0).toNodeId());
  }

  @Test void createDag_unknownTask_rejects400() {
    long t = newTask("0 */5 * * * *");
    assertThrows(IllegalArgumentException.class, () -> dagRepo.createDag("d", "desc", "cron",
        List.of(new DagRepository.NodeInput("A", t, 0), new DagRepository.NodeInput("B", 999999L, 1)),
        List.of()));
    assertEquals(0, dagRepo.findAllDags().size(), "tx rolled back — no partial dag");
  }

  @Test void createDag_duplicateNodeKey_rejects() {
    long t = newTask("0 */5 * * * *");
    assertThrows(IllegalArgumentException.class, () -> dagRepo.createDag("d", "desc", "cron",
        List.of(new DagRepository.NodeInput("A", t, 0), new DagRepository.NodeInput("A", t, 1)),
        List.of()));
    assertEquals(0, dagRepo.findAllDags().size());
  }

  @Test void createDag_duplicateEdge_rejects() {
    long t = newTask("0 */5 * * * *");
    assertThrows(IllegalArgumentException.class, () -> dagRepo.createDag("d", "desc", "cron",
        List.of(new DagRepository.NodeInput("A", t, 0), new DagRepository.NodeInput("B", t, 1)),
        List.of(new DagRepository.EdgeInput("A", "B"), new DagRepository.EdgeInput("A", "B"))));
    assertEquals(0, dagRepo.findAllDags().size());
  }

  @Test void createDag_selfLoop_rejects() {
    long t = newTask("0 */5 * * * *");
    assertThrows(IllegalArgumentException.class, () -> dagRepo.createDag("d", "desc", "cron",
        List.of(new DagRepository.NodeInput("A", t, 0)),
        List.of(new DagRepository.EdgeInput("A", "A"))));
    assertEquals(0, dagRepo.findAllDags().size());
  }

  @Test void createDag_cycle_rejects() {
    long t = newTask("0 */5 * * * *");
    assertThrows(IllegalArgumentException.class, () -> dagRepo.createDag("d", "desc", "cron",
        List.of(new DagRepository.NodeInput("A", t, 0), new DagRepository.NodeInput("B", t, 1),
            new DagRepository.NodeInput("C", t, 2)),
        List.of(new DagRepository.EdgeInput("A", "B"), new DagRepository.EdgeInput("B", "C"),
            new DagRepository.EdgeInput("C", "A"))));
    assertEquals(0, dagRepo.findAllDags().size(), "atomic rollback — no dag persists");
  }

  /** 游标续扫:两条 PENDING run 逐页取,第二页不重复第一页、扫尽后空页回卷。 */
  @Test void findActiveRunsPage_resumesPastCursor_exhaustsToEmpty() {
    long dagId = newDag();
    dagRepo.createScheduledRun(dagId, Instant.ofEpochMilli(1L));
    dagRepo.createScheduledRun(dagId, Instant.ofEpochMilli(2L));
    var all = dagRepo.findActiveRunsPage(0L, 1);
    assertEquals(1, all.size());
    var page2 = dagRepo.findActiveRunsPage(all.get(0).id(), 1);
    assertEquals(1, page2.size());
    assertFalse(page2.get(0).id() == all.get(0).id(), "第二页不重复第一页");
    var tail = dagRepo.findActiveRunsPage(page2.get(0).id(), 10);
    assertTrue(tail.isEmpty(), "扫尽");
  }

  @Test void createScheduledRun_isIdempotentByKey() {
    long dagId = newDag();
    Instant trig = Instant.ofEpochMilli(123456789L);

    dagRepo.createScheduledRun(dagId, trig);
    dagRepo.createScheduledRun(dagId, trig);

    var runs = dagRepo.findRuns(dagId);
    assertEquals(1, runs.size(), "replay by same idempotency key → exactly 1 dag_run");
    assertEquals(2, dagRepo.findNodesOfRun(runs.get(0).id()).size(), "no duplicate node rows");
  }

  @Test void createManualRun_createsDistinctRuns() {
    long dagId = newDag();
    var r1 = dagRepo.createManualRun(dagId);
    var r2 = dagRepo.createManualRun(dagId);

    assertNotEquals(r1.idempotencyKey(), r2.idempotencyKey(), "distinct manual uuids");
    assertEquals(2, dagRepo.findRuns(dagId).size());
  }

  @Test void markNodeSpawned_setsExecutionAndRunning() {
    long dagId = newDag();
    long runId = dagRepo.createScheduledRun(dagId, Instant.now()).id();
    var nodes = dagRepo.findNodesOfRun(runId);
    long nodeA = nodes.get(0).id();
    long taskId = nodes.get(0).taskId();
    long eid = shardRepo.createParentWithShards(taskId, "seed-"+System.nanoTime(), 1).id();

    assertTrue(dagRepo.markNodeSpawned(nodeA, eid));
    DagRunNode reloaded = dagRepo.findNode(nodeA).get();
    assertEquals(DagRunNodeStatus.RUNNING, reloaded.status());
    assertEquals(eid, reloaded.executionId());

    Integer spawned = jdbc.queryForObject(
        "SELECT count(*) FROM dag_run_node_outcome WHERE node_id=? AND status='RUNNING' AND detail='spawned'",
        Integer.class, nodeA);
    assertEquals(1, spawned, "spawn 落 RUNNING('spawned') outcome");

    assertFalse(dagRepo.markNodeSpawned(nodeA, eid), "node no longer PENDING → second spawn fails");
  }

  @Test void markNodeStatus_terminal_derivesWithOutcome() {
    long dagId = newDag();
    long runId = dagRepo.createScheduledRun(dagId, Instant.now()).id();
    var nodes = dagRepo.findNodesOfRun(runId);
    long nodeA = nodes.get(0).id();
    long taskId = nodes.get(0).taskId();
    long eid = shardRepo.createParentWithShards(taskId, "seed-"+System.nanoTime(), 1).id();
    dagRepo.markNodeSpawned(nodeA, eid); // → RUNNING

    assertTrue(dagRepo.markNodeStatus(nodeA, DagRunNodeStatus.SUCCESS, "all shards ok"));
    DagRunNode s = dagRepo.findNode(nodeA).get();
    assertEquals(DagRunNodeStatus.SUCCESS, s.status());
    assertNotNull(s.finishedAt(), "terminal node writes finished_at");

    Integer succ = jdbc.queryForObject(
        "SELECT count(*) FROM dag_run_node_outcome WHERE node_id=? AND status='SUCCESS'",
        Integer.class, nodeA);
    assertEquals(1, succ, "terminal node 落 SUCCESS outcome");

    long nodeB = nodes.get(1).id(); // still PENDING
    assertThrows(IllegalStateException.class,
        () -> dagRepo.markNodeStatus(nodeB, DagRunNodeStatus.FAILED, "boom"),
        "PENDING→FAILED is not a valid transition");
  }

  @Test void finalizeRun_cas_idempotent() {
    long dagId = newDag();
    long runId = dagRepo.createScheduledRun(dagId, Instant.now()).id();

    assertTrue(dagRepo.finalizeRun(runId, DagRunStatus.SUCCESS, "all nodes ok"));
    DagRun r = dagRepo.findRun(runId).get();
    assertEquals(DagRunStatus.SUCCESS, r.status());
    assertNotNull(r.finishedAt());

    Integer outcome = jdbc.queryForObject(
        "SELECT count(*) FROM dag_run_outcome WHERE dag_run_id=? AND status='SUCCESS'",
        Integer.class, runId);
    assertEquals(1, outcome);

    assertFalse(dagRepo.finalizeRun(runId, DagRunStatus.CANCELED, "again"),
        "CAS 0 → already terminal → idempotent false");
    Integer all = jdbc.queryForObject(
        "SELECT count(*) FROM dag_run_outcome WHERE dag_run_id=?", Integer.class, runId);
    assertEquals(1, all, "no extra run outcome after CAS miss");
  }

  @Test void requestCancelRun_setsFlag() {
    long dagId = newDag();
    long runId = dagRepo.createScheduledRun(dagId, Instant.now()).id();

    dagRepo.requestCancelRun(runId);
    assertEquals(true, jdbc.queryForObject(
        "SELECT cancel_requested FROM dag_run WHERE id=?", Boolean.class, runId));

    long run2 = dagRepo.createScheduledRun(dagId, Instant.now().plusSeconds(60)).id();
    dagRepo.finalizeRun(run2, DagRunStatus.SUCCESS, "done");
    assertDoesNotThrow(() -> dagRepo.requestCancelRun(run2),
        "cancel on a terminal run is a silent no-op");
    assertEquals(false, jdbc.queryForObject(
        "SELECT cancel_requested FROM dag_run WHERE id=?", Boolean.class, run2));
  }

  @Test void countActiveRuns() {
    long dagId = newDag();
    dagRepo.createScheduledRun(dagId, Instant.ofEpochMilli(1L));
    dagRepo.createScheduledRun(dagId, Instant.ofEpochMilli(2L));
    long r3 = dagRepo.createScheduledRun(dagId, Instant.ofEpochMilli(3L)).id();
    dagRepo.finalizeRun(r3, DagRunStatus.SUCCESS, "done");

    assertEquals(2, dagRepo.countActiveRuns(dagId));
  }

  /** 全局口径:跨所有 dag 计入非终态 run(终态不计);无 dag_id 过滤。 */
  @Test void countActiveRuns_global() {
    long dagA = newDag();
    long dagB = newDag();
    dagRepo.createScheduledRun(dagA, Instant.ofEpochMilli(1L));
    dagRepo.createScheduledRun(dagA, Instant.ofEpochMilli(2L));
    long rb = dagRepo.createScheduledRun(dagB, Instant.ofEpochMilli(3L)).id();
    dagRepo.finalizeRun(rb, DagRunStatus.SUCCESS, "done");

    assertEquals(2, dagRepo.countActiveRuns()); // A×2 非终态;rb 已终态不计
  }

  /** 建 DAG 可配置每节点重试预算(1a);findNodes 回读新列。 */
  @Test void createDag_persistsNodeRetryConfig() {
    long t = newTask("0 */5 * * * *");
    var dag = dagRepo.createDag("d", "desc", "0 */5 * * * *",
        List.of(new DagRepository.NodeInput("A", t, 0, 3, 2500)),
        List.of());
    var node = dagRepo.findNodes(dag.id()).get(0);
    assertEquals(3, node.nodeMaxRetries(), "node_max_retries 落库回读");
    assertEquals(2500L, node.nodeBackoffMs(), "node_backoff_ms 落库回读");
  }

  /** 建 DAG 可配置每节点 join 条件(1b run_if);findNodes 回读新列。
   *  未显式传 join 的既有便捷构造默认 all_success,保证零调用方改动语义不回退。 */
  @Test void createDag_persistsRunIf() {
    long t = newTask("0 */5 * * * *");
    var dag = dagRepo.createDag("d", "desc", "0 */5 * * * *",
        List.of(new DagRepository.NodeInput("A", t, 0, 0, 5000, "any_success"),
            new DagRepository.NodeInput("B", t, 1)), // 便捷 3 参 → 默认 all_success
        List.of());
    var nodes = dagRepo.findNodes(dag.id());
    assertEquals("any_success", nodes.get(0).runIf(), "run_if 落库回读");
    assertEquals("all_success", nodes.get(1).runIf(), "未传 join 默认 all_success");
  }

  /** 节点级重试回绕(1a):RUNNING → PENDING,attempt+1、next_retry_at 落库、execution_id 清空,落 PENDING outcome。
   *  随后重 spawn:markNodeSpawned 覆写新 execution + 清退避门,attempt 继续保留(计预算)。 */
  @Test void scheduleNodeRetry_rewindsRunningToPendingBackoff_clearsExecution_thenRespawns() {
    long dagId = newDag();
    long runId = dagRepo.createScheduledRun(dagId, Instant.now()).id();
    var nodes = dagRepo.findNodesOfRun(runId);
    long nodeA = nodes.get(0).id();
    long taskId = nodes.get(0).taskId();
    long eid = shardRepo.createParentWithShards(taskId, "seed-"+System.nanoTime(), 1).id();
    dagRepo.markNodeSpawned(nodeA, eid); // → RUNNING, execution_id=eid

    Instant retryAt = Instant.now().plusSeconds(30);
    assertTrue(dagRepo.scheduleNodeRetry(nodeA, retryAt, 2, "shards failed; will retry"));

    DagRunNode n = dagRepo.findNode(nodeA).get();
    assertEquals(DagRunNodeStatus.PENDING, n.status(), "重试把 RUNNING 回绕到 PENDING");
    assertEquals(2, n.attempt(), "attempt 写入新计数");
    assertNotNull(n.nextRetryAt(), "next_retry_at 落库");
    assertNull(n.executionId(), "待重试 → execution_id 清空(重 spawn 才建新执行)");
    Integer outcome = jdbc.queryForObject(
        "SELECT count(*) FROM dag_run_node_outcome WHERE node_id=? AND status='PENDING'",
        Integer.class, nodeA);
    assertEquals(1, outcome, "重试回绕落 PENDING outcome");

    long eid2 = shardRepo.createParentWithShards(taskId, "seed2-"+System.nanoTime(), 1).id();
    assertTrue(dagRepo.markNodeSpawned(nodeA, eid2));
    DagRunNode respawned = dagRepo.findNode(nodeA).get();
    assertEquals(eid2, respawned.executionId(), "重 spawn 覆写新 execution");
    assertNull(respawned.nextRetryAt(), "重 spawn 清空退避门");
    assertEquals(2, respawned.attempt(), "attempt 保留不重置(继续计预算)");
  }

  /** operator 重跑重置 attempt/退避门(1a):重跑即重获完整重试预算。 */
  /** 1d:建 DAG 带跨 DAG 依赖 → depends_on_dag_id 落库回读、cron 写 NULL(依赖取代 cron)。 */
  @Test void createDag_withDep_persistsDependsOnDagId_nullCron() {
    long t = newTask("0 */5 * * * *");
    long up = dagRepo.createDag("up", "desc", "0 */5 * * * *",
        List.of(new DagRepository.NodeInput("A", t, 0)), List.of()).id();
    var down = dagRepo.createDag("down", "desc", null, up,
        List.of(new DagRepository.NodeInput("X", t, 0)), List.of());
    assertNull(down.cron(), "依赖取代 cron → cron 写 NULL");
    assertEquals(up, down.dependsOnDagId(), "depends_on_dag_id 落库回读");
    assertEquals(up, dagRepo.findDag(down.id()).orElseThrow().dependsOnDagId());
  }

  /** 1d:被依赖 DAG 不存在 → IllegalArgumentException(400),整事务回滚(不含任何 DAG 残留)。 */
  @Test void createDag_depNonexistentDag_rejects400() {
    long t = newTask("0 */5 * * * *");
    assertThrows(IllegalArgumentException.class, () -> dagRepo.createDag("down", "desc", null, 999999L,
        List.of(new DagRepository.NodeInput("X", t, 0)), List.of()));
    assertEquals(0, dagRepo.findAllDags().size(), "rollback — no dag persists");
  }

  /** 1d:依赖链 A←B←C 无环 → 接受(A,B,C 各自成立,非自身、非环)。 */
  @Test void createDag_dependencyChain_acceptsAcyclicChain() {
    long t = newTask("0 */5 * * * *");
    List<DagRepository.NodeInput> nodes = List.of(new DagRepository.NodeInput("A", t, 0));
    long a = dagRepo.createDag("a", "desc", "0 */5 * * * *", nodes, List.of()).id();
    long b = dagRepo.createDag("b", "desc", null, a, nodes, List.of()).id();
    long c = dagRepo.createDag("c", "desc", null, b, nodes, List.of()).id();
    assertEquals(b, dagRepo.findDag(c).orElseThrow().dependsOnDagId());
    assertEquals(a, dagRepo.findDag(b).orElseThrow().dependsOnDagId());
  }

  /** 1d:findEnabledDependents 只列启用且非暂停的下游;暂停的被排除;与上游无关的 DAG 不列。 */
  @Test void findEnabledDependents_listsEnabledNotPausedOnly() {
    long t = newTask("0 */5 * * * *");
    List<DagRepository.NodeInput> nodes = List.of(new DagRepository.NodeInput("A", t, 0));
    long up = dagRepo.createDag("up", "desc", "0 */5 * * * *", nodes, List.of()).id();
    long d1 = dagRepo.createDag("d1", "desc", null, up, nodes, List.of()).id();
    long d2 = dagRepo.createDag("d2", "desc", null, up, nodes, List.of()).id();
    dagRepo.setPaused(d2, true); // 暂停 → 排除
    long unrelated = dagRepo.createDag("other", "desc", "0 */5 * * * *", nodes, List.of()).id();
    jdbc.update("UPDATE app_dag SET enabled=false WHERE id=?", d1); // 停用 → 排除(无 API,直接 SQL)

    var deps = dagRepo.findEnabledDependents(up);
    assertEquals(0, deps.size(), "两个下游一暂停一停用 → 无启用且非暂停的依赖项");
    assertEquals(0, dagRepo.findEnabledDependents(unrelated).size(), "无下游的 DAG → 空");
  }

  /** 1d:createDependencyRun 幂等 by key dep:{上游runId};重放同一上游 run → 复用同一下游 run,不重复建节点。 */
  @Test void createDependencyRun_idempotentByDepKey_materializesDownstreamNodes() {
    long t = newTask("0 */5 * * * *");
    long up = dagRepo.createDag("up", "desc", "0 */5 * * * *",
        List.of(new DagRepository.NodeInput("A", t, 0)), List.of()).id();
    long down = dagRepo.createDag("down", "desc", null, up,
        List.of(new DagRepository.NodeInput("X", t, 0), new DagRepository.NodeInput("Y", t, 1)),
        List.of()).id();
    long upstreamRun = dagRepo.createManualRun(up).id();

    DagRun r1 = dagRepo.createDependencyRun(down, upstreamRun);
    DagRun r2 = dagRepo.createDependencyRun(down, upstreamRun); // 重放(崩溃自愈)
    assertEquals(r1.id(), r2.id(), "同一上游 run 重复派生 → 幂等复用同一下游 run");
    assertEquals(IdempotencyKeys.forDagDep(upstreamRun), r1.idempotencyKey(),
        "idempotency key = dep:{上游runId}");
    assertEquals("dag:" + upstreamRun, r1.triggerReason(), "trigger_reason = dag:{上游runId}");
    assertEquals(1, dagRepo.findRuns(down).size());
    assertEquals(2, dagRepo.findNodesOfRun(r1.id()).size(), "下游 run 按定义快照全部 PENDING 节点");
    DagRun back = dagRepo.findRun(r1.id()).orElseThrow();
    assertEquals(DagRunStatus.PENDING, back.status(), "下游 run 初始 PENDING,待引擎推进");
  }

  /** operator 重跑重置 attempt/退避门(1a):重跑即重获完整重试预算。 */
  @Test void rerunNodeToExecution_resetsAttemptAndRetryGate() {
    long dagId = newDag();
    long runId = dagRepo.createScheduledRun(dagId, Instant.now()).id();
    var nodes = dagRepo.findNodesOfRun(runId);
    long nodeA = nodes.get(0).id();
    long taskId = nodes.get(0).taskId();
    long eid = shardRepo.createParentWithShards(taskId, "seed-"+System.nanoTime(), 1).id();
    dagRepo.markNodeSpawned(nodeA, eid);
    dagRepo.markNodeStatus(nodeA, DagRunNodeStatus.FAILED, "shards failed"); // 终态
    jdbc.update("UPDATE dag_run_node SET attempt=3, next_retry_at=now() + interval '1 hour' WHERE id=?",
        nodeA);

    long eid2 = shardRepo.createParentWithShards(taskId, "rerun-"+System.nanoTime(), 1).id();
    assertTrue(dagRepo.rerunNodeToExecution(runId, nodeA, eid2));

    DagRunNode n = dagRepo.findNode(nodeA).get();
    assertEquals(DagRunNodeStatus.RUNNING, n.status());
    assertEquals(0, n.attempt(), "operator 重跑重置 attempt → 重获完整重试预算");
    assertNull(n.nextRetryAt(), "operator 重跑清除重试退避门");
    assertEquals(eid2, n.executionId());
  }

  /** 1c:createRun 封印整份快照——dag_version + 节点行为列(run_if/node_max_retries/node_backoff_ms)+ 边(dag_run_edge)。 */
  @Test void createRun_sealsVersionBehaviorAndEdges() {
    long t = newTask("0 */5 * * * *");
    long dagId = dagRepo.createDag("d", "desc", "0 */5 * * * *",
        List.of(new DagRepository.NodeInput("A", t, 0, 2, 1000, "any_success"),
            new DagRepository.NodeInput("B", t, 1, 0, 5000, "all_success")),
        List.of(new DagRepository.EdgeInput("A", "B"))).id();
    long runId = dagRepo.createScheduledRun(dagId, Instant.now()).id();

    DagRun r = dagRepo.findRun(runId).orElseThrow();
    assertEquals(1, r.dagVersion(), "run 封印定义版本号(dag_version)");
    var nodes = dagRepo.findNodesOfRun(runId);
    assertEquals(2, nodes.size());
    assertEquals("any_success", nodes.get(0).runIf(), "run 节点封印 run_if 快照");
    assertEquals(2, nodes.get(0).nodeMaxRetries(), "run 节点封印重试预算快照");
    assertEquals(1000, nodes.get(0).nodeBackoffMs(), "run 节点封印退避快照");
    assertEquals("all_success", nodes.get(1).runIf(), "默认 run_if 在封印时落 all_success 而非 NULL");

    var edges = dagRepo.findRunEdges(runId);
    assertEquals(1, edges.size(), "run 封印 A→B 边(dag_run_edge)");
    assertEquals(nodes.get(0).id(), edges.get(0).fromNodeId(), "边 from 引用 run 内 A 节点");
    assertEquals(nodes.get(1).id(), edges.get(0).toNodeId(), "边 to 引用 run 内 B 节点");
  }

  /** 1c:编辑(updateDag)整份定义重写 + version++ ——仅影响未来新 run;已封印历史/在飞 run 快照原样保留。 */
  @Test void updateDag_rewritesDefinition_versionsUp_sealedRunUntouched() {
    long t = newTask("0 */5 * * * *");
    long dagId = dagRepo.createDag("d", "desc", "0 */5 * * * *",
        List.of(new DagRepository.NodeInput("A", t, 0), new DagRepository.NodeInput("B", t, 1)),
        List.of(new DagRepository.EdgeInput("A", "B"))).id();
    assertEquals(1, dagRepo.findDag(dagId).orElseThrow().version(), "新建 version=1");
    long oldRun = dagRepo.createScheduledRun(dagId, Instant.now()).id(); // 封印 v1 快照
    assertEquals(1, dagRepo.findRun(oldRun).orElseThrow().dagVersion());

    // 编辑:改名 + 去掉 B 及其边(整份重写)
    Dag updated = dagRepo.updateDag(dagId, "d2", "desc2", "0 * * * * *", null,
        List.of(new DagRepository.NodeInput("A", t, 0)), List.of());
    assertEquals(2, updated.version(), "编辑 → version++");
    assertEquals("d2", dagRepo.findDag(dagId).orElseThrow().name());
    assertEquals(1, dagRepo.findNodes(dagId).size(), "定义现只剩 A");

    // 历史/在飞旧 run 不受影响:封印 v1 快照原样.
    assertEquals(1, dagRepo.findRun(oldRun).orElseThrow().dagVersion());
    assertEquals(2, dagRepo.findNodesOfRun(oldRun).size(), "旧 run 仍含 B");
    assertEquals(1, dagRepo.findRunEdges(oldRun).size(), "旧 run 仍封印 A→B");

    // 新 run 封印 v2:仅 A 且无边.
    DagRun newRun = dagRepo.createScheduledRun(dagId, Instant.now());
    assertEquals(2, dagRepo.findRun(newRun.id()).orElseThrow().dagVersion(), "新 run 封印 v2");
    assertEquals(1, dagRepo.findNodesOfRun(newRun.id()).size(), "新 run 只封印新定义的单节点");
    assertEquals(0, dagRepo.findRunEdges(newRun.id()).size(), "新 run 无边");
  }

  /** 1c:编辑若引入自身依赖 → 拒绝且整事务回滚(version 不 ++)。 */
  @Test void updateDag_selfDepend_rejects() {
    long dagId = newDag();
    long t = dagRepo.findNodes(dagId).get(0).taskId();
    assertThrows(IllegalArgumentException.class, () -> dagRepo.updateDag(dagId, "d", "desc", null, dagId,
        List.of(new DagRepository.NodeInput("A", t, 0)), List.of()));
    assertEquals(1, dagRepo.findDag(dagId).orElseThrow().version(), "失败事务回滚 → 版本不 ++");
  }
}
