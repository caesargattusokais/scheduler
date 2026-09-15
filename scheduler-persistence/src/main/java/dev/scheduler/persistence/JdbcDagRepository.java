package dev.scheduler.persistence;

import dev.scheduler.core.Dag;
import dev.scheduler.core.DagEdge;
import dev.scheduler.core.DagNode;
import dev.scheduler.core.DagRun;
import dev.scheduler.core.DagRunNode;
import dev.scheduler.core.DagRunNodeStatus;
import dev.scheduler.core.DagRunNodeTransitions;
import dev.scheduler.core.DagRunStatus;
import dev.scheduler.core.IdempotencyKeys;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class JdbcDagRepository implements DagRepository {
  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;

  public JdbcDagRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
    this.tx = new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
  }

  private static final RowMapper<Dag> DAG_MAP = (rs, i) -> new Dag(
      rs.getLong("id"), rs.getString("name"), rs.getString("description"), rs.getString("cron"),
      rs.getBoolean("enabled"), rs.getBoolean("paused"),
      rs.getTimestamp("created_at") != null ? rs.getTimestamp("created_at").toInstant() : null,
      rs.getTimestamp("updated_at") != null ? rs.getTimestamp("updated_at").toInstant() : null);
  private static final RowMapper<DagNode> NODE_MAP = (rs, i) -> new DagNode(
      rs.getLong("id"), rs.getLong("dag_id"), rs.getString("node_key"), rs.getLong("task_id"),
      rs.getInt("sort_order"));
  private static final RowMapper<DagEdge> EDGE_MAP = (rs, i) -> new DagEdge(
      rs.getLong("id"), rs.getLong("dag_id"), rs.getLong("from_node_id"), rs.getLong("to_node_id"));
  private static final RowMapper<DagRun> RUN_MAP = (rs, i) -> new DagRun(
      rs.getLong("id"), rs.getLong("dag_id"), rs.getString("idempotency_key"),
      DagRunStatus.valueOf(rs.getString("status")), rs.getString("trigger_reason"),
      rs.getBoolean("cancel_requested"),
      rs.getTimestamp("finished_at") != null ? rs.getTimestamp("finished_at").toInstant() : null,
      rs.getTimestamp("created_at") != null ? rs.getTimestamp("created_at").toInstant() : null);
  private static final RowMapper<DagRunNode> RUN_NODE_MAP = (rs, i) -> {
    long eid = rs.getLong("execution_id");
    Long exec = rs.wasNull() ? null : eid;
    return new DagRunNode(
        rs.getLong("id"), rs.getLong("dag_run_id"), rs.getString("node_key"), rs.getLong("task_id"),
        exec,
        DagRunNodeStatus.valueOf(rs.getString("status")), rs.getInt("sort_order"),
        rs.getString("detail"),
        rs.getTimestamp("created_at") != null ? rs.getTimestamp("created_at").toInstant() : null,
        rs.getTimestamp("finished_at") != null ? rs.getTimestamp("finished_at").toInstant() : null);
  };

  // ---- 定义 ----

  @Override public Dag createDag(String name, String description, String cron,
                                 List<NodeInput> nodes, List<EdgeInput> edges) {
    Long[] dagId = new Long[1];
    tx.executeWithoutResult(s -> {
      Long did = jdbc.queryForObject(
          "INSERT INTO app_dag (name, description, cron) VALUES (?,?,?) RETURNING id",
          Long.class, name, description, cron);
      dagId[0] = did;
      Map<String, Long> byKey = new LinkedHashMap<>();
      for (NodeInput n : nodes) {
        List<Integer> exists = jdbc.queryForList(
            "SELECT 1 FROM app_task WHERE id=?", Integer.class, n.taskId());
        if (exists.isEmpty()) throw new IllegalArgumentException(
            "node '" + n.nodeKey() + "' references unknown task " + n.taskId());
        if (byKey.put(n.nodeKey(), -1L) != null) throw new IllegalArgumentException(
            "duplicate node key '" + n.nodeKey() + "'");
        Long nid = jdbc.queryForObject(
            "INSERT INTO app_dag_node (dag_id, node_key, task_id, sort_order) VALUES (?,?,?,?) RETURNING id",
            Long.class, did, n.nodeKey(), n.taskId(), n.sortOrder());
        byKey.put(n.nodeKey(), nid);
      }
      List<long[]> resolved = new ArrayList<>();
      for (EdgeInput e : edges) {
        Long from = byKey.get(e.from());
        Long to = byKey.get(e.to());
        if (from == null || to == null) throw new IllegalArgumentException(
            "edge references unknown node key '" + (from == null ? e.from() : e.to()) + "'");
        if (from.equals(to)) throw new IllegalArgumentException(
            "self-loop edge " + e.from() + "->" + e.to() + " is not allowed");
        boolean dup = resolved.stream().anyMatch(r -> r[0] == from && r[1] == to);
        if (dup) throw new IllegalArgumentException("duplicate edge " + e.from() + "->" + e.to());
        resolved.add(new long[]{from, to});
      }
      assertAcyclic(byKey.values().stream().filter(v -> v > 0).toList(), resolved);
      for (long[] r : resolved) {
        jdbc.update("INSERT INTO dag_edge (dag_id, from_node_id, to_node_id) VALUES (?,?,?)",
            did, r[0], r[1]);
      }
    });
    return findDag(dagId[0]).orElseThrow();
  }

  /** DFS 无环校验:任一节点有前向边(正在 DFS 栈上)即环 → 抛 IllegalArgumentException(400),回滚整个建 DAG 事务。 */
  private void assertAcyclic(List<Long> nodeIds, List<long[]> edges) {
    Map<Long, List<Long>> adj = new HashMap<>();
    for (long[] e : edges) adj.computeIfAbsent(e[0], k -> new ArrayList<>()).add(e[1]);
    Map<Long, Integer> state = new HashMap<>(); // 0=unvisited 1=in-stack 2=done
    for (long nid : nodeIds) {
      if (dfsCycle(nid, adj, state)) throw new IllegalArgumentException(
          "cycle detected: dag must be acyclic");
    }
  }
  private boolean dfsCycle(long nid, Map<Long, List<Long>> adj, Map<Long, Integer> state) {
    int s = state.getOrDefault(nid, 0);
    if (s == 1) return true;      // back-edge → cycle
    if (s == 2) return false;     // already proven acyclic
    state.put(nid, 1);
    for (long next : adj.getOrDefault(nid, List.of())) {
      if (dfsCycle(next, adj, state)) return true;
    }
    state.put(nid, 2);
    return false;
  }

  @Override public Optional<Dag> findDag(long id) {
    return jdbc.query("SELECT * FROM app_dag WHERE id=?", DAG_MAP, id).stream().findFirst();
  }
  @Override public List<Dag> findAllDags() {
    return jdbc.query("SELECT * FROM app_dag ORDER BY id", DAG_MAP);
  }
  @Override public List<Dag> findDagsPage(String name, int limit, int offset) {
    List<Object> a = new ArrayList<>();
    if (name != null && !name.isBlank()) a.add("%" + name.trim() + "%");
    a.add(limit); a.add(offset);
    return jdbc.query("SELECT * FROM app_dag WHERE 1=1"
        + (name != null && !name.isBlank() ? " AND name ILIKE ?" : "")
        + " ORDER BY id LIMIT ? OFFSET ?", DAG_MAP, a.toArray());
  }
  @Override public long countDags(String name) {
    if (name != null && !name.isBlank()) {
      Long c = jdbc.queryForObject("SELECT count(*) FROM app_dag WHERE name ILIKE ?",
          Long.class, "%" + name.trim() + "%");
      return c == null ? 0 : c;
    }
    Long c = jdbc.queryForObject("SELECT count(*) FROM app_dag", Long.class);
    return c == null ? 0 : c;
  }
  @Override public List<Dag> findCronEnabledDagsPage(long afterId, int limit) {
    return jdbc.query(
        "SELECT * FROM app_dag WHERE enabled AND NOT paused AND cron IS NOT NULL AND id > ?"
            + " ORDER BY id LIMIT ?", DAG_MAP, afterId, limit);
  }
  @Override public List<DagNode> findNodes(long dagId) {
    return jdbc.query("SELECT * FROM app_dag_node WHERE dag_id=? ORDER BY sort_order, id", NODE_MAP, dagId);
  }
  @Override public List<DagEdge> findEdges(long dagId) {
    return jdbc.query("SELECT * FROM dag_edge WHERE dag_id=? ORDER BY id", EDGE_MAP, dagId);
  }
  @Override public void setPaused(long dagId, boolean paused) {
    jdbc.update("UPDATE app_dag SET paused=?, updated_at=now() WHERE id=?", paused, dagId);
  }

  // ---- 运行 ----

  @Override public DagRun createScheduledRun(long dagId, Instant triggerAt) {
    return createRun(dagId, IdempotencyKeys.forDagTrigger(dagId, triggerAt), "scheduled");
  }
  @Override public DagRun createManualRun(long dagId) {
    return createRun(dagId, IdempotencyKeys.forManualDagRun(dagId), "manual");
  }
  private DagRun createRun(long dagId, String key, String reason) {
    long[] runId = new long[1];
    tx.executeWithoutResult(s -> {
      Long id = jdbc.queryForObject("""
        INSERT INTO dag_run (dag_id, status, idempotency_key, trigger_reason)
        VALUES (?, 'PENDING', ?, ?)
        ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
        RETURNING id""", Long.class, dagId, key, reason);
      long existing = jdbc.queryForObject(
          "SELECT count(*) FROM dag_run_node WHERE dag_run_id=?", Long.class, id);
      if (existing == 0) { // 首次物化:按定义节点快照 task_id 插入全部 PENDING 节点(重放不重复插)
        List<DagNode> nodes = findNodes(dagId);
        jdbc.batchUpdate("""
          INSERT INTO dag_run_node (dag_run_id, node_key, task_id, sort_order)
          VALUES (?, ?, ?, ?)""",
          nodes.stream().map(n -> new Object[]{id, n.nodeKey(), n.taskId(), n.sortOrder()}).toList());
      }
      runId[0] = id;
    });
    return findRun(runId[0]).orElseThrow();
  }

  @Override public Optional<DagRun> findRun(long runId) {
    return jdbc.query("SELECT * FROM dag_run WHERE id=?", RUN_MAP, runId).stream().findFirst();
  }
  @Override public Optional<DagRunNode> findNode(long nodeId) {
    return jdbc.query("SELECT * FROM dag_run_node WHERE id=?", RUN_NODE_MAP, nodeId).stream().findFirst();
  }
  @Override public List<DagRun> findRuns(Long dagId) {
    return dagId == null
        ? jdbc.query("SELECT * FROM dag_run ORDER BY id DESC", RUN_MAP)
        : jdbc.query("SELECT * FROM dag_run WHERE dag_id=? ORDER BY id DESC", RUN_MAP, dagId);
  }
  @Override public List<DagRun> findRunsPage(Long dagId, String status, int limit, int offset) {
    StringBuilder sql = new StringBuilder("SELECT * FROM dag_run WHERE 1=1");
    List<Object> a = new ArrayList<>();
    if (dagId != null) { sql.append(" AND dag_id=?"); a.add(dagId); }
    if (status != null && !status.isBlank()) { sql.append(" AND status=?"); a.add(status); }
    sql.append(" ORDER BY id DESC LIMIT ? OFFSET ?");
    a.add(limit); a.add(offset);
    return jdbc.query(sql.toString(), RUN_MAP, a.toArray());
  }
  @Override public long countRuns(Long dagId, String status) {
    StringBuilder sql = new StringBuilder("SELECT count(*) FROM dag_run WHERE 1=1");
    List<Object> a = new ArrayList<>();
    if (dagId != null) { sql.append(" AND dag_id=?"); a.add(dagId); }
    if (status != null && !status.isBlank()) { sql.append(" AND status=?"); a.add(status); }
    Long c = jdbc.queryForObject(sql.toString(), Long.class, a.toArray());
    return c == null ? 0 : c;
  }
  @Override public List<DagRun> findActiveRunsPage(long afterId, int limit) {
    return jdbc.query(
        "SELECT * FROM dag_run WHERE status='PENDING' AND id > ? ORDER BY id LIMIT ?",
        RUN_MAP, afterId, limit);
  }
  @Override public List<DagRunNode> findNodesOfRun(long runId) {
    return jdbc.query(
        "SELECT * FROM dag_run_node WHERE dag_run_id=? ORDER BY sort_order, id", RUN_NODE_MAP, runId);
  }
  @Override public List<DagRunNode> findNonTerminalNodes(long runId) {
    return jdbc.query(
        "SELECT * FROM dag_run_node WHERE dag_run_id=? AND status IN ('PENDING','RUNNING')"
            + " ORDER BY sort_order, id", RUN_NODE_MAP, runId);
  }

  @Override public boolean markNodeSpawned(long nodeId, long executionId) {
    final boolean[] ok = {false};
    tx.executeWithoutResult(s -> {
      int upd = jdbc.update(
          "UPDATE dag_run_node SET status='RUNNING', execution_id=? WHERE id=? AND status='PENDING'",
          executionId, nodeId);
      if (upd == 0) return; // 已非 PENDING(已被推进)→ 静默,不落误导性 outcome
      jdbc.update("INSERT INTO dag_run_node_outcome (node_id, status, detail) VALUES (?,?,?)",
          nodeId, "RUNNING", "spawned");
      ok[0] = true;
    });
    return ok[0];
  }

  @Override public boolean markNodeStatus(long nodeId, DagRunNodeStatus to, String detail) {
    DagRunNode cur = findNode(nodeId).orElseThrow(() -> new IllegalStateException("no dag_run_node " + nodeId));
    if (!DagRunNodeTransitions.canTransition(cur.status(), to)) {
      throw new IllegalStateException("illegal transition " + cur.status() + " -> " + to);
    }
    final boolean[] ok = {false};
    tx.executeWithoutResult(s -> {
      int upd = jdbc.update("""
        UPDATE dag_run_node SET status=?, detail=?,
          finished_at=CASE WHEN ? THEN now() ELSE finished_at END
          WHERE id=? AND status=?""",
          to.name(), detail, to.isTerminal(), nodeId, cur.status().name());
      if (upd == 0) return; // CAS 0 → 行已被他方改走,静默
      jdbc.update("INSERT INTO dag_run_node_outcome (node_id, status, detail) VALUES (?,?,?)",
          nodeId, to.name(), detail);
      ok[0] = true;
    });
    return ok[0];
  }

  @Override public boolean finalizeRun(long runId, DagRunStatus terminal, String detail) {
    return tx.execute(s -> {
      int upd = jdbc.update(
          "UPDATE dag_run SET status=?, finished_at=now() WHERE id=? AND status='PENDING'",
          terminal.name(), runId);
      if (upd == 0) return false; // CAS 0 → 已终态,幂等跳过
      jdbc.update("INSERT INTO dag_run_outcome (dag_run_id, status, detail) VALUES (?,?,?)",
          runId, terminal.name(), detail);
      return true;
    });
  }

  @Override public boolean rerunNodeToExecution(long runId, long nodeId, long newExecutionId) {
    final boolean[] ok = {false};
    tx.executeWithoutResult(s -> {
      int upd = jdbc.update("""
        UPDATE dag_run_node SET status='RUNNING', execution_id=?, finished_at=NULL
        WHERE id=? AND status IN ('SUCCESS','FAILED','SKIPPED','CANCELED')""",
        newExecutionId, nodeId);
      if (upd == 0) return; // 节点非终态/竞态 → 静默
      jdbc.update("UPDATE dag_run SET status='PENDING', finished_at=NULL WHERE id=? AND status<>'PENDING'",
          runId); // 重开 run,引擎随后重扫重派生(幂等)
      jdbc.update("INSERT INTO dag_run_node_outcome (node_id, status, detail) VALUES (?,?,?)",
          nodeId, "RUNNING", "node rerun");
      ok[0] = true;
    });
    return ok[0];
  }

  @Override public void requestCancelRun(long runId) {
    jdbc.update("UPDATE dag_run SET cancel_requested=true WHERE id=? AND status='PENDING'", runId);
  }

  @Override public long countActiveRuns(long dagId) {
    Long c = jdbc.queryForObject(
        "SELECT count(*) FROM dag_run WHERE dag_id=? AND status='PENDING'", Long.class, dagId);
    return c == null ? 0 : c;
  }

  @Override public long countActiveRuns() {
    Long c = jdbc.queryForObject(
        "SELECT count(*) FROM dag_run WHERE status='PENDING'", Long.class);
    return c == null ? 0 : c;
  }
}
