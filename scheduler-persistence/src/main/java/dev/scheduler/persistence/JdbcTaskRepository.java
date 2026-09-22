package dev.scheduler.persistence;
import dev.scheduler.core.Task;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import java.util.ArrayList;
import java.util.List;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

public class JdbcTaskRepository implements TaskRepository {
  private final JdbcTemplate jdbc;
  public JdbcTaskRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

  private static final RowMapper<Task> MAP = (rs, i) -> new Task(
      rs.getLong("id"), rs.getString("name"), rs.getString("kind"),
      rs.getString("handler_ref"), rs.getString("cron"),
      rs.getInt("shard_count"), rs.getInt("timeout_seconds"),
      rs.getInt("max_retries"), rs.getLong("backoff_ms"),
      rs.getString("retryable_failure_pattern"), rs.getInt("max_active_concurrent"),
      rs.getBoolean("enabled"), rs.getBoolean("paused"),
      rs.getString("retry_mode"), (Long) rs.getObject("retry_cap_ms"),
      (Long) rs.getObject("retry_budget_ms"),
      rs.getString("timezone"), (Integer) rs.getObject("interval_seconds"),
      arrayToStrings(rs.getArray("event_routes")),
      rs.getString("success_policy_type"), (Integer) rs.getObject("success_policy_value"));

  /** text[] 列 → List<String>(无参/null → 空列表);JDBC getArray 即 Spring 映射的 java.sql.Array。 */
  private static List<String> arrayToStrings(java.sql.Array arr) {
    if (arr == null) return List.of();
    try {
      Object[] raw = (Object[]) arr.getArray();
      if (raw == null) return List.of();
      List<String> out = new ArrayList<>(raw.length);
      for (Object o : raw) if (o != null) out.add(o.toString());
      return out;
    } catch (java.sql.SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** List<String> → text[] 绑定值(空/缺省 → NULL 数组)。 */
  private static java.sql.Array stringsToArray(java.sql.Connection con, List<String> routes) {
    try {
      if (routes == null || routes.isEmpty()) return con.createArrayOf("text", new Object[0]);
      return con.createArrayOf("text", routes.toArray());
    } catch (java.sql.SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public Task create(Task t) {
    KeyHolder kh = new GeneratedKeyHolder();
    jdbc.update(con -> {
      var ps = con.prepareStatement("""
        INSERT INTO app_task (name, kind, handler_ref, cron, shard_count, timeout_seconds,
                              max_retries, backoff_ms, retryable_failure_pattern, max_active_concurrent,
                              retry_mode, retry_cap_ms, retry_budget_ms, timezone, interval_seconds,
                              event_routes, success_policy_type, success_policy_value)
        VALUES (?,?,?,?,?,?,?,?,?,?,COALESCE(?, 'exponential'),?,?,?,?,?,?,?)""",
          new String[]{"id"});
      ps.setString(1, t.name()); ps.setString(2, t.kind()); ps.setString(3, t.handlerRef());
      ps.setString(4, t.cron()); ps.setInt(5, t.shardCount()); ps.setInt(6, t.timeoutSeconds());
      ps.setInt(7, t.maxRetries()); ps.setLong(8, t.backoffMs());
      ps.setString(9, t.retryableFailurePattern()); ps.setInt(10, t.maxActiveConcurrent());
      ps.setString(11, t.retryMode());
      ps.setObject(12, t.retryCapMs(), java.sql.Types.BIGINT);
      ps.setObject(13, t.retryBudgetMs(), java.sql.Types.BIGINT);
      ps.setString(14, t.timezone());
      ps.setObject(15, t.intervalSeconds(), java.sql.Types.INTEGER);
      ps.setArray(16, stringsToArray(con, t.eventRoutes()));
      ps.setString(17, t.successPolicyType());
      ps.setObject(18, t.successPolicyValue(), java.sql.Types.INTEGER);
      return ps;
    }, kh);
    return new Task(kh.getKey().longValue(), t.name(), t.kind(), t.handlerRef(), t.cron(),
        t.shardCount(), t.timeoutSeconds(), t.maxRetries(), t.backoffMs(),
        t.retryableFailurePattern(), t.maxActiveConcurrent(), true, false,
        t.retryMode(), t.retryCapMs(), t.retryBudgetMs(), t.timezone(), t.intervalSeconds(),
        t.eventRoutes(), t.successPolicyType(), t.successPolicyValue());
  }

  @Override public Optional<Task> findById(long id) {
    return jdbc.query("SELECT * FROM app_task WHERE id=?", MAP, id).stream().findFirst();
  }
  @Override public List<Task> findScheduleEnabledPage(long afterId, int limit) {
    return jdbc.query(
        "SELECT * FROM app_task WHERE enabled AND NOT paused"
            + " AND (cron IS NOT NULL OR interval_seconds IS NOT NULL) AND id > ?"
            + " ORDER BY id LIMIT ?", MAP, afterId, limit);
  }
  @Override public List<Task> findAll() { return jdbc.query("SELECT * FROM app_task ORDER BY id", MAP); }

  /** WHERE 片段(name ILIKE / paused 过滤),与 {@link #whereArgs} 配套。 */
  private String where(String name, Boolean paused) {
    StringBuilder w = new StringBuilder();
    if (name != null && !name.isBlank()) w.append(" AND name ILIKE ?");
    if (paused != null) w.append(" AND paused = ?");
    return w.toString();
  }
  private List<Object> filterArgs(String name, Boolean paused) {
    List<Object> a = new ArrayList<>();
    if (name != null && !name.isBlank()) a.add("%" + name.trim() + "%");
    if (paused != null) a.add(paused);
    return a;
  }

  @Override public List<Task> findPage(String name, Boolean paused, int limit, int offset) {
    List<Object> a = filterArgs(name, paused);
    a.add(limit); a.add(offset);
    return jdbc.query("SELECT * FROM app_task WHERE 1=1" + where(name, paused)
            + " ORDER BY id LIMIT ? OFFSET ?", MAP, a.toArray());
  }

  @Override public long count(String name, Boolean paused) {
    Long c = jdbc.queryForObject("SELECT count(*) FROM app_task WHERE 1=1" + where(name, paused),
        Long.class, filterArgs(name, paused).toArray());
    return c == null ? 0 : c;
  }
  @Override public boolean update(long id, Task t) {
    int rows = jdbc.update(con -> {
      var ps = con.prepareStatement("""
        UPDATE app_task SET name=?, kind=?, handler_ref=?, cron=?, shard_count=?,
               timeout_seconds=?, max_retries=?, backoff_ms=?,
               retryable_failure_pattern=?, max_active_concurrent=?, paused=?,
               retry_mode=COALESCE(?, 'exponential'), retry_cap_ms=?, retry_budget_ms=?,
               timezone=?, interval_seconds=?, event_routes=?,
               success_policy_type=?, success_policy_value=?, updated_at=now()
        WHERE id=?""");
      ps.setString(1, t.name()); ps.setString(2, t.kind()); ps.setString(3, t.handlerRef());
      ps.setString(4, t.cron()); ps.setInt(5, t.shardCount()); ps.setInt(6, t.timeoutSeconds());
      ps.setInt(7, t.maxRetries()); ps.setLong(8, t.backoffMs());
      ps.setString(9, t.retryableFailurePattern()); ps.setInt(10, t.maxActiveConcurrent());
      ps.setBoolean(11, t.paused()); ps.setString(12, t.retryMode());
      ps.setObject(13, t.retryCapMs(), java.sql.Types.BIGINT);
      ps.setObject(14, t.retryBudgetMs(), java.sql.Types.BIGINT);
      ps.setString(15, t.timezone());
      ps.setObject(16, t.intervalSeconds(), java.sql.Types.INTEGER);
      ps.setArray(17, stringsToArray(con, t.eventRoutes()));
      ps.setString(18, t.successPolicyType());
      ps.setObject(19, t.successPolicyValue(), java.sql.Types.INTEGER);
      ps.setLong(20, id);
      return ps;
    });
    return rows > 0;
  }

  /** 事件触发:enabled、非 paused 且 event_routes 包含 {@code routeKey}(用 PostgreSQL contains @> 判定)的任务,按 id 升序。
   *  SQL 的 @> 要求左侧为数组、右为单元素构造(ARRAY[?]);空路由的任务天然不匹配。 */
  @Override public List<Task> findEnabledByRoute(String routeKey) {
    return jdbc.query("""
        SELECT * FROM app_task
        WHERE enabled AND NOT paused AND event_routes @> ARRAY[?]::text[]
        ORDER BY id""", MAP, routeKey);
  }
  @Override public void setPaused(long id, boolean paused) {
    jdbc.update("UPDATE app_task SET paused=?, updated_at=now() WHERE id=?", paused, id);
  }

  @Override public long executionCount(long taskId) {
    return jdbc.queryForObject("SELECT count(*) FROM execution WHERE task_id=?", Long.class, taskId);
  }

  @Override public long dagReferenceCount(long taskId) {
    // app_dag_node:当前 DAG 结构引用;dag_run_node:历史运行快照引用(task_id 同为外键)
    return jdbc.queryForObject("""
        SELECT (SELECT count(*) FROM app_dag_node WHERE task_id=?)
             + (SELECT count(*) FROM dag_run_node WHERE task_id=?)""",
        Long.class, taskId, taskId);
  }

  /** 物理删除,守卫保证被子记录引用时不动(返回 0);守卫与 FK 双保险。 */
  @Override public boolean delete(long id) {
    int rows = jdbc.update("""
        DELETE FROM app_task t
        WHERE t.id = ?
          AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.task_id = t.id)
          AND NOT EXISTS (SELECT 1 FROM app_dag_node d WHERE d.task_id = t.id)
          AND NOT EXISTS (SELECT 1 FROM dag_run_node r WHERE r.task_id = t.id)""", id);
    return rows > 0;
  }
}
