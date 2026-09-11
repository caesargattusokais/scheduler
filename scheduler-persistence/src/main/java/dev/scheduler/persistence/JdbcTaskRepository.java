package dev.scheduler.persistence;
import dev.scheduler.core.Task;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
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
      rs.getBoolean("enabled"), rs.getBoolean("paused"));

  @Override public Task create(Task t) {
    KeyHolder kh = new GeneratedKeyHolder();
    jdbc.update(con -> {
      var ps = con.prepareStatement("""
        INSERT INTO app_task (name, kind, handler_ref, cron, shard_count, timeout_seconds,
                              max_retries, backoff_ms, retryable_failure_pattern, max_active_concurrent)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
          new String[]{"id"});
      ps.setString(1, t.name()); ps.setString(2, t.kind()); ps.setString(3, t.handlerRef());
      ps.setString(4, t.cron()); ps.setInt(5, t.shardCount()); ps.setInt(6, t.timeoutSeconds());
      ps.setInt(7, t.maxRetries()); ps.setLong(8, t.backoffMs());
      ps.setString(9, t.retryableFailurePattern()); ps.setInt(10, t.maxActiveConcurrent());
      return ps;
    }, kh);
    return new Task(kh.getKey().longValue(), t.name(), t.kind(), t.handlerRef(), t.cron(),
        t.shardCount(), t.timeoutSeconds(), t.maxRetries(), t.backoffMs(),
        t.retryableFailurePattern(), t.maxActiveConcurrent(), true, false);
  }

  @Override public Optional<Task> findById(long id) {
    return jdbc.query("SELECT * FROM app_task WHERE id=?", MAP, id).stream().findFirst();
  }
  @Override public List<Task> findCronEnabled() {
    return jdbc.query("SELECT * FROM app_task WHERE enabled AND NOT paused AND cron IS NOT NULL", MAP);
  }
  @Override public List<Task> findAll() { return jdbc.query("SELECT * FROM app_task ORDER BY id", MAP); }
  @Override public boolean update(long id, Task t) {
    int rows = jdbc.update("""
        UPDATE app_task SET name=?, kind=?, handler_ref=?, cron=?, shard_count=?,
               timeout_seconds=?, max_retries=?, backoff_ms=?,
               retryable_failure_pattern=?, max_active_concurrent=?, paused=?, updated_at=now()
        WHERE id=?""",
        t.name(), t.kind(), t.handlerRef(), t.cron(), t.shardCount(), t.timeoutSeconds(),
        t.maxRetries(), t.backoffMs(), t.retryableFailurePattern(), t.maxActiveConcurrent(),
        t.paused(), id);
    return rows > 0;
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
