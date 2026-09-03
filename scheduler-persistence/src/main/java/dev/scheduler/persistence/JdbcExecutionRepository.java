package dev.scheduler.persistence;

import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.ExecutionTransitions;
import java.time.Instant;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class JdbcExecutionRepository implements ExecutionRepository {
  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;

  public JdbcExecutionRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
    this.tx = new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
  }

  private static final RowMapper<Execution> MAP = (rs, i) -> new Execution(
      rs.getLong("id"), rs.getLong("task_id"),
      ExecutionStatus.valueOf(rs.getString("status")), rs.getString("idempotency_key"),
      rs.getString("args"), rs.getInt("shard_index"), rs.getInt("shard_count"),
      rs.getInt("attempt"), rs.getString("worker_id"),
      rs.getTimestamp("lease_until") != null ? rs.getTimestamp("lease_until").toInstant() : null,
      rs.getTimestamp("next_retry_at") != null ? rs.getTimestamp("next_retry_at").toInstant() : null,
      rs.getTimestamp("started_at") != null ? rs.getTimestamp("started_at").toInstant() : null,
      rs.getTimestamp("finished_at") != null ? rs.getTimestamp("finished_at").toInstant() : null,
      rs.getString("result_payload"));

  @Override public long createDue(Execution e) {
    String sql = """
      INSERT INTO execution (task_id, status, idempotency_key, args, shard_index, shard_count, attempt)
      VALUES (?, 'DUE', ?, ?, ?, ?, ?)
      ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
      RETURNING id""";
    Long id = jdbc.queryForObject(sql, Long.class,
        e.taskId(), e.idempotencyKey(), e.args(), e.shardIndex(), e.shardCount(), e.attempt());
    return id;
  }

  @Override public Optional<Execution> findCandidate(long taskId) {
    return jdbc.query(
        "SELECT * FROM execution WHERE task_id=? AND status='DUE' ORDER BY id LIMIT 1",
        MAP, taskId).stream().findFirst();
  }

  @Override public boolean claim(long executionId, long taskId, String workerId,
                                 Instant leaseUntil, int maxConcurrent) {
    String update = """
      WITH active AS (
        SELECT count(*) AS c FROM execution
         WHERE task_id=? AND status='RUNNING' AND lease_until > now()
      )
      UPDATE execution e SET status='RUNNING', worker_id=?, lease_until=?,
             attempt=attempt+1, started_at=COALESCE(started_at, now())
        FROM active, app_task t
       WHERE e.id=? AND e.status='DUE' AND t.id=e.task_id AND active.c < ?""";
    java.sql.Timestamp lease = java.sql.Timestamp.from(leaseUntil);
    final boolean[] ok = {false};
    tx.executeWithoutResult(s -> {
      if (jdbc.update(update, taskId, workerId, lease, executionId, maxConcurrent) == 1) {
        jdbc.update("INSERT INTO execution_outcome (execution_id, status, detail) VALUES (?,?,?)",
            executionId, "RUNNING", "claim");
        ok[0] = true;
      }
    });
    return ok[0];
  }

  @Override public Optional<Execution> findById(long id) {
    return jdbc.query("SELECT * FROM execution WHERE id=?", MAP, id).stream().findFirst();
  }

  @Override public long countActive(long taskId) {
    Long c = jdbc.queryForObject(
        "SELECT count(*) FROM execution WHERE task_id=? AND status='RUNNING' AND lease_until > now()",
        Long.class, taskId);
    return c == null ? 0 : c;
  }

  @Override public void markStatus(long id, ExecutionStatus to, String workerId, String detail) {
    Execution cur = findById(id).orElseThrow(() -> new IllegalStateException("no execution " + id));
    if (!ExecutionTransitions.canTransition(cur.status(), to)) {
      throw new IllegalStateException("illegal transition " + cur.status() + " -> " + to);
    }
    tx.executeWithoutResult(s -> {
      jdbc.update("""
        UPDATE execution SET status=?, worker_id=COALESCE(?, worker_id),
          finished_at=CASE WHEN ? IN ('SUCCESS','FAILED','CANCELED') THEN now() ELSE finished_at END
          WHERE id=?""", to.name(), workerId, to.name(), id);
      jdbc.update("INSERT INTO execution_outcome (execution_id, status, detail) VALUES (?,?,?)",
          id, to.name(), detail);
    });
  }
}