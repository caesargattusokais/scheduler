package dev.scheduler.persistence;

import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.ExecutionTransitions;
import dev.scheduler.core.Shard;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class JdbcShardRepository implements ShardRepository {
  private static final Logger log = LoggerFactory.getLogger(JdbcShardRepository.class);
  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;

  public JdbcShardRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
    this.tx = new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
  }

  private static final RowMapper<Execution> PARENT_MAP = (rs, i) -> new Execution(
      rs.getLong("id"), rs.getLong("task_id"),
      ExecutionStatus.valueOf(rs.getString("status")), rs.getString("idempotency_key"),
      rs.getString("args"), rs.getInt("shard_index"), rs.getInt("shard_count"),
      rs.getInt("attempt"), rs.getString("worker_id"),
      rs.getTimestamp("lease_until") != null ? rs.getTimestamp("lease_until").toInstant() : null,
      rs.getTimestamp("next_retry_at") != null ? rs.getTimestamp("next_retry_at").toInstant() : null,
      rs.getTimestamp("started_at") != null ? rs.getTimestamp("started_at").toInstant() : null,
      rs.getTimestamp("finished_at") != null ? rs.getTimestamp("finished_at").toInstant() : null,
      rs.getString("result_payload"));

  private static final RowMapper<Shard> MAP = (rs, i) -> new Shard(
      rs.getLong("id"), rs.getLong("execution_id"), rs.getInt("shard_index"),
      rs.getString("shard_data"), ExecutionStatus.valueOf(rs.getString("status")),
      rs.getInt("attempt"), rs.getString("worker_id"),
      rs.getTimestamp("lease_until") != null ? rs.getTimestamp("lease_until").toInstant() : null,
      rs.getTimestamp("next_retry_at") != null ? rs.getTimestamp("next_retry_at").toInstant() : null,
      rs.getBoolean("cancel_requested"), rs.getBoolean("dead_letter"),
      rs.getTimestamp("started_at") != null ? rs.getTimestamp("started_at").toInstant() : null,
      rs.getTimestamp("finished_at") != null ? rs.getTimestamp("finished_at").toInstant() : null,
      rs.getString("result_payload"));

  @Override public Execution createParentWithShards(long taskId, String parentKey, int shardCount) {
    // 父 execution:创建(状态 DUE,幂等 by parent_key,works on update 兼容多次扫描)
    Long parentId = jdbc.queryForObject("""
      INSERT INTO execution (task_id, status, idempotency_key, shard_index, shard_count)
      VALUES (?, 'DUE', ?, 0, ?)
      ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
      RETURNING id""", Long.class, taskId, parentKey, shardCount);
    final long pid = parentId;
    tx.executeWithoutResult(s -> {
      int existing = jdbc.queryForObject(
          "SELECT count(*) FROM execution_shard WHERE execution_id=?", Integer.class, pid);
      if (existing == 0) { // 首次物化:插入 N 个 shard(幂等:父重复创建不重复插)
        jdbc.batchUpdate("""
          INSERT INTO execution_shard (execution_id, shard_index, status)
          VALUES (?, ?, 'DUE')""",
          java.util.stream.IntStream.range(0, shardCount)
              .mapToObj(i -> new Object[]{pid, i}).toList());
      }
    });
    return findParent(pid).orElseThrow();
  }

  @Override public Optional<Execution> findParent(long executionId) {
    return jdbc.query("SELECT * FROM execution WHERE id=?", PARENT_MAP, executionId).stream().findFirst();
  }

  @Override public List<Shard> findShards(long executionId) {
    return jdbc.query(
        "SELECT * FROM execution_shard WHERE execution_id=? ORDER BY shard_index", MAP, executionId);
  }

  @Override public Optional<Shard> findShard(long shardId) {
    return jdbc.query("SELECT * FROM execution_shard WHERE id=?", MAP, shardId).stream().findFirst();
  }

  @Override public Optional<Shard> findCandidate(long taskId) {
    // 重试闸:仅当 next_retry_at 为空或已到期待时才视为可领取(否则被重试调度待到点才放行)。
    // JOIN parent execution 以取 task_id(作用对象为 shard)。
    return jdbc.query(
        "SELECT s.* FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
            + " WHERE e.task_id=? AND s.status='DUE'"
            + " AND (s.next_retry_at IS NULL OR s.next_retry_at <= now()) ORDER BY s.id LIMIT 1",
        MAP, taskId).stream().findFirst();
  }

  @Override public boolean claim(long shardId, long taskId, String workerId,
                                 Instant leaseUntil, int maxConcurrent) {
    // 受 task 级 maxConcurrent 闸门:active = 该任务下仍持有有效租约的 RUNNING shard 数。
    String update = """
      WITH active AS (
        SELECT count(*) AS c FROM execution_shard s
         JOIN execution e ON e.id = s.execution_id
         WHERE e.task_id=? AND s.status='RUNNING' AND s.lease_until > now()
      )
      UPDATE execution_shard s SET status='RUNNING', worker_id=?, lease_until=?,
             attempt=attempt+1, started_at=COALESCE(started_at, now())
        FROM active, (SELECT id, task_id FROM execution) e
       WHERE s.id=? AND s.status='DUE' AND e.id=s.execution_id AND e.task_id=? AND active.c < ?""";
    java.sql.Timestamp lease = java.sql.Timestamp.from(leaseUntil);
    final boolean[] ok = {false};
    tx.executeWithoutResult(s -> {
      if (jdbc.update(update, taskId, workerId, lease, shardId, taskId, maxConcurrent) == 1) {
        jdbc.update("INSERT INTO execution_shard_outcome (shard_id, status, detail) VALUES (?,?,?)",
            shardId, "RUNNING", "claim");
        ok[0] = true;
      }
    });
    return ok[0];
  }

  @Override public long countActive(long taskId) {
    Long c = jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
            + " WHERE e.task_id=? AND s.status='RUNNING' AND s.lease_until > now()",
        Long.class, taskId);
    return c == null ? 0 : c;
  }

  @Override public List<ExpiredShard> findExpiredRunning(long taskId) {
    return jdbc.query(
        "SELECT s.id, s.attempt FROM execution_shard s JOIN execution e ON e.id = s.execution_id"
            + " WHERE e.task_id=? AND s.status='RUNNING' AND s.lease_until <= now()",
        (rs, i) -> new ExpiredShard(rs.getLong("id"), rs.getInt("attempt")), taskId);
  }

  @Override public boolean markStatus(long shardId, ExecutionStatus to, String workerId, String detail) {
    Shard cur = findShard(shardId).orElseThrow(() -> new IllegalStateException("no shard " + shardId));
    if (!ExecutionTransitions.canTransition(cur.status(), to)) {
      throw new IllegalStateException("illegal transition " + cur.status() + " -> " + to);
    }
    // 事务内以"读取到的旧状态"做原子 CAS:若 0 行,说明行已被其他动作改走(如另一 shard 某些竞态/回收后
    // 状态变化),cancel/回收竞态视为良性,不落误导性 outcome 静默返回;非法路径已由上面的 read-validate 先行拦截。
    final boolean[] ok = {false};
    tx.executeWithoutResult(s -> {
      int updated = jdbc.update("""
        UPDATE execution_shard SET status=?, worker_id=COALESCE(?, worker_id),
          finished_at=CASE WHEN ? IN ('SUCCESS','FAILED','CANCELED') THEN now() ELSE finished_at END
          WHERE id=? AND status=?""", to.name(), workerId, to.name(), shardId, cur.status().name());
      if (updated == 0) {
        log.debug("markStatus lost CAS race: shard {} no longer {} (owner/status changed); "
            + "suppressing outcome", shardId, cur.status().name());
        return;
      }
      jdbc.update("INSERT INTO execution_shard_outcome (shard_id, status, detail) VALUES (?,?,?)",
          shardId, to.name(), detail);
      ok[0] = true;
    });
    return ok[0];
  }

  @Override public boolean markStatusOwned(long shardId, ExecutionStatus to, String ownerWorkerId, String detail) {
    Shard cur = findShard(shardId).orElseThrow(() -> new IllegalStateException("no shard " + shardId));
    if (!ExecutionTransitions.canTransition(cur.status(), to)) return false; // 行已在他处终态 → stale,静默
    final boolean[] ok = {false};
    tx.executeWithoutResult(s -> {
      int updated = jdbc.update("""
        UPDATE execution_shard SET status=?, worker_id=COALESCE(?, worker_id),
          finished_at=CASE WHEN ? IN ('SUCCESS','FAILED','CANCELED') THEN now() ELSE finished_at END
          WHERE id=? AND status=? AND worker_id=?""",
          to.name(), ownerWorkerId, to.name(), shardId, cur.status().name(), ownerWorkerId);
      if (updated == 0) {
        log.debug("markStatusOwned lost ownership race: shard {} no longer owned by {}; "
            + "suppressing outcome", shardId, ownerWorkerId);
        return;
      }
      jdbc.update("INSERT INTO execution_shard_outcome (shard_id, status, detail) VALUES (?,?,?)",
          shardId, to.name(), detail);
      ok[0] = true;
    });
    return ok[0];
  }

  @Override public void scheduleRetry(long shardId, Instant retryAt, String detail) {
    // FAILED -> DUE,期待值时延由调用方(RetryPolicy,注入 clock)预先算好,仓库只写值。
    // CAS on status='FAILED':0 行=竞态/非 FAILED,静默跳过,不落误导性 outcome。
    java.sql.Timestamp ts = java.sql.Timestamp.from(retryAt);
    tx.executeWithoutResult(s -> {
      int updated = jdbc.update(
          "UPDATE execution_shard SET status='DUE', next_retry_at=? WHERE id=? AND status='FAILED'",
          ts, shardId);
      if (updated == 0) {
        log.debug("scheduleRetry lost CAS race: shard {} no longer FAILED; "
            + "suppressing outcome", shardId);
        return;
      }
      jdbc.update("INSERT INTO execution_shard_outcome (shard_id, status, detail) VALUES (?,?,?)",
          shardId, "DUE", detail);
    });
  }

  @Override public void markDeadLetter(long shardId, String detail) {
    // 仅置 dead_letter 标记,不动 status,故无 outcome 行。
    tx.executeWithoutResult(s -> {
      int updated = jdbc.update(
          "UPDATE execution_shard SET dead_letter=true WHERE id=? AND status='FAILED'", shardId);
      if (updated == 0) {
        log.debug("markDeadLetter lost CAS race: shard {} no longer FAILED; skipping (detail: {})",
            shardId, detail);
      }
    });
  }

  @Override public boolean isCancelRequested(long shardId) {
    Boolean b = jdbc.queryForObject("SELECT cancel_requested FROM execution_shard WHERE id=?",
        Boolean.class, shardId);
    return b != null && b;
  }
}
