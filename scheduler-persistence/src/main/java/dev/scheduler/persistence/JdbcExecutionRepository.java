package dev.scheduler.persistence;

import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.ExecutionTransitions;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class JdbcExecutionRepository implements ExecutionRepository {
  private static final Logger log = LoggerFactory.getLogger(JdbcExecutionRepository.class);
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
      rs.getString("result_payload"), (Long) rs.getObject("rerun_of"));

  @Override public long createDue(Execution e) {
    String sql = """
      INSERT INTO execution (task_id, status, idempotency_key, args, shard_index, shard_count, attempt, started_at)
      VALUES (?, 'DUE', ?, ?, ?, ?, ?, now())
      ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
      RETURNING id""";
    Long id = jdbc.queryForObject(sql, Long.class,
        e.taskId(), e.idempotencyKey(), e.args(), e.shardIndex(), e.shardCount(), e.attempt());
    return id;
  }

  @Override public Optional<Execution> findCandidate(long taskId) {
    // 重试闸:仅当 next_retry_at 为空或已到期待时才视为可领取(否则被重试调度待到点才放行)。
    return jdbc.query(
        "SELECT * FROM execution WHERE task_id=? AND status='DUE'"
            + " AND (next_retry_at IS NULL OR next_retry_at <= now()) ORDER BY id LIMIT 1",
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

  @Override public List<ExpiredRun> findExpiredRunning(long taskId) {
    return jdbc.query(
        "SELECT id, attempt FROM execution WHERE task_id=? AND status='RUNNING'"
            + " AND lease_until <= now()",
        (rs, i) -> new ExpiredRun(rs.getLong("id"), rs.getInt("attempt")), taskId);
  }

  @Override public boolean markStatus(long id, ExecutionStatus to, String workerId, String detail) {
    Execution cur = findById(id).orElseThrow(() -> new IllegalStateException("no execution " + id));
    if (!ExecutionTransitions.canTransition(cur.status(), to)) {
      throw new IllegalStateException("illegal transition " + cur.status() + " -> " + to);
    }
    // 事务内以"读取到的旧状态"做原子 CAS:若 0 行,说明行已被其他动作(如 worker 恰在此间 claim 成
    // RUNNING 的 cancel)改走,cancel-claim 竞态视为良性,不落误导性 outcome 静默返回;非法路径仍由
    // 上面的 read-validate 先行拦截。
    final boolean[] ok = {false};
    tx.executeWithoutResult(s -> {
      int updated = jdbc.update("""
        UPDATE execution SET status=?, worker_id=COALESCE(?, worker_id),
          finished_at=CASE WHEN ? IN ('SUCCESS','FAILED','CANCELED') THEN now() ELSE finished_at END
          WHERE id=? AND status=?""", to.name(), workerId, to.name(), id, cur.status().name());
      if (updated == 0) {
        log.debug("markStatus lost CAS race: execution {} no longer {} (owner/status changed); "
            + "suppressing outcome", id, cur.status().name());
        return;
      }
      jdbc.update("INSERT INTO execution_outcome (execution_id, status, detail) VALUES (?,?,?)",
          id, to.name(), detail);
      ok[0] = true;
    });
    return ok[0];
  }

  @Override public boolean markStatusOwned(long id, ExecutionStatus to, String ownerWorkerId, String detail) {
    Execution cur = findById(id).orElseThrow(() -> new IllegalStateException("no execution " + id));
    if (!ExecutionTransitions.canTransition(cur.status(), to)) return false; // 行已在他处终态 → stale,静默
    final boolean[] ok = {false};
    tx.executeWithoutResult(s -> {
      int updated = jdbc.update("""
        UPDATE execution SET status=?, worker_id=COALESCE(?, worker_id),
          finished_at=CASE WHEN ? IN ('SUCCESS','FAILED','CANCELED') THEN now() ELSE finished_at END
          WHERE id=? AND status=? AND worker_id=?""",
          to.name(), ownerWorkerId, to.name(), id, cur.status().name(), ownerWorkerId);
      if (updated == 0) {
        log.debug("markStatusOwned lost ownership race: execution {} no longer owned by {}; "
            + "suppressing outcome", id, ownerWorkerId);
        return;
      }
      jdbc.update("INSERT INTO execution_outcome (execution_id, status, detail) VALUES (?,?,?)",
          id, to.name(), detail);
      ok[0] = true;
    });
    return ok[0];
  }

  @Override public void scheduleRetry(long id, Instant retryAt, String detail) {
    // FAILED -> DUE,期待值时延由调用方(RetryPolicy,注入 clock)预先算好,仓库只写值。
    // CAS on status='FAILED':0 行=竞态/非 FAILED,静默跳过,不落误导性 outcome。
    java.sql.Timestamp ts = java.sql.Timestamp.from(retryAt);
    tx.executeWithoutResult(s -> {
      int updated = jdbc.update(
          "UPDATE execution SET status='DUE', next_retry_at=? WHERE id=? AND status='FAILED'",
          ts, id);
      if (updated == 0) {
        log.debug("scheduleRetry lost CAS race: execution {} no longer FAILED; "
            + "suppressing outcome", id);
        return;
      }
      jdbc.update("INSERT INTO execution_outcome (execution_id, status, detail) VALUES (?,?,?)",
          id, "DUE", detail);
    });
  }

  @Override public void markDeadLetter(long id, String detail) {
    // 仅置 dead_letter 标记,不动 status,故无 outcome 行。
    tx.executeWithoutResult(s -> {
      int updated = jdbc.update(
          "UPDATE execution SET dead_letter=true WHERE id=? AND status='FAILED'", id);
      if (updated == 0) {
        log.debug("markDeadLetter lost CAS race: execution {} no longer FAILED; skipping (detail: {})",
            id, detail);
      }
    });
  }

  @Override public boolean requestCancel(long id) {
    // 仅 RUNNING 生效且不动状态(取消请求非状态迁移,真实转 CANCELED 由 worker markStatus 落)。
    // CAS on status='RUNNING':0 行=DUE/终态或竞态被 claim 走,返回 false,不落 outcome。
    return jdbc.update(
        "UPDATE execution SET cancel_requested=true WHERE id=? AND status='RUNNING'", id) == 1;
  }

  @Override public boolean isCancelRequested(long id) {
    Boolean b = jdbc.queryForObject("SELECT cancel_requested FROM execution WHERE id=?",
        Boolean.class, id);
    return b != null && b;
  }

  @Override public List<Execution> findDeadLetters() {
    // DLQ 读取:FAILED 且已置 dead_letter 标记,按 id 升序。
    return jdbc.query(
        "SELECT * FROM execution WHERE status='FAILED' AND dead_letter ORDER BY id", MAP);
  }

  @Override public ExecutionSli sli() {
    // 单查询一次算齐三项:1h/24h 窗口按 finished_at 切(终态必写 finished_at)。p95 用 percentile_cont(0.95)
    // 对 (finished_at - started_at) 完成延迟(epoch 秒 × 1000 → ms),无样本(24h 内无终态)为 NULL → 兜 0。
    String sql = """
      SELECT
        count(*) FILTER (WHERE status IN ('SUCCESS','CANCELED') AND finished_at >= now() - interval '1 hour') AS completed1h,
        count(*) FILTER (WHERE status = 'FAILED' AND finished_at >= now() - interval '1 hour') AS failed1h,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM (finished_at - started_at)) * 1000.0)
          FILTER (WHERE finished_at >= now() - interval '24 hours') AS p95_ms
      FROM execution
      WHERE status IN ('SUCCESS','CANCELED','FAILED')""";
    return jdbc.query(sql, rs -> {
      if (!rs.next()) return new ExecutionSli(0, 0, 0.0);
      long completed = rs.getLong("completed1h");
      long failed = rs.getLong("failed1h");
      double p95 = rs.getObject("p95_ms") == null ? 0.0 : rs.getDouble("p95_ms");
      return new ExecutionSli(completed, failed, p95);
    });
  }

  @Override public boolean requeue(long id) {
    // 手动重新入队:FAILD → DUE,复位 attempt/next_retry_at/dead_letter。dead_letter 仅是标记非闸门,
    // CAS 只 on status='FAILED':0 行=已非 FAILED(竞态/终态)时视为丢失,静默返回 false,不落误导性 outcome。
    final boolean[] ok = {false};
    tx.executeWithoutResult(s -> {
      int updated = jdbc.update(
          "UPDATE execution SET status='DUE', attempt=0, next_retry_at=NULL, dead_letter=false "
              + "WHERE id=? AND status='FAILED'", id);
      if (updated == 0) {
        log.debug("requeue lost CAS race: execution {} no longer FAILED; suppressing outcome", id);
        return;
      }
      jdbc.update("INSERT INTO execution_outcome (execution_id, status, detail) VALUES (?,?,?)",
          id, "DUE", "requeue");
      ok[0] = true;
    });
    return ok[0];
  }
}
