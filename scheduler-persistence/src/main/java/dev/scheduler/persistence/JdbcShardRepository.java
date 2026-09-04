package dev.scheduler.persistence;

import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class JdbcShardRepository implements ShardRepository {
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
}