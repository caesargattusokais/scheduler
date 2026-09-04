package dev.scheduler.server.service;

import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.ShardRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/** 执行读取服务:承载 GET /executions 的原生 SQL 查询与行映射,以及详情(父 header + 其 shard)。只读,无行为改动。 */
public class ExecutionQueryService {

  private final JdbcTemplate jdbc;
  private final ExecutionRepository executions;
  private final ShardRepository shards;

  public ExecutionQueryService(JdbcTemplate jdbc, ExecutionRepository executions,
                               ShardRepository shards) {
    this.jdbc = jdbc;
    this.executions = executions;
    this.shards = shards;
  }

  private static final RowMapper<Execution> ROW = (rs, i) -> new Execution(
      rs.getLong("id"), rs.getLong("task_id"),
      ExecutionStatus.valueOf(rs.getString("status")), rs.getString("idempotency_key"),
      rs.getString("args"), rs.getInt("shard_index"), rs.getInt("shard_count"),
      rs.getInt("attempt"), rs.getString("worker_id"),
      rs.getTimestamp("lease_until") != null ? rs.getTimestamp("lease_until").toInstant() : null,
      rs.getTimestamp("next_retry_at") != null ? rs.getTimestamp("next_retry_at").toInstant() : null,
      rs.getTimestamp("started_at") != null ? rs.getTimestamp("started_at").toInstant() : null,
      rs.getTimestamp("finished_at") != null ? rs.getTimestamp("finished_at").toInstant() : null,
      rs.getString("result_payload"));

  /** 执行列表:taskId/status 可选过滤,id 倒序。语义与行为与原执行控制器一致。 */
  public List<Execution> list(Long taskId, String status) {
    StringBuilder sql = new StringBuilder("SELECT * FROM execution WHERE true");
    List<Object> args = new ArrayList<>();
    if (taskId != null) {
      sql.append(" AND task_id=?");
      args.add(taskId);
    }
    if (status != null && !status.isBlank()) {
      sql.append(" AND status=?");
      args.add(status);
    }
    sql.append(" ORDER BY id DESC");
    return jdbc.query(sql.toString(), ROW, args.toArray());
  }

  /** 派生控制面展示的父 status:父非终态(DUE/RUNNING)且 ≥1 RUNNING shard → 读作 RUNNING。读取只映射,不写库。 */
  public static ExecutionStatus deriveStatus(Execution parent, List<Shard> shards) {
    ExecutionStatus s = parent.status();
    if ((s == ExecutionStatus.DUE || s == ExecutionStatus.RUNNING)
        && shards.stream().anyMatch(sh -> sh.status() == ExecutionStatus.RUNNING)) {
      return ExecutionStatus.RUNNING;
    }
    return s;
  }

  /** 执行详情:父 header + 其全部分片(按 shard_index 升序)。展示的父 status 为派生值,DB 中父仍存原值。 */
  public Optional<ExecutionDetail> getDetail(long id) {
    return executions.findById(id).map(parent -> {
      List<Shard> ss = shards.findShards(id);
      return new ExecutionDetail(id, parent.taskId(), deriveStatus(parent, ss), ss.size(), ss);
    });
  }
}
