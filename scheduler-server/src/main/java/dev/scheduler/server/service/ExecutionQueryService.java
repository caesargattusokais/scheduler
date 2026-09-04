package dev.scheduler.server.service;

import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.persistence.ExecutionRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/** 执行读取服务:承载 GET /executions 的原生 SQL 查询与行映射。只读,无行为改动(自 ExecutionController 单点迁出)。 */
public class ExecutionQueryService {

  private final JdbcTemplate jdbc;
  private final ExecutionRepository executions;

  public ExecutionQueryService(JdbcTemplate jdbc, ExecutionRepository executions) {
    this.jdbc = jdbc;
    this.executions = executions;
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

  /** 执行详情。委托仓储单一来源 findById,不新增第二条查询。 */
  public Optional<Execution> get(long id) {
    return executions.findById(id);
  }
}