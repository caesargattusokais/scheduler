package dev.scheduler.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class JdbcWorkerRepository implements WorkerRepository {
  private final JdbcTemplate jdbc;

  public JdbcWorkerRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  private static final RowMapper<WorkerRegistration> MAP = (rs, i) -> from(rs);

  private static WorkerRegistration from(ResultSet rs) throws SQLException {
    String refs = rs.getString("refs");
    return new WorkerRegistration(
        rs.getString("id"),
        refs == null || refs.isBlank() ? List.of() : Arrays.asList(refs.split(",")),
        rs.getTimestamp("last_seen").toInstant(),
        rs.getString("status"));
  }

  @Override
  public void upsertHeartbeat(WorkerRegistration w) {
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES (?, ?, ?, ?) "
        + "ON CONFLICT (id) DO UPDATE SET refs=EXCLUDED.refs, "
        + "last_seen=EXCLUDED.last_seen, status=EXCLUDED.status",
        w.id(), String.join(",", w.refs()), java.sql.Timestamp.from(w.lastSeen()), w.status());
  }

  @Override
  public List<WorkerRegistration> findAllAlive(Instant lastSeenAtLeast) {
    return jdbc.query("SELECT id, refs, last_seen, status FROM worker "
        + "WHERE status='ALIVE' AND last_seen >= ?", MAP, java.sql.Timestamp.from(lastSeenAtLeast));
  }

  @Override
  public void purgeStale(Instant olderThan) {
    jdbc.update("DELETE FROM worker WHERE last_seen < ?", java.sql.Timestamp.from(olderThan));
  }
}