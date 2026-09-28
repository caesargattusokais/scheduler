package dev.scheduler.persistence;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;

public class JdbcWebhookRepository implements WebhookRepository {
  private final JdbcTemplate jdbc;

  public JdbcWebhookRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  private static final String COLS =
      "id, url, secret, kinds, enabled, max_attempts, backoff_ms, created_at,"
          + " scope_mode, selected_task_ids, selected_dag_ids";

  @Override
  public List<Webhook> list() {
    return jdbc.query("SELECT " + COLS + " FROM app_webhook ORDER BY id",
        (rs, row) -> new Webhook(
            rs.getLong("id"),
            rs.getString("url"),
            rs.getString("secret"),
            kinds(rs.getArray("kinds")),
            rs.getBoolean("enabled"),
            rs.getInt("max_attempts"),
            rs.getLong("backoff_ms"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getString("scope_mode"),
            ids(rs.getArray("selected_task_ids")),
            ids(rs.getArray("selected_dag_ids"))));
  }

  @Override
  public long create(String url, String secret, List<String> kinds, boolean enabled,
                     int maxAttempts, long backoffMs,
                     String scopeMode, List<Long> selectedTaskIds, List<Long> selectedDagIds) {
    List<Long> ids = jdbc.query(
        "INSERT INTO app_webhook (url, secret, kinds, enabled, max_attempts, backoff_ms,"
            + " scope_mode, selected_task_ids, selected_dag_ids)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
        (PreparedStatementSetter) ps -> {
          bind(ps, url, secret, kinds, enabled, maxAttempts, backoffMs,
              scopeMode, selectedTaskIds, selectedDagIds);
        }, (rs, row) -> rs.getLong(1));
    return ids.get(0);
  }

  @Override
  public boolean update(long id, String url, String secret, List<String> kinds, boolean enabled,
                        int maxAttempts, long backoffMs,
                        String scopeMode, List<Long> selectedTaskIds, List<Long> selectedDagIds) {
    return jdbc.update(
        "UPDATE app_webhook SET url = ?, secret = ?, kinds = ?, enabled = ?,"
            + " max_attempts = ?, backoff_ms = ?, scope_mode = ?,"
            + " selected_task_ids = ?, selected_dag_ids = ? WHERE id = ?",
        (PreparedStatementSetter) ps -> {
          bind(ps, url, secret, kinds, enabled, maxAttempts, backoffMs,
              scopeMode, selectedTaskIds, selectedDagIds);
          ps.setLong(10, id);
        }) > 0;
  }

  @Override
  public boolean delete(long id) {
    return jdbc.update("DELETE FROM app_webhook WHERE id = ?", id) > 0;
  }

  /** 绑定前 9 个业务列(url → selected_dag_ids);kinds/selected_*_ids 以 PG 数组落库。 */
  private static void bind(PreparedStatement ps, String url, String secret, List<String> kinds,
                           boolean enabled, int maxAttempts, long backoffMs,
                           String scopeMode, List<Long> selectedTaskIds, List<Long> selectedDagIds)
      throws SQLException {
    ps.setString(1, url);
    ps.setString(2, secret);
    ps.setArray(3, ps.getConnection().createArrayOf("text", kinds.toArray(new String[0])));
    ps.setBoolean(4, enabled);
    ps.setInt(5, maxAttempts);
    ps.setLong(6, backoffMs);
    ps.setString(7, scopeMode);
    ps.setArray(8, ps.getConnection().createArrayOf("bigint", selectedTaskIds.toArray(new Long[0])));
    ps.setArray(9, ps.getConnection().createArrayOf("bigint", selectedDagIds.toArray(new Long[0])));
  }

  private static List<String> kinds(java.sql.Array array) {
    if (array == null) return Collections.emptyList();
    try {
      Object raw = array.getArray();
      return raw == null ? Collections.emptyList() : Arrays.asList((String[]) raw);
    } catch (SQLException e) {
      return Collections.emptyList();
    }
  }

  /** bigint[] → List<Long>;PG 返回 Long[]/long[],统一按 Number 取值以避免装箱类型漂移。 */
  private static List<Long> ids(java.sql.Array array) {
    if (array == null) return Collections.emptyList();
    try {
      Object raw = array.getArray();
      List<Long> out = new ArrayList<>();
      if (raw != null) for (Object o : (Object[]) raw) out.add(((Number) o).longValue());
      return out;
    } catch (SQLException e) {
      return Collections.emptyList();
    }
  }
}