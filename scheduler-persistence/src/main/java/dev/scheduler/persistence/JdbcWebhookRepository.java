package dev.scheduler.persistence;

import java.sql.PreparedStatement;
import java.sql.SQLException;
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
      "id, url, secret, kinds, enabled, max_attempts, backoff_ms, created_at";

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
            rs.getTimestamp("created_at").toInstant()));
  }

  @Override
  public long create(String url, String secret, List<String> kinds, boolean enabled,
                     int maxAttempts, long backoffMs) {
    List<Long> ids = jdbc.query(
        "INSERT INTO app_webhook (url, secret, kinds, enabled, max_attempts, backoff_ms)"
            + " VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
        (PreparedStatementSetter) ps -> {
          bind(ps, url, secret, kinds, enabled, maxAttempts, backoffMs);
        }, (rs, row) -> rs.getLong(1));
    return ids.get(0);
  }

  @Override
  public boolean update(long id, String url, String secret, List<String> kinds, boolean enabled,
                        int maxAttempts, long backoffMs) {
    return jdbc.update(
        "UPDATE app_webhook SET url = ?, secret = ?, kinds = ?, enabled = ?,"
            + " max_attempts = ?, backoff_ms = ? WHERE id = ?",
        (PreparedStatementSetter) ps -> {
          bind(ps, url, secret, kinds, enabled, maxAttempts, backoffMs);
          ps.setLong(7, id);
        }) > 0;
  }

  @Override
  public boolean delete(long id) {
    return jdbc.update("DELETE FROM app_webhook WHERE id = ?", id) > 0;
  }

  /** 绑定前 6 个业务列(url → backoff_ms);kinds 以 text[] 落库。 */
  private static void bind(PreparedStatement ps, String url, String secret, List<String> kinds,
                           boolean enabled, int maxAttempts, long backoffMs) throws SQLException {
    ps.setString(1, url);
    ps.setString(2, secret);
    ps.setArray(3, ps.getConnection().createArrayOf("text", kinds.toArray(new String[0])));
    ps.setBoolean(4, enabled);
    ps.setInt(5, maxAttempts);
    ps.setLong(6, backoffMs);
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
}