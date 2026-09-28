package dev.scheduler.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class JdbcRuntimeConfigRepository implements RuntimeConfigRepository {
  private static final String COLS = "key, value, updated_by, updated_at";
  private final JdbcTemplate jdbc;

  public JdbcRuntimeConfigRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

  private static final RowMapper<RuntimeConfigRow> ROW = (ResultSet rs, int rn) -> {
    try {
      return new RuntimeConfigRow(
          rs.getString("key"),
          rs.getString("value"),
          rs.getString("updated_by"),
          rs.getTimestamp("updated_at").toInstant());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  };

  @Override public Optional<RuntimeConfigRow> find(String key) {
    return jdbc.query("SELECT " + COLS + " FROM app_runtime_config WHERE key = ?", ROW, key)
        .stream().findFirst();
  }

  @Override public List<RuntimeConfigRow> findAll() {
    return jdbc.query("SELECT " + COLS + " FROM app_runtime_config ORDER BY key", ROW);
  }

  @Override public void upsert(String key, String value, String operator) {
    jdbc.update("INSERT INTO app_runtime_config (key, value, updated_by) VALUES (?, ?, ?) "
        + "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, "
        + "updated_by = EXCLUDED.updated_by, updated_at = now()", key, value, operator);
  }
}