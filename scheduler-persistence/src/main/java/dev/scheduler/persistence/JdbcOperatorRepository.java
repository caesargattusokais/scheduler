package dev.scheduler.persistence;

import dev.scheduler.core.OperatorEntry;
import dev.scheduler.core.OperatorRole;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcOperatorRepository implements OperatorRepository {
  private final JdbcTemplate jdbc;

  public JdbcOperatorRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public Optional<OperatorRole> roleOf(String name) {
    List<String> roles = jdbc.query("SELECT role FROM app_operator WHERE name = ? AND active",
        (rs, i) -> rs.getString(1), name);
    return roles.isEmpty() ? Optional.empty() : Optional.of(OperatorRole.valueOf(roles.get(0)));
  }

  @Override
  public List<OperatorEntry> list() {
    return jdbc.query("SELECT name, role, active FROM app_operator ORDER BY name",
        (rs, i) -> new OperatorEntry(rs.getString("name"),
            OperatorRole.valueOf(rs.getString("role")), rs.getBoolean("active")));
  }

  @Override
  public void upsert(String name, OperatorRole role, boolean active) {
    jdbc.update("INSERT INTO app_operator (name, role, active) VALUES (?, ?, ?) "
            + "ON CONFLICT (name) DO UPDATE SET role = EXCLUDED.role, active = EXCLUDED.active",
        name, role.name(), active);
  }

  @Override
  public void deactivate(String name) {
    jdbc.update("UPDATE app_operator SET active = false WHERE name = ?", name);
  }

  @Override
  public Optional<String> activePasswordHash(String name) {
    List<String> hashes = jdbc.query(
        "SELECT password_hash FROM app_operator WHERE name = ? AND active AND password_hash IS NOT NULL",
        (rs, i) -> rs.getString(1), name);
    return hashes.isEmpty() ? Optional.empty() : Optional.of(hashes.get(0));
  }

  @Override
  public void setPassword(String name, String bcryptHash) {
    jdbc.update("UPDATE app_operator SET password_hash = ? WHERE name = ?", bcryptHash, name);
  }

  @Override
  public List<String> namesWithoutPassword() {
    return jdbc.query("SELECT name FROM app_operator WHERE password_hash IS NULL ORDER BY name",
        (rs, i) -> rs.getString("name"));
  }

  @Override
  public Optional<Boolean> mustChangePassword(String name) {
    List<Boolean> flags = jdbc.query(
        "SELECT must_change_password FROM app_operator WHERE name = ?",
        (rs, i) -> rs.getBoolean(1), name);
    return flags.isEmpty() ? Optional.empty() : Optional.of(flags.get(0));
  }

  @Override
  public void setMustChangePassword(String name, boolean v) {
    jdbc.update("UPDATE app_operator SET must_change_password = ? WHERE name = ?", v, name);
  }
}