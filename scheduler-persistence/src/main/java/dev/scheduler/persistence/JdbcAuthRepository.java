package dev.scheduler.persistence;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;

/** {@link AuthRepository} 的 JDBC 实现。令牌明文经 SHA-256 散列后落库,明文不入库。 */
public class JdbcAuthRepository implements AuthRepository {
  private final JdbcTemplate jdbc;

  public JdbcAuthRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public Instant now() {
    // PGJDBC 42.6 的 getObject(col, Instant.class) 不支持(仅 Timestamp/LocalDateTime/OffsetDateTime)。
    // 经 Timestamp.toInstant 收敛到 DB now()(与 JdbcAuthRepositoryTest 既有读法同源,保证语义一致)。
    return jdbc.queryForObject("SELECT now()", Timestamp.class).toInstant();
  }

  @Override
  public Optional<Session> resolve(String rawToken) {
    String hash = AuthHashing.sha256(rawToken);
    List<Map<String, Object>> rows = jdbc.queryForList(
        "SELECT operator_name, created_at, expires_at FROM app_auth_session"
            + " WHERE token_hash = ? AND revoked_at IS NULL AND expires_at > now()"
            + " AND now() - created_at < interval '5 days'",
        hash);
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    Map<String, Object> r = rows.get(0);
    return Optional.of(new Session((String) r.get("operator_name"),
        ((Timestamp) r.get("created_at")).toInstant(),
        ((Timestamp) r.get("expires_at")).toInstant()));
  }

  @Override
  public void create(String rawToken, String operator, Duration ttl) {
    // expires_at 由 SQL 以 DB now() 计算,统一 DB 时钟,避免应用/DB 时区漂移。
    jdbc.update(
        "INSERT INTO app_auth_session (token_hash, operator_name, created_at, expires_at)"
            + " VALUES (?, ?, now(), now() + (?) * interval '1 second')",
        AuthHashing.sha256(rawToken), operator, ttl.getSeconds());
  }

  @Override
  public void revoke(String rawToken) {
    jdbc.update("UPDATE app_auth_session SET revoked_at = now() WHERE token_hash = ?",
        AuthHashing.sha256(rawToken));
  }

  @Override
  public void revokeAllForOperator(String operator) {
    jdbc.update(
        "UPDATE app_auth_session SET revoked_at = now()"
            + " WHERE operator_name = ? AND revoked_at IS NULL",
        operator);
  }

  @Override
  public Optional<Instant> lockedUntil(String name) {
    List<Timestamp> locked = jdbc.query(
        "SELECT locked_until FROM app_login_attempt WHERE name = ? AND locked_until > now()",
        (rs, i) -> rs.getTimestamp(1), name);
    return locked.isEmpty() ? Optional.empty() : Optional.of(locked.get(0).toInstant());
  }

  @Override
  public void recordFailure(String name, int maxLockSeconds) {
    // 一步 UPSERT:冲突时读当前 failures 并对其 +1,locked_until = DB now() + min(2^newFailures, max)。
    jdbc.update(
        "INSERT INTO app_login_attempt (name, failures, locked_until)"
            + " VALUES (?, 1, now() + least(power(2, 1)::int, ?) * interval '1 second')"
            + " ON CONFLICT (name) DO UPDATE SET"
            + " failures = app_login_attempt.failures + 1,"
            + " locked_until = now() + least(power(2, app_login_attempt.failures + 1)::int, ?)"
            + " * interval '1 second'",
        name, maxLockSeconds, maxLockSeconds);
  }

  @Override
  public void resetLockout(String name) {
    jdbc.update("DELETE FROM app_login_attempt WHERE name = ?", name);
  }
}