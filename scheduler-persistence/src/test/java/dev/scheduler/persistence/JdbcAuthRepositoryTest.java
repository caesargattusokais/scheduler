package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.OperatorRole;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcAuthRepositoryTest extends AbstractPostgresTest {
  // 会话令牌:64-char lowercase hex(与生产 SecureRandom 32 bytes 同形,测试用固定值便于断言散列)。
  private static final String TOKEN =
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  private static final String TOKEN2 =
      "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
  private static final String TOKEN3 =
      "1111111122222222333333334444444455555555666666667777777788888888";

  private JdbcAuthRepository auth;
  private JdbcOperatorRepository operators;

  @BeforeEach
  void clean() {
    jdbc.execute("TRUNCATE app_auth_session, app_login_attempt RESTART IDENTITY CASCADE");
    jdbc.execute("TRUNCATE app_operator RESTART IDENTITY CASCADE");
    reseedAliceBob();
    auth = new JdbcAuthRepository(jdbc);
    operators = new JdbcOperatorRepository(jdbc);
  }

  private void reseedAliceBob() {
    jdbc.update("INSERT INTO app_operator (name, role, active) VALUES (?,?,true),(?,?,true)",
        "alice", OperatorRole.ADMIN.name(), "bob", OperatorRole.OPERATOR.name());
  }

  @Test
  void migration_createsPasswordColumn_andAuthTables() {
    Long pw = jdbc.queryForObject(
        "SELECT count(*) FROM information_schema.columns"
            + " WHERE table_name='app_operator' AND column_name='password_hash'", Long.class);
    assertEquals(1L, pw);
    Long sess = jdbc.queryForObject(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='app_auth_session'",
        Long.class);
    Long att = jdbc.queryForObject(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='app_login_attempt'",
        Long.class);
    assertEquals(1L, sess);
    assertEquals(1L, att);
  }

  @Test
  void create_thenResolve_ok() {
    auth.create(TOKEN, "alice", Duration.ofHours(8));
    var s = auth.resolve(TOKEN);
    assertTrue(s.isPresent());
    assertEquals("alice", s.get().operator());
    assertTrue(s.get().created().isBefore(s.get().expires()), "created < expires");
  }

  @Test
  void token_sha256Hashed_storedNotPlaintext() {
    auth.create(TOKEN, "alice", Duration.ofHours(1));
    String stored = jdbc.queryForObject(
        "SELECT token_hash FROM app_auth_session WHERE operator_name='alice'", String.class);
    assertEquals(AuthHashing.sha256(TOKEN), stored, "落库应为 SHA-256(token)");
    assertEquals(0L, jdbc.queryForObject(
        "SELECT count(*) FROM app_auth_session WHERE token_hash = ?", Long.class, TOKEN),
        "token 明文不得入库");
  }

  @Test
  void resolve_wrongTokenAndRevoked_empty() {
    auth.create(TOKEN, "alice", Duration.ofHours(8));
    assertTrue(auth.resolve(TOKEN2).isEmpty(), "错误 token → empty");
    auth.revoke(TOKEN);
    assertTrue(auth.resolve(TOKEN).isEmpty(), "撤销后 → empty");
  }

  @Test
  void resolve_expired_empty() {
    auth.create(TOKEN, "alice", Duration.ofHours(8));
    jdbc.update("UPDATE app_auth_session SET expires_at = now() - interval '1 minute' WHERE token_hash = ?",
        AuthHashing.sha256(TOKEN));
    assertTrue(auth.resolve(TOKEN).isEmpty(), "expires_at 已过 → empty");
  }

  @Test
  void resolve_absoluteLifetimeExceeded_empty() {
    auth.create(TOKEN, "alice", Duration.ofHours(8));
    // 即使 expires_at 未到,绝对寿命(自创建 > 5d)也应使会话失效。
    jdbc.update("UPDATE app_auth_session SET created_at = now() - interval '6 days' WHERE token_hash = ?",
        AuthHashing.sha256(TOKEN));
    assertTrue(auth.resolve(TOKEN).isEmpty(), "绝对寿命超 5d → empty");
  }

  @Test
  void revokeAllForOperator_revokesAll() {
    auth.create(TOKEN, "alice", Duration.ofHours(1));
    auth.create(TOKEN2, "alice", Duration.ofHours(1));
    auth.create(TOKEN3, "bob", Duration.ofHours(1));
    auth.revokeAllForOperator("alice");
    assertTrue(auth.resolve(TOKEN).isEmpty());
    assertTrue(auth.resolve(TOKEN2).isEmpty());
    var bob = auth.resolve(TOKEN3);
    assertTrue(bob.isPresent() && bob.get().operator().equals("bob"), "bob 会话不受影响");
  }

  @Test
  void activeSessions_listsOnlyLiveOrdered_byCreated() {
    auth.create(TOKEN, "alice", Duration.ofHours(8));   // 活动
    auth.create(TOKEN2, "alice", Duration.ofHours(8));  // 将被撤销
    auth.create(TOKEN3, "alice", Duration.ofHours(8));  // 将被过期
    auth.revoke(TOKEN2);
    jdbc.update("UPDATE app_auth_session SET expires_at = now() - interval '1 minute' WHERE token_hash = ?",
        AuthHashing.sha256(TOKEN3));

    var rows = auth.activeSessions("alice");
    assertEquals(1, rows.size(), "只应列出未撤销且未过期的活动会话");
    assertEquals(AuthHashing.sha256(TOKEN).substring(0, 10), rows.get(0).tokenPrefix(),
        "tokenPrefix = token_hash 前 10 位(非秘密展示键)");
    assertTrue(rows.get(0).createdAt().isBefore(rows.get(0).expiresAt()), "created < expires");
    assertEquals(0, auth.activeSessions("bob").size(), "无会话 → 空");
  }

  @Test
  void revokeAllForOperator_returnsAffectedActiveCount() {
    auth.create(TOKEN, "alice", Duration.ofHours(1));
    auth.create(TOKEN2, "alice", Duration.ofHours(1));
    auth.create(TOKEN3, "bob", Duration.ofHours(1));
    auth.revoke(TOKEN2); // 已撤销的不计入返回
    assertEquals(1, auth.revokeAllForOperator("alice"), "应返回本次实际置 revoked 的活动会话数");
    assertTrue(auth.resolve(TOKEN).isEmpty());
    assertTrue(auth.resolve(TOKEN2).isEmpty(), "本已撤销的仍不可解析");
    var bob = auth.resolve(TOKEN3);
    assertTrue(bob.isPresent(), "bob 会话不受影响");
  }

  @Test
  void recordFailure_backoffEscalates_cappedAt30s() {
    auth.recordFailure("alice", 30);
    Instant one = auth.lockedUntil("alice").get();
    assertTrue(one.isAfter(Instant.now()), "一次失败即锁定 > now");

    auth.recordFailure("alice", 30);
    Instant two = auth.lockedUntil("alice").get();
    assertTrue(two.isAfter(one), "连续失败退避递增");

    // 幂等累积多次(递增指数)应被上限 30s 封顶。
    for (int i = 0; i < 12; i++) auth.recordFailure("alice", 30);
    // locked_until 由 DB now() 计算,JVM 与 Testcontainers 容器时钟偏斜 ~16s → 用 DB now() 比较(与判活/上报同源,见 runtime-deepening 备忘)。
    Instant dbNow = jdbc.queryForObject("SELECT now()", java.sql.Timestamp.class).toInstant();
    long cap = Duration.between(dbNow, auth.lockedUntil("alice").get()).toSeconds();
    assertTrue(cap >= 0 && cap <= 30, "backoff 封顶 ≤ 30s,实际=" + cap);
  }

  @Test
  void now_returnsDBCurrentTime() {
    Instant a = auth.now();
    Instant b = auth.now();
    assertTrue(!b.isBefore(a), "DB now() 应单调不减");
    Instant db = jdbc.queryForObject("SELECT now()", java.sql.Timestamp.class).toInstant();
    assertTrue(Duration.between(db, a).abs().toSeconds() < 5, "auth.now() 应与裸 SELECT now() 同源(误差<5s)");
  }

  @Test
  void resetLockout_clears() {
    auth.recordFailure("alice", 30);
    assertTrue(auth.lockedUntil("alice").isPresent());
    auth.resetLockout("alice");
    assertTrue(auth.lockedUntil("alice").isEmpty(), "重置后不再锁定");
  }

  @Test
  void operator_setAndGetPassword_activeRequired() {
    assertEquals(0L, jdbc.queryForObject(
        "SELECT count(*) FROM app_operator WHERE name='alice' AND active AND password_hash IS NOT NULL", Long.class));

    operators.setPassword("alice", "bcrypt-hash");
    assertEquals("bcrypt-hash", operators.activePasswordHash("alice").orElseThrow());

    // 停用 → 即使有哈希也不再返回。
    operators.deactivate("alice");
    assertTrue(operators.activePasswordHash("alice").isEmpty());

    // 重新启用但清空哈希(无密)→ empty。
    operators.upsert("alice", OperatorRole.ADMIN, true);
    operators.setPassword("alice", null);
    assertTrue(operators.activePasswordHash("alice").isEmpty());
  }

  @Test
  void recordFailure_separateOperatorsIndependent() {
    auth.recordFailure("alice", 30);
    assertTrue(auth.lockedUntil("alice").isPresent());
    assertTrue(auth.lockedUntil("bob").isEmpty(), "不同操作者退避互不影响");
  }
}