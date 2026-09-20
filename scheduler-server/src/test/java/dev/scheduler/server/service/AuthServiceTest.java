package dev.scheduler.server.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.AuditRepository;
import dev.scheduler.persistence.AuthRepository;
import dev.scheduler.persistence.AuthRepository.Session;
import dev.scheduler.persistence.OperatorRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * AuthService 纯单测(硬编码假仓储,不连 DB):登录退避/验密/成功三支,resolve 轮换与强退数学,
 * token 长度委托,登出撤销。审计经 FakeAuditor 捕获 access.denied。
 */
class AuthServiceTest {
  private final PasswordEncoder enc = new BCryptPasswordEncoder();
  private FakeAuth auth;
  private FakeAuditor auditor;
  private AuthService svc;

  @BeforeEach
  void setUp() {
    auth = new FakeAuth();
    auditor = new FakeAuditor();
    svc = new AuthService(auth, new FakeOperators(), auditor);
  }

  // ---- login ----

  /** 已锁定 → 拒绝且不验密(省 BCrypt)、不累计失败、不建会话,但仍记 access.denied。 */
  @Test
  void login_lockedBeforeVerify_rejectedWithoutVerify() {
    auth.locked = Optional.of(auth.now.plusSeconds(30));
    var r = svc.login("alice", "whatever");
    assertTrue(r.isEmpty());
    assertTrue(auth.failures.isEmpty(), "锁定不累计失败");
    assertTrue(auth.created.isEmpty(), "锁定不建会话");
    assertEquals(List.of("alice"), auditor.denied, "locked 分支也记 access.denied");
  }

  /** 验密失败 → 记 access.denied + recordFailure(退避累计),不建会话。 */
  @Test
  void login_badPassword_recordFailureAndDenied() {
    auth.locked = Optional.empty();
    svc = new AuthService(auth, withHash(enc.encode("right-secret")), auditor);
    var r = svc.login("alice", "wrong-pass");
    assertTrue(r.isEmpty());
    assertEquals(List.of("alice"), auth.failures, "验密失败累计退避");
    assertEquals(List.of("alice"), auditor.denied);
    assertTrue(auth.created.isEmpty());
  }

  /** 成功 → 清零退避 + 建 8h 会话 + token 64-char hex,无 denied。 */
  @Test
  void login_success_resetsLockoutAndCreatesSession() {
    auth.locked = Optional.empty();
    svc = new AuthService(auth, withHash(enc.encode("secret-pass")), auditor);
    var r = svc.login("alice", "secret-pass");
    assertTrue(r.isPresent());
    assertEquals("alice", r.get().operator());
    assertEquals(List.of("alice"), auth.resets, "成功清零退避");
    assertEquals(1, auth.created.size(), "成功建会话");
    assertEquals(AuthService.TOKEN_TTL, auth.lastTtl, "会话 TTL=8h");
    assertEquals(64, r.get().token().length(), "令牌为 64-char hex(服务端生成)");
    assertEquals(auth.now.plus(AuthService.TOKEN_TTL), r.get().expiresAt(), "expiresAt 取 DB 时钟");
    assertTrue(auditor.denied.isEmpty(), "成功不记 denied");
  }

  /** 缺哈希(未登记/停用)→ 与验密失败同路径:denied + 退避。 */
  @Test
  void login_noActiveHash_deniedAndBackoff() {
    auth.locked = Optional.empty();
    var r = svc.login("alice", "secret-pass"); // FakeOperators 默认无哈希
    assertTrue(r.isEmpty());
    assertEquals(List.of("alice"), auth.failures);
    assertEquals(List.of("alice"), auditor.denied);
  }

  // ---- resolve ----

  /** 会话仍有效且剩余 TTL ≥ 半程(8h in 则 no-op)→ 不轮换。 */
  @Test
  void resolve_goodSessionNotDueRotation_doesNotRotate() {
    auth.resolved = Optional.of(
        new Session("alice", auth.now.minusSeconds(60), auth.now.plus(Duration.ofHours(8))));
    var rr = svc.resolve("tok");
    assertEquals("alice", rr.operator());
    assertNull(rr.rotatedToken(), "剩余 ≥ 半程 → 不轮换");
    assertTrue(auth.created.isEmpty());
    assertTrue(auth.revoked.isEmpty());
  }

  /** 剩余 TTL < 半程(2h in 8h)→ 轮换:撤销旧 + 建新,返回新 token。 */
  @Test
  void resolve_remainingBelowHalf_rotates() {
    auth.resolved = Optional.of(
        new Session("alice", auth.now.minusSeconds(60), auth.now.plus(Duration.ofHours(2))));
    var rr = svc.resolve("tok");
    assertEquals("alice", rr.operator());
    assertNotNull(rr.rotatedToken(), "剩余 < 半程 → 轮换出新 token");
    assertEquals(List.of("tok"), auth.revoked, "轮换撤销旧会话");
    assertTrue(auth.created.contains(rr.rotatedToken()), "新 token 已建库");
  }

  /** 轮换建新会话须把旧会话的 created(绝对寿命基线)透传给 create,而非重置为 now()。 */
  @Test
  void resolve_rotateBranch_preservesOriginalCreatedAt() {
    Instant origin = auth.now.minus(Duration.ofDays(4).plusHours(23)); // ≈5d 边缘但仍 <5d(可解析)
    auth.resolved = Optional.of(new Session("alice", origin, auth.now.plus(Duration.ofHours(2))));
    var rr = svc.resolve("tok");
    assertNotNull(rr.rotatedToken());
    assertEquals(List.of(origin), auth.createdAts,
        "轮换 create 应传旧会话 created_at,保留绝对寿命基线而非重置 now()");
  }

  /** 登录建会话 created_at 缺省(传 null)→ 由持久层以 DB now() 落库。 */
  @Test
  void login_create_passesNullCreatedAt() {
    auth.locked = Optional.empty();
    svc = new AuthService(auth, withHash(enc.encode("secret-pass")), auditor);
    svc.login("alice", "secret-pass");
    assertEquals(1, auth.createdAts.size());
    assertEquals(null, auth.createdAts.get(0), "登录建会话 created_at=DB now()(传 null)");
  }

  /** 绝对寿命超限(now-created ≥ 5d)→ 撤销旧并强制重登(返回 null operator)。 */
  @Test
  void resolve_absoluteLifetimeExceeded_forcesRelogin() {
    auth.resolved = Optional.of(
        new Session("alice", auth.now.minus(Duration.ofDays(6)), auth.now.plus(Duration.ofHours(1))));
    var rr = svc.resolve("tok");
    assertNull(rr.operator(), "超绝对寿命 → 强制重登(null operator)");
    assertEquals(List.of("tok"), auth.revoked, "坏会话撤销");
    assertTrue(auth.created.isEmpty(), "强制重登不建新会话");
  }

  /** 未命中会话 → null(匿名);AuthService 不自己校验 token 长度,原样委托 repo.resolve。 */
  @Test
  void resolve_absentSessionOrShortToken_passthroughToRepo() {
    auth.resolved = Optional.empty();
    assertNull(svc.resolve("abc123"), "短 token 亦委托 repo.resolve(空 → null)");
    assertTrue(auth.revoked.isEmpty(), "未命中不撤销任何会话");
  }

  // ---- logout ----

  @Test
  void logout_revokesSession() {
    svc.logout("tok");
    assertEquals(List.of("tok"), auth.revoked);
  }

  // ---- fakes ----

  private FakeOperators withHash(String hash) {
    FakeOperators o = new FakeOperators();
    o.passwordHash = Optional.of(hash);
    return o;
  }

  /** 可控的假 AuthRepository:记录建/撤/退避/重置 + 可拨 now()。 */
  private static final class FakeAuth implements AuthRepository {
    Instant now = Instant.parse("2026-01-01T10:00:00Z");
    Optional<Instant> locked = Optional.empty();
    Optional<Session> resolved = Optional.empty();
    final List<String> created = new ArrayList<>();
    final List<String> revoked = new ArrayList<>();
    final List<String> failures = new ArrayList<>();
    final List<String> resets = new ArrayList<>();
    final List<Instant> createdAts = new ArrayList<>();
    Duration lastTtl;

    @Override public Instant now() { return now; }
    @Override public Optional<Session> resolve(String rawToken) { return resolved; }
    @Override public void create(String rawToken, String operator, Duration ttl) {
      create(rawToken, operator, ttl, null); // 登录:created_at 缺省 → DB now()
    }
    @Override public void create(String rawToken, String operator, Duration ttl, Instant createdAt) {
      lastTtl = ttl;
      created.add(rawToken);
      createdAts.add(createdAt); // 轮换应传原 created_at;登录传 null
    }
    @Override public void revoke(String rawToken) { revoked.add(rawToken); }
    @Override public void revokeAllForOperator(String operator) { }
    @Override public Optional<Instant> lockedUntil(String name) { return locked; }
    @Override public void recordFailure(String name, int maxLockSeconds) { failures.add(name); }
    @Override public void resetLockout(String name) { resets.add(name); }
  }

  /** 可控口令哈希的假 OperatorRepository(仅需 activePasswordHash)。 */
  private static final class FakeOperators implements OperatorRepository {
    Optional<String> passwordHash = Optional.empty();

    @Override public Optional<dev.scheduler.core.OperatorRole> roleOf(String name) { return Optional.empty(); }
    @Override public List<dev.scheduler.core.OperatorEntry> list() { return List.of(); }
    @Override public Optional<String> activePasswordHash(String name) { return passwordHash; }
    @Override public void upsert(String name, dev.scheduler.core.OperatorRole role, boolean active) { }
    @Override public void deactivate(String name) { }
    @Override public void setPassword(String name, String bcryptHash) { }
    @Override public List<String> namesWithoutPassword() { return List.of(); }
  }

  /** 捕获 access.denied 的假审计(覆写 5-arg record,不经序列化)。 */
  private static final class FakeAuditor extends AuditRecorder {
    final List<String> denied = new ArrayList<>();

    FakeAuditor() { super((AuditRepository) null, new ObjectMapper()); }

    @Override
    public void record(String operator, String action, TargetType target, long targetId,
                       Map<String, Object> meta) {
      if ("access.denied".equals(action)) denied.add(operator);
    }
  }
}