package dev.scheduler.server.service;

import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.AuthRepository;
import dev.scheduler.persistence.AuthRepository.Session;
import dev.scheduler.persistence.OperatorRepository;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

/**
 * 操作者强认证:口令登录(退避 + BCrypt 校验)→ 短 TTL 轮换会话;cookie 会话解析 + 轮换。
 * 会话 create/expire/resolve 与轮换判定均取 DB 时钟({@link AuthRepository#now()}),不依赖应用时钟,
 * 避免应用/DB 时区漂移(绝对会话寿命 cap 已在 repo.resolve 的 SQL 兜底,此处轮换/强退再复查)。
 * 令牌明文仅服务端生成({@link #randomToken()}),落库为 SHA-256(明文不入库)。
 */
@Service
public class AuthService {
  /** 会话 TTL:轮换使剩余 TTL < 半程即滚动续期。 */
  static final Duration TOKEN_TTL = Duration.ofHours(8);
  /** 绝对会话寿命:超限强制重登(轮换可续短 TTL,但不突破绝对上限)。 */
  static final Duration ABSOLUTE_SESSION = Duration.ofDays(5);
  /** 登录失败退避上限(秒):locked_until 封顶,递增指数幂。 */
  static final int BACKOFF_CAP = 30;

  private final AuthRepository auth;
  private final OperatorRepository operators;
  private final AuditRecorder auditor;
  private final PasswordPolicy policy;
  private final PasswordEncoder enc;
  private final SecureRandom rng = new SecureRandom();

  public AuthService(AuthRepository auth, OperatorRepository operators, AuditRecorder auditor, PasswordPolicy policy) {
    this.auth = auth;
    this.operators = operators;
    this.auditor = auditor;
    this.policy = policy;
    this.enc = new BCryptPasswordEncoder();
  }

  /** 登录结果:操作者 + 新令牌明文 + DB 时钟到期时刻。 */
  public record LoginResult(String operator, String token, Instant expiresAt) {}

  /** 会话解析结果:{@code rotatedToken} 非 null 表示已轮换(调用方须回写新 cookie)。 */
  public record ResolveResult(String operator, String rotatedToken) {}

  /**
   * 口令登录。返回 empty 的情形(均记 access.denied 审计,operator=提交名):
   * 已锁定(locked_until > DB now(),不验密节省 BCrypt)/ 无活跃口令哈希 / BCrypt 验密失败(并累计退避)。
   */
  public Optional<LoginResult> login(String name, String password) {
    Optional<Instant> locked = auth.lockedUntil(name);
    if (locked.isPresent()) {
      auditor.record(name, "access.denied", TargetType.NONE, 0L, deniedMeta("/api/v1/auth/login", "account locked (backoff)"));
      return Optional.empty();
    }
    Optional<String> hash = operators.activePasswordHash(name);
    if (hash.isEmpty() || !enc.matches(password, hash.get())) {
      auth.recordFailure(name, BACKOFF_CAP);
      auditor.record(name, "access.denied", TargetType.NONE, 0L,
          deniedMeta("/api/v1/auth/login", hash.isEmpty() ? "no active password" : "bad credentials"));
      return Optional.empty();
    }
    auth.resetLockout(name);
    String token = randomToken();
    auth.create(token, name, TOKEN_TTL);
    Instant expiresAt = auth.now().plus(TOKEN_TTL);
    auditor.record(name, "auth.login", TargetType.NONE, 0L, Map.of("expiresAt", expiresAt.toString()));
    return Optional.of(new LoginResult(name, token, expiresAt));
  }

  /**
   * 会话解析(由拦截器每请求调用):DB 时钟判 TTL 余量。
   * <ul>
   *   <li>无效/过期会话 → null(匿名)。</li>
   *   <li>剩余 TTL &lt; TOKEN_TTL/2 → 轮换:撤销旧、建新,返回 operator + 新 token(已落库)。</li>
   *   <li>now-created ≥ 绝对寿命(防御性复查)→ 撤销旧、强制重登(返回 null operator,调用方视为未认证)。</li>
   *   <li>否则 no-op,不轮换。</li>
   * </ul>
   */
  public ResolveResult resolve(String rawToken) {
    Optional<Session> s = auth.resolve(rawToken);
    if (s.isEmpty()) return null;
    Session sess = s.get();
    String operator = sess.operator();
    Instant now = auth.now();
    boolean rotate = Duration.between(now, sess.expires()).compareTo(TOKEN_TTL.dividedBy(2)) < 0;
    boolean expiredAbs = Duration.between(sess.created(), now).compareTo(ABSOLUTE_SESSION) >= 0;
    if (!rotate && !expiredAbs) {
      return new ResolveResult(operator, null);
    }
    auth.revoke(rawToken); // 轮换撤销旧 / 强退撤销坏会话,同款"(旧)即作废"语义
    if (expiredAbs) {
      return new ResolveResult(null, null); // 强制重登
    }
    String newTk = randomToken();
    // 以旧会话的 created 作新行 created_at:使绝对寿命基线(自首次登录 ≤ 5d)跨轮换保留,而非重置 now() 续杯。
    auth.create(newTk, operator, TOKEN_TTL, sess.created());
    return new ResolveResult(operator, newTk);
  }

  /** 登出:撤销指定会话(不自觉认,存在则作废)。 */
  public void logout(String rawToken) {
    auth.revoke(rawToken);
  }

  /**
   * 自助改密(已登录上下文):复查当前口令(不匹配 → empty,记 access.denied)→ 长度校验(过短 → IAE)→
   * 落新 BCrypt + 清除强制改密标 + 撤销该操作者全部旧会话 + 签发全新会话(created_at=now():新会话绝对寿命
   * 自改密重计——每一次改密都要求持有口令,是更强的重认证,非纯失窃令牌可续杯)。返回新 LoginResult,
   * 调用方须把新 token 写回 cookie 实现无缝续期。
   */
  public Optional<LoginResult> changePassword(String operator, String currentRaw, String newRaw) {
    Optional<String> hash = operators.activePasswordHash(operator);
    if (hash.isEmpty() || !enc.matches(currentRaw, hash.get())) {
      auditor.record(operator, "access.denied", TargetType.NONE, 0L,
          deniedMeta("/api/v1/auth/change-password", "bad current password on self password change"));
      return Optional.empty();
    }
    policy.validate(newRaw); // 长度 + 复杂度(含数字)
    policy.rejectIfReused(operator, newRaw); // 不得复用当前或最近 historySize 条口令
    policy.pushHistory(operator); // 校验通过后把当前活跃口令压入历史(回读旧哈希,落新密前)
    operators.setPassword(operator, enc.encode(newRaw));
    operators.setMustChangePassword(operator, false); // 人类选定口径 → 不再强制首登改密
    auth.revokeAllForOperator(operator); // 旧/其他会话全作废
    String token = randomToken();
    auth.create(token, operator, TOKEN_TTL); // 新会话自改密重计绝对寿命
    Instant expiresAt = auth.now().plus(TOKEN_TTL);
    auditor.record(operator, "auth.change_password", TargetType.NONE, 0L, Map.of());
    return Optional.of(new LoginResult(operator, token, expiresAt));
  }

  /** 服务端强随机令牌:32 字节 → 64-char lowercase hex(与持久层测试种子同形)。 */
  private String randomToken() {
    byte[] b = new byte[32];
    rng.nextBytes(b);
    return HexFormat.of().formatHex(b);
  }

  private static Map<String, Object> deniedMeta(String path, String reason) {
    return Map.of("path", path, "method", "POST", "reason", reason, "required", "OPERATOR");
  }
}