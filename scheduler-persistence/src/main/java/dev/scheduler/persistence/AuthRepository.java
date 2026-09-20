package dev.scheduler.persistence;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/** 强认证持久层:会话令牌 + 登录失败退避。口令哈希见 {@link OperatorRepository}。 */
public interface AuthRepository {
  /** 会话解析结果:操作者 + 创建/过期时刻(均由 DB 时钟回填)。 */
  record Session(String operator, Instant created, Instant expires) {}

  /** DB 时钟 now():会话 create/expire/resolve 与 AuthService 轮换判定同源,避免应用/DB 时区漂移。 */
  Instant now();

  /** 令牌明文(64-char hex)→ 有效会话(未撤销、未过期、绝对寿命 < 5d);无效 → empty。 */
  Optional<Session> resolve(String rawToken);

  /** 建会话:存 SHA-256(token),expires_at = DB now() + ttl。 */
  void create(String rawToken, String operator, Duration ttl);

  /** 撤销指定 token 会话(登出/轮换旧);不存在则 no-op。 */
  void revoke(String rawToken);

  /** 撤销该操作者全部活动会话(改密/deactivate)。 */
  void revokeAllForOperator(String operator);

  /** name 当前锁定截止时刻;仅当已锁定(> DB now())才返回。 */
  Optional<Instant> lockedUntil(String name);

  /** 记录一次失败:failures+1,locked_until = DB now() + min(2^failures, maxLockSeconds)。 */
  void recordFailure(String name, int maxLockSeconds);

  /** 登录成功清零退避。 */
  void resetLockout(String name);
}