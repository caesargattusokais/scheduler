package dev.scheduler.server.service;

import dev.scheduler.persistence.OperatorRepository;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

/**
 * 操作者口令策略:统一「选口令」路径(ADMIN 设密与自助改密)的长度/复杂度校验 + 最近 N 次防重。
 * 配置 {@code scheduler.auth.password-policy.*}:minLength(默认 8)、requireDigit(默认 true,须含数字)、
 * historySize(默认 5,不允许与最近 N 次或当前口令相同)。
 *
 * <p>引导路径({@link #validateBootstrap(String)})刻意只校验长度——共享默认口令(boot-pass)是低强度引导,
 * 靠 must_change_password 强制首登改密,复杂度由改后真正选定的口令满足,避免引导默认被策略卡死。
 */
@Component
@ConfigurationProperties(prefix = "scheduler.auth.password-policy")
public class PasswordPolicy {
  private int minLength = 8;
  private boolean requireDigit = true;
  private int historySize = 5;

  private final OperatorRepository operators;
  private final PasswordEncoder enc = new BCryptPasswordEncoder();

  public PasswordPolicy(OperatorRepository operators) {
    this.operators = operators;
  }

  public int getMinLength() { return minLength; }
  public void setMinLength(int minLength) { this.minLength = minLength; }
  public boolean isRequireDigit() { return requireDigit; }
  public void setRequireDigit(boolean requireDigit) { this.requireDigit = requireDigit; }
  public int getHistorySize() { return historySize; }
  public void setHistorySize(int historySize) { this.historySize = historySize; }

  /** 全量策略(长度 + 复杂度)→ 违反抛 IAE,文案供前端原样展示。 */
  public void validate(String raw) {
    requireLength(raw);
    if (requireDigit && !containsDigit(raw)) {
      throw new IllegalArgumentException("password must contain at least one digit");
    }
  }

  /** 引导路径:仅长度((共享默认口令是刻意低强度引导)。 */
  public void validateBootstrap(String raw) {
    requireLength(raw);
  }

  /** 防重:新口令不得等于当前活跃口令或最近 historySize 条任一历史口令(BCrypt 逐一比对)。 */
  public void rejectIfReused(String operator, String raw) {
    for (String hash : candidateHashes(operator)) {
      if (enc.matches(raw, hash)) {
        throw new IllegalArgumentException(
            "password must not be the same as the current or any of the last " + historySize + " used");
      }
    }
  }

  /** 把当前活跃口令哈希压入历史(供下次改密防重);无活跃口令(首次设密)则 no-op。 */
  public void pushHistory(String operator) {
    operators.activePasswordHash(operator)
        .ifPresent(hash -> operators.pushPasswordHistory(operator, hash, historySize));
  }

  /** 防重比对源:当前活跃哈希 + 全部历史哈希(最新在前)。 */
  private List<String> candidateHashes(String operator) {
    List<String> candidates = new ArrayList<>();
    operators.activePasswordHash(operator).ifPresent(candidates::add);
    candidates.addAll(operators.passwordHistoryHashes(operator));
    return candidates;
  }

  private void requireLength(String raw) {
    if (raw == null || raw.length() < minLength) {
      throw new IllegalArgumentException("password must be at least " + minLength + " chars");
    }
  }

  private static boolean containsDigit(String raw) {
    for (int i = 0; i < raw.length(); i++) {
      if (Character.isDigit(raw.charAt(i))) return true;
    }
    return false;
  }
}