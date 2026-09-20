package dev.scheduler.server.service;

import dev.scheduler.persistence.AuthRepository;
import dev.scheduler.persistence.OperatorRepository;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

/** 操作者口令管理:长度校验 → 存在性校验 → BCrypt 编码落库 → 改密/停用即撤销该操作者全部活动会话。 */
@Service
public class OperatorPasswordService {
  public static final int MIN_PASSWORD = 8;

  private final OperatorRepository operators;
  private final AuthRepository auth;
  private final PasswordEncoder enc = new BCryptPasswordEncoder();

  public OperatorPasswordService(OperatorRepository operators, AuthRepository auth) {
    this.operators = operators;
    this.auth = auth;
  }

  private void requireLength(String raw) {
    if (raw == null || raw.length() < MIN_PASSWORD)
      throw new IllegalArgumentException("password must be at least " + MIN_PASSWORD + " chars");
  }

  /** 该操作者是否已登记(不存在则 true,setPassword 对未知名静默 no-op,故先校验避免"成功但没改到")。 */
  private void requireRegistered(String name) {
    if (operators.list().stream().noneMatch(e -> e.name().equals(name)))
      throw new IllegalArgumentException("unknown operator: " + name);
  }

  /** 设/改口令:长度 → 存在 → 编码落库 → 撤销该操作者全部会话。 */
  public void setPassword(String name, String raw) {
    requireLength(raw);
    requireRegistered(name);
    operators.setPassword(name, enc.encode(raw));
    auth.revokeAllForOperator(name);
  }

  /** 停用操作者并撤销其全部会话(把 deactivate 的会话副作用收口到 service)。 */
  public void deactivate(String name) {
    operators.deactivate(name);
    auth.revokeAllForOperator(name);
  }

  /** 引导默认口令:仅对当前无口令的操作者应用(不覆盖管理员已设口令);仍走长度校验 + 编码落库。 */
  public void bootstrap(String name, String raw) {
    requireLength(raw);
    if (!operators.namesWithoutPassword().contains(name)) return; // 已有口令 → 跳过,不覆盖
    operators.setPassword(name, enc.encode(raw));
    auth.revokeAllForOperator(name);
  }
}