package dev.scheduler.server.service;

import dev.scheduler.persistence.AuthRepository;
import dev.scheduler.persistence.OperatorRepository;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

/** 操作者口令管理:口令策略(长度/复杂度/防重) → 存在性校验 → BCrypt 编码落库 → 改密/停用即撤销该操作者全部活动会话。 */
@Service
public class OperatorPasswordService {
  public static final int MIN_PASSWORD = 8;

  private final OperatorRepository operators;
  private final AuthRepository auth;
  private final PasswordPolicy policy;
  private final PasswordEncoder enc = new BCryptPasswordEncoder();

  public OperatorPasswordService(OperatorRepository operators, AuthRepository auth, PasswordPolicy policy) {
    this.operators = operators;
    this.auth = auth;
    this.policy = policy;
  }

  /** 校验操作者是否已登记;未登记则抛 IllegalArgumentException(setPassword 落库是对未知名行的静默 no-op,
   *  故先校验,避免误把未知名当作"成功改密")。 */
  private void requireRegistered(String name) {
    if (operators.list().stream().noneMatch(e -> e.name().equals(name)))
      throw new IllegalArgumentException("unknown operator: " + name);
  }

  /** 设/改口令:策略(长度+复杂度+防重) → 存在 → 旧哈希入史 → 编码落库 → 撤销该操作者全部会话。 */
  public void setPassword(String name, String raw) {
    policy.validate(raw);
    requireRegistered(name);
    policy.rejectIfReused(name, raw);
    policy.pushHistory(name); // 防重校验通过后,把当前活跃口令压入历史(回落库前,读到的是旧口令)
    operators.setPassword(name, enc.encode(raw));
    auth.revokeAllForOperator(name);
    operators.setMustChangePassword(name, false); // 人类选定口径 → 不强制首登改密
  }

  /** 停用操作者并撤销其全部会话(把 deactivate 的会话副作用收口到 service)。 */
  public void deactivate(String name) {
    operators.deactivate(name);
    auth.revokeAllForOperator(name);
  }

  /** 引导默认口令:仅对当前无口令的操作者应用(不覆盖管理员已设口令);走引导策略校验(仅长度)+ 编码落库。
   *  该口令为共享默认 → 置 must_change_password=true,操作者须在首登改密。 */
  public void bootstrap(String name, String raw) {
    policy.validateBootstrap(raw);
    if (!operators.namesWithoutPassword().contains(name)) return; // 已有口令 → 跳过,不覆盖
    operators.setPassword(name, enc.encode(raw));
    auth.revokeAllForOperator(name);
    operators.setMustChangePassword(name, true); // 共享默认 → 强制首登改密
  }
}