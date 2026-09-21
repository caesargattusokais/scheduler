package dev.scheduler.server.web;

import dev.scheduler.core.OperatorEntry;
import dev.scheduler.core.OperatorRole;
import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.AuthRepository;
import dev.scheduler.persistence.AuthRepository.SessionInfo;
import dev.scheduler.persistence.OperatorRepository;
import dev.scheduler.server.security.CurrentOperator;
import dev.scheduler.server.service.AuditRecorder;
import dev.scheduler.server.service.OperatorPasswordService;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 操作者目录管理端(整体由 OperatorInterceptor 收口到 ADMIN)。 */
@RestController
@RequestMapping("/api/v1/operators")
public class OperatorController {
  private final OperatorRepository operators;
  private final OperatorPasswordService passwords;
  private final AuthRepository auth;
  private final AuditRecorder auditor;
  private final CurrentOperator current;

  public OperatorController(OperatorRepository operators, OperatorPasswordService passwords,
                            AuthRepository auth, AuditRecorder auditor, CurrentOperator current) {
    this.operators = operators;
    this.passwords = passwords;
    this.auth = auth;
    this.auditor = auditor;
    this.current = current;
  }

  public record OperatorRequest(String name, OperatorRole role, boolean active) {}

  @GetMapping
  public List<OperatorEntry> list() {
    return operators.list();
  }

  /** upsert:name/role 必填,active 缺省 false(调用方显式给 true 登记)。 */
  @PostMapping
  public OperatorEntry upsert(@RequestBody OperatorRequest req) {
    if (req.name() == null || req.name().isBlank()) throw new IllegalArgumentException("name is required");
    if (req.role() == null) throw new IllegalArgumentException("role is required (OPERATOR|ADMIN)");
    operators.upsert(req.name().trim(), req.role(), req.active());
    return operators.list().stream()
        .filter(e -> e.name().equals(req.name().trim()))
        .findFirst()
        .orElse(new OperatorEntry(req.name().trim(), req.role(), req.active()));
  }

  /** 设/改操作者口令(长度校验 + BCrypt 落库 + 撤销该操作者全部会话)。 */
  @PostMapping("/{name}/password")
  public void setPassword(@PathVariable String name, @RequestBody Map<String, String> body) {
    passwords.setPassword(name, body.get("password"));
  }

  @PostMapping("/{name}/deactivate")
  public void deactivate(@PathVariable String name) {
    passwords.deactivate(name);
  }

  /** 活动会话视图:列该操作者未撤销且未过期的会话(建立/到期时刻 + 截断哈希展示键)。ADMIN。 */
  @GetMapping("/{name}/sessions")
  public List<SessionInfo> sessions(@PathVariable String name) {
    return auth.activeSessions(name);
  }

  /** 强制登出:撤销该操作者全部活动会话(疑似受攻陷时当下中止其会话),返回本次撤销数并留 operator.sessions.revoke 审计。 */
  @PostMapping("/{name}/sessions/revoke")
  public Map<String, Object> revokeSessions(@PathVariable String name) {
    int revoked = auth.revokeAllForOperator(name);
    auditor.record(current.get(), "operator.sessions.revoke", TargetType.NONE, 0L,
        Map.of("operator", name, "revoked", revoked));
    return Map.of("revoked", revoked);
  }
}