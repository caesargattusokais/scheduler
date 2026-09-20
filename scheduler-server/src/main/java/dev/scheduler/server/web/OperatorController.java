package dev.scheduler.server.web;

import dev.scheduler.core.OperatorEntry;
import dev.scheduler.core.OperatorRole;
import dev.scheduler.persistence.OperatorRepository;
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

  public OperatorController(OperatorRepository operators, OperatorPasswordService passwords) {
    this.operators = operators;
    this.passwords = passwords;
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
}