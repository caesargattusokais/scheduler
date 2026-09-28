package dev.scheduler.server.web;

import dev.scheduler.server.security.CurrentOperator;
import dev.scheduler.server.service.RuntimeConfigService;
import dev.scheduler.server.service.RuntimeConfigService.RuntimeConfigEntry;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 运行时设置(热更新):读开放,写 ADMIN。写会白名单 + 类型校验 + 记 runtime-config.update 审计。 */
@RestController
@RequestMapping("/api/v1/runtime-config")
public class RuntimeConfigController {
  private final RuntimeConfigService settings;
  private final CurrentOperator current;

  public RuntimeConfigController(RuntimeConfigService settings, CurrentOperator current) {
    this.settings = settings;
    this.current = current;
  }

  @GetMapping
  public java.util.List<RuntimeConfigEntry> list() {
    return settings.list();
  }

  public record ValueReq(String value) {}

  @PutMapping("/{key}")
  public RuntimeConfigEntry set(@PathVariable String key, @RequestBody ValueReq req) {
    settings.set(key, req.value, current.get());
    return settings.list().stream().filter(e -> e.key().equals(key)).findFirst().orElseThrow();
  }
}