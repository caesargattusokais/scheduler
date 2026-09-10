package dev.scheduler.server.web;

import dev.scheduler.server.handler.HandlerRegistry;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 已注册 handler 枚举,供任务表单下拉框取候选值,避免手填错 ref。 */
@RestController
@RequestMapping("/api/v1/handlers")
public class HandlerController {

  private final HandlerRegistry registry;

  public HandlerController(HandlerRegistry registry) {
    this.registry = registry;
  }

  @GetMapping
  public List<String> list() {
    return registry.refs();
  }
}