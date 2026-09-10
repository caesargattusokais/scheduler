package dev.scheduler.server.web;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 已注册 handler 枚举,供任务表单下拉框取候选值,避免手填错 ref。
 *  M6.2:数据源 = 进程内 ∪ 存活 worker 并集,与 TaskController 建/改校验同源。 */
@RestController
@RequestMapping("/api/v1/handlers")
public class HandlerController {

  private final AvailableHandlerRefs availableRefs;

  public HandlerController(AvailableHandlerRefs availableRefs) {
    this.availableRefs = availableRefs;
  }

  @GetMapping
  public List<String> list() {
    return availableRefs.refs().stream().sorted().toList();
  }
}