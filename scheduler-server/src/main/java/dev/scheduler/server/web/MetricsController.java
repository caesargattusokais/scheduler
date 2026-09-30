package dev.scheduler.server.web;

import dev.scheduler.server.service.ExecutionMetrics;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 执行 SLO 指标端点(4-2):DB 快照聚合,补齐 /actuator/prometheus 流式 gauge 没有的维度。
 * 具体聚合逻辑抽取到共享 {@link ExecutionMetrics}(SLO 端点与告警引擎 AlertEngine 同名鼎格,避免口径漂移),
 * 本控制器仅做委托:读查询、无副作用。
 */
@RestController
@RequestMapping("/api/v1/metrics/executions")
public class MetricsController {
  private final ExecutionMetrics metrics;

  public MetricsController(ExecutionMetrics metrics) {
    this.metrics = metrics;
  }

  /** 执行 SLO 快照(读,开放):windowSeconds 为最近分析窗口(秒,默认 3600)。 */
  @GetMapping
  public ExecutionMetrics.ExecutionSlo snapshots(@RequestParam(required = false) Integer windowSeconds) {
    return metrics.snapshots(windowSeconds);
  }
}
