package dev.scheduler.server.web;

import dev.scheduler.core.AlertEpisodeView;
import dev.scheduler.persistence.AlertRepository;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 告警读 API(读开放,无需操作者会话):GET /api/v1/alerts/active 非分页活跃(OPEN)列;
 *  GET /api/v1/alerts/history 分页 RESOLVED 历史列(镜像 AuditController 的 Page/Paging 信封)。 */
@RestController
@RequestMapping("/api/v1/alerts")
public class AlertsController {
  private final AlertRepository alerts;

  public AlertsController(AlertRepository alerts) {
    this.alerts = alerts;
  }

  @GetMapping("/active")
  public List<AlertEpisodeView> active() {
    return alerts.findActive();
  }

  @GetMapping("/history")
  public Page<AlertEpisodeView> history(
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    List<AlertEpisodeView> items = alerts.findResolved(p.limit(), p.offset());
    return new Page<>(items, alerts.countResolved(), p.offset(), p.limit());
  }
}