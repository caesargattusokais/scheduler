package dev.scheduler.server.web;

import com.fasterxml.jackson.databind.JsonNode;
import dev.scheduler.persistence.EventRepository;
import dev.scheduler.persistence.InboundEvent;
import dev.scheduler.core.TargetType;
import dev.scheduler.server.security.CurrentOperator;
import dev.scheduler.server.service.AuditRecorder;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 3b 事件触发器入站:接受事件定义,落库为入站事件 outbox PENDING 行;分派由 leader 门控 EventEngine 周期完成。
 *  POST 幂等:相同 dedupeKey 重放命中既有行,不新建(返回既有行,status 反映其当前分派状态)。 */
@RestController
@RequestMapping("/api/v1/events")
public class EventController {

  /** POST 请求体。dedupeKey 为幂等与防重放源(必填);payload 为任意 JSON(缺省默认空对象)。 */
  public record CreateEventRequest(String routeKey, JsonNode payload, String dedupeKey) {}

  private final EventRepository events;
  private final AuditRecorder auditor;
  private final CurrentOperator current;

  public EventController(EventRepository events, AuditRecorder auditor, CurrentOperator current) {
    this.events = events;
    this.auditor = auditor;
    this.current = current;
  }

  @PostMapping
  public ResponseEntity<InboundEvent> create(@RequestBody CreateEventRequest req) {
    if (req == null || req.routeKey() == null || req.routeKey().isBlank()) {
      throw new IllegalArgumentException("routeKey is required");
    }
    if (req.dedupeKey() == null || req.dedupeKey().isBlank()) {
      throw new IllegalArgumentException("dedupeKey is required"); // 幂等/防重放源不可缺
    }
    EventRepository.EnqueueResult res = events.enqueue(req.routeKey(),
        req.payload() == null || req.payload().isNull() ? "{}" : req.payload().toString(), req.dedupeKey());
    // 事件投递审计:who+路由+幂等键+(是否重放命中既有行);不记 payload(可能敏感/偏大)。
    auditor.record(current.get(), "event.submit", TargetType.NONE, 0L, Map.of(
        "eventId", res.id(),
        "routeKey", req.routeKey(),
        "dedupeKey", req.dedupeKey(),
        "replayed", res.replayed()));
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(events.findById(res.id()).orElseThrow());
  }

  /** 事件详情:按 id 直读单条(事件页展示分派结果)。 */
  @GetMapping("/{id}")
  public InboundEvent get(@PathVariable long id) {
    return events.findById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "event " + id));
  }

  /** 事件列表(只读,审计权同):按 id 降序分页,用于观察入站事件的分派状态。 */
  @GetMapping
  public Page<InboundEvent> list(
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    return new Page<>(events.findPage(p.limit(), p.offset()), events.count(), p.offset(), p.limit());
  }

  /** 真实出现过的 route key 去重列表(任务表单事件路由下拉候选;字面路径优先于 /{id})。 */
  @GetMapping("/routes")
  public List<String> routes() {
    return events.distinctRouteKeys();
  }
}