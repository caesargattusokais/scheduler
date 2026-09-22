package dev.scheduler.server.web;

import com.fasterxml.jackson.databind.JsonNode;
import dev.scheduler.persistence.EventRepository;
import dev.scheduler.persistence.InboundEvent;
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

  public EventController(EventRepository events) {
    this.events = events;
  }

  @PostMapping
  public ResponseEntity<InboundEvent> create(@RequestBody CreateEventRequest req) {
    if (req == null || req.routeKey() == null || req.routeKey().isBlank()) {
      throw new IllegalArgumentException("routeKey is required");
    }
    if (req.dedupeKey() == null || req.dedupeKey().isBlank()) {
      throw new IllegalArgumentException("dedupeKey is required"); // 幂等/防重放源不可缺
    }
    long id = events.enqueue(req.routeKey(),
        req.payload() == null || req.payload().isNull() ? "{}" : req.payload().toString(), req.dedupeKey());
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(events.findById(id).orElseThrow());
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
}