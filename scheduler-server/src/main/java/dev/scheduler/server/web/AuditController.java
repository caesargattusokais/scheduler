package dev.scheduler.server.web;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.persistence.AuditRepository;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 审计只读 API:GET /api/v1/audits 过滤 + 分页,occurred_at DESC(镜像 TaskController.list 信封)。 */
@RestController
@RequestMapping("/api/v1/audits")
public class AuditController {
  private final AuditRepository audits;
  public AuditController(AuditRepository audits) { this.audits = audits; }

  @GetMapping
  public Page<AuditEntry> list(
      @RequestParam(required = false) String operator,
      @RequestParam(required = false) String action,
      @RequestParam(required = false) String targetType,
      @RequestParam(required = false) Long targetId,
      @RequestParam(required = false) String from,
      @RequestParam(required = false) String to,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    Instant fromT = parseInstant(from);
    Instant toT = parseInstant(to);
    return new Page<>(audits.findPage(operator, action, targetType, targetId, fromT, toT,
            p.limit(), p.offset()),
        audits.count(operator, action, targetType, targetId, fromT, toT),
        p.offset(), p.limit());
  }

  private static Instant parseInstant(String s) {
    if (s == null || s.isBlank()) return null;
    try {
      return Instant.parse(s); // ISO-8601 带偏移
    } catch (DateTimeParseException e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "invalid `from`/`to` (expected ISO-8601 with offset): " + s);
    }
  }
}