package dev.scheduler.server.web;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.core.AuditIntegrity;
import dev.scheduler.persistence.AuditRepository;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 审计只读 API:GET /api/v1/audits 过滤 + 分页,occurred_at DESC(镜像 TaskController.list 信封);/export 全量 CSV。 */
@RestController
@RequestMapping("/api/v1/audits")
public class AuditController {
  private final AuditRepository audits;
  public AuditController(AuditRepository audits) { this.audits = audits; }

  /** 导出上限:一次性拉全量,超过即截断(append-only 审计表防无界内存)。 */
  private static final int MAX_EXPORT_ROWS = 50_000;
  /** 分页批量拉取步长。 */
  private static final int EXPORT_PAGE_SIZE = 1_000;

  @GetMapping
  public Page<AuditEntry> list(
      @RequestParam(required = false) String operator,
      @RequestParam(required = false) String action,
      @RequestParam(required = false) String targetType,
      @RequestParam(required = false) Long targetId,
      @RequestParam(required = false) String from,
      @RequestParam(required = false) String to,
      @RequestParam(required = false) Boolean hasDiff,
      @RequestParam(required = false) String diffField,
      @RequestParam(required = false) String beforeField,
      @RequestParam(required = false) String metaField,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    Instant fromT = parseInstant(from);
    Instant toT = parseInstant(to);
    return new Page<>(audits.findPage(operator, action, targetType, targetId, fromT, toT,
            hasDiff, diffField, beforeField, metaField, p.limit(), p.offset()),
        audits.count(operator, action, targetType, targetId, fromT, toT,
            hasDiff, diffField, beforeField, metaField),
        p.offset(), p.limit());
  }

  /** 导出:与 list 相同过滤的全部(至多 {@link #MAX_EXPORT_ROWS})匹配行,text/csv 附件。
   *  UTF-8 BOM 保证 Excel 正确解码中文;operator/JSON 均按 CSV 规则转义,且前缀 =,+,-,@ 防公式注入。 */
  @GetMapping("/export")
  public ResponseEntity<byte[]> export(
      @RequestParam(required = false) String operator,
      @RequestParam(required = false) String action,
      @RequestParam(required = false) String targetType,
      @RequestParam(required = false) Long targetId,
      @RequestParam(required = false) String from,
      @RequestParam(required = false) String to,
      @RequestParam(required = false) Boolean hasDiff,
      @RequestParam(required = false) String diffField,
      @RequestParam(required = false) String beforeField,
      @RequestParam(required = false) String metaField) {
    Instant fromT = parseInstant(from);
    Instant toT = parseInstant(to);
    StringBuilder csv = new StringBuilder();
    csv.append("occurredAt,operator,action,targetType,targetId,source,meta,diff,before\n");
    int offset = 0;
    int emitted = 0;
    while (emitted < MAX_EXPORT_ROWS) {
      List<AuditEntry> page = audits.findPage(operator, action, targetType, targetId, fromT, toT,
          hasDiff, diffField, beforeField, metaField, EXPORT_PAGE_SIZE, offset);
      if (page.isEmpty()) break;
      int take = Math.min(page.size(), MAX_EXPORT_ROWS - emitted);
      for (int i = 0; i < take; i++) {
        AuditEntry e = page.get(i);
        csv.append(csvCell(e.occurredAt().toString())).append(',')
            .append(csvCell(e.operator())).append(',')
            .append(csvCell(e.action())).append(',')
            .append(csvCell(e.targetType())).append(',')
            .append(e.targetId()).append(',')
            .append(csvCell(e.source())).append(',')
            .append(csvCell(e.meta())).append(',')
            .append(csvCell(e.diff())).append(',')
            .append(csvCell(e.before())).append('\n');
      }
      emitted += take;
      offset += take;
      if (take < page.size()) break; // 已触达上限,停止
      if (page.size() < EXPORT_PAGE_SIZE) break; // 无更多行
    }
    // UTF-8 BOM(Excel 解码中文),整体转 byte[] 以便前置 BOM。
    byte[] bom = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
    byte[] body = csv.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] out = new byte[bom.length + body.length];
    System.arraycopy(bom, 0, out, 0, bom.length);
    System.arraycopy(body, 0, out, bom.length, body.length);
    return ResponseEntity.ok()
        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"audits.csv\"")
        .contentType(MediaType.parseMediaType("text/csv; charset=utf-8"))
        .body(out);
  }

  /** 取证链完整性:全量入链 + 无篡改 → verified(true);异常 → 给出最靠前的被篡改行 id。 */
  @GetMapping("/integrity")
  public AuditIntegrity integrity() { return audits.integrity(); }

  /** CSV 单元格:null → 空;含逗号/引号/换行 → 双引号包裹并把内部双引号翻倍;
   *  以 = + - @ 开头(Excel 公式注入向量)前置单引号。 */
  private static String csvCell(String s) {
    if (s == null) return "";
    if (!s.isEmpty() && (s.charAt(0) == '=' || s.charAt(0) == '+'
        || s.charAt(0) == '-' || s.charAt(0) == '@')) {
      s = "'" + s;
    }
    if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0) {
      s = '"' + s.replace("\"", "\"\"") + '"';
    }
    return s;
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