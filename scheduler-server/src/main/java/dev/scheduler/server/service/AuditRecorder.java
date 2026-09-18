package dev.scheduler.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.AuditRepository;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 操作审计记录器:控制器写 handler 在操作成功落库后调用,追加一条审计。
 * <ul>
 *   <li>operator 缺省/空 → 'anonymous'(服务端无认证,语义在 X-Operator 头)。</li>
 *   <li>meta 为 Map,经 ObjectMapper 序列化为 JSON 文本后交由持久层落 JSONB(persistence 无 Jackson)。</li>
 *   <li>审计非阻断:序列化或 insert 失败只 log.warn,不抛、不改变用户操作结果(操作已先提交)。</li>
 * </ul>
 */
public class AuditRecorder {
  private static final Logger log = LoggerFactory.getLogger(AuditRecorder.class);
  private final AuditRepository audits;
  private final ObjectMapper json;

  public AuditRecorder(AuditRepository audits, ObjectMapper json) {
    this.audits = audits;
    this.json = json;
  }

  /** source 为 null(保留列,未启用来源追踪)。 */
  public void record(String operator, String action, TargetType target, long targetId, Map<String, Object> meta) {
    record(operator, action, target, targetId, meta, null);
  }

  public void record(String operator, String action, TargetType target, long targetId,
                     Map<String, Object> meta, String source) {
    String who = (operator == null || operator.isBlank()) ? "anonymous" : operator;
    String metaJson = null;
    if (meta != null) {
      try {
        metaJson = json.writeValueAsString(meta);
      } catch (JsonProcessingException e) {
        log.warn("audit meta serialization failed; recording without meta: {} {} {}", who, action, targetId, e);
      }
    }
    try {
      audits.record(who, action, target, targetId, metaJson, source);
    } catch (RuntimeException e) {
      log.warn("audit record failed (non-blocking): {} {} target={} id={}", who, action, target.db(), targetId, e);
    }
  }
}