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
 *   <li>meta 为 Map,经 ObjectMapper 序列化为 JSON 文本后交由持久层落 JSONB(persistence 无 Jackson);before 为操作前全量快照。</li>
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
    record(operator, action, target, targetId, meta, null, null, null);
  }

  /** 带 before/after 字段级 diff + 操作前全量快照(task.update 等 task.* 动作);meta 后态、diff 变差、
   *  before 操作前全量 Task(replace 子项目2 的 (meta,diff) overload)。 */
  public void record(String operator, String action, TargetType target, long targetId,
                     Map<String, Object> meta, Map<String, Object> diff, Map<String, Object> before) {
    record(operator, action, target, targetId, meta, diff, before, null);
  }

  private void record(String operator, String action, TargetType target, long targetId,
                      Map<String, Object> meta, Map<String, Object> diff, Map<String, Object> before, String source) {
    String who = (operator == null || operator.isBlank()) ? "anonymous" : operator;
    try {
      audits.record(who, action, target, targetId, serialize(meta), serialize(diff), serialize(before), source);
    } catch (RuntimeException e) {
      log.warn("audit record failed (non-blocking): {} {} target={} id={}", who, action, target.db(), targetId, e);
    }
  }

  /** meta/diff 序列化;null 或序列化失败 → null(失败只 warn,审计非阻断)。 */
  private String serialize(Map<String, Object> meta) {
    if (meta == null) return null;
    try { return json.writeValueAsString(meta); }
    catch (JsonProcessingException e) { log.warn("audit meta serialization failed; recording without it", e); return null; }
  }
}