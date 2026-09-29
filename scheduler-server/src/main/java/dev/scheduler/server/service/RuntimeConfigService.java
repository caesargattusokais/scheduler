package dev.scheduler.server.service;

import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.RuntimeConfigRepository;
import dev.scheduler.persistence.RuntimeConfigRow;
import dev.scheduler.server.config.RuntimeConfigKeys;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 运行时配置服务:读走 DB 主键单行(无缓存),写走白名单 + 类型校验 + 审计。 */
public class RuntimeConfigService {
  private static final Logger log = LoggerFactory.getLogger(RuntimeConfigService.class);
  private final RuntimeConfigRepository repo;
  private final AuditRecorder auditor;

  public RuntimeConfigService(RuntimeConfigRepository repo, AuditRecorder auditor) {
    this.repo = repo;
    this.auditor = auditor;
  }

  public long getLong(String key, long fallback) {
    RuntimeConfigRow row = repo.find(key).orElse(null);
    if (row == null) return fallback;
    try {
      return Long.parseLong(row.value().trim());
    } catch (NumberFormatException e) {
      log.warn("runtime config {} has non-numeric value '{}'; falling back to {}", key, row.value(), fallback);
      return fallback;
    }
  }

  /** 比率阈值读取(failure-rate 等):值必须是可解析的 double,否则回落 fallback。 */
  public double getDouble(String key, double fallback) {
    RuntimeConfigRow row = repo.find(key).orElse(null);
    if (row == null) return fallback;
    try {
      return Double.parseDouble(row.value().trim());
    } catch (NumberFormatException e) {
      log.warn("runtime config {} has non-numeric value '{}'; falling back to {}", key, row.value(), fallback);
      return fallback;
    }
  }

  public boolean getFlag(String key, boolean fallback) {
    RuntimeConfigRow row = repo.find(key).orElse(null);
    if (row == null) return fallback;
    String v = row.value().trim();
    if ("true".equalsIgnoreCase(v) || "1".equals(v)) return true;
    if ("false".equalsIgnoreCase(v) || "0".equals(v)) return false;
    log.warn("runtime config {} has non-boolean value '{}'; falling back to {}", key, row.value(), fallback);
    return fallback;
  }

  public boolean isSuspended() { return getFlag(RuntimeConfigKeys.SUSPEND, false); }

  public List<RuntimeConfigEntry> list() {
    List<RuntimeConfigEntry> out = new ArrayList<>();
    for (String key : RuntimeConfigKeys.DEFAULTS.keySet()) {
      RuntimeConfigRow row = repo.find(key).orElse(null);
      if (row == null) {
        out.add(new RuntimeConfigEntry(key, RuntimeConfigKeys.defaultOf(key), "default", null, null));
      } else {
        out.add(new RuntimeConfigEntry(key, row.value(), "db", row.updatedBy(), row.updatedAt()));
      }
    }
    return out;
  }

  /** 白名单 + 类型校验;通过后入库并记审计 runtime-config.update。 */
  public void set(String key, String rawValue, String operator) {
    if (!RuntimeConfigKeys.isKnown(key)) {
      throw new IllegalArgumentException("unknown runtime config key: " + key);
    }
    String value = (rawValue == null ? "" : rawValue.trim());
    validateValue(key, value);
    // 审计记 prior:此前 DB 值(无则 null = 改前走 fallback/默认),取证「从哪改到哪」。
    String prior = repo.find(key).map(RuntimeConfigRow::value).orElse(null);
    repo.upsert(key, value, operator);
    java.util.Map<String, Object> meta = new java.util.LinkedHashMap<>();
    meta.put("key", key);
    meta.put("value", value);
    meta.put("prior", prior);
    auditor.record(operator, "runtime-config.update", TargetType.NONE, 0L, meta);
  }

  private void validateValue(String key, String value) {
    String fallback = RuntimeConfigKeys.defaultOf(key);
    if (key.equals(RuntimeConfigKeys.SUSPEND)) {
      if (!value.equals("true") && !value.equals("false")) {
        throw new IllegalArgumentException("suspend must be true or false, got: " + value);
      }
      return;
    }
    if (fallback != null) {
      try {
        if (key.endsWith("sec") || key.endsWith("ms") || key.endsWith("days")) {
          Long.parseLong(value);
        } else if (key.equals("worker.capacity")) {
          Long.parseLong(value);
        } else if (key.equals(RuntimeConfigKeys.ALERT_FAILURE_RATE)
                || key.equals(RuntimeConfigKeys.ALERT_TASK_FAILURE_RATE)) {
          Double.parseDouble(value);
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("invalid numeric value for " + key + ": " + value);
      }
    }
  }

  /** 展示条目:来源 default=白名单默认未写;db=DB 命中。 */
  public record RuntimeConfigEntry(String key, String value, String source,
                                   String updatedBy, Instant updatedAt) {}
}