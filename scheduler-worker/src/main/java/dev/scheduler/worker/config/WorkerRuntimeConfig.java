package dev.scheduler.worker.config;

import dev.scheduler.persistence.JdbcRuntimeConfigRepository;
import dev.scheduler.persistence.RuntimeConfigRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/** worker 侧运行时配置只读器:同一张 app_runtime_config 表,worker 只读(不写不审计)。
 *  worker 独立上下文,不能依赖 server 的 RuntimeConfigService bean。 */
public class WorkerRuntimeConfig {
  private static final Logger log = LoggerFactory.getLogger(WorkerRuntimeConfig.class);
  private final JdbcRuntimeConfigRepository repo;

  public WorkerRuntimeConfig(JdbcTemplate jdbc) { this.repo = new JdbcRuntimeConfigRepository(jdbc); }

  public long getLong(String key, long fallback) {
    RuntimeConfigRow row = repo.find(key).orElse(null);
    if (row == null) return fallback;
    try { return Long.parseLong(row.value().trim()); }
    catch (NumberFormatException e) {
      log.warn("runtime config {} non-numeric '{}'; fallback {}", key, row.value(), fallback);
      return fallback;
    }
  }

  public boolean getFlag(String key, boolean fallback) {
    RuntimeConfigRow row = repo.find(key).orElse(null);
    if (row == null) return fallback;
    String v = row.value().trim();
    if ("true".equalsIgnoreCase(v) || "1".equals(v)) return true;
    if ("false".equalsIgnoreCase(v) || "0".equals(v)) return false;
    return fallback;
  }
}