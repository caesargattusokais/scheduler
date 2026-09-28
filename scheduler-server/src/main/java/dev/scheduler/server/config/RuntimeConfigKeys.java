package dev.scheduler.server.config;

import java.util.LinkedHashMap;
import java.util.Map;

/** app_runtime_config 可调项白名单:key → 默认值(fallback)。仅此处注册的 key 可写;未注册 get 一律回落。
 *  刻意排除 lease.seconds / dlq.max-replays(见 spec §3.1.1 注释)。
 *  worker.idle-ms / worker.heartbeat.interval-ms 为**重启级** @Value(worker 侧绑定一次),非热键,故不入此表。 */
public final class RuntimeConfigKeys {
  private RuntimeConfigKeys() {}

  public static final String SCAN_DELAY = "loop.scan-delay-ms";
  public static final String RECONCILE_DELAY = "reconcile.delay-ms";
  public static final String DAG_DELAY = "dag.delay-ms";
  public static final String EVENT_DELAY = "event.scan-delay-ms";
  public static final String NOTIFICATION_DELAY = "notifications.dispatch-delay-ms";
  public static final String DLQ_DELAY = "dlq.replay-delay-ms";
  public static final String AUDIT_RETENTION_DELAY = "audit.retention.delay-ms";
  public static final String AUDIT_RETENTION_DAYS = "audit.retention.days";
  public static final String WORKER_CAPACITY = "worker.capacity";
  public static final String WORKER_SHUTDOWN_GRACE_SEC = "worker.shutdown-grace-sec";
  public static final String SUSPEND = "suspend";

  /** 白名单 map:key → 默认值。LinkedHashMap 保序(list() 展示稳定)。 */
  public static final Map<String, String> DEFAULTS = new LinkedHashMap<>();
  static {
    DEFAULTS.put(SCAN_DELAY, "5000");
    DEFAULTS.put(RECONCILE_DELAY, "15000");
    DEFAULTS.put(DAG_DELAY, "5000");
    DEFAULTS.put(EVENT_DELAY, "5000");
    DEFAULTS.put(NOTIFICATION_DELAY, "1000");
    DEFAULTS.put(DLQ_DELAY, "1000");
    DEFAULTS.put(AUDIT_RETENTION_DELAY, "3600000");
    DEFAULTS.put(AUDIT_RETENTION_DAYS, "0");
    DEFAULTS.put(WORKER_CAPACITY, "1");
    DEFAULTS.put(WORKER_SHUTDOWN_GRACE_SEC, "30");
    DEFAULTS.put(SUSPEND, "false");
  }

  public static boolean isKnown(String key) { return DEFAULTS.containsKey(key); }
  public static String defaultOf(String key) { return DEFAULTS.get(key); }
}