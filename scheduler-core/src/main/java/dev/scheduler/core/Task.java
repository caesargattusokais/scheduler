package dev.scheduler.core;

/** 任务定义(低频变更)。字段对齐 spec §1.2。
 *  3a 触发时钟:timezone 仅解释 cron(默认 UTC);intervalSeconds 非空 = 间隔触发(epoch 对齐边界,与 task 时区无关),
 *  否则走 cron。二者互斥(建/改任务校验恰具其一)。 */
public record Task(
    Long id, String name, String kind, String handlerRef,
    String cron, int shardCount, int timeoutSeconds,
    int maxRetries, long backoffMs, String retryableFailurePattern,
    int maxActiveConcurrent, boolean enabled, boolean paused,
    String retryMode, Long retryCapMs, Long retryBudgetMs,
    String timezone, Integer intervalSeconds) {
  public Task {
    if (shardCount < 1) throw new IllegalArgumentException("shardCount must be >= 1");
    if (maxActiveConcurrent < 1) throw new IllegalArgumentException("maxActiveConcurrent must be >= 1");
  }

  /** 仅含核心配置的便捷构造(既有 13 参调用点/测试 fixture 用);重试策略与触发时钟取缺省:mode null(=exponential)、无封顶/整轮预算、时区 UTC、cron 触发。 */
  public Task(Long id, String name, String kind, String handlerRef,
      String cron, int shardCount, int timeoutSeconds,
      int maxRetries, long backoffMs, String retryableFailurePattern,
      int maxActiveConcurrent, boolean enabled, boolean paused) {
    this(id, name, kind, handlerRef, cron, shardCount, timeoutSeconds,
        maxRetries, backoffMs, retryableFailurePattern, maxActiveConcurrent, enabled, paused,
        null, null, null);
  }

  /** 含重试策略的便捷构造(既有 15 参调用点);触发时钟取缺省(时区 UTC、cron 触发)。 */
  public Task(Long id, String name, String kind, String handlerRef,
      String cron, int shardCount, int timeoutSeconds,
      int maxRetries, long backoffMs, String retryableFailurePattern,
      int maxActiveConcurrent, boolean enabled, boolean paused,
      String retryMode, Long retryCapMs, Long retryBudgetMs) {
    this(id, name, kind, handlerRef, cron, shardCount, timeoutSeconds,
        maxRetries, backoffMs, retryableFailurePattern, maxActiveConcurrent, enabled, paused,
        retryMode, retryCapMs, retryBudgetMs, "UTC", null);
  }
}
