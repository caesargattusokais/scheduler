package dev.scheduler.core;

/** 任务定义(低频变更)。字段对齐 spec §1.2。 */
public record Task(
    Long id, String name, String kind, String handlerRef,
    String cron, int shardCount, int timeoutSeconds,
    int maxRetries, long backoffMs, String retryableFailurePattern,
    int maxActiveConcurrent, boolean enabled, boolean paused,
    String retryMode, Long retryCapMs, Long retryBudgetMs) {
  public Task {
    if (shardCount < 1) throw new IllegalArgumentException("shardCount must be >= 1");
    if (maxActiveConcurrent < 1) throw new IllegalArgumentException("maxActiveConcurrent must be >= 1");
  }

  /** 仅含核心配置的便捷构造(既有 13 参调用点/测试 fixture 用);重试策略参数取缺省:mode null(=exponential)、无封顶覆盖、无整轮预算。 */
  public Task(Long id, String name, String kind, String handlerRef,
      String cron, int shardCount, int timeoutSeconds,
      int maxRetries, long backoffMs, String retryableFailurePattern,
      int maxActiveConcurrent, boolean enabled, boolean paused) {
    this(id, name, kind, handlerRef, cron, shardCount, timeoutSeconds,
        maxRetries, backoffMs, retryableFailurePattern, maxActiveConcurrent, enabled, paused,
        null, null, null);
  }
}
