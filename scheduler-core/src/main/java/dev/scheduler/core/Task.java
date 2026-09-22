package dev.scheduler.core;

import java.util.List;

/** 任务定义(低频变更)。字段对齐 spec §1.2。
 *  3a/3b 触发时钟:触发方式三选一互斥——① cron 在任务时区解释(timezone 默认 UTC);② intervalSeconds 非空 =
 *  间隔触发(epoch 对齐边界,与 task 时区无关);③ eventRoutes 非空 = 事件触发(订阅这些路由 key 的入站事件,
 *  每条事件 createdAt 一条 run)。三选一由建/改任务校验。 */
public record Task(
    Long id, String name, String kind, String handlerRef,
    String cron, int shardCount, int timeoutSeconds,
    int maxRetries, long backoffMs, String retryableFailurePattern,
    int maxActiveConcurrent, boolean enabled, boolean paused,
    String retryMode, Long retryCapMs, Long retryBudgetMs,
    String timezone, Integer intervalSeconds, List<String> eventRoutes,
    String successPolicyType, Integer successPolicyValue) {
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
        retryMode, retryCapMs, retryBudgetMs, "UTC", null, List.of(), "NONE", null);
  }

  /** 既有 18 参完整构造(不含事件路由);事件路由取空列表。仅供事件路由无关的现有调用点/测试 fixture 推进编译。 */
  public Task(Long id, String name, String kind, String handlerRef,
      String cron, int shardCount, int timeoutSeconds,
      int maxRetries, long backoffMs, String retryableFailurePattern,
      int maxActiveConcurrent, boolean enabled, boolean paused,
      String retryMode, Long retryCapMs, Long retryBudgetMs,
      String timezone, Integer intervalSeconds) {
    this(id, name, kind, handlerRef, cron, shardCount, timeoutSeconds,
        maxRetries, backoffMs, retryableFailurePattern, maxActiveConcurrent, enabled, paused,
        retryMode, retryCapMs, retryBudgetMs, timezone, intervalSeconds, List.of(), "NONE", null);
  }

  public boolean hasEventRoutes() {
    return eventRoutes != null && !eventRoutes.isEmpty();
  }
}
