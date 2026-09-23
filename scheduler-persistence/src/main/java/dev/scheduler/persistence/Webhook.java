package dev.scheduler.persistence;

import java.time.Instant;
import java.util.List;

/** app_webhook 行映射:投递端点订阅。kinds 为空 → 订阅全部 event kind。 */
public record Webhook(
    long id,
    String url,
    String secret,
    List<String> kinds,
    boolean enabled,
    int maxAttempts,
    long backoffMs,
    Instant createdAt) {}