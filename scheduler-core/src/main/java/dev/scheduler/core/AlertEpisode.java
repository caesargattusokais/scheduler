package dev.scheduler.core;

import java.time.Instant;

/** 一条告警事件(收敛读模型)。status 演进 PENDING → OPEN → RESOLVED。
 *  openedAt/openedValue 为 OPEN 确认时快照,resolvedAt/resolvedValue 为回落时快照,后 4 字段事务态可空。 */
public record AlertEpisode(
    long id, String key, String rule, String targetType, Long targetId, String severity,
    String status, int sampleCount, String value,
    Instant openedAt, String openedValue, Instant resolvedAt, String resolvedValue) {}