package dev.scheduler.core;

import java.time.Instant;

/** 告警对外的展示形状(controller/前端用)。由 JdbcAlertRepository 查询 join target_name 后组装;
 *  resolvedAt/resolvedValue 在 OPEN 时为 null,openedValue 在 OPEN 时非 null。 */
public record AlertEpisodeView(
    long id, String rule, String targetType, Long targetId, String targetName,
    String severity, String status, String value,
    Instant openedAt, String openedValue, Instant resolvedAt, String resolvedValue) {}