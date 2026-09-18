package dev.scheduler.core;

import java.time.Instant;

/**
 * 一条操作审计记录(append-only 读模型)。meta 为原始 JSON 文本或 null:写端由 server 层
 * AuditRecorder 用 Jackson 序列化,持久层不依赖 Jackson,读 API 原样透出、前端 JSON.parse 展示。
 */
public record AuditEntry(
    long id,
    Instant occurredAt,
    String operator,
    String action,
    String targetType, // TargetType.db(),如 "task"
    long targetId,
    String meta,       // JSON 文本或 null
    String source) {}