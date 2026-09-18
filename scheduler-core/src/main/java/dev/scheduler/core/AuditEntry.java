package dev.scheduler.core;

import java.time.Instant;

/**
 * 一条操作审计记录(append-only 读模型)。meta/diff 为原始 JSON 文本或 null:写端由 server 层
 * AuditRecorder 用 Jackson 序列化,持久层不依赖 Jackson,读 API 原样透出、前端 JSON.parse 展示。
 * diff 为 before/after 变更的 JSON 文本(形如 {field: [before, after]} 或嵌套 dict)或 null。
 */
public record AuditEntry(
    long id,
    Instant occurredAt,
    String operator,
    String action,
    String targetType, // TargetType.db(),如 "task"
    long targetId,
    String meta,       // 操作后态 JSON 文本或 null
    String source,     // 请求方标识,通常 null
    String diff) {}