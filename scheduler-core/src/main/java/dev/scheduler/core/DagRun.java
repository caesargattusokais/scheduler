package dev.scheduler.core;
import java.time.Instant;
/** 一次 DAG 执行头(spec §1.2)。存储 status 仅 PENDING→终态。 */
public record DagRun(Long id, Long dagId, String idempotencyKey, DagRunStatus status,
    String triggerReason, boolean cancelRequested, Instant finishedAt, Instant createdAt) {}
