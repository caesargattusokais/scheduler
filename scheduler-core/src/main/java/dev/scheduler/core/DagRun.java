package dev.scheduler.core;
import java.time.Instant;
/** 一次 DAG 执行头(spec §1.2)。存储 status 仅 PENDING→终态。
 *  1c dagVersion:createRun 时封印的 DAG 定义版本号(旧 run 为 NULL/未知)。 */
public record DagRun(Long id, Long dagId, String idempotencyKey, DagRunStatus status,
    String triggerReason, boolean cancelRequested, Integer dagVersion, Instant finishedAt, Instant createdAt) {}
