package dev.scheduler.server.service;

import dev.scheduler.core.ExecutionStatus;
import java.time.Instant;

/** DLQ 行(只读投影):Shard 全字段 + 所属任务的任务名/handlerRef + 分片最近一次 FAILED outcome 的 detail(失败日志)。
 *  供控制面死信页展示,不写回 DB。 */
public record DlqView(
    long id, long executionId, int shardIndex, String shardData, ExecutionStatus status,
    int attempt, String workerId, Instant leaseUntil, Instant nextRetryAt,
    boolean cancelRequested, boolean deadLetter, Instant startedAt, Instant finishedAt,
    String resultPayload, String taskName, String handlerRef, String failureDetail) {}