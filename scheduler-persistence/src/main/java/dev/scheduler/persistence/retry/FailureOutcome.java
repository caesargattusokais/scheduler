package dev.scheduler.persistence.retry;

/** 失败分片被统一判定后的下场:重排回队 vs 转死信(含 FAIL_FAST)。 */
public enum FailureOutcome { REQUEUED, DEAD_LETTER }