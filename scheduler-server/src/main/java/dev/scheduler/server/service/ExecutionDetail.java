package dev.scheduler.server.service;

import dev.scheduler.core.ExecutionStatus;
import java.time.Instant;
import java.util.List;

/** 执行详情(只读投影):父 header + 其全部分片。status 为派生父状态——父非终态且含 RUNNING shard 时读作 RUNNING,
 *  只做读取映射,从不写回 DB(父行仍存原 DUE/终态值)。每个分片附 failureDetail:该分片最近一次 FAILED outcome 的
 *  detail(失败日志),无失败记录则为 null。 */
public record ExecutionDetail(
    long id, long taskId, ExecutionStatus status, int shardCount, Long rerunOf, List<ShardView> shards) {

  /** 父详情里的一个分片视图:Shard 全字段 + failureDetail(失败日志)。JSON 平铺,较 {@code Shard} 仅多一格。 */
  public record ShardView(
      long id, long executionId, int shardIndex, String shardData, ExecutionStatus status,
      int attempt, String workerId, Instant leaseUntil, Instant nextRetryAt,
      boolean cancelRequested, boolean deadLetter, Instant startedAt, Instant finishedAt,
      String resultPayload, String failureDetail) {}
}