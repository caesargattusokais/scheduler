package dev.scheduler.server.service;

import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import java.util.List;

/** 执行详情(只读投影):父 header + 其全部分片。status 为派生父状态——父非终态且含 RUNNING shard 时读作 RUNNING,
 *  只做读取映射,从不写回 DB(父行仍存原 DUE/终态值)。 */
public record ExecutionDetail(
    long id, long taskId, ExecutionStatus status, int shardCount, Long rerunOf, List<Shard> shards) {}
