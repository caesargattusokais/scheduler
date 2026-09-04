package dev.scheduler.persistence;
import dev.scheduler.core.Execution;
import dev.scheduler.core.Shard;
import java.util.List;
import java.util.Optional;

/** 分片执行(execution_shard)。本任务范围:父/N-shard 创建 + 读取。认领/写回/回收等由后续任务加入。 */
public interface ShardRepository {

  /** 创建父 execution(状态 DUE,幂等 by parentKey)+ 其 N 个 shard。单事务,父重复创建不重复插 shard。 */
  Execution createParentWithShards(long taskId, String parentKey, int shardCount);

  /** 按父 execution id 读父行。 */
  Optional<Execution> findParent(long executionId);

  /** 给定父 execution 下的全部分片,按 shard_index 升序。 */
  List<Shard> findShards(long executionId);

  /** 按 shard id 读单条分片。 */
  Optional<Shard> findShard(long shardId);
}