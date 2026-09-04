package dev.scheduler.persistence;
import static org.junit.jupiter.api.Assertions.*;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.core.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcShardRepositoryTest extends AbstractPostgresTest {
  private final JdbcTaskRepository taskRepo = new JdbcTaskRepository(jdbc);
  private final JdbcShardRepository shardRepo = new JdbcShardRepository(jdbc);

  @BeforeEach void clean() {
    jdbc.update("TRUNCATE app_task, execution, execution_shard, execution_shard_outcome "
        + "RESTART IDENTITY CASCADE");
  }

  private long newTask(int shardCount, int maxActiveConcurrent) {
    Task t = taskRepo.create(new Task(null, "t"+System.nanoTime(), "cron", "demo", "*/5 * * * *",
        shardCount, 300, 0, 1000, null, maxActiveConcurrent, true, false));
    return t.id();
  }

  private int shardCount(long executionId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard WHERE execution_id=?", Integer.class, executionId);
  }

  @Test void createParentAndShards_oneParentWithNShards() {
    long taskId = newTask(3, 8);
    var parent = shardRepo.createParentWithShards(taskId, "p:k1", 3);

    assertNotNull(parent.id());
    assertEquals(3, parent.shardCount(), "父 shard_count=3");
    assertEquals(ExecutionStatus.DUE, parent.status());
    assertEquals(3, shardCount(parent.id()), "N=3 → 3 个 shard");

    var shards = shardRepo.findShards(parent.id());
    assertEquals(3, shards.size());
    for (int i = 0; i < 3; i++) {
      assertEquals(i, shards.get(i).shardIndex(), "shard_index 0..2 升序");
      assertEquals(parent.id(), shards.get(i).executionId());
      assertEquals(ExecutionStatus.DUE, shards.get(i).status());
    }
  }

  @Test void createParent_sameKey_idempotent_noDupShards() {
    long taskId = newTask(3, 8);
    String parentKey = "p:k2";

    var first = shardRepo.createParentWithShards(taskId, parentKey, 3);
    var second = shardRepo.createParentWithShards(taskId, parentKey, 3);

    assertEquals(first.id(), second.id(), "same parentKey → 同一父 id");
    assertEquals(3, shardCount(first.id()), "父重复创建不重复插 shard,总数仍 N");
  }

  @Test void findShards_orderedByIndex() {
    long taskId = newTask(4, 8);
    var parent = shardRepo.createParentWithShards(taskId, "p:k3", 4);

    var shards = shardRepo.findShards(parent.id());

    assertEquals(4, shards.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(i, shards.get(i).shardIndex(), "按 shard_index 升序");
    }
  }

  @Test void createParent_shardCountOne_stillParentPlusOneShard() {
    long taskId = newTask(1, 8);
    var parent = shardRepo.createParentWithShards(taskId, "p:k4", 1);

    assertEquals(1, parent.shardCount());
    assertEquals(1, shardCount(parent.id()), "N=1 → 1 父 + 1 shard(统一模型)");
    var shard = shardRepo.findShards(parent.id()).get(0);
    assertEquals(0, shard.shardIndex());
    // findShard 按 id 读单条
    var byId = shardRepo.findShard(shard.id());
    assertEquals(shard.id(), byId.get().id());
    assertEquals(parent.id(), byId.get().executionId());
  }
}