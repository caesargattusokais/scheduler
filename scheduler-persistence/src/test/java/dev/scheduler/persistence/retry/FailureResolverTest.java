package dev.scheduler.persistence.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.AbstractPostgresTest;
import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.ShardRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 失败后统一判定(重试 vs 死信)集成测试:按 Task 携带的预算 W / maxRetries 决策。 */
class FailureResolverTest extends AbstractPostgresTest {
  private final JdbcTaskRepository taskRepo = new JdbcTaskRepository(jdbc);
  private final JdbcShardRepository shardRepo = new JdbcShardRepository(jdbc);
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T10:00:00Z"), ZoneOffset.UTC);

  @BeforeEach void clean() {
    jdbc.update("TRUNCATE app_task, execution, execution_shard, execution_shard_outcome "
        + "RESTART IDENTITY CASCADE");
  }

  private FailureResolver resolver() {
    return new FailureResolver(shardRepo, new RetryPolicy(), CLOCK);
  }

  /** 建任务 + 扇出 + 认领(attempt=1)+ 落 FAILED,返回 task 与 shardId。 */
  private Fixture seed(Long budgetMs, int maxRetries) {
    Task t = taskRepo.create(new Task(null, "t", "cron", "demo", "*/5 * * * *",
        1, 30, maxRetries, 1000, null, 8, true, false, null, null, budgetMs));
    long parentId = shardRepo.createParentWithShards(t.id(), "p:" + System.nanoTime(), 1).id();
    long shard0 = shardRepo.findShards(parentId).get(0).id();
    shardRepo.claim(shard0, t.id(), "w1", Instant.now().plusSeconds(60), 8);
    shardRepo.markStatus(shard0, ExecutionStatus.FAILED, "w1", "boom");
    return new Fixture(t, shard0);
  }

  private record Fixture(Task task, long shardId) {}

  private boolean deadLetterFlag(long sid) {
    return Boolean.TRUE.equals(jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=?", Boolean.class, sid));
  }

  private long dueOutcome(long sid) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='DUE'",
        Long.class, sid);
  }

  /** 预算 W 已耗尽(即便 maxRetries 有余)→ 直接死信,不重试。 */
  @Test void budgetExhausted_goesDeadLetter_evenWithRetriesLeft() {
    Fixture fx = seed(0L, 3);
    jdbc.update("UPDATE execution_shard SET retry_budget_until=now() - interval '1 second' WHERE id=?",
        fx.shardId);

    resolver().handle(fx.task, fx.shardId, 1, "boom");

    assertEquals(ExecutionStatus.FAILED, shardRepo.findShard(fx.shardId).orElseThrow().status());
    assertTrue(deadLetterFlag(fx.shardId), "预算 W 耗尽 → 即便 maxRetries 有余也转死信");
    assertEquals(0L, dueOutcome(fx.shardId), "预算耗尽 → 无 DUE(不重试)");
  }

  /** 预算未耗尽 → 按 maxRetries 重试(现状回归)。 */
  @Test void budgetNotExhausted_retriesByCount() {
    Fixture fx = seed(300_000L, 3);

    resolver().handle(fx.task, fx.shardId, 1, "boom");

    assertEquals(ExecutionStatus.DUE, shardRepo.findShard(fx.shardId).orElseThrow().status(),
        "窗口未耗尽 → 按次数重试");
    assertFalse(deadLetterFlag(fx.shardId));
    assertEquals(1L, dueOutcome(fx.shardId));
  }

  /** 未设 W → 恒按次数重试(现状回归)。 */
  @Test void noBudget_retriesByCount_unchanged() {
    Fixture fx = seed(null, 3);

    resolver().handle(fx.task, fx.shardId, 1, "boom");

    assertEquals(ExecutionStatus.DUE, shardRepo.findShard(fx.shardId).orElseThrow().status(),
        "无 W → 现状按次数重试");
    assertFalse(deadLetterFlag(fx.shardId));
  }

  /** attempt 超 maxRetries → 死信(预算无关的既有分支)。 */
  @Test void maxRetriesExceeded_goesDeadLetter() {
    Fixture fx = seed(null, 1);

    resolver().handle(fx.task, fx.shardId, 2, "boom");

    assertTrue(deadLetterFlag(fx.shardId), "attempt 超 maxRetries → 死信");
  }
}