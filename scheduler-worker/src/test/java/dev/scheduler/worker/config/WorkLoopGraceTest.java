package dev.scheduler.worker.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.persistence.retry.FailureResolver;
import dev.scheduler.persistence.retry.RetryPolicy;
import dev.scheduler.worker.execute.AbstractExecutorWorkerTest;
import dev.scheduler.worker.execute.ExecutorWorker;
import dev.scheduler.worker.handler.HandlerRegistry;
import dev.scheduler.worker.handler.MapHandlerRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 停机排空 + capacity 热改:验证 stopAndDrain 阻在在途 handler、resize 收紧并发上限。用真 PG,短时序断言。 */
class WorkLoopGraceTest extends AbstractExecutorWorkerTest {

  private TaskRepository tasks;
  private ShardRepository shards;
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T10:00:00Z"), ZoneOffset.UTC);

  private ExecutorWorker newWorker(String workerId, HandlerRegistry registry) {
    return new ExecutorWorker(tasks, shards, registry, workerId,
        new FailureResolver(shards, new RetryPolicy(), CLOCK), CLOCK, 120, 30);
  }

  @BeforeEach void clearAndInit() {
    jdbc.execute("TRUNCATE app_task, execution, execution_shard, execution_shard_outcome "
        + "RESTART IDENTITY CASCADE");
    tasks = new JdbcTaskRepository(jdbc);
    shards = new JdbcShardRepository(jdbc);
  }

  private long createTask(int shardCount) {
    return tasks.create(new dev.scheduler.core.Task(null, "t", "cron", "block", "0 */5 * * * *",
        shardCount, 300, 0, 1000, null, 8, true, false)).id();
  }

  private void seedParentAndShards(long taskId, int n) {
    shards.createParentWithShards(taskId, "p:" + System.nanoTime(), n);
  }

  private long count(String condition) {
    return jdbc.queryForObject("SELECT count(*) FROM execution_shard WHERE " + condition, Long.class);
  }

  private void awaitCount(String condition, long want) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (count(condition) < want && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
  }

  /** stopAndDrain 阻在在途 handler:release 前 drain 不返回(release 排队后才停新认领),release 后排空返回。 */
  @Test void stopAndDrain_waitsForInflight() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    var registry = new MapHandlerRegistry(
        List.of(() -> new WorkerLoopTest.BlockingHandler(entered, release)));

    long taskId = createTask(3);
    seedParentAndShards(taskId, 3);

    WorkerConfig.WorkLoop loop =
        new WorkerConfig.WorkLoop(newWorker("worker-a", registry), 2, 32, 10, new WorkerRuntimeConfig(jdbc));
    AtomicBoolean drainReturned = new AtomicBoolean(false);
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS), "handler 进入并阻塞(占住一个在途槽)");

      Thread drainer = new Thread(() -> {
        try { Thread.sleep(50); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        loop.stopAndDrain(10);
        drainReturned.set(true);
      });
      drainer.start();
      Thread.sleep(300); // 给 drainer 足够时间进入 stopAndDrain 并阻在在途 handler
      assertFalse(drainReturned.get(), "在途 handler 未释放前 drain 不应返回");

      release.countDown(); // 放行 → 在途归零 → drain 返回
      drainer.join(TimeUnit.SECONDS.toMillis(5));
      assertTrue(drainReturned.get(), "排空后 drain 应返回");
    } finally {
      release.countDown();
      loop.destroy();
    }
  }

  /** resize(1) 收紧:并发认领至多 1 片在途,第 2 片保持 DUE;释放后仍可全执行。 */
  @Test void resize_decreasesThroughput() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    var registry = new MapHandlerRegistry(
        List.of(() -> new WorkerLoopTest.BlockingHandler(entered, release)));

    WorkerConfig.WorkLoop loop =
        new WorkerConfig.WorkLoop(newWorker("worker-a", registry), 2, 32, 10, new WorkerRuntimeConfig(jdbc));
    try {
      loop.resize(1); // 先收紧并发到 1,再铺 2 片任务,避免 2 排拍在收紧前抢先认领
      long taskId = createTask(2);
      seedParentAndShards(taskId, 2);

      assertTrue(entered.await(5, TimeUnit.SECONDS), "收紧后 1 个槽位认领并进入 handler");
      awaitCount("status='RUNNING' AND worker_id='worker-a'", 1);
      assertEquals(1, count("status='RUNNING' AND worker_id='worker-a'"),
          "resize(1) → 至多 1 片 RUNNING");
      assertEquals(1, count("status='DUE'"), "第 2 片因并发上限保持 DUE");

      release.countDown();
      awaitCount("status='SUCCESS'", 2);
      assertEquals(2, count("status='SUCCESS'"), "并发上限只限同时,不限总量——第 2 片最终也执行");
    } finally {
      release.countDown();
      loop.destroy();
    }
  }
}