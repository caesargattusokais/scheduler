package dev.scheduler.worker.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.persistence.retry.FailureResolver;
import dev.scheduler.persistence.retry.RetryPolicy;
import dev.scheduler.worker.execute.AbstractExecutorWorkerTest;
import dev.scheduler.worker.execute.ExecutorWorker;
import dev.scheduler.worker.handler.ExecutionHandler;
import dev.scheduler.worker.handler.HandlerContext;
import dev.scheduler.worker.handler.HandlerRegistry;
import dev.scheduler.worker.handler.MapHandlerRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 4c 扩容:WorkLoop(capacity 线程池)让单 worker 进程并发认领至多 capacity 分片,且不超限。用真 PG。 */
class WorkerLoopTest extends AbstractExecutorWorkerTest {

  private TaskRepository tasks;
  private ShardRepository shards;

  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T10:00:00Z"), ZoneOffset.UTC);

  /** 卡闸 handler:进入后 countDown 并阻塞到 release,供测「在途分片被 capacity 封顶」的窗口。 */
  static class BlockingHandler implements ExecutionHandler {
    final CountDownLatch entered;
    final CountDownLatch release;
    BlockingHandler(CountDownLatch entered, CountDownLatch release) {
      this.entered = entered; this.release = release;
    }
    @Override public String ref() { return "block"; }
    @Override public void handle(HandlerContext ctx) {
      entered.countDown();
      try { release.await(10, TimeUnit.SECONDS); } // 阻塞当前执行线程,占满一个槽位
      catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
  }

  @BeforeEach void clearAndInit() {
    jdbc.execute("TRUNCATE app_task, execution, execution_shard, execution_shard_outcome "
        + "RESTART IDENTITY CASCADE");
    tasks = new JdbcTaskRepository(jdbc);
    shards = new JdbcShardRepository(jdbc);
  }

  private ExecutorWorker newWorker(String workerId, HandlerRegistry registry) {
    return new ExecutorWorker(tasks, shards, registry, workerId,
        new FailureResolver(shards, new RetryPolicy(), CLOCK), CLOCK, 120, 30);
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

  private void awaitSuccessCount(long want) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (count("status='SUCCESS'") < want && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(want, count("status='SUCCESS'"));
  }

  /** capacity=2:3 分片任务并行认领至多 2 片在途(第 3 片保持 DUE),释放后空槽再补认领第 3 片,最终全 SUCCESS。 */
  @Test void capacity2_boundsInFlightThenCompletesRest() throws Exception {
    CountDownLatch entered = new CountDownLatch(2);   // 期望 2 个槽位各进入一片
    CountDownLatch release = new CountDownLatch(1);
    var registry = new MapHandlerRegistry(List.of(() -> new BlockingHandler(entered, release)));

    long taskId = createTask(3);
    seedParentAndShards(taskId, 3);

    WorkerConfig.WorkLoop loop = new WorkerConfig.WorkLoop(newWorker("worker-a", registry), 2, 10);
    try {
      // 两个执行线程各自认领一片并阻塞在 handle → 恰好 2 片在途,第 3 片无空闲槽位认领。
      assertTrue(entered.await(5, TimeUnit.SECONDS), "2 个槽位各认领一片并进入 handler");
      assertEquals(2, count("status='RUNNING' AND worker_id='worker-a'"),
          "capacity=2 → 至多 2 片 RUNNING 租归于该 worker");
      assertEquals(1, count("status='DUE'"), "第 3 片无空闲槽位认领,保持 DUE");

      release.countDown(); // 放行 → 前 2 片回 SUCCESS,空出槽位补认领第 3 片
      awaitSuccessCount(3);
      assertEquals(0, count("status='DUE'"), "capacity 只限并发,不限总量——第 3 片最终也被执行");
    } finally {
      loop.destroy();
    }
  }
}