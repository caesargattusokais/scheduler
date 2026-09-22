package dev.scheduler.server.reconcile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.core.Execution;
import dev.scheduler.core.ExecutionStatus;
import dev.scheduler.core.Shard;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.NotificationRepository;
import dev.scheduler.persistence.OutboundNotification;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.persistence.retry.FailureResolver;
import dev.scheduler.persistence.retry.RetryPolicy;
import dev.scheduler.server.service.NotificationFirer;
import dev.scheduler.server.service.NotificationHub;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** 真 PG:确定性校验 Reconciler 回收过期租约孤儿 RUNNING shard + 父级终态汇聚(M3,作用对象为 shard)。 */
@Testcontainers
class ReconcilerTest {
  @Container static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>("postgres:16-alpine");
  private static JdbcTemplate jdbc;

  /** 固定时钟:断言 next_retry_at 由注入时钟 + 退避算出,而非实时/DB 时钟(租约豁免只用 DB now())。 */
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T10:00:00Z"), ZoneOffset.UTC);

  private TaskRepository tasks;
  private ShardRepository shards;
  private final List<Fired> fired = new ArrayList<>();

  @BeforeAll static void setUp() {
    Flyway.configure()
        .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
        .load().migrate();
    jdbc = new JdbcTemplate(new DriverManagerDataSource(
        PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()));
  }

  @BeforeEach void clearTables() {
    jdbc.execute("TRUNCATE app_task, execution, execution_shard, execution_shard_outcome,"
        + " execution_outcome RESTART IDENTITY CASCADE");
    tasks = new JdbcTaskRepository(jdbc);
    shards = new JdbcShardRepository(jdbc);
    fired.clear(); // 通知事件捕获(内存 fake repo,不落 outbox 表)。
  }

  private long createTask(int shardCount, int maxRetries, long backoffMs, String pattern) {
    return tasks.create(new Task(null, "t", "cron", "demo", "0 */5 * * * *",
        shardCount, 300, maxRetries, backoffMs, pattern, 5, true, false)).id();
  }

  /** §3 timeoutSeconds>0 的任务。 */
  private long createTaskTimed(int timeoutSeconds) {
    return tasks.create(new Task(null, "t", "cron", "demo", "0 */5 * * * *",
        1, timeoutSeconds, 0, 1000, null, 5, true, false)).id();
  }

  /** 建父 execution + N 个 DUE shard(单事务),返回父 id。 */
  private long seedParentWithShards(long taskId, int shardCount) {
    return shards.createParentWithShards(taskId, "p:" + System.nanoTime(), shardCount).id();
  }

  private Shard shard(long parentId, int idx) {
    return shards.findShards(parentId).get(idx);
  }

  /** 直接以 SQL 置某 shard 的 status(可选 lease 表达式,如 "now() - interval '1 hour'")以在指定相态播种。
   *  RUNNING 播种同时置 attempt=1(真实已认领的孤儿 attempt 已递增),使重试判定与真实一致。 */
  private void setShard(Shard s, String status, String leaseExpr) {
    String attempt = "RUNNING".equals(status) ? ", attempt=1" : "";
    if (leaseExpr == null) {
      jdbc.update("UPDATE execution_shard SET status=?" + attempt + " WHERE id=?", status, s.id());
    } else {
      jdbc.update("UPDATE execution_shard SET status=?, lease_until=" + leaseExpr + attempt
          + " WHERE id=?", status, s.id());
    }
  }

  private long shardOutcomeCount(long shardId, String status) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status=?",
        Long.class, shardId, status);
  }

  private long parentOutcomeCount(long parentId, String status) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM execution_outcome WHERE execution_id=? AND status=?",
        Long.class, parentId, status);
  }

  private Reconciler reconciler() {
    return new Reconciler(tasks, shards,
        new FailureResolver(shards, new RetryPolicy(), CLOCK),
        new NotificationFirer(new NotificationHub(new CapturingRepo(fired), new ObjectMapper())),
        "reconciler", 30);
  }

  /** 事件捕获:把 fire 到的 kind/targetType/targetId/idempotencyKey 记入共享列表,供断言点火点。 */
  private record Fired(String kind, String targetType, Long targetId, String idempotencyKey) { }

  private static final class CapturingRepo implements NotificationRepository {
    private final List<Fired> out;
    CapturingRepo(List<Fired> out) { this.out = out; }
    @Override public long enqueue(String kind, String op, String tt, Long tid, String p, String key) {
      out.add(new Fired(kind, tt, tid, key));
      return out.size();
    }
    @Override public List<OutboundNotification> due(int limit) { return List.of(); }
    @Override public void markSent(long id) { }
    @Override public void markRetry(long id, Instant at, String err) { }
    @Override public void markFailed(long id, String err) { }
  }

  /** 孤儿 shard 回收:租约过期的 RUNNING 被认领方(reconciler)落 FAILED,不可重试 → DLQ。 */
  @Test void orphanExpiredShard_onNonRetryableTask_isReclaimedToDlq() {
    long taskId = createTask(1, 0, 1000, null); // maxRetries=0 → 不可重试
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    setShard(shard(parentId, 0), "RUNNING", "now() - interval '1 hour'");

    int n = reconciler().scanOnce();

    assertEquals(1, n);
    Shard s = shards.findShard(sid).orElseThrow();
    assertEquals(ExecutionStatus.FAILED, s.status(), "孤儿回收 → 转 FAILED");
    assertEquals("reconciler", s.workerId(), "FAILED 由 reconciler 标记");
    assertEquals(1L, shardOutcomeCount(sid, "FAILED"), "落一条 FAILED outcome");
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=?", Boolean.class, sid),
        "不可重试 → FailureResolver DLQ 分支置 dead_letter=true");
    assertEquals(0L, shardOutcomeCount(sid, "DUE"), "耗尽:无 DUE outcome");
  }

  /** 孤儿 shard 回收:可重试 → 回 DUE 待再次认领,next_retry_at 由注入时钟 + 退避算出。 */
  @Test void orphanExpiredShard_onRetryableTask_isRequeuedWithClockBasedNextRetryAt() {
    long taskId = createTask(1, 2, 1000, null); // maxRetries=2, 无 pattern → 可重试
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    setShard(shard(parentId, 0), "RUNNING", "now() - interval '1 hour'");

    int n = reconciler().scanOnce();

    assertEquals(1, n);
    Shard s = shards.findShard(sid).orElseThrow();
    assertEquals(ExecutionStatus.DUE, s.status(), "可重试 → 回 DUE 待再次认领");
    assertEquals(CLOCK.instant().plusMillis(1000), s.nextRetryAt(),
        "next_retry_at = 注入时钟 + attempt1 退避(backoff=1000ms),而非实时时钟");
    assertEquals(1L, shardOutcomeCount(sid, "DUE"), "重试落 DUE outcome");
  }

  /** 多条过期孤儿 RUNNING shard 一并回收,单行竞态跳过不中止整批。 */
  @Test void multipleExpiredOrphans_allReclaimedAtOnce() {
    long taskId = createTask(2, 0, 1000, null);
    long parentId = seedParentWithShards(taskId, 2);
    long a = shard(parentId, 0).id();
    long b = shard(parentId, 1).id();
    setShard(shards.findShard(a).orElseThrow(), "RUNNING", "now() - interval '1 hour'");
    setShard(shards.findShard(b).orElseThrow(), "RUNNING", "now() - interval '30 minutes'");

    int n = reconciler().scanOnce();

    assertEquals(2, n, "两条过期孤儿都回收");
    assertEquals(ExecutionStatus.FAILED, shards.findShard(a).orElseThrow().status());
    assertEquals(ExecutionStatus.FAILED, shards.findShard(b).orElseThrow().status());
  }

  /** 租约仍有效的 RUNNING shard 不被回收。 */
  @Test void freshLeaseRunningShard_isLeftUntouched() {
    long taskId = createTask(1, 0, 1000, null);
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    setShard(shard(parentId, 0), "RUNNING", "now() + interval '60 seconds'");

    int n = reconciler().scanOnce();

    assertEquals(0, n, "租约仍有效 → 不回收");
    assertEquals(ExecutionStatus.RUNNING, shards.findShard(sid).orElseThrow().status(),
        "新鲜 RUNNING 原样不动");
    assertEquals(0L, shardOutcomeCount(sid, "FAILED"), "未回收 → 无失败 outcome");
  }

  /** 全部兄弟 SUCCESS → 父 SUCCESS。第二次 scan 幂等不重复落父 outcome。 */
  @Test void allShardsSuccess_parentBecomesSuccess() {
    long taskId = createTask(2, 2, 1000, null);
    long parentId = seedParentWithShards(taskId, 2);
    long sid = shard(parentId, 0).id();
    for (Shard s : shards.findShards(parentId)) {
      setShard(s, "SUCCESS", null);
    }

    int n = reconciler().scanOnce();

    assertEquals(0, n, "无孤儿可回收,仅父汇聚");
    assertEquals(ExecutionStatus.SUCCESS, shards.findParent(parentId).orElseThrow().status(),
        "全部 shard SUCCESS → 父 SUCCESS");
    assertEquals(1L, parentOutcomeCount(parentId, "SUCCESS"), "父落 SUCCESS outcome");
    assertEquals(ExecutionStatus.SUCCESS, shards.findShard(sid).orElseThrow().status(),
        "shard 本身保持 SUCCESS,父汇聚不动 shard");

    reconciler().scanOnce(); // 幂等:父已 SUCCESS,不再汇聚
    assertEquals(1L, parentOutcomeCount(parentId, "SUCCESS"), "父终态后不重复落 outcome");
    assertTrue(fired.contains(new Fired("execution.completed", "execution", parentId,
        "parent:execution.completed:" + parentId)), "父 SUCCESS → 发 execution.completed 事件");
    assertEquals(1, fired.stream().filter(f -> f.kind().equals("execution.completed")).count(),
        "父终态后仅发一次(幂等 key 去重)");
  }

  /**
   * FAIL_FAST:一兄弟孤儿回收耗尽 → DLQ 分支取消同父兄弟(RUNNING→cancel_requested、DUE→CANCELED);
   * 待 RUNNING 兄弟协作退场到终态后,父汇聚为 FAILED(any-FAILED 优先于 SUCCESS/CANCELED)。
   */
  @Test void oneShardFailed_parentBecomesFailed_andFailFast_cancelsSiblings() {
    long taskId = createTask(3, 0, 1000, null); // maxRetries=0 → 失败即 DLQ → FAIL_FAST
    long parentId = seedParentWithShards(taskId, 3);
    Shard trigger = shard(parentId, 0);
    Shard runningSib = shard(parentId, 1);
    Shard dueSib = shard(parentId, 2);
    long runningSid = runningSib.id();
    long dueSid = dueSib.id();
    // 触发片:过期孤儿 RUNNING(回收→FAILED→DLQ);兄弟:一个有效租约 RUNNING、一个 DUE。
    setShard(trigger, "RUNNING", "now() - interval '1 hour'");
    setShard(runningSib, "RUNNING", "now() + interval '60 seconds'");

    int n = reconciler().scanOnce();

    assertEquals(1, n, "仅触发片被作为孤儿回收");
    assertEquals(ExecutionStatus.FAILED, shards.findShard(trigger.id()).orElseThrow().status(),
        "触发片回收 → FAILED");
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=?", Boolean.class, trigger.id()),
        "耗尽 → DLQ 分支被触发");
    // FAIL_FAST:cancelSiblings 置位 RUNNING 兄弟、DUE 兄弟直编 CANCELED。
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT cancel_requested FROM execution_shard WHERE id=?", Boolean.class, runningSid),
        "RUNNING 兄弟 → cancel_requested=true(FailFast)");
    assertEquals(ExecutionStatus.RUNNING, shards.findShard(runningSid).orElseThrow().status(),
        "RUNNING 兄弟仍留 RUNNING,等 worker 自检协作退场");
    assertEquals(ExecutionStatus.CANCELED, shards.findShard(dueSid).orElseThrow().status(),
        "DUE 兄弟被 FAIL_FAST 直编 CANCELED");
    assertEquals(1L, shardOutcomeCount(dueSid, "CANCELED"));
    // 尚有 RUNNING 兄弟未终态 → 父暂不汇聚。
    assertEquals(ExecutionStatus.DUE, shards.findParent(parentId).orElseThrow().status(),
        "有兄弟未终态 → 父保持 DUE");

    // 模拟 worker 看到 cancel_requested 协作退场为 CANCELED;随后 scan 汇聚:any-FAILED → 父 FAILED。
    setShard(shards.findShard(runningSid).orElseThrow(), "CANCELED", null);
    reconciler().scanOnce();
    assertEquals(ExecutionStatus.FAILED, shards.findParent(parentId).orElseThrow().status(),
        "any-FAILED(且全部终态)→ 父 FAILED");
    assertEquals(1L, parentOutcomeCount(parentId, "FAILED"));
    assertTrue(fired.contains(new Fired("execution.failed", "execution", parentId,
        "parent:execution.failed:" + parentId)), "父 FAILED → 发 execution.failed");
    assertTrue(fired.contains(new Fired("execution.dead_letter", "shard", trigger.id(),
        "dlq:" + trigger.id() + ":1")), "DLQ 触发片 → 发 execution.dead_letter(attempt=1)");
  }

  /** 无 FAILED 时,任一兄弟 CANCELED → 父 CANCELED(优先于 SUCCESS)。 */
  @Test void oneShardCancelled_parentBecomesCancelled() {
    long taskId = createTask(2, 2, 1000, null);
    long parentId = seedParentWithShards(taskId, 2);
    for (Shard s : shards.findShards(parentId)) {
      setShard(s, "SUCCESS", null);
    }
    setShard(shard(parentId, 1), "CANCELED", null);

    reconciler().scanOnce();

    assertEquals(ExecutionStatus.CANCELED, shards.findParent(parentId).orElseThrow().status(),
        "无 FAILED 但有 CANCELED → 父 CANCELED");
    assertEquals(1L, parentOutcomeCount(parentId, "CANCELED"));
  }

  /** §3 分布式超时:设了 timeoutSeconds>0,RUNNING 且 started_at 超限 → 被铁 FAILED + runtime timeout。 */
  @Test void shardOverRuntime_isReclaimedAsFailed() {
    long taskId = createTaskTimed(3);
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='w',"
        + " lease_until=now()+interval '60 seconds', attempt=1,"
        + " started_at=now() - interval '10 minutes' WHERE id=?", sid);
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status)"
        + " VALUES ('w', '', now() - interval '5 seconds', 'ALIVE')"); // 心跳新鲜:排除活性分支,单测超时分支

    assertEquals(1, reconciler().scanOnce(), "超时 → 回收");
    assertEquals(ExecutionStatus.FAILED, shards.findShard(sid).orElseThrow().status());
    assertEquals(1L, shardOutcomeCount(sid, "FAILED"));
    Long rt = jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE shard_id=? AND status='FAILED' AND detail='runtime timeout'",
        Long.class, sid);
    assertEquals(1L, rt, "failure detail = runtime timeout");
    assertTrue(fired.contains(new Fired("execution.timeout", "shard", sid,
        "timeout:" + sid + ":1")), "超时回收 → 发 execution.timeout(attempt=1)");
    // 单分片父随之汇聚 FAILED → 父级 execution.failed 亦发。
    assertTrue(fired.contains(new Fired("execution.failed", "execution", parentId,
        "parent:execution.failed:" + parentId)), "父终态 FAILED → 发 execution.failed");
  }

  /** §3 对照:timeoutSeconds=0(不超时)即便 started_at 极旧也不被铁。 */
  @Test void timeoutZero_shardNeverTimedOut() {
    long taskId = createTaskTimed(0);
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id='wz',"
        + " lease_until=now()+interval '60 seconds', attempt=1,"
        + " started_at=now() - interval '10 minutes' WHERE id=?", sid);
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status)"
        + " VALUES ('wz', '', now() - interval '5 seconds', 'ALIVE')"); // 心跳新鲜:排除活性分支,单测超时=0

    assertEquals(0, reconciler().scanOnce());
    assertEquals(ExecutionStatus.RUNNING, shards.findShard(sid).orElseThrow().status());
    assertEquals(0L, shardOutcomeCount(sid, "FAILED"));
  }

  /** §3:未认领(worker_id NULL)的异常 RUNNING 行不参与超时判定。 */
  @Test void unclaimedRunningShard_notTimedOut() {
    long taskId = createTaskTimed(3);
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=NULL,"
        + " lease_until=now()+interval '60 seconds',"
        + " started_at=now() - interval '10 minutes' WHERE id=?", sid);

    assertEquals(0, reconciler().scanOnce());
    assertEquals(ExecutionStatus.RUNNING, shards.findShard(sid).orElseThrow().status());
  }

  /** §3:worker 心跳仍新鲜但已超时 → 仍被铁(超时与活性两分支互不干扰,都汇入统一 fail 路径)。 */
  @Test void liveButOverRuntime_shardStillTimedOut() {
    long taskId = createTaskTimed(3);
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    String owner = "w-live";
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=?,"
        + " lease_until=now()+interval '60 seconds', attempt=1,"
        + " started_at=now() - interval '10 minutes' WHERE id=?", owner, sid);
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status)"
        + " VALUES (?, '', now() - interval '5 seconds', 'ALIVE')", owner);

    assertEquals(1, reconciler().scanOnce(), "活 worker 但超时 → 仍铁 FAILED");
    assertEquals(ExecutionStatus.FAILED, shards.findShard(sid).orElseThrow().status());
  }

  /** 父仍需等待未终态兄弟:1 SUCCESS + 1 RUNNING + 1 DUE → 父不动不下场。 */
  @Test void parentNotAllTerminal_staysNonTerminal() {
    long taskId = createTask(3, 2, 1000, null);
    long parentId = seedParentWithShards(taskId, 3);
    setShard(shard(parentId, 0), "SUCCESS", null);
    setShard(shard(parentId, 1), "RUNNING", "now() + interval '60 seconds'"); // 有效租约,不回收

    reconciler().scanOnce();

    assertEquals(0L, parentOutcomeCount(parentId, "SUCCESS"), "未全员终态 → 父不汇聚");
    assertEquals(0L, parentOutcomeCount(parentId, "FAILED"));
    assertEquals(0L, parentOutcomeCount(parentId, "CANCELED"));
    Execution parent = shards.findParent(parentId).orElseThrow();
    assertEquals(ExecutionStatus.DUE, parent.status(), "仍有 RUNNING+DUE 兄弟 → 父保持 DUE");
  }

  /** §1 活性优先接管:owner 心跳失联且租约未过期 → 在租约到期前被立即回收(零永久 RUNNING)。 */
  @Test void orphanWithHeartbeatStaleOwner_reclaimedBeforeLeaseExpiry() {
    long taskId = createTask(1, 0, 1000, null); // maxRetries=0 → FAILED 即 DLQ
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    String owner = "w-gone";
    // 真实 claim 语义:worker_id=owner、RUNNING、租约仍有效(+60s)。
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=?,"
        + " lease_until=now()+interval '60 seconds', attempt=1 WHERE id=?", owner, sid);
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status)"
        + " VALUES (?, '', now() - interval '2 minutes', 'ALIVE')", owner); // 心跳失联 > stale30

    assertEquals(1, reconciler().scanOnce(), "心跳失联 owner → 租约到期前被活性优先回收");
    Shard s = shards.findShard(sid).orElseThrow();
    assertEquals(ExecutionStatus.FAILED, s.status(), "活性优先回收 → FAILED");
    assertEquals(1L, shardOutcomeCount(sid, "FAILED"), "回收落 FAILED outcome(归因)");
    assertEquals(Boolean.TRUE, jdbc.queryForObject(
        "SELECT dead_letter FROM execution_shard WHERE id=?", Boolean.class, sid),
        "不可重试 → DLQ 分支");
  }

  /** §1 负例:owner 心跳新鲜 + 租约有效 → 不被活性优先误收。 */
  @Test void shardWithHeartbeatAliveOwner_isLeftUntouched() {
    long taskId = createTask(1, 0, 1000, null);
    long parentId = seedParentWithShards(taskId, 1);
    long sid = shard(parentId, 0).id();
    String owner = "w-alive";
    jdbc.update("UPDATE execution_shard SET status='RUNNING', worker_id=?,"
        + " lease_until=now()+interval '60 seconds', attempt=1 WHERE id=?", owner, sid);
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status)"
        + " VALUES (?, '', now() - interval '5 seconds', 'ALIVE')", owner);

    assertEquals(0, reconciler().scanOnce(), "owner 心跳新鲜 → 不回收");
    assertEquals(ExecutionStatus.RUNNING, shards.findShard(sid).orElseThrow().status());
  }
}

