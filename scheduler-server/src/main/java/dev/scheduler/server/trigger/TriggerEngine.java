package dev.scheduler.server.trigger;

import dev.scheduler.core.IdempotencyKeys;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.leader.LeaderElection;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronExpression;

/** 定时触发引擎:选为 leader 时扫描 cron/interval 任务,为窗口内命中的 tick 幂等登记一条 DUE。
 *  3a 触发时钟:intervalSeconds 非空走 {@link #triggerInterval}(epoch 对齐边界,时区无关);否则走
 *  {@link #triggerCron}(在任务时区解释 cron)。二者互斥(建/改任务已校验恰具其一)。 */
public class TriggerEngine {
  private static final Logger log = LoggerFactory.getLogger(TriggerEngine.class);
  private final TaskRepository tasks;
  private final ExecutionRepository executions;
  private final ShardRepository shards;
  private final LeaderElection leader;
  private final Clock clock;
  private final int batchSize;   // 单 tick 最多处理的任务数(§4 游标分批;页满推进、页未满回卷)
  private long lastTaskId;       // 游标:上次扫到的最大任务 id;回卷=0 表示下轮从头

  public TriggerEngine(TaskRepository tasks, ExecutionRepository executions,
                       ShardRepository shards, LeaderElection leader, Clock clock,
                       int scanBatchSize) {
    this.tasks = tasks;
    this.executions = executions;
    this.shards = shards;
    this.leader = leader;
    this.clock = clock;
    this.batchSize = scanBatchSize;
  }

  /** 扫描一次:仅 leader;每 tick 至多处理 batchSize 个任务(游标分批),对窗口内命中的任务各登记一条 DUE(不设配额闸)。 */
  public void scanOnce() {
    if (!leader.isLeader()) return; // 非 leader 节点不得创建执行
    List<Task> page = tasks.findScheduleEnabledPage(lastTaskId, batchSize);
    for (Task t : page) {
      try {
        if (t.intervalSeconds() != null) triggerInterval(t);
        else triggerCron(t);
      } catch (RuntimeException bad) {
        // 单条坏定义(历史存量)只跳过该任务,不拖累本 tick 其余任务触发。新坏定义已由建/改预检拦下。
        log.warn("skipping trigger for task {}: {}", t.id(), bad.toString());
      }
    }
    lastTaskId = (page.size() == batchSize) ? page.get(page.size() - 1).id() : 0L;
  }

  /** cron 触发:在任务时区解释 cron,本分钟窗(-61s)内含 tick → 登记 DUE。时钟就走 clock 的绝对 instant,
   *  只是把该 instant 投射到任务时区匹配 cron;幂等键用 fired 的绝对 instant(时区不变其值),重放自愈。 */
  private void triggerCron(Task t) {
    ZoneId zone = ZoneId.of(t.timezone());   // 建/改已校验合法;历史缺省 'UTC'
    ZonedDateTime now = clock.instant().atZone(zone);
    var cron = CronExpression.parse(t.cron());
    ZonedDateTime fired = cron.next(now.minusSeconds(61));
    // 命中即无条件下发 DUE(背压 §2.4):不在此处做配额拦截,避免丢弃 tick;
    // 饱和任务的积压由 claim 侧的 CAS+配额(active.c < maxConcurrent)负责消化。
    if (fired != null && !fired.isAfter(now)) { // 该分钟窗内含一个 tick
      shards.createParentWithShards(t.id(),
          IdempotencyKeys.forTrigger(t.id(), fired.toInstant()), t.shardCount());
    }
  }

  /** interval 触发:每当跨过一个新的 epoch 对齐边界(floorDiv(nowSec, interval) * interval)登记一条 DUE。
   *  幂等键 = 该边界 instant,同一边界内反复 scan 是同一键(upsert 自愈);跨过下一边界才换新键建新执行。
   *  interval 相对 UTC 的绝对时刻对齐,与任务时区无关。 */
  private void triggerInterval(Task t) {
    long interval = t.intervalSeconds();
    long boundarySec = Math.floorDiv(clock.instant().getEpochSecond(), interval) * interval;
    shards.createParentWithShards(t.id(),
        IdempotencyKeys.forTrigger(t.id(), Instant.ofEpochSecond(boundarySec)), t.shardCount());
  }
}
