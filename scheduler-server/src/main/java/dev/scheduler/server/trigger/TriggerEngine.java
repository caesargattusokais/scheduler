package dev.scheduler.server.trigger;

import dev.scheduler.core.IdempotencyKeys;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.leader.LeaderElection;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronExpression;

/** 定时触发引擎:选为 leader 时扫描 cron 任务,为本分钟窗命中的 tick 幂等登记一条 DUE。 */
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

  /** 扫描一次:仅 leader;每 tick 至多处理 batchSize 个任务(游标分批),对分钟窗内有 tick 的任务各登记一条 DUE(不设配额闸)。 */
  public void scanOnce() {
    if (!leader.isLeader()) return; // 非 leader 节点不得创建执行
    // Spring 6 CronExpression.next 只接受 Temporal(ZonedDateTime/LocalDateTime),不接受 Instant。
    ZoneId zone = clock.getZone();
    ZonedDateTime now = clock.instant().atZone(zone);
    List<Task> page = tasks.findCronEnabledPage(lastTaskId, batchSize);
    for (Task t : page) {
      try {
        var cron = CronExpression.parse(t.cron());
        ZonedDateTime fired = cron.next(now.minusSeconds(61));
        // 命中即无条件下发 DUE(背压 §2.4):不在此处做配额拦截,避免丢弃 tick;
        // 饱和任务的积压由 claim 侧的 CAS+配额(active.c < maxConcurrent)负责消化。
        if (fired != null && !fired.isAfter(now)) { // 该分钟窗内含一个 tick
          shards.createParentWithShards(t.id(),
              IdempotencyKeys.forTrigger(t.id(), fired.toInstant()), t.shardCount());
        }
      } catch (RuntimeException badCron) {
        // 单条坏 cron(历史存量)只跳过该任务,不拖累本 tick 其余任务触发。新坏 cron 已在建/改时被预检拦下。
        log.warn("skipping cron trigger for task {} due to bad cron '{}': {}",
            t.id(), t.cron(), badCron.toString());
      }
    }
    lastTaskId = (page.size() == batchSize) ? page.get(page.size() - 1).id() : 0L;
  }
}
