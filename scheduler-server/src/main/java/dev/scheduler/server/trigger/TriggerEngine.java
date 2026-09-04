package dev.scheduler.server.trigger;

import dev.scheduler.core.Execution;
import dev.scheduler.core.IdempotencyKeys;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.leader.LeaderElection;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.springframework.scheduling.support.CronExpression;

/** 定时触发引擎:选为 leader 时扫描 cron 任务,为本分钟窗命中的 tick 幂等登记一条 DUE。 */
public class TriggerEngine {
  private final TaskRepository tasks;
  private final ExecutionRepository executions;
  private final LeaderElection leader;
  private final Clock clock;

  public TriggerEngine(TaskRepository tasks, ExecutionRepository executions,
                       LeaderElection leader, Clock clock) {
    this.tasks = tasks;
    this.executions = executions;
    this.leader = leader;
    this.clock = clock;
  }

  /** 扫描一次:仅 leader;对 enabled 且非 paused、分钟窗内有 tick、且配额有余的任务各登记一条 DUE。 */
  public void scanOnce() {
    if (!leader.isLeader()) return; // 非 leader 节点不得创建执行
    // Spring 6 CronExpression.next 只接受 Temporal(ZonedDateTime/LocalDateTime),不接受 Instant。
    ZoneId zone = clock.getZone();
    ZonedDateTime now = clock.instant().atZone(zone);
    for (Task t : tasks.findCronEnabled()) {
      var cron = CronExpression.parse(t.cron());
      ZonedDateTime fired = cron.next(now.minusSeconds(61));
      if (fired != null && !fired.isAfter(now)) { // 该分钟窗内含一个 tick
        if (executions.countActive(t.id()) < t.maxActiveConcurrent()) { // 配额有余
          executions.createDue(Execution.ofDue(t.id(),
              IdempotencyKeys.forTrigger(t.id(), fired.toInstant(), 0), t.shardCount()));
        }
      }
    }
  }
}