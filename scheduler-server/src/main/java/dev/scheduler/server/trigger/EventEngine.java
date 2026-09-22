package dev.scheduler.server.trigger;

import dev.scheduler.core.Execution;
import dev.scheduler.core.IdempotencyKeys;
import dev.scheduler.core.Task;
import dev.scheduler.persistence.EventRepository;
import dev.scheduler.persistence.InboundEvent;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.server.leader.LeaderElection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 3b 事件触发引擎:选为 leader 时扫描入站事件 outbox(app_task_event)的 PENDING 行,把事件 route_key 匹配到
 *  订阅该路由的启用任务,为每条事件幂等登记一个父 execution(键 = event:{dedupeKey})。
 *  语义(已批):「路由 key 订阅」— 事件路由到订阅它的任务;「每条事件 → 一次 run」— 分派到订阅该路由的首个
 * (id 最小)任务。事件无订阅者时仍被消费(标记 DISPATCHED),避免流浪事件永驻 PENDING 拖累扫描。
 *  失败的单条事件仅告警跳过(下个 tick 因仍 PENDING 自然重试);父创建幂等,重试不重复建轮。 */
public class EventEngine {
  private static final Logger log = LoggerFactory.getLogger(EventEngine.class);
  private final EventRepository events;
  private final TaskRepository tasks;
  private final ShardRepository shards;
  private final LeaderElection leader;
  private final int batchSize; // 单 tick 最多分派的事件数(§4 游标:页处理完回卷,下 tick 续)

  public EventEngine(EventRepository events, TaskRepository tasks, ShardRepository shards,
                     LeaderElection leader, int batchSize) {
    this.events = events;
    this.tasks = tasks;
    this.shards = shards;
    this.leader = leader;
    this.batchSize = batchSize;
  }

  /** 扫描一次:仅 leader;处理至多 batchSize 条 PENDING 事件。 */
  public void scanOnce() {
    if (!leader.isLeader()) return; // 非 leader 节点不得触发执行
    List<InboundEvent> page = events.pending(batchSize);
    for (InboundEvent evt : page) {
      try {
        dispatch(evt);
      } catch (RuntimeException bad) {
        // 单条坏数据只跳过该事件;行仍 PENDING,下 tick 重试(不丢)。
        log.warn("skipping dispatch of event {}: {}", evt.id(), bad.toString());
      }
    }
  }

  private void dispatch(InboundEvent evt) {
    List<Task> subscribed = tasks.findEnabledByRoute(evt.routeKey());
    if (subscribed.isEmpty()) {
      // 路由暂无人订阅:事件仍被消费(不触发任何轮)。
      events.markDispatched(evt.id(), null, null);
      return;
    }
    Task t = subscribed.get(0); // 一条事件 → 一轮(首个订阅者,id 最小)。
    Execution parent = shards.createParentWithShards(t.id(),
        IdempotencyKeys.forEvent(evt.dedupeKey()), t.shardCount());
    events.markDispatched(evt.id(), t.id(), parent.id());
  }
}