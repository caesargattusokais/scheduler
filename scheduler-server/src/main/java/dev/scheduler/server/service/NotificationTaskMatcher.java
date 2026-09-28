package dev.scheduler.server.service;

import dev.scheduler.core.Shard;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.ExecutionRepository;
import dev.scheduler.persistence.OutboundNotification;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.Webhook;

/** 通知订阅的任务维度判定,dispatcher(投递过滤)与 controller(投递历史「投递给」推导)同源复用。
 *  语义与 Webhook.scopeMode 对应:ALL 恒命中;INCLUDE 仅选中任务/所属 DAG 命中;EXCLUDE 取反。 */
public class NotificationTaskMatcher {
  private static final String INCLUDE = "INCLUDE";
  private static final String EXCLUDE = "EXCLUDE";

  private final ExecutionRepository executions;
  private final ShardRepository shards;
  private final DagRepository dags;

  public NotificationTaskMatcher(ExecutionRepository executions, ShardRepository shards,
                                 DagRepository dags) {
    this.executions = executions;
    this.shards = shards;
    this.dags = dags;
  }

  /** 通知对应的执行 task_id:execution 目标 → 执行行;shard 目标 → 分片所属执行行;其他/解析不出 → null。 */
  Long taskIdOf(OutboundNotification n) {
    if (n.targetId() == null) return null;
    try {
      if ("execution".equals(n.targetType())) {
        return executions.findById(n.targetId()).map(x -> x.taskId()).orElse(null);
      }
      if ("shard".equals(n.targetType())) {
        Shard s = shards.findShard(n.targetId()).orElse(null);
        if (s == null || s.executionId() == null) return null;
        return executions.findById(s.executionId()).map(x -> x.taskId()).orElse(null);
      }
    } catch (RuntimeException e) {
      // 执行索引不稳定时(回收/竞态)不因解析失败而丢告警 → 按命中放行。
      return null;
    }
    return null;
  }

  /** 任务维度判定:ALL/未知 → 恒真;解析不出 taskId → 放行(不误杀告警);否则按 scopeMode + 选中集。 */
  public boolean matches(Webhook w, OutboundNotification n) {
    if (!INCLUDE.equals(w.scopeMode()) && !EXCLUDE.equals(w.scopeMode())) return true;
    Long taskId = taskIdOf(n);
    if (taskId == null) return true;
    boolean inSelection = w.selectedTaskIds().contains(taskId)
        || (!w.selectedDagIds().isEmpty()
            && dags.findDagIdsContainingTask(taskId).stream().anyMatch(w.selectedDagIds()::contains));
    return INCLUDE.equals(w.scopeMode()) ? inSelection : !inSelection;
  }
}