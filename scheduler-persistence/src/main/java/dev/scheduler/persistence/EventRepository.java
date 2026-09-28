package dev.scheduler.persistence;

import java.util.List;
import java.util.Optional;

/** 入站事件 outbox(app_task_event):PENDING 落库 → leader 门控 EventEngine 分派。 */
public interface EventRepository {

  /** 入队结果:replayed=true 表示同 dedupeKey 命中既有行(幂等重放,未新建)。 */
  record EnqueueResult(long id, boolean replayed) {}

  /** 幂等入队:dedupe_key 唯一(重复提交 → 命中既有行);返回落库/以存行 id 及是否为新行。 */
  EnqueueResult enqueue(String routeKey, String payloadJson, String dedupeKey);

  /** 按 id 读单条(供 POST 回显、事件详情)。 */
  Optional<InboundEvent> findById(long id);

  /** 取待分派 PENDING 事件,最多 limit 条、id 升序。 */
  List<InboundEvent> pending(int limit);

  /** 分派完成:回填所触发的任务与父 execution id,status→DISPATCHED、dispatched_at=now();
   *  taskId/executionId 可为 null(事件无订阅者时仅标记消费,不关联任务);CAS on status='PENDING':
   *  0 行 = 已被他方分派(幂等,tick 间不重复触发)。 */
  void markDispatched(long id, Long taskId, Long executionId);

  /** 事件页列表:按 id 降序(最新在前)分页。 */
  List<InboundEvent> findPage(int limit, int offset);

  /** 全部事件计数(事件页表头)。 */
  long count();

  /** 系统真实出现过的 route key 去重(按最近入队倒序),供任务表单事件路由点选。 */
  List<String> distinctRouteKeys();
}