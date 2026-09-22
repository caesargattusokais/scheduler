package dev.scheduler.core;
import java.time.Instant;
import java.util.UUID;

public final class IdempotencyKeys {
  private IdempotencyKeys() {}

  /** task + 触发时刻(毫秒) → 父 execution 的全局幂等键。父为该次触发的唯一代表(去掉 shardIndex 段)。 */
  public static String forTrigger(long taskId, Instant triggerAt) {
    return taskId + ":" + triggerAt.toEpochMilli();
  }

  /** dag 调度触发 → dag_run 全局幂等键(镜像 forTrigger 的 task+epochMs 形态)。 */
  public static String forDagTrigger(long dagId, Instant triggerAt) {
    return "dag:" + dagId + ":" + triggerAt.toEpochMilli();
  }

  /** dag 手动触发 → dag_run 全局幂等键。 */
  public static String forManualDagRun(long dagId) {
    return "dag:" + dagId + ":manual:" + UUID.randomUUID();
  }

  /** dag 节点重跑 → 新 execution 全局幂等键(每次新建 UUID,节点重跑每次独立一轮)。 */
  public static String forNodeRerun(long runId, String nodeKey) {
    return "dag:" + runId + ":node:" + nodeKey + ":rerun:" + UUID.randomUUID();
  }

  /** 3b 入站事件(dedupeKey)→ 父 execution 全局幂等键:重放同一事件 → 同一键(upsert 自愈,一条事件至多一轮)。 */
  public static String forEvent(String dedupeKey) {
    return "event:" + dedupeKey;
  }

  /** 1d 跨 DAG 依赖:上游 dag_run(成功)→ 下游 dag_run 全局幂等键。上游每次都从尾部派生新 run,键 = dep:{上游runId},
   *  上游不同 run 各生成一个下游 run(事件链语义);重复派生同一下游 → 幂等复用(崩溃重放自愈)。 */
  public static String forDagDep(long upstreamRunId) {
    return "dep:" + upstreamRunId;
  }
}
