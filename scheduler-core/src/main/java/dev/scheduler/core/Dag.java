package dev.scheduler.core;
import java.time.Instant;

/** DAG 定义头(spec §1.1)。dependsOnDagId:1d 跨 DAG 依赖——声明依赖某上游 DAG,上游一次 run 成功 → 下游幂等跑一次
 *  (事件链);非空时与 cron 互斥(设了依赖则 cron 须为空,凡可派生的下游再被触发器扫到)。 */
public record Dag(Long id, String name, String description, String cron, Long dependsOnDagId,
    boolean enabled, boolean paused, Instant createdAt, Instant updatedAt) {}
