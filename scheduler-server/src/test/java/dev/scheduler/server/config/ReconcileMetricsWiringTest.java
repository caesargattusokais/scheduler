package dev.scheduler.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.scheduler.persistence.ReconcileDecisionRepository;
import dev.scheduler.persistence.ShardRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/**
 * E3 对账观测 gauge 的「活性窗同源」守卫(spec §12:积压判据复用 E1 活体窗,stale 秒与 Reconciler 同源)。
 *
 * <p>动机:Reconciler 的回收判据取自可配置的 {@code scheduler.worker.stale-after-seconds},而积压 gauge 曾
 * 硬编码 30。两者一旦漂移,运维调大 stale 秒后 gauge 会把「对账器有意保留 RUNNING」的分片计为积压 → 误报。
 * 此测试直接读装配现场:喂 60 秒进 bean,断言 gauge 求值确实把 60 传给仓储。
 */
class ReconcileMetricsWiringTest {

  /** 装配现场断言:gauge 求值(scrape 时)把配置的活性窗原样透传给仓储,而非类内常量。 */
  @Test void reconcileBacklogGauges_useConfiguredStaleWindow() {
    ReconcileDecisionRepository decisions = mock(ReconcileDecisionRepository.class);
    ShardRepository shards = mock(ShardRepository.class);
    when(shards.countReclaimableRunning(null, 60)).thenReturn(7L);
    when(shards.maxReclaimableAgeSeconds(60)).thenReturn(120L);
    Beans beans = new Beans();
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    beans.reconcileMetrics(decisions, shards, 60).bindTo(registry);

    // lazy gauge:求值发生在读表时,故此处读值即触发仓储调用。
    assertEquals(7.0, registry.get("scheduler_reconcile_backlog").gauge().value());
    assertEquals(120.0, registry.get("scheduler_reconcile_backlog_max_age_seconds").gauge().value());

    verify(shards).countReclaimableRunning(null, 60);
    verify(shards).maxReclaimableAgeSeconds(60);
  }

  /** 4 个 phase gauge 各自 lazy 查库;聚合 gauge 已移除——它的值 = timeout + orphan,PromQL 可自行导出,
   *  不值得为它多养一张卡(以及为去重它而生的跨 gauge 缓存)。 */
  @Test void phaseGauges_queryPerPhase_andAggregateGaugeIsGone() {
    ReconcileDecisionRepository decisions = mock(ReconcileDecisionRepository.class);
    ShardRepository shards = mock(ShardRepository.class);
    when(decisions.countPhaseLastHour("timeout_reclaim")).thenReturn(2L);
    when(decisions.countPhaseLastHour("orphan_reclaim")).thenReturn(3L);
    when(decisions.countPhaseLastHour("parent_aggregate")).thenReturn(4L);
    when(decisions.countPhaseLastHour("near_miss")).thenReturn(5L);
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new Beans().reconcileMetrics(decisions, shards, 30).bindTo(registry);

    assertEquals(2.0, registry.get("scheduler_reconcile_timeout_reclaimed_1h").gauge().value());
    assertEquals(3.0, registry.get("scheduler_reconcile_orphan_reclaimed_1h").gauge().value());
    assertEquals(4.0, registry.get("scheduler_reconcile_aggregated_1h").gauge().value());
    assertEquals(5.0, registry.get("scheduler_reconcile_near_miss_1h").gauge().value());

    assertNull(registry.find("scheduler_reconcile_reclaimed_1h").gauge(),
        "聚合 gauge 已移除(值可由两分量在 PromQL 里相加导出)");
  }
}
