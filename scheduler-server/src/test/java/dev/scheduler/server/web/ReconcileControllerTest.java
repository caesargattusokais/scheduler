package dev.scheduler.server.web;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import dev.scheduler.persistence.ReconcileDecision;
import dev.scheduler.persistence.ReconcileDecisionRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.ShardRepository.ReclaimableShard;
import java.util.List;
import org.junit.jupiter.api.Test;

/** ReconcileController 只读映射单测:分页裁剪 + Page 信封回显。 */
class ReconcileControllerTest {

  @Test void backlog_mapsPagingAndFilter() {
    ShardRepository shards = mock(ShardRepository.class);
    ReconcileDecisionRepository decisions = mock(ReconcileDecisionRepository.class);
    when(shards.findReclaimableRunning(null, 30, 1, 0))
        .thenReturn(List.of(new ReclaimableShard(42L, 7L, 3L, "w1", 1, 120L)));
    when(shards.countReclaimableRunning(null, 30)).thenReturn(1L);
    ReconcileController c = new ReconcileController(shards, decisions, 30);

    Page<ReclaimableShard> p = c.backlog(null, 1, 0);

    assertEquals(1, p.items().size());
    assertEquals(42L, p.items().get(0).id());
    assertEquals(120L, p.items().get(0).ageSeconds());
    assertEquals(1L, p.total());
    assertEquals(1, p.limit());
    assertEquals(0, p.offset());
  }

  @Test void activity_mapsRecentAndCount() {
    ShardRepository shards = mock(ShardRepository.class);
    ReconcileDecisionRepository decisions = mock(ReconcileDecisionRepository.class);
    when(decisions.findRecent(5, 0))
        .thenReturn(List.of(ReconcileDecision.of("near_miss", "shard", 9L, null, "r", "SKIPPED",
            "RUNNING", "RUNNING", 1, null)));
    when(decisions.count()).thenReturn(1L);
    ReconcileController c = new ReconcileController(shards, decisions, 30);

    Page<ReconcileDecision> p = c.activity(5, 0);

    assertEquals(1, p.items().size());
    assertEquals("near_miss", p.items().get(0).phase());
    assertEquals(1L, p.total());
  }

  /** §12 同源:积压视图的活性窗必须来自注入配置(与 Reconciler 同值),而非类内硬编码 30——
   *  否则运维调大 stale 秒后,视图会列出对账器有意保留 RUNNING 的分片。 */
  @Test void backlog_usesConfiguredStaleWindow() {
    ShardRepository shards = mock(ShardRepository.class);
    ReconcileDecisionRepository decisions = mock(ReconcileDecisionRepository.class);
    when(shards.findReclaimableRunning(null, 60, 10, 0)).thenReturn(List.of());
    when(shards.countReclaimableRunning(null, 60)).thenReturn(0L);
    ReconcileController c = new ReconcileController(shards, decisions, 60);

    c.backlog(null, 10, 0);

    verify(shards).findReclaimableRunning(null, 60, 10, 0);
    verify(shards).countReclaimableRunning(null, 60);
  }
}