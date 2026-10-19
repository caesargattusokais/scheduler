package dev.scheduler.server.web;

import dev.scheduler.persistence.ReconcileDecision;
import dev.scheduler.persistence.ReconcileDecisionRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.ShardRepository.ReclaimableShard;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** E3 对账可观测只读视图:backlog = 当前待回收积压明细;activity = 最近决策流水(append-only reconcile_decision)。
 *  积压判据的活性窗与 Reconciler 同源(scheduler.worker.stale-after-seconds,spec §12)——视图必须列出
 *  对账器真会回收的集合,而非另一套硬编码窗口。 */
@RestController
@RequestMapping("/api/v1/reconcile")
public class ReconcileController {
  private final ShardRepository shards;
  private final ReconcileDecisionRepository decisions;
  private final int staleAfterSeconds;

  public ReconcileController(ShardRepository shards, ReconcileDecisionRepository decisions,
                             @Value("${scheduler.worker.stale-after-seconds:30}") int staleAfterSeconds) {
    this.shards = shards;
    this.decisions = decisions;
    this.staleAfterSeconds = staleAfterSeconds;
  }

  @GetMapping("/backlog")
  public Page<ReclaimableShard> backlog(
      @RequestParam(required = false) Long taskId,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    List<ReclaimableShard> items =
        shards.findReclaimableRunning(taskId, staleAfterSeconds, p.limit(), p.offset());
    long total = shards.countReclaimableRunning(taskId, staleAfterSeconds);
    return new Page<>(items, total, p.offset(), p.limit());
  }

  @GetMapping("/activity")
  public Page<ReconcileDecision> activity(
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    List<ReconcileDecision> items = decisions.findRecent(p.limit(), p.offset());
    long total = decisions.count();
    return new Page<>(items, total, p.offset(), p.limit());
  }
}