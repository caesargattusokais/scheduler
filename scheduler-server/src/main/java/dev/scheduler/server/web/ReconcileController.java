package dev.scheduler.server.web;

import dev.scheduler.persistence.ReconcileDecision;
import dev.scheduler.persistence.ReconcileDecisionRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.ShardRepository.ReclaimableShard;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** E3 对账可观测只读视图:backlog = 当前待回收积压明细;activity = 最近决策流水(append-only reconcile_decision)。 */
@RestController
@RequestMapping("/api/v1/reconcile")
public class ReconcileController {
  private final ShardRepository shards;
  private final ReconcileDecisionRepository decisions;
  private static final int RECLAIMABLE_STALE_SECONDS = 30;

  public ReconcileController(ShardRepository shards, ReconcileDecisionRepository decisions) {
    this.shards = shards;
    this.decisions = decisions;
  }

  @GetMapping("/backlog")
  public Page<ReclaimableShard> backlog(
      @RequestParam(required = false) Long taskId,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    List<ReclaimableShard> items =
        shards.findReclaimableRunning(taskId, RECLAIMABLE_STALE_SECONDS, p.limit(), p.offset());
    long total = shards.countReclaimableRunning(taskId, RECLAIMABLE_STALE_SECONDS);
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