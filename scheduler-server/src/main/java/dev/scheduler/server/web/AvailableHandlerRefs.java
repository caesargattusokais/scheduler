package dev.scheduler.server.web;

import dev.scheduler.persistence.WorkerRegistration;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.worker.handler.HandlerRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/** M6.2:控制面可校验的 handlerRef 并集 = 进程内 registry.refs() ∪ 存活 worker 表 refs。
 *  过渡期(server 仍内嵌跑 executor,直到 M6.3):进程内 ref 仍合法;M6.3 移除内嵌后本类坍缩为纯
 *  存活-worker 视角,恰合 spec §3。存活以 last_seen 距今 ≤ LIVE_WINDOW 判定(DB 层 findAllAlive 已滤 status=ALIVE)。 */
public class AvailableHandlerRefs {
  private static final Duration LIVE_WINDOW = Duration.ofSeconds(30);

  private final HandlerRegistry inProcess;
  private final WorkerRepository workers;
  private final Clock clock;

  public AvailableHandlerRefs(HandlerRegistry inProcess, WorkerRepository workers, Clock clock) {
    this.inProcess = inProcess;
    this.workers = workers;
    this.clock = clock;
  }

  /** 进程内 ref ∪ 存活 worker ref 并集。 */
  public Set<String> refs() {
    Set<String> all = new HashSet<>(inProcess.refs());
    for (WorkerRegistration w : workers.findAllAlive(clock.instant().minus(LIVE_WINDOW))) {
      all.addAll(w.refs());
    }
    return all;
  }
}