package dev.scheduler.server.web;

import dev.scheduler.persistence.WorkerRegistration;
import dev.scheduler.persistence.WorkerRepository;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/** M6.3:控制面可校验的 handlerRef = 存活 worker 注册表 refs 并集(纯 worker 视角,spec §3)。
 *  存活以 last_seen 距今 ≤ LIVE_WINDOW 判定(DB 层 findAllAlive 已滤 status=ALIVE)。 */
public class AvailableHandlerRefs {
  private static final Duration LIVE_WINDOW = Duration.ofSeconds(30);

  private final WorkerRepository workers;
  private final Clock clock;

  public AvailableHandlerRefs(WorkerRepository workers, Clock clock) {
    this.workers = workers;
    this.clock = clock;
  }

  /** 存活 worker ref 并集。 */
  public Set<String> refs() {
    Set<String> all = new HashSet<>();
    for (WorkerRegistration w : workers.findAllAlive(clock.instant().minus(LIVE_WINDOW))) {
      all.addAll(w.refs());
    }
    return all;
  }
}
