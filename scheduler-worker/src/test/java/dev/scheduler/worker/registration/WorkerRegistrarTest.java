package dev.scheduler.worker.registration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.persistence.WorkerRegistration;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.worker.handler.DemoHandler;
import dev.scheduler.worker.handler.HandlerRegistry;
import dev.scheduler.worker.handler.MapHandlerRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkerRegistrarTest {
  private static final class Fake implements WorkerRepository {
    final List<WorkerRegistration> rows = new ArrayList<>();
    final List<Instant> purged = new ArrayList<>();
    final List<String> offlined = new ArrayList<>();
    @Override public void upsertHeartbeat(WorkerRegistration w) { rows.add(w); }
    @Override public List<WorkerRegistration> findAllAlive(Instant since) {
      return rows.stream().filter(w -> w.lastSeen().isAfter(since)).toList();
    }
    @Override public void purgeStale(Instant olderThan) { purged.add(olderThan); }
    @Override public void markOffline(String workerId, Instant at) {
      offlined.add(workerId);
      rows.replaceAll(w -> w.id().equals(workerId)
          ? new WorkerRegistration(w.id(), w.refs(), at, w.status()) : w);
    }
  }

  @Test
  void heartbeat_reportsRegistryRefsAsAliveWorker() {
    Fake fake = new Fake();
    var registry = new MapHandlerRegistry(List.of(() -> new DemoHandler()));
    var clock = Clock.fixed(Instant.parse("2026-09-10T00:00:00Z"), ZoneOffset.UTC);
    new WorkerRegistrar(fake, registry, "w1", clock).heartbeat();

    WorkerRegistration w = fake.rows.get(0);
    assertEquals("w1", w.id());
    assertEquals(List.of("demo"), w.refs());
    assertEquals("ALIVE", w.status());
    assertEquals(clock.instant(), w.lastSeen());
    assertEquals(clock.instant().minusSeconds(60), fake.purged.get(0));
  }

  @Test
  void deregister_leavesWorkerOutOfAliveWindow() {
    Fake fake = new Fake();
    var registry = new MapHandlerRegistry(List.of(() -> new DemoHandler()));
    var clock = Clock.fixed(Instant.parse("2026-09-10T00:00:00Z"), ZoneOffset.UTC);
    var registrar = new WorkerRegistrar(fake, registry, "w1", clock);
    registrar.heartbeat(); // 先注册,使其本在存活视界内
    registrar.deregister(); // 把 last_seen 置极早 → 离开 30s 存活视界

    assertEquals(List.of("w1"), fake.offlined, "deregister 调用 markOffline(workerId, 极早)");
    assertTrue(fake.findAllAlive(clock.instant().minusSeconds(30)).stream()
            .noneMatch(w -> w.id().equals("w1")),
        "deregister 后 w1 不再被 findAllAlive(now-30s) 返回");
  }
}