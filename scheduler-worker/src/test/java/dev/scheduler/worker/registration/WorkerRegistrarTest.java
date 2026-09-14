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
    @Override public void upsertHeartbeat(WorkerRegistration w) { rows.add(w); }
    @Override public List<WorkerRegistration> findAllAlive(Instant since) { return List.of(); }
    @Override public void purgeStale(Instant olderThan) { purged.add(olderThan); }
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
}