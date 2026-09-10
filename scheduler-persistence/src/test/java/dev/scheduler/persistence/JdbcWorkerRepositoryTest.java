package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcWorkerRepositoryTest extends AbstractPostgresTest {
  private final JdbcWorkerRepository repo = new JdbcWorkerRepository(jdbc);
  private final Instant BASE = Instant.parse("2026-09-10T00:00:00Z");

  @BeforeEach void clean() {
    jdbc.update("TRUNCATE worker");
  }

  @Test
  void upsertHeartbeat_createsThenUpdatesSingleRow() {
    repo.upsertHeartbeat(new WorkerRegistration("w1", List.of("demo"), BASE, "ALIVE"));
    repo.upsertHeartbeat(new WorkerRegistration("w1", List.of("demo", "ping"), BASE.plusSeconds(5), "ALIVE"));
    assertEquals(List.of("w1"), jdbc.queryForList("SELECT id FROM worker", String.class));
    assertEquals("demo,ping",
        jdbc.queryForObject("SELECT refs FROM worker WHERE id='w1'", String.class));
  }

  @Test
  void findAllAlive_filtersByStatusAndFreshness() {
    repo.upsertHeartbeat(new WorkerRegistration("fresh", List.of("demo"), BASE.plusSeconds(60), "ALIVE"));
    repo.upsertHeartbeat(new WorkerRegistration("stale", List.of("old"), BASE.minusSeconds(60), "ALIVE"));
    repo.upsertHeartbeat(new WorkerRegistration("dead", List.of("x"), BASE.plusSeconds(60), "DEAD"));
    List<WorkerRegistration> alive = repo.findAllAlive(BASE);
    assertEquals(List.of("fresh"), alive.stream().map(WorkerRegistration::id).toList());
    assertEquals(List.of("demo"), alive.get(0).refs());
  }
}