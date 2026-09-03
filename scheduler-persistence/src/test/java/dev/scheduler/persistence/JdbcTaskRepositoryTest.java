package dev.scheduler.persistence;
import static org.junit.jupiter.api.Assertions.*;
import dev.scheduler.core.Task;
import org.junit.jupiter.api.Test;

class JdbcTaskRepositoryTest extends AbstractPostgresTest {
  @Test void roundTrip() {
    var repo = new JdbcTaskRepository(jdbc);
    var created = repo.create(new Task(null, "t1", "cron", "demo", "*/5 * * * *",
        1, 300, 0, 1000, null, 8, true, false));
    assertTrue(created.id() > 0);
    assertTrue(repo.findById(created.id()).isPresent());
    assertEquals(1, repo.findCronEnabled().size());
    repo.setPaused(created.id(), true);
    assertTrue(repo.findCronEnabled().isEmpty());
  }
}
