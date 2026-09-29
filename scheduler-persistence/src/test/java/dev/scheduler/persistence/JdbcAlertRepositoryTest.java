package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.*;

import dev.scheduler.core.AlertEpisode;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;

class JdbcAlertRepositoryTest extends AbstractPostgresTest {
  private final JdbcAlertRepository repo = new JdbcAlertRepository(jdbc);

  @BeforeEach void clean() {
    jdbc.update("TRUNCATE alert_episode, app_task, app_dag RESTART IDENTITY CASCADE");
  }

  private AlertEpisode episode(String key) {
    return new AlertEpisode(0, key, "cpu-high", "none", null, "warning",
        "PENDING", 1, "95", null, null, null, null);
  }

  @Test void insertThenFindActiveByKeyRoundTrips() {
    AlertEpisode saved = repo.insert(episode("a:b"));

    assertTrue(saved.id() > 0, "insert must return a row with generated id");
    var found = repo.findActiveByKey("a:b");
    assertTrue(found.isPresent());
    AlertEpisode f = found.get();
    assertEquals(saved.id(), f.id());
    assertEquals("a:b", f.key());
    assertEquals("cpu-high", f.rule());
    assertEquals("PENDING", f.status());
    assertEquals(1, f.sampleCount());
  }

  @Test void findAllActive_returnsOnlyNonTerminalRows() {
    repo.insert(episode("active:pending"));
    repo.insert(new AlertEpisode(0, "active:open", "cpu-high", "none", null, "warning",
        "OPEN", 5, "98", Instant.now(), "98", null, null));
    AlertEpisode doomed = repo.insert(episode("prune:resolved"));
    repo.update(new AlertEpisode(doomed.id(), doomed.key(), doomed.rule(), doomed.targetType(),
        doomed.targetId(), doomed.severity(), "RESOLVED", doomed.sampleCount(), doomed.value(),
        doomed.openedAt(), doomed.openedValue(), Instant.now(), "97"));

    var active = repo.findAllActive();
    assertEquals(2, active.size(), "only PENDING/OPEN rows, not RESOLVED");
    assertTrue(active.stream().anyMatch(e -> e.key().equals("active:pending")));
    assertTrue(active.stream().anyMatch(a -> a.key().equals("active:open")));
    assertTrue(active.stream().noneMatch(a -> a.status().equals("RESOLVED")),
        "RESOLVED row must be excluded");
  }

  @Test void secondNonTerminalSameKeyThrowsDuplicateKey() {
    repo.insert(episode("dup:key"));
    assertThrows(DuplicateKeyException.class,
        () -> repo.insert(episode("dup:key")),
        "same key with a live PENDING/OPEN row must hit partial unique");
  }

  @Test void resolveMovesRowFromActiveToResolved() {
    AlertEpisode saved = repo.insert(episode("life:k"));
    repo.update(new AlertEpisode(saved.id(), saved.key(), saved.rule(), saved.targetType(),
        saved.targetId(), saved.severity(), "RESOLVED", saved.sampleCount(), saved.value(),
        saved.openedAt(), saved.openedValue(), Instant.now(), "95"));

    assertEquals(0, repo.findActive().size(), "OPEN list must be empty after resolve");
    assertEquals(1, repo.countResolved(), "one resolved history row");
    var resolved = repo.findResolved(10, 0);
    assertEquals(1, resolved.size());
    assertEquals(saved.id(), resolved.get(0).id());
  }

  @Test void sameKeyReinsertAfterResolveAllowedAndKeepsHistory() {
    AlertEpisode saved = repo.insert(episode("again:k"));
    repo.update(new AlertEpisode(saved.id(), "again:k", "cpu-high", "none", null, "warning",
        "RESOLVED", 1, "95", null, null, Instant.now(), "95"));

    // partial unique 已释放,同 key 可再开新 PENDING
    AlertEpisode second = repo.insert(episode("again:k"));
    assertEquals("PENDING", repo.findActiveByKey("again:k").map(AlertEpisode::status)
        .orElseThrow(() -> new AssertionError("a new live row must exist after resolve")),
        "re-inserted same key is live PENDING");

    // 二段也回落 → 同 key 两条独立 history 并存
    repo.update(new AlertEpisode(second.id(), second.key(), second.rule(), second.targetType(),
        second.targetId(), second.severity(), "RESOLVED", second.sampleCount(), second.value(),
        second.openedAt(), second.openedValue(), Instant.now(), "97"));
    assertEquals(2, repo.countResolved(),
        "findResolved must show both history rows (two separate episodes)");
  }

  @Test void pruneResolvedDeletesOnlyOldRows() {
    String base = "prune:k";
    // 老行 resolved_at = now()-90d,新行 resolved_at = now()
    jdbc.update("INSERT INTO alert_episode (key, rule, target_type, severity, status, sample_count, value,"
            + " resolved_at) VALUES (?, 'cpu-high', 'none', 'warning', 'RESOLVED', 1, '90', ?)",
        base + "-old", Timestamp.from(Instant.now().minus(90, ChronoUnit.DAYS)));
    jdbc.update("INSERT INTO alert_episode (key, rule, target_type, severity, status, sample_count, value,"
            + " resolved_at) VALUES (?, 'cpu-high', 'none', 'warning', 'RESOLVED', 1, '99', ?)",
        base + "-new", Timestamp.from(Instant.now()));

    long deleted = repo.pruneResolved(Instant.now().minus(30, ChronoUnit.DAYS));

    assertEquals(1, deleted, "only the > 30-day-old RESOLVED row must be pruned");
    assertEquals(1, repo.countResolved(), "the recent row survives");
  }

  @Test void deleteByKeyRemovesActiveRow() {
    repo.insert(episode("del:k"));
    assertTrue(repo.findActiveByKey("del:k").isPresent());

    repo.deleteByKey("del:k");

    assertTrue(repo.findActiveByKey("del:k").isEmpty(),
        "deleteByKey must remove the live row");
  }

  @Test void findActiveJoinsTargetName() {
    Long taskId = jdbc.queryForObject(
        "INSERT INTO app_task (name, handler_ref) VALUES ('CPU-Task', 'demo') RETURNING id", Long.class);
    AlertEpisode saved = repo.insert(new AlertEpisode(0, "join:k", "cpu-high", "task", taskId,
        "warning", "OPEN", 1, "96", Instant.now(), "96", null, null));

    var active = repo.findActive();
    assertEquals(1, active.size());
    assertEquals("CPU-Task", active.get(0).targetName(),
        "LEFT JOIN app_task must surface target_name for target_type='task'");
    assertEquals(saved.id(), active.get(0).id());
  }
}