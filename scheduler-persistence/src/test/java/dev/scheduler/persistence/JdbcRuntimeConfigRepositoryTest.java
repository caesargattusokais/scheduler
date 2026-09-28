package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcRuntimeConfigRepositoryTest extends AbstractPostgresTest {
  private JdbcRuntimeConfigRepository repo;

  @BeforeEach
  void beforeEach() {
    jdbc.execute("TRUNCATE app_runtime_config RESTART IDENTITY CASCADE");
    repo = new JdbcRuntimeConfigRepository(jdbc);
  }

  @Test void find_missing_returnsEmpty() {
    assertTrue(repo.find("nope").isEmpty());
  }

  @Test void upsert_insertsThenUpdates_inPlace() {
    repo.upsert("a", "1", "alice");
    RuntimeConfigRow r = repo.find("a").orElseThrow();
    assertEquals("1", r.value());
    assertEquals("alice", r.updatedBy());
    assertTrue(r.updatedAt() != null);

    repo.upsert("a", "2", "bob");
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_runtime_config", Long.class),
        "upsert 不新增行");
    assertEquals("2", repo.find("a").orElseThrow().value(), "值被更新");
    assertEquals("bob", repo.find("a").orElseThrow().updatedBy(), "操作者被更新");
  }

  @Test void findAll_sortsByKey() {
    repo.upsert("b", "2", "x");
    repo.upsert("a", "1", "x");
    assertEquals(List.of("a", "b"), repo.findAll().stream().map(RuntimeConfigRow::key).toList());
  }
}