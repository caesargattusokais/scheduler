package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class SchemaSmokeTest {
  @Container static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>("postgres:16-alpine");

  @BeforeAll static void pre() {
    org.flywaydb.core.Flyway.configure()
        .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
        .load().migrate();
  }

  @Test void tablesExist() {
    var jdbc = new JdbcTemplate(new org.springframework.jdbc.datasource
        .DriverManagerDataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()));
    Integer n = jdbc.queryForObject(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'", Integer.class);
    assertTrue(n >= 3);
  }

  @Test void reliabilityColumnsExist() {
    assertColumnType("cancel_requested");
    assertColumnType("dead_letter");
  }

  private void assertColumnType(String column) {
    String dataType = jdbc().queryForObject(
        "SELECT data_type FROM information_schema.columns"
            + " WHERE table_schema='public' AND table_name='execution' AND column_name=?",
        String.class, column);
    assertEquals("boolean", dataType);
  }

  private JdbcTemplate jdbc() {
    return new JdbcTemplate(new org.springframework.jdbc.datasource
        .DriverManagerDataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()));
  }

  @Test void reliabilityColumnsDefaultFalse() {
    Long taskId = jdbc().queryForObject(
        "INSERT INTO app_task (name, handler_ref) VALUES ('m2-test', 'test') RETURNING id",
        Long.class);
    JdbcTemplate jdbc = jdbc();
    Long executionId = jdbc.queryForObject(
        "INSERT INTO execution (task_id, status, idempotency_key)"
            + " VALUES (?, 'scheduled', 'm2-test') RETURNING id",
        Long.class, taskId);
    var row = jdbc.queryForMap(
        "SELECT cancel_requested, dead_letter FROM execution WHERE id=?",
        executionId);
    assertEquals(false, row.get("cancel_requested"));
    assertEquals(false, row.get("dead_letter"));
  }
}
