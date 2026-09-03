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
}
