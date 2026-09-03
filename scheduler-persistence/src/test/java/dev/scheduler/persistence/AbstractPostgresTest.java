package dev.scheduler.persistence;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public abstract class AbstractPostgresTest {
  @Container static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>("postgres:16-alpine");
  protected static JdbcTemplate jdbc;

  @BeforeAll static void setUp() {
    org.flywaydb.core.Flyway.configure()
        .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
        .load().migrate();
    jdbc = new JdbcTemplate(new DriverManagerDataSource(
        PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()));
  }
}
