package dev.scheduler.server.execute;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** server 模块的 PG 测试基类(execute 包本地副本):复用 Flyway 迁移建表。 */
@Testcontainers
abstract class AbstractExecutorWorkerTest {
  @Container static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>("postgres:16-alpine");
  protected static JdbcTemplate jdbc;

  @BeforeAll static void setUp() {
    Flyway.configure()
        .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
        .load().migrate();
    jdbc = new JdbcTemplate(new DriverManagerDataSource(
        PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()));
  }
}