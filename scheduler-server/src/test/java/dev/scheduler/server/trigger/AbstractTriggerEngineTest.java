package dev.scheduler.server.trigger;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** server 模块的 PG 测试基类:复用 Flyway 迁移(经 scheduler-persistence 主依赖带入 V1 迁移)建表。 */
@Testcontainers
abstract class AbstractTriggerEngineTest {
  @Container static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>("postgres:16-alpine");
  protected static JdbcTemplate jdbc;

  /** 固定时钟,确定性而非墙钟:每 5 分钟的 10:05:00 正好是一个 tick 边界,命中即 fired == now。 */
  protected static final Clock FIXED_CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T10:05:00Z"), ZoneOffset.UTC);

  @BeforeAll static void setUp() {
    Flyway.configure()
        .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
        .load().migrate();
    jdbc = new JdbcTemplate(new DriverManagerDataSource(
        PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()));
  }
}
