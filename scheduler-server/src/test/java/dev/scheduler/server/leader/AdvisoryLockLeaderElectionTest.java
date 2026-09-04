package dev.scheduler.server.leader;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class AdvisoryLockLeaderElectionTest {
  @Container static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>("postgres:16-alpine");
  protected static JdbcTemplate jdbc;

  @BeforeAll static void setUp() {
    jdbc = new JdbcTemplate(new DriverManagerDataSource(
        PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()));
  }

  @Test void singleLeaderMaintainedAcrossInstances() {
    var first = new AdvisoryLockLeaderElection(jdbc);
    try {
      // First instance wins the session advisory lock on its dedicated connection.
      assertTrue(first.isLeader());

      // A second instance constructed while the first still holds the lock
      // must NOT become leader (single-leader invariant: no flip-flop).
      var second = new AdvisoryLockLeaderElection(jdbc);
      try {
        assertFalse(second.isLeader());
      } finally {
        second.close();
      }
    } finally {
      first.close();
    }
  }
}
