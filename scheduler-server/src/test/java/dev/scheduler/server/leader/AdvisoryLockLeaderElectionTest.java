package dev.scheduler.server.leader;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
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

  /** MED-3 回归:leader 的持锁连接被静默断开(DB 重启/空闲踢线/网络抖动)后,Postgres 释放会话锁;
   *  心跳复检应把 isLeader 降为 false,令调度停止,杜绝双主。短心跳 (100ms) 加速断言。 */
  @Test void leaderDegradesToFollowerWhenLockConnectionDropped() throws Exception {
    var leader = new AdvisoryLockLeaderElection(jdbc, 100);
    try {
      assertTrue(leader.isLeader());

      // 模拟持锁连接被对端静默断开:直接关闭其底层 Connection(不从选举走 close(),close 会主动释放锁)。
      Field f = AdvisoryLockLeaderElection.class.getDeclaredField("lockConnection");
      f.setAccessible(true);
      Connection conn = (Connection) f.get(leader);
      conn.close();

      // 轮询等心跳(100ms)降级:最多 5s。
      boolean degraded = false;
      for (int i = 0; i < 50 && !degraded; i++) {
        Thread.sleep(100);
        degraded = !leader.isLeader();
      }
      assertFalse(leader.isLeader(),
          "持锁连接断开后,心跳复检应把 leader 降级为 follower(停调度,防双主)");
    } finally {
      leader.close();
    }
  }
}
