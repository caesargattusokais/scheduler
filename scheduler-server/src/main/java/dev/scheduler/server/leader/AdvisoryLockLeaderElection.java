package dev.scheduler.server.leader;

import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

public class AdvisoryLockLeaderElection implements LeaderElection {
  private static final Logger log = LoggerFactory.getLogger(AdvisoryLockLeaderElection.class);
  private static final long LOCK_KEY = 0x5343484544L; // "SCHED"
  private final Connection lockConnection;
  private final boolean leader;

  public AdvisoryLockLeaderElection(JdbcTemplate jdbc) {
    Connection c = null;
    boolean acquired = false;
    try {
      c = jdbc.getDataSource().getConnection();
      try (var rs = c.createStatement().executeQuery(
              "SELECT pg_try_advisory_lock(" + LOCK_KEY + ")")) {
        rs.next();
        acquired = rs.getBoolean(1);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("failed to acquire advisory lock", e);
    }
    if (acquired) {
      this.lockConnection = c;
      this.leader = true;
      log.info("node is leader (session advisory lock #{} held)", Long.toHexString(LOCK_KEY));
    } else {
      try { c.close(); } catch (SQLException ignored) { /* best-effort */ }
      this.lockConnection = null;
      this.leader = false;
    }
  }

  @Override public boolean isLeader() { return leader; }

  /** Release the session advisory lock and its dedicated connection. */
  public void close() {
    if (lockConnection == null) return;
    try { lockConnection.createStatement().execute("SELECT pg_advisory_unlock(" + LOCK_KEY + ")"); }
    catch (SQLException e) { log.warn("failed to release advisory lock", e); }
    finally {
      try { lockConnection.close(); }
      catch (SQLException e) { log.warn("failed to close lock connection", e); }
    }
  }
}