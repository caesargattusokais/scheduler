package dev.scheduler.server.leader;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 会话级 advisory lock 选主 + 心跳复检(MED-3)。
 * 启动时在专属连接上 pg_try_advisory_lock 争取 leader。此后以固定周期对同一连接复检锁是否仍由
 * 本会话独占持有:连接死亡(DB 重启/空闲踢线/网络抖动)→ Postgres 释放会话锁 → 复检抛 SQLException;
 * 或锁被另一节点抢走(连接仍在但 pg_try_advisory_lock 返回 false)→ 两者都把 isLeader 降为 false、
 * 停止调度(降级是终局:不重夺、不自愈,杜绝双主)。单节点恒 true,多节点 HA 下失锁即让位。
 */
public class AdvisoryLockLeaderElection implements LeaderElection {
  private static final Logger log = LoggerFactory.getLogger(AdvisoryLockLeaderElection.class);
  private static final long LOCK_KEY = 0x5343484544L; // "SCHED"
  /** 默认复检周期(ms);测试可用短周期构造器加速断言。 */
  static final long DEFAULT_HEARTBEAT_MS = 2_000L;

  private Connection lockConnection; // close() 会置空以支持可重入;probe 以局部引用读取防并发改写
  private final ScheduledExecutorService heartbeat;
  private volatile boolean leader;

  public AdvisoryLockLeaderElection(JdbcTemplate jdbc) {
    this(jdbc, DEFAULT_HEARTBEAT_MS);
  }

  /** 包内:可注入心跳周期。生产走默认;测试用短周期验证失锁降级路径。 */
  AdvisoryLockLeaderElection(JdbcTemplate jdbc, long heartbeatMs) {
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
      if (c != null) {
        try { c.close(); } catch (SQLException ignored) { /* best-effort */ }
      }
      throw new IllegalStateException("failed to acquire advisory lock", e);
    }
    if (acquired) {
      this.lockConnection = c;
      this.leader = true;
      log.info("node is leader (session advisory lock #{} held)", Long.toHexString(LOCK_KEY));
      this.heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "leader-heartbeat");
        t.setDaemon(true);
        return t;
      });
      this.heartbeat.scheduleWithFixedDelay(this::probeLock, heartbeatMs, heartbeatMs,
          TimeUnit.MILLISECONDS);
    } else {
      try { c.close(); } catch (SQLException ignored) { /* best-effort */ }
      this.lockConnection = null;
      this.leader = false;
      this.heartbeat = null;
    }
  }

  /** 心跳复检:在持锁连接上再试一次 advisory lock。返回 true=锁仍归本会话独占(pg 会话锁对同会话幂等);
   *  返回 false=锁已被其它会话抢走;抛 SQLException=持锁连接已死 → 双路径都降级。降级是终局,不重夺。 */
  private void probeLock() {
    Connection c = lockConnection; // 局部引用:与 close() 并发置空时,只丢掉本拍到空,不 NPE
    if (c == null) return;
    try (var rs = c.createStatement()
            .executeQuery("SELECT pg_try_advisory_lock(" + LOCK_KEY + ")")) {
      rs.next();
      if (!rs.getBoolean(1)) {
        degrade("advisory lock taken over by another session");
      }
    } catch (SQLException e) {
      degrade("advisory-lock connection lost: " + e.getMessage());
    }
  }

  private void degrade(String reason) {
    if (!leader) return; // 已降级,幂等
    leader = false;
    log.warn("leader lock lost ({}); degrading to follower and stopping scheduling", reason);
    if (heartbeat != null) heartbeat.shutdownNow();
  }

  @Override public boolean isLeader() { return leader; }

  /** Release the session advisory lock and its dedicated connection. Idempotent: second call no-ops. */
  public void close() {
    if (heartbeat != null) heartbeat.shutdownNow();
    if (lockConnection == null) return;
    final Connection c = lockConnection;
    lockConnection = null; // 先置空,保证可重入(第二次 close 走上方短路直接返回)
    try { c.createStatement().execute("SELECT pg_advisory_unlock(" + LOCK_KEY + ")"); }
    catch (SQLException e) { log.warn("failed to release advisory lock", e); }
    finally {
      try { c.close(); }
      catch (SQLException e) { log.warn("failed to close lock connection", e); }
    }
  }
}