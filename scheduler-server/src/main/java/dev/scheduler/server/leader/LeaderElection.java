package dev.scheduler.server.leader;

public interface LeaderElection {
  boolean isLeader();

  /** 停机/退位时释放本会话持有的 leader 锁(幂等)。滚动发布时让另一副本立即选主。 */
  default void shutdown() {}
}
