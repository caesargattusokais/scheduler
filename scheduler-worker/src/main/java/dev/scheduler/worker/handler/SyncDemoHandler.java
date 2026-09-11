package dev.scheduler.worker.handler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 真实业务演示 handler(ref=sync):把大数据源表(demo_sync_source)数据同步到目标表(demo_sync_target)。
 * 按主键 id 把 [min,max] 均匀切成 shardCount 段,本分片只同步自己那段(区间不相交、幂等 INSERT ... ON CONFLICT DO NOTHING),
 * 并回写该段源表 synced_at 作为已同步标记。分片并行由调度器驱动,验证千万级批处理下的分摊与父汇聚。
 * 每分片 resultPayload 汇报(lo, hi, synced=实际插入行数)。
 */
public class SyncDemoHandler implements ExecutionHandler {
  private static final String TARGET_COLS =
      "id, batch_no, acct_no, amount, quantity, rate, active, category, email, region, memo, check_flag, cfg_json, created_at, updated_at";

  private final JdbcTemplate jdbc;
  /** shardId → 该分片同步简报(跨分片并发写,按全局唯一 shardId 隔离)。 */
  private final ConcurrentMap<Long, String> results = new ConcurrentHashMap<>();

  public SyncDemoHandler(JdbcTemplate jdbc) { this.jdbc = jdbc; }

  @Override public String ref() { return "sync"; }

  @Override public void handle(HandlerContext ctx) {
    long[] mm = jdbc.queryForObject(
        "SELECT min(id), max(id) FROM demo_sync_source",
        (rs, i) -> new long[] { rs.getLong(1), rs.getLong(2) });
    if (mm == null || mm[1] < mm[0]) { // 空表
      results.put(ctx.shardId(), "{\"shard\":" + ctx.shardIndex() + ",\"synced\":0,\"note\":\"empty\"}");
      return;
    }
    long min = mm[0], max = mm[1];
    long span = max - min + 1;
    long step = (span + ctx.shardCount() - 1) / ctx.shardCount();
    long lo = min + (long) ctx.shardIndex() * step;
    long hi = (ctx.shardIndex() == ctx.shardCount() - 1)
        ? max : Math.min(max, min + (long) (ctx.shardIndex() + 1) * step - 1);
    if (lo > hi) {
      results.put(ctx.shardId(), "{\"shard\":" + ctx.shardIndex() + ",\"lo\":" + lo + ",\"hi\":" + hi + ",\"synced\":0}");
      return;
    }

    Integer synced = jdbc.queryForObject("""
        WITH ins AS (
          INSERT INTO demo_sync_target (%s)
          SELECT %s FROM demo_sync_source
          WHERE id BETWEEN ? AND ?
          ON CONFLICT (id) DO NOTHING
          RETURNING id)
        SELECT count(*) FROM ins""".formatted(TARGET_COLS, TARGET_COLS), Integer.class, lo, hi);
    jdbc.update("UPDATE demo_sync_source SET synced_at=now() WHERE id BETWEEN ? AND ?", lo, hi);

    results.put(ctx.shardId(), "{\"shard\":%d,\"lo\":%d,\"hi\":%d,\"synced\":%d}"
        .formatted(ctx.shardIndex(), lo, hi, synced == null ? 0 : synced));
  }

  @Override public String resultPayload(HandlerContext ctx) {
    return results.getOrDefault(ctx.shardId(), "{}");
  }
}