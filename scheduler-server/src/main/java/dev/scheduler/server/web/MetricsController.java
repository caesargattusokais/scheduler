package dev.scheduler.server.web;

import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 执行 SLO 指标深化(4-2):DB 快照聚合,补 /actuator/prometheus 流式 gauge 没有的维度。
 * 全部以最近 windowSeconds(默认 1h)内的终态执行为窗口:
 * <ul>
 *   <li>父延迟 p50/p95(execution.finished−started,兼容 Percentile 空 → 0);</li>
 *   <li>分片平均时长(execution_shard finished−started);</li>
 *   <li>per-task 吞吐 + 成功率(近窗完成+失败行);</li>
 *   <li>近窗失败细分:failed(父 FAILED)/ deadLettered(分片坏信标)/ timedOut(分片 outcome detail=runtime timeout)。</li>
 * </ul> */
@RestController
@RequestMapping("/api/v1/metrics/executions")
public class MetricsController {
  private final JdbcTemplate jdbc;

  public MetricsController(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /** 执行 SLO 快照(读,开放):windowSeconds 为最近分析窗口(秒,默认 3600)。 */
  @GetMapping
  public ExecutionSlo snapshots(@RequestParam(required = false) Integer windowSeconds) {
    int window = (windowSeconds == null || windowSeconds < 1) ? 3600 : windowSeconds;
    Double p50 = parentPct(0.50, window);
    Double p95 = parentPct(0.95, window);
    Double shardAvg = jdbc.queryForObject(
        "SELECT avg(extract(epoch FROM (finished_at - started_at)) * 1000.0)"
            + " FROM execution_shard WHERE finished_at IS NOT NULL AND started_at IS NOT NULL"
            + " AND finished_at >= now() - (? * interval '1 second')",
        Double.class, window);
    long failed = jdbc.queryForObject(
        "SELECT count(*) FROM execution WHERE status='FAILED'"
            + " AND finished_at >= now() - (? * interval '1 second')", Long.class, window);
    long deadLettered = jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard WHERE dead_letter"
            + " AND finished_at >= now() - (? * interval '1 second')", Long.class, window);
    long timedOut = jdbc.queryForObject(
        "SELECT count(*) FROM execution_shard_outcome WHERE status='FAILED'"
            + " AND detail='runtime timeout' AND created_at >= now() - (? * interval '1 second')",
        Long.class, window);
    return new ExecutionSlo(
        window,
        p50 == null ? 0 : Math.round(p50),
        p95 == null ? 0 : Math.round(p95),
        shardAvg == null ? 0 : Math.round(shardAvg),
        perTask(window),
        new RecentFailures(failed, deadLettered, timedOut));
  }

  private Double parentPct(double fraction, int window) {
    return jdbc.queryForObject(
        "SELECT percentile_cont(" + fraction + ") WITHIN GROUP"
            + " (ORDER BY extract(epoch FROM (finished_at - started_at)) * 1000.0)"
            + " FROM execution WHERE finished_at IS NOT NULL AND started_at IS NOT NULL"
            + " AND finished_at >= now() - (? * interval '1 second')",
        Double.class, window);
  }

  /** per-task:近窗完成 + 失败终态行,GROUP BY 任务 → 吞吐(完成+失败)与成功率(完成/(完成+失败),无样本 → 0)。 */
  private List<TaskMetric> perTask(int window) {
    return jdbc.query(
        "SELECT t.id, t.name,"
            + " count(*) FILTER (WHERE e.status IN ('SUCCESS','CANCELED')) AS ok,"
            + " count(*) FILTER (WHERE e.status='FAILED') AS bad"
            + " FROM execution e JOIN app_task t ON t.id = e.task_id"
            + " WHERE e.finished_at >= now() - (? * interval '1 second')"
            + " GROUP BY t.id, t.name ORDER BY t.id",
        (rs, row) -> {
          long ok = rs.getLong("ok");
          long bad = rs.getLong("bad");
          long throughput = ok + bad;
          double rate = throughput == 0 ? 0.0 : (double) ok / (double) throughput;
          return new TaskMetric(rs.getLong("id"), rs.getString("name"), throughput, rate);
        }, window);
  }

  public record ExecutionSlo(
      int windowSeconds,
      long parentLatencyP50Ms,
      long parentLatencyP95Ms,
      long shardAvgDurationMs,
      List<TaskMetric> perTask,
      RecentFailures recentFailures) {}

  public record TaskMetric(long taskId, String taskName, long throughput, double successRate) {}

  public record RecentFailures(long failed, long deadLettered, long timedOut) {}
}