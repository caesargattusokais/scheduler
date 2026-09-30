package dev.scheduler.server.service;

import dev.scheduler.core.AlertEpisode;
import dev.scheduler.core.AuditIntegrity;
import dev.scheduler.persistence.AlertRepository;
import dev.scheduler.persistence.AuditRepository;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.DagRepository.DagRunFailedCount;
import dev.scheduler.persistence.NotificationRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.server.config.RuntimeConfigKeys;
import dev.scheduler.server.service.ExecutionMetrics.ExecutionSlo;
import dev.scheduler.server.service.ExecutionMetrics.TaskMetric;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 告警引擎(五方向·2):每评估拍读热键阈值,跨 8 条规则(全局 + per-task + per-DAG)汇聚信号,
 * 并把每条 {@code alert_episode} 按键推进 PENDING→OPEN→RESOLVED 状态机。
 *
 * 纯类 + 构造器注入(镜像 {@link RuntimeConfigService}/{@link ExecutionMetrics}),不加 @Component;
 * 由 Task 6 作为 @Bean 装配,每拍调 {@link #evaluateOnce()}。
 */
public class AlertEngine {
  /** 采样基数下界:total 不足则阈值类规则跳过(避免噪声)。 */
  private static final int MIN_TOTAL = 3;
  private static final int DAG_FAIL_WINDOW_SECONDS = 3600;
  private static final int WORKER_OFFLINE_WINDOW_SECONDS = 30;

  private final ExecutionMetrics metrics;
  private final ShardRepository shards;
  private final NotificationRepository notifications;
  private final AuditRepository audits;
  private final WorkerRepository workers;
  private final DagRepository dags;
  private final RuntimeConfigService settings;
  private final AlertRepository repo;

  public AlertEngine(ExecutionMetrics metrics, ShardRepository shards,
                     NotificationRepository notifications, AuditRepository audits,
                     WorkerRepository workers, DagRepository dags,
                     RuntimeConfigService settings, AlertRepository repo) {
    this.metrics = metrics;
    this.shards = shards;
    this.notifications = notifications;
    this.audits = audits;
    this.workers = workers;
    this.dags = dags;
    this.settings = settings;
    this.repo = repo;
  }

  /** 一个告警信号:规则归属(rule/target) + 严重级 + 观测值 + 是否超阈(active)。 */
  private record Signal(String rule, String targetType, Long targetId, String severity,
                        String value, boolean active) {}

  /**
   * 单拍评估:剪枝过期 RESOLVED 历史,再逐信号走状态机,最后 reconcile 兜底 —— 对「本拍不再产出信号」的活跃
   * 行(见下)以合成 inactive 信号强制回落,堵死第 3 类栅控发射器(dag-run-failed GROUP BY 缺行、
   * task-failure-rate/failure-rate 的 MIN_TOTAL `continue`)由此缺失信号致 episode 永 OPEN 的恢复漏洞。Task 6
   * AlertLoop 每拍调。
   */
  public void evaluateOnce() {
    int open = clampWindow(settings.getLong(RuntimeConfigKeys.ALERT_OPEN_SAMPLES, 3L));
    int recover = clampWindow(settings.getLong(RuntimeConfigKeys.ALERT_RECOVER_SAMPLES, 3L));
    long keepDays = settings.getLong(RuntimeConfigKeys.ALERT_HISTORY_KEEP_DAYS, 7L);
    if (keepDays > 0) repo.pruneResolved(Instant.now().minus(Duration.ofDays(keepDays)));
    List<Signal> signals = collectSignals();
    Set<String> produced = new HashSet<>();
    for (Signal s : signals) {
      produced.add(keyOf(s));
      handle(s, open, recover);
    }
    reconcile(produced, open, recover);
  }

  /** 对仍在 PENDING/OPEN、但本拍无对应信号的 episode,用其自身 info 合成 inactive 信号走标准 -recover 回落。 */
  private void reconcile(Set<String> produced, int open, int recover) {
    for (AlertEpisode ep : repo.findAllActive()) {
      if (produced.contains(ep.key())) continue; // 该 key 已由常态信号处理,免双计
      handle(new Signal(ep.rule(), ep.targetType(), ep.targetId(), ep.severity(), ep.value(), false),
          open, recover);
    }
  }

  /** sample 窗口开/收 clamp:至少 1、上限 10000,防 Long 溢 int 的 toIntExact 抛异常每拍卡死循环。 */
  private static int clampWindow(long v) {
    return (int) Math.min(Math.max(v, 1L), 10000L);
  }

  /** 组装 8 条规则信号(阈=0/停用即跳过该规则)。 */
  private List<Signal> collectSignals() {
    List<Signal> out = new ArrayList<>();
    ExecutionSlo slo = metrics.snapshots(null);
    Instant now = Instant.now();

    // audit-tamper(全局, critical):取证链完整性。
    if (settings.getFlag(RuntimeConfigKeys.ALERT_AUDIT_TAMPER, true)) {
      AuditIntegrity integrity = audits.integrity();
      String value = integrity.chainedRecords() + "/" + integrity.totalRecords();
      out.add(new Signal("audit-tamper", null, null, "critical", value, !integrity.isVerified()));
    }

    // dlq-depth(全局, high)。
    long dlqThr = settings.getLong(RuntimeConfigKeys.ALERT_DLQ_DEPTH, 1L);
    if (dlqThr > 0) {
      long depth = shards.countDeadLetter();
      out.add(new Signal("dlq-depth", null, null, "high", Long.toString(depth), depth >= dlqThr));
    }

    // delivery-failed(全局, high)。
    long dfThr = settings.getLong(RuntimeConfigKeys.ALERT_DELIVERY_FAILED, 1L);
    if (dfThr > 0) {
      long n = notifications.count(null, "FAILED");
      out.add(new Signal("delivery-failed", null, null, "high", Long.toString(n), n >= dfThr));
    }

    // failure-rate(全局, medium):总=Σ throughput,坏=Σ bad,率=bad/total。
    double frThr = settings.getDouble(RuntimeConfigKeys.ALERT_FAILURE_RATE, 0.05);
    if (frThr > 0) {
      long total = 0, bad = 0;
      for (TaskMetric t : slo.perTask()) {
        total += t.throughput();
        bad += Math.round(t.throughput() * (1 - t.successRate()));
      }
      if (total >= MIN_TOTAL) {
        double rate = (double) bad / total;
        out.add(new Signal("failure-rate", null, null, "medium",
            String.format("%.0f%%", rate * 100), rate > frThr));
      }
    }

    // p95-latency(全局, medium)。
    long p95Thr = settings.getLong(RuntimeConfigKeys.ALERT_P95_LATENCY_MS, 5000L);
    if (p95Thr > 0) {
      long p95 = slo.parentLatencyP95Ms();
      out.add(new Signal("p95-latency", null, null, "medium", Long.toString(p95), p95 > p95Thr));
    }

    // worker-offline(全局, medium):无存活 worker。
    if (settings.getFlag(RuntimeConfigKeys.ALERT_WORKER_OFFLINE, true)) {
      boolean offline = workers.findAllAlive(now.minusSeconds(WORKER_OFFLINE_WINDOW_SECONDS)).isEmpty();
      out.add(new Signal("worker-offline", null, null, "medium", offline ? "全部下线" : "存活", offline));
    }

    // task-failure-rate(per-task, high)。
    double tfrThr = settings.getDouble(RuntimeConfigKeys.ALERT_TASK_FAILURE_RATE, 0.2);
    if (tfrThr > 0) {
      for (TaskMetric t : slo.perTask()) {
        if (t.throughput() < MIN_TOTAL) continue;
        double rate = 1 - t.successRate();
        out.add(new Signal("task-failure-rate", "task", t.taskId(), "high",
            String.format("%.0f%%", rate * 100), rate > tfrThr));
      }
    }

    // dag-run-failed(per-dag, high)。
    long dagThr = settings.getLong(RuntimeConfigKeys.ALERT_DAG_RUN_FAILED, 1L);
    if (dagThr > 0) {
      for (DagRunFailedCount d : dags.perDagFailedRunsSince(now.minusSeconds(DAG_FAIL_WINDOW_SECONDS))) {
        out.add(new Signal("dag-run-failed", "dag", d.dagId(), "high",
            Long.toString(d.count()), d.count() >= dagThr));
      }
    }

    return out;
  }

  /** 状态机迁移:按键把活跃行推 PENDING→OPEN→RESOLVED;PENDING 未确认回落即删。 */
  private void handle(Signal s, int open, int recover) {
    String key = keyOf(s);
    Optional<AlertEpisode> ex = repo.findActiveByKey(key);
    if (ex.isEmpty()) {
      if (s.active()) repo.insert(newPENDING(key, s));
      return;
    }
    AlertEpisode e = ex.get();
    if (s.active()) {
      int next = e.sampleCount() > 0 ? e.sampleCount() + 1 : 1;
      if ("PENDING".equals(e.status()) && next >= open) {
        repo.update(with(key, s, "OPEN", next, e, Instant.now(), s.value(), null, null));
      } else {
        repo.update(with(key, s, e.status(), next, e, e.openedAt(), e.openedValue(), e.resolvedAt(), e.resolvedValue()));
      }
    } else {
      int next = e.sampleCount() < 0 ? e.sampleCount() - 1 : -1;
      if ("OPEN".equals(e.status()) && next <= -recover) {
        repo.update(with(key, s, "RESOLVED", next, e, e.openedAt(), e.openedValue(), Instant.now(), s.value()));
      } else if ("PENDING".equals(e.status()) && next <= -recover) {
        repo.deleteByKey(key);
      } else {
        repo.update(with(key, s, e.status(), next, e, e.openedAt(), e.openedValue(), e.resolvedAt(), e.resolvedValue()));
      }
    }
  }

  /** 按键:全局=裸规则 id;per-task / per-dag 带 targetType:id 后缀。 */
  private String keyOf(Signal s) {
    if (s.targetType() == null) return s.rule();
    return switch (s.targetType()) {
      case "task" -> s.rule() + ":task:" + s.targetId();
      case "dag" -> s.rule() + ":dag:" + s.targetId();
      default -> s.rule();
    };
  }

  /** 新建 PENDING 行(首个 active 拍,sampleCount=1)。 */
  private AlertEpisode newPENDING(String key, Signal s) {
    return new AlertEpisode(0L, key, s.rule(), s.targetType(), s.targetId(), s.severity(),
        "PENDING", 1, s.value(), null, null, null, null);
  }

  /** 迁移:保留既有 id,按下一条信号覆写可变列(openedAt/openedValue/resolvedAt/resolvedValue 由调用方决定)。 */
  private AlertEpisode with(String key, Signal s, String status, int next, AlertEpisode e,
                            Instant openedAt, String openedValue, Instant resolvedAt, String resolvedValue) {
    return new AlertEpisode(e.id(), key, s.rule(), s.targetType(), s.targetId(), s.severity(),
        status, next, s.value(), openedAt, openedValue, resolvedAt, resolvedValue);
  }
}
