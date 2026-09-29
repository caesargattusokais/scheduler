package dev.scheduler.server.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dev.scheduler.core.AlertEpisode;
import dev.scheduler.core.AlertEpisodeView;
import dev.scheduler.core.AuditIntegrity;
import dev.scheduler.persistence.AlertRepository;
import dev.scheduler.persistence.AuditRepository;
import dev.scheduler.persistence.DagRepository;
import dev.scheduler.persistence.DagRepository.DagRunFailedCount;
import dev.scheduler.persistence.NotificationRepository;
import dev.scheduler.persistence.RuntimeConfigRepository;
import dev.scheduler.persistence.RuntimeConfigRow;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.server.config.RuntimeConfigKeys;
import dev.scheduler.server.service.ExecutionMetrics.ExecutionSlo;
import dev.scheduler.server.service.ExecutionMetrics.RecentFailures;
import dev.scheduler.server.service.ExecutionMetrics.TaskMetric;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * AlertEngine 状态机核心自足单测。外部依赖用最小 Mockito stub 承接口、内存 RuntimeConfigRepository 走真
 * RuntimeConfigService(getLong/getDouble/getFlag),核心断言全落在 FakeAlertRepo 记录的 upsert/delete 序列与存储
 * 状态,镜像既有 service 单测(如 RuntimeConfigServiceTest)手法。
 */
class AlertEngineTest {

  /** 内存 runtime-config 假仓储:直接 priory 值,再经真 RuntimeConfigService 走 getLong/getDouble/getFlag。 */
  static final class Config implements RuntimeConfigRepository {
    final Map<String, String> rows = new HashMap<>();
    Config put(String key, String value) { rows.put(key, value); return this; }
    public Optional<RuntimeConfigRow> find(String key) {
      return Optional.ofNullable(rows.get(key)).map(v -> new RuntimeConfigRow(key, v, "t", Instant.now()));
    }
    public List<RuntimeConfigRow> findAll() {
      return rows.entrySet().stream()
          .map(e -> new RuntimeConfigRow(e.getKey(), e.getValue(), "t", Instant.now())).toList();
    }
    public void upsert(String key, String value, String operator) { rows.put(key, value); }
    RuntimeConfigService service() { return new RuntimeConfigService(this, null); }
  }

  /** 全规则停用基线(open/recover/keep 可调),各测再按需启用目标规则。 */
  static Config base(int open, int recover, long keep) {
    return new Config()
        .put(RuntimeConfigKeys.ALERT_OPEN_SAMPLES, Integer.toString(open))
        .put(RuntimeConfigKeys.ALERT_RECOVER_SAMPLES, Integer.toString(recover))
        .put(RuntimeConfigKeys.ALERT_HISTORY_KEEP_DAYS, Long.toString(keep))
        .put(RuntimeConfigKeys.ALERT_AUDIT_TAMPER, "0")
        .put(RuntimeConfigKeys.ALERT_DLQ_DEPTH, "0")
        .put(RuntimeConfigKeys.ALERT_DELIVERY_FAILED, "0")
        .put(RuntimeConfigKeys.ALERT_FAILURE_RATE, "0.0")
        .put(RuntimeConfigKeys.ALERT_P95_LATENCY_MS, "0")
        .put(RuntimeConfigKeys.ALERT_WORKER_OFFLINE, "0")
        .put(RuntimeConfigKeys.ALERT_TASK_FAILURE_RATE, "0.0")
        .put(RuntimeConfigKeys.ALERT_DAG_RUN_FAILED, "0");
  }

  /** 内存 AlertRepository:记录 insert/update/delete/prune 序列,维护活跃/已解决存储。 */
  static final class FakeAlertRepo implements AlertRepository {
    final Map<String, AlertEpisode> active = new LinkedHashMap<>();
    final Map<String, AlertEpisode> resolved = new LinkedHashMap<>();
    final List<String> ops = new ArrayList<>();
    long nextId = 1;

    public Optional<AlertEpisode> findActiveByKey(String key) { return Optional.ofNullable(active.get(key)); }

    public AlertEpisode insert(AlertEpisode e) {
      AlertEpisode withId = new AlertEpisode(nextId++, e.key(), e.rule(), e.targetType(), e.targetId(),
          e.severity(), e.status(), e.sampleCount(), e.value(), e.openedAt(), e.openedValue(),
          e.resolvedAt(), e.resolvedValue());
      active.put(e.key(), withId);
      ops.add("insert:" + e.key() + ":" + e.status() + ":" + e.sampleCount());
      return withId;
    }

    public void update(AlertEpisode e) {
      if ("RESOLVED".equals(e.status())) {
        active.remove(e.key());
        resolved.put(e.key(), e);
      } else {
        active.put(e.key(), e);
      }
      ops.add("update:" + e.key() + ":" + e.status() + ":" + e.sampleCount());
    }

    public void deleteByKey(String key) {
      active.remove(key);
      ops.add("delete:" + key);
    }

    public long pruneResolved(Instant cutoff) {
      long n = resolved.values().stream()
          .filter(r -> r.resolvedAt() != null && r.resolvedAt().isBefore(cutoff)).count();
      resolved.entrySet().removeIf(en -> en.getValue().resolvedAt() != null
          && en.getValue().resolvedAt().isBefore(cutoff));
      ops.add("prune:" + n);
      return n;
    }

    public List<AlertEpisodeView> findActive() { return Collections.emptyList(); }
    public List<AlertEpisodeView> findResolved(int limit, int offset) { return Collections.emptyList(); }
    public long countResolved() { return resolved.size(); }
  }

  /** 单测接线:Mocks 承重接口 + 内存 alert repo;按目标规则喂 SLO 信号。 */
  static final class Harness {
    final ExecutionMetrics metrics = mock(ExecutionMetrics.class);
    final ShardRepository shards = mock(ShardRepository.class);
    final NotificationRepository notifs = mock(NotificationRepository.class);
    final AuditRepository audits = mock(AuditRepository.class);
    final WorkerRepository workers = mock(WorkerRepository.class);
    final DagRepository dags = mock(DagRepository.class);
    final FakeAlertRepo alerts = new FakeAlertRepo();
    final RuntimeConfigService settings;
    final AlertEngine engine;

    Harness(Config cfg) {
      this.settings = cfg.service();
      this.engine = new AlertEngine(metrics, shards, notifs, audits, workers, dags, settings, alerts);
    }
    Harness slo(ExecutionSlo s) { when(metrics.snapshots(any())).thenReturn(s); return this; }
    AlertEpisode episode(String key) { return alerts.findActiveByKey(key).orElseThrow(); }
  }

  private static ExecutionSlo slo(long p95, List<TaskMetric> perTask) {
    return new ExecutionSlo(3600, 0, p95, 0, perTask, new RecentFailures(0, 0, 0));
  }

  // ---- 状态机:PENDING→OPEN→RESOLVED 与未确认回落 ----

  @Test void singleActive_thenBelowNotRecover_staysPendingNeverOpens() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_P95_LATENCY_MS, "100"))
        .slo(slo(200, List.of()));
    h.engine.evaluateOnce(); // active → PENDING s=1
    h.slo(slo(50, List.of()));
    h.engine.evaluateOnce(); // below, -1 未达 -recover
    AlertEpisode e = h.episode("p95-latency");
    assertEquals("PENDING", e.status());
    assertEquals(-1, e.sampleCount());
    assertTrue(h.alerts.ops.stream().noneMatch(op -> op.contains(":OPEN:")),
        "PENDING 前不该 OPEN");
  }

  @Test void continuousActive_reachesOpen_withTimestamps() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_P95_LATENCY_MS, "100"))
        .slo(slo(200, List.of()));
    h.engine.evaluateOnce(); // s=1 PENDING
    h.engine.evaluateOnce(); // s=2 PENDING
    h.engine.evaluateOnce(); // s=3 OPEN
    AlertEpisode e = h.episode("p95-latency");
    assertEquals("OPEN", e.status());
    assertEquals(3, e.sampleCount());
    assertEquals("200", e.openedValue());
    assertNotNull(e.openedAt());
    assertNull(e.resolvedAt());
    assertTrue(h.alerts.ops.get(h.alerts.ops.size() - 1).startsWith("update:p95-latency:OPEN:3"));
  }

  @Test void open_thenContinuousBelow_reachesResolved() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_P95_LATENCY_MS, "100"))
        .slo(slo(200, List.of()));
    h.engine.evaluateOnce();
    h.engine.evaluateOnce();
    h.engine.evaluateOnce(); // OPEN
    h.slo(slo(50, List.of()));
    h.engine.evaluateOnce(); // OPEN s=-1
    h.engine.evaluateOnce(); // OPEN s=-2
    h.engine.evaluateOnce(); // OPEN s=-3 → RESOLVED
    assertTrue(h.alerts.findActiveByKey("p95-latency").isEmpty(), "已解决行应离开活跃集");
    AlertEpisode r = h.alerts.resolved.get("p95-latency");
    assertNotNull(r);
    assertEquals("RESOLVED", r.status());
    assertEquals(-3, r.sampleCount());
    assertEquals("200", r.openedValue());
    assertEquals("50", r.resolvedValue());
    assertNotNull(r.resolvedAt());
  }

  @Test void pending_unconfirmedContinuousBelow_deletes() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_P95_LATENCY_MS, "100"))
        .slo(slo(200, List.of()));
    h.engine.evaluateOnce(); // PENDING s=1
    h.slo(slo(50, List.of()));
    h.engine.evaluateOnce(); // -1
    h.engine.evaluateOnce(); // -2
    h.engine.evaluateOnce(); // -3 → delete,无 RESOLVED
    assertTrue(h.alerts.findActiveByKey("p95-latency").isEmpty());
    assertTrue(h.alerts.resolved.containsKey("p95-latency") == false, "未确认回落不产生历史");
    assertTrue(h.alerts.ops.contains("delete:p95-latency"));
  }

  @Test void thresholdZero_disablesRule_noSignal() {
    Harness h = new Harness(base(3, 3, 0));
    h.slo(slo(9999, List.of(new TaskMetric(1, "a", 10, 0.1))));
    h.engine.evaluateOnce();
    assertTrue(h.alerts.ops.isEmpty(), "阈=0 规则不产出信号,不应有任何 upsert");
    assertTrue(h.alerts.active.isEmpty());
  }

  // ---- per-task / per-dag 独立键 ----

  @Test void perTask_twoOverTwoKeys_underNotEmitted() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_TASK_FAILURE_RATE, "0.2")).slo(
        slo(0, List.of(
            new TaskMetric(1, "a", 10, 0.5),  // rate 0.5 > 0.2 → active
            new TaskMetric(2, "b", 10, 0.1),  // rate 0.9 > 0.2 → active
            new TaskMetric(3, "c", 10, 0.95)  // rate 0.05 ≤ 0.2 → inactive,无键
        )));
    h.engine.evaluateOnce();
    assertEquals(2, h.alerts.active.size(), "两任务超阈 → 两独立键");
    assertTrue(h.alerts.findActiveByKey("task-failure-rate:task:1").isPresent());
    assertTrue(h.alerts.findActiveByKey("task-failure-rate:task:2").isPresent());
    assertTrue(h.alerts.findActiveByKey("task-failure-rate:task:3").isEmpty());
  }

  @Test void perTask_oneOverOneUnder_onlyOneKey() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_TASK_FAILURE_RATE, "0.2")).slo(
        slo(0, List.of(
            new TaskMetric(1, "a", 10, 0.5),  // active
            new TaskMetric(2, "b", 10, 0.9)   // rate 0.1 ≤ 0.2 → inactive
        )));
    h.engine.evaluateOnce();
    assertEquals(1, h.alerts.active.size());
    assertTrue(h.alerts.findActiveByKey("task-failure-rate:task:1").isPresent());
  }

  @Test void perDag_twoOver_twoIndependentKeys() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_DAG_RUN_FAILED, "1"));
    when(h.dags.perDagFailedRunsSince(any())).thenReturn(List.of(
        new DagRunFailedCount(10, 2), new DagRunFailedCount(11, 1)));
    h.engine.evaluateOnce();
    assertEquals(2, h.alerts.active.size(), "两 DAG 超阈 → 两独立键");
    assertTrue(h.alerts.findActiveByKey("dag-run-failed:dag:10").isPresent());
    assertTrue(h.alerts.findActiveByKey("dag-run-failed:dag:11").isPresent());
  }

  @Test void perDag_oneOverOneUnder_onlyOneKey() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_DAG_RUN_FAILED, "2"));
    when(h.dags.perDagFailedRunsSince(any())).thenReturn(List.of(
        new DagRunFailedCount(10, 2),   // 2 ≥ 2 → active
        new DagRunFailedCount(11, 1)    // 1 < 2 → inactive,无键
    ));
    h.engine.evaluateOnce();
    assertEquals(1, h.alerts.active.size());
    assertTrue(h.alerts.findActiveByKey("dag-run-failed:dag:10").isPresent());
  }

  // ---- 剪枝与额外规则 ----

  @Test void evaluateOnce_prunesResolvedOlderThanKeepDays() {
    Instant now = Instant.now();
    Harness h = new Harness(base(3, 3, 7)); // keep=7,全规则停用 → 仅走剪枝
    h.alerts.resolved.put("old", new AlertEpisode(0, "old", "p95-latency", null, null, "medium",
        "RESOLVED", -3, "0", null, null, now.minusSeconds(8L * 86400), "x"));
    h.alerts.resolved.put("fresh", new AlertEpisode(0, "fresh", "p95-latency", null, null, "medium",
        "RESOLVED", -3, "0", null, null, now, "x"));
    h.engine.evaluateOnce();
    assertEquals(1, h.alerts.resolved.size(), "keep-days 前的已解决行被剪");
    assertFalse(h.alerts.resolved.containsKey("old"), "过期行应被删");
    assertTrue(h.alerts.resolved.containsKey("fresh"), "新鲜行保留");
    assertTrue(h.alerts.ops.contains("prune:1"));
  }

  @Test void auditTamper_tamperedChain_admin() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_AUDIT_TAMPER, "1"));
    when(h.audits.integrity()).thenReturn(new AuditIntegrity(5, 3, 42L));
    h.engine.evaluateOnce();
    AlertEpisode e = h.alerts.findActiveByKey("audit-tamper").orElseThrow();
    assertEquals("critical", e.severity());
    assertEquals("PENDING", e.status());
    assertEquals(1, e.sampleCount());
    assertEquals("3/5", e.value());
  }

  @Test void auditTamper_cleanChain_inactive_noSignal() {
    Harness h = new Harness(base(3, 3, 0).put(RuntimeConfigKeys.ALERT_AUDIT_TAMPER, "1"));
    when(h.audits.integrity()).thenReturn(new AuditIntegrity(5, 5, null));
    h.engine.evaluateOnce();
    assertTrue(h.alerts.findActiveByKey("audit-tamper").isEmpty(), "链无误不产生活跃行");
    assertTrue(h.alerts.ops.isEmpty(), "未确认即无再次 upsert");
  }
}