package dev.scheduler.server.reconcile;

import dev.scheduler.persistence.ReconcileDecision;
import dev.scheduler.persistence.ReconcileDecisionRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.function.Supplier;

/** 对账器可观测接缝:决策落库(record)+ scan 计时/失败计数(timedScan)。单一仓存 + MeterRegistry。 */
public class ReconcileTelemetry {
  private final ReconcileDecisionRepository decisions;
  private final Timer scanDuration;
  private final Counter scanFailures;

  public ReconcileTelemetry(ReconcileDecisionRepository decisions, MeterRegistry registry) {
    this.decisions = decisions;
    this.scanDuration = Timer.builder("scheduler_reconcile_scan_duration")
        .description("reconciler scanOnce() duration")
        .register(registry);
    this.scanFailures = Counter.builder("scheduler_reconcile_scan_failures_total")
        .description("reconciler scanOnce() failures")
        .register(registry);
  }

  /** 追加决策原子。 */
  public void record(ReconcileDecision d) { decisions.record(d); }

  /** 计时包裹 scan 体:异常自增失败计数后重抛,保留 ConfigurableLoop「兜底不杀线程」语义。 */
  public <T> T timedScan(Supplier<T> scan) {
    Timer.Sample sample = Timer.start();
    try {
      return scan.get();
    } catch (RuntimeException e) {
      scanFailures.increment();
      throw e;
    } finally {
      sample.stop(scanDuration);
    }
  }
}