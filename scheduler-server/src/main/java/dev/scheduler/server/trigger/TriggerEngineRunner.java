package dev.scheduler.server.trigger;

/** 固定周期驱动触发扫描的薄封装。调度注解由 Task 10 的 Boot 装配负责,此处保持普通 POJO。 */
public class TriggerEngineRunner {
  private final TriggerEngine engine;

  public TriggerEngineRunner(TriggerEngine engine) {
    this.engine = engine;
  }

  /** 每次扫描调用一次。 */
  public void run() {
    engine.scanOnce();
  }
}