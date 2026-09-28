package dev.scheduler.server.config;

import dev.scheduler.server.service.RuntimeConfigService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

/** 自调度循环基类:每次 tick 完成后再读一次 delay(RuntimeConfig→热更新)排下一拍。
 *  每拍 catch Throwable 兜底不杀线程。destroy() 停排拍。 */
public abstract class ConfigurableLoop implements DisposableBean {
  private static final Logger log = LoggerFactory.getLogger(ConfigurableLoop.class);
  private static final long SUSPEND_HEARTBEAT_MS = 5_000L; // suspend 时保留的长间隔心跳
  private final ScheduledExecutorService pool;
  protected final RuntimeConfigService settings;
  private final String delayKey;
  private final long fallbackMs;
  private volatile boolean running = true;

  protected ConfigurableLoop(RuntimeConfigService settings, String delayKey, long fallbackMs) {
    this.settings = settings;
    this.delayKey = delayKey;
    this.fallbackMs = fallbackMs;
    this.pool = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "loop-" + delayKey);
      t.setDaemon(true);
      return t;
    });
  }

  /** 子类实现:单拍逻辑(原 @Scheduled tick 体);leader 门控由子类自理。 */
  protected abstract void loopOnce();

  /** @Bean 工厂方法在装配后调用,启动第一拍。 */
  public final void start() {
    schedule();
  }

  private void schedule() {
    pool.schedule(this::tick, currentDelay(), TimeUnit.MILLISECONDS);
  }

  private void tick() {
    if (!running) return;
    if (settings.isSuspended()) {
      pool.schedule(this::tick, SUSPEND_HEARTBEAT_MS, TimeUnit.MILLISECONDS);
      return;
    }
    try {
      loopOnce();
    } catch (Throwable t) {
      log.warn("loop {} tick failed; continuing next tick", delayKey, t);
    }
    schedule(); // 每拍完成后再读 delay → 热更新生效
  }

  /** 每拍重读当前 delay(DB 值优先,fallback 兜底)。 */
  private long currentDelay() {
    long v = settings.getLong(delayKey, fallbackMs);
    return v > 0 ? v : fallbackMs;
  }

  @Override public void destroy() {
    running = false;
    pool.shutdownNow();
  }
}