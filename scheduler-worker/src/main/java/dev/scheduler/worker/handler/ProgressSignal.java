package dev.scheduler.worker.handler;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * E3 进度心跳信号:handler 在自己的批处理循环边界周期调用 {@link #touch()} 置位;worker 的续租线程在每次
 * {@code renewLease} 成功后 {@link #consume()} 读并清零,合并 flush 成一次 {@code recordProgress}(至多每续租周期一次)。
 * 纯布尔语义——只表达「刚有过推进」,不携带时间戳/百分比/阶段。
 */
public final class ProgressSignal {
  private final AtomicBoolean progressed = new AtomicBoolean(false);

  /** handler 侧上报:置位「自上次 flush 后有过推进」。任意线程/任意频率调用皆安全(幂等 set)。 */
  public void touch() { progressed.set(true); }

  /** worker 侧采读:读并清零。返回是否存在未 flush 的推进信号。 */
  public boolean consume() { return progressed.getAndSet(false); }
}
