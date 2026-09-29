package dev.scheduler.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.scheduler.persistence.RuntimeConfigRepository;
import dev.scheduler.persistence.RuntimeConfigRow;
import dev.scheduler.server.service.RuntimeConfigService;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** 7 个服务器 ConfigurableLoop 的「delay 热键布线」守卫:每个 loop 构造时传给基类的 delayKey 与 fallback 默认值
 *  必须与 {@link RuntimeConfigKeys} 白名单的键及默认一一对应。
 *
 *  <p>动机:这些键/默认值硬编码在各 loop 构造器(AUDIT 的 days 除外),若未来有人把某 loop 误接到另一个 delay
 *  键或写错 fallback,运行时只会在「DB 里写了那个键」才暴露,常态静默。此测试直接读装配现场断言,杜绝换键/漂移。
 *
 *  <p>实现:7 个 loop 的构造器仅存依赖 + {@code super(...)} + start(),不触碰引擎/选主;首拍延迟≥fallback
 *  (≥1s),断言完立即 {@code destroy()} 停排拍 → 引擎/leader 依赖传 {@code null} 绝无 tick 触发、无后台线程泄漏。
 */
class LoopWiringTest {

  /** 空运行时仓库(读恒回落)：验证 loop 用 fallback 而非 DB 值，与装配现场解耦。 */
  private static final RuntimeConfigService EMPTY = new RuntimeConfigService(new RuntimeConfigRepository() {
    @Override public Optional<RuntimeConfigRow> find(String key) { return Optional.empty(); }
    @Override public List<RuntimeConfigRow> findAll() { return List.of(); }
    @Override public void upsert(String key, String value, String operator) {}
  }, null);

  private static String delayKeyOf(ConfigurableLoop loop) {
    try {
      Field f = ConfigurableLoop.class.getDeclaredField("delayKey");
      f.setAccessible(true);
      return (String) f.get(loop);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("read delayKey", e);
    }
  }

  private static long fallbackOf(ConfigurableLoop loop) {
    try {
      Field f = ConfigurableLoop.class.getDeclaredField("fallbackMs");
      f.setAccessible(true);
      return f.getLong(loop);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("read fallbackMs", e);
    }
  }

  /** 断言该 loop 的 delayKey == 白名单键，fallback == 该键对应默认值；随后 destroy 防后台线程泄漏。 */
  private static void assertWiring(ConfigurableLoop loop, String expectedKey) {
    try {
      assertEquals(expectedKey, delayKeyOf(loop), "delayKey 必须钉在白名单键上");
      assertEquals((long) Long.parseLong(RuntimeConfigKeys.DEFAULTS.get(expectedKey)),
          fallbackOf(loop), "fallback 必须与 RuntimeConfigKeys 默认值一致");
    } finally {
      loop.destroy();
    }
  }

  @Test void scanLoop_wiredToScanDelay() {
    assertWiring(new Beans.ScanLoop(EMPTY, null), RuntimeConfigKeys.SCAN_DELAY);
  }

  @Test void reconcileLoop_wiredToReconcileDelay() {
    assertWiring(new Beans.ReconcileLoop(EMPTY, null, null), RuntimeConfigKeys.RECONCILE_DELAY);
  }

  @Test void auditRetentionLoop_wiredToRetentionDelay() {
    assertWiring(new Beans.AuditRetentionLoop(EMPTY, null, null), RuntimeConfigKeys.AUDIT_RETENTION_DELAY);
  }

  @Test void dagLoop_wiredToDagDelay() {
    assertWiring(new Beans.DagLoop(EMPTY, null), RuntimeConfigKeys.DAG_DELAY);
  }

  @Test void eventLoop_wiredToEventDelay() {
    assertWiring(new Beans.EventLoop(EMPTY, null), RuntimeConfigKeys.EVENT_DELAY);
  }

  @Test void notificationLoop_wiredToDispatchDelay() {
    assertWiring(new Beans.NotificationLoop(EMPTY, null, null), RuntimeConfigKeys.NOTIFICATION_DELAY);
  }

  @Test void dlqReplayLoop_wiredToDlqDelay() {
    assertWiring(new Beans.DlqReplayLoop(EMPTY, null, null), RuntimeConfigKeys.DLQ_DELAY);
  }

  @Test void alertLoop_wiredToAlertDelay() {
    assertWiring(new Beans.AlertLoop(EMPTY, null, null), RuntimeConfigKeys.ALERT_DELAY);
  }
}