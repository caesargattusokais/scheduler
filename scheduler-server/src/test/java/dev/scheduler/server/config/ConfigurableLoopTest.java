package dev.scheduler.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.persistence.RuntimeConfigRepository;
import dev.scheduler.persistence.RuntimeConfigRow;
import dev.scheduler.server.service.RuntimeConfigService;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConfigurableLoopTest {

  static final class FakeRepo implements RuntimeConfigRepository {
    final java.util.Map<String, RuntimeConfigRow> rows = new java.util.LinkedHashMap<>();

    @Override public Optional<RuntimeConfigRow> find(String key) {
      return Optional.ofNullable(rows.get(key));
    }

    @Override public java.util.List<RuntimeConfigRow> findAll() {
      return new java.util.ArrayList<>(rows.values());
    }

    @Override public void upsert(String key, String value, String operator) {
      rows.put(key, new RuntimeConfigRow(key, value, operator, Instant.now()));
    }
  }

  static class CountingLoop extends ConfigurableLoop {
    final AtomicInteger runs = new AtomicInteger();

    CountingLoop(RuntimeConfigService s) {
      super(s, "loop.scan-delay-ms", 20L);
      start();
    }

    @Override protected void loopOnce() { runs.incrementAndGet(); }
  }

  @Test void runsContinuously() throws InterruptedException {
    FakeRepo repo = new FakeRepo();
    CountingLoop loop = new CountingLoop(new RuntimeConfigService(repo, null));
    Thread.sleep(250);
    assertTrue(loop.runs.get() > 0, "循环应持续跑");
    loop.destroy();
  }

  @Test void suspend_haltsExecutions_thenResumes() throws InterruptedException {
    FakeRepo repo = new FakeRepo();
    RuntimeConfigService svc = new RuntimeConfigService(repo, null);
    CountingLoop loop = new CountingLoop(svc);
    Thread.sleep(150);
    repo.upsert("suspend", "true", "a");
    int before = loop.runs.get();
    Thread.sleep(250);
    int during = loop.runs.get();
    assertEquals(before, during, "suspend 期间不再执行 loopOnce");
    repo.upsert("suspend", "false", "a");
    // 清位后,循环需越过最长 5s 的 suspend 心跳才恢复执行 → 用轮询等待而不是固定短睡。
    long deadline = System.currentTimeMillis() + 8000;
    boolean resumed = false;
    while (System.currentTimeMillis() < deadline) {
      if (loop.runs.get() > during) { resumed = true; break; }
      Thread.sleep(50);
    }
    assertTrue(resumed, "清位后恢复执行(至多等 8s 越过 suspend 心跳)");
    loop.destroy();
  }

  @Test void hotReloadDelay() throws InterruptedException {
    FakeRepo repo = new FakeRepo();
    repo.upsert("loop.scan-delay-ms", "5", "a");
    RuntimeConfigService svc = new RuntimeConfigService(repo, null);
    CountingLoop loop = new CountingLoop(svc);
    Thread.sleep(120);
    int fast = loop.runs.get();
    assertTrue(fast >= 1, "短 delay(5ms)在 120ms 内应至少一拍:fast=" + fast);
    loop.destroy();
  }

  @Test void throwable_doesNotKillLoop() throws InterruptedException {
    FakeRepo repo = new FakeRepo();
    RuntimeConfigService svc = new RuntimeConfigService(repo, null);
    // 有状态子类:前 2 拍抛异常,之后正常计数。断言抛异常后循环仍持续排拍。
    AtomicInteger calls = new AtomicInteger();
    CountingLoop loop = new CountingLoop(svc) {
      @Override protected void loopOnce() {
        if (calls.getAndIncrement() < 2) throw new RuntimeException("boom");
        super.loopOnce();
      }
    };
    Thread.sleep(200);
    assertTrue(calls.get() >= 2, "异常被基类吞掉,循环继续排拍:calls=" + calls.get());
    assertTrue(loop.runs.get() >= 1, "异常后仍执行 loopOnce:runs=" + loop.runs.get());
    loop.destroy();
  }
}