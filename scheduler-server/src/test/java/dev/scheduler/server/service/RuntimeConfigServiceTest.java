package dev.scheduler.server.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import dev.scheduler.persistence.RuntimeConfigRepository;
import dev.scheduler.persistence.RuntimeConfigRow;
import dev.scheduler.server.config.RuntimeConfigKeys;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RuntimeConfigServiceTest {

  /** 内存 fake repo:find/findAll/upsert 交互即验证。 */
  static final class FakeRepo implements RuntimeConfigRepository {
    final Map<String, RuntimeConfigRow> rows = new LinkedHashMap<>();
    public Optional<RuntimeConfigRow> find(String key) { return Optional.ofNullable(rows.get(key)); }
    public List<RuntimeConfigRow> findAll() { return new ArrayList<>(rows.values()); }
    public void upsert(String key, String value, String operator) {
      rows.put(key, new RuntimeConfigRow(key, value, operator, Instant.now()));
    }
  }

  /** 空审计:set_string 用不着的场合传 null 即可;set_valid 用 Mockito mock 断言 audit 调用。 */
  @Test void getLong_fallsBackWhenMissing() {
    assertEquals(5000L, new RuntimeConfigService(new FakeRepo(), null).getLong("loop.scan-delay-ms", 5000L));
  }

  @Test void getLong_usesDbValue() {
    FakeRepo repo = new FakeRepo();
    repo.upsert("loop.scan-delay-ms", "2500", "a");
    assertEquals(2500L, new RuntimeConfigService(repo, null).getLong("loop.scan-delay-ms", 5000L));
  }

  @Test void getLong_invalidValue_fallsBack() {
    FakeRepo repo = new FakeRepo();
    repo.upsert("loop.scan-delay-ms", "abc", "a");
    assertEquals(5000L, new RuntimeConfigService(repo, null).getLong("loop.scan-delay-ms", 5000L));
  }

  @Test void isSuspended_defaultFalse() {
    assertFalse(new RuntimeConfigService(new FakeRepo(), null).isSuspended());
  }

  @Test void list_allWhitelistKeysShowDefaultWhenUnset() {
    RuntimeConfigService svc = new RuntimeConfigService(new FakeRepo(), null);
    List<RuntimeConfigService.RuntimeConfigEntry> all = svc.list();
    assertEquals(RuntimeConfigKeys.DEFAULTS.size(), all.size(), "白名单全列出");
    assertTrue(all.stream().allMatch(e -> e.source().equals("default")));
  }

  @Test void set_unknownKey_rejects() {
    assertThrows(IllegalArgumentException.class,
        () -> new RuntimeConfigService(new FakeRepo(), null).set("nope", "1", "alice"));
  }

  @Test void set_valid_persistsAndAudits() {
    FakeRepo repo = new FakeRepo();
    AuditRecorder auditor = mock(AuditRecorder.class);
    RuntimeConfigService svc = new RuntimeConfigService(repo, auditor);
    svc.set("loop.scan-delay-ms", "9999", "alice");
    assertEquals("9999", repo.find("loop.scan-delay-ms").orElseThrow().value());
    verify(auditor).record(eq("alice"), eq("runtime-config.update"),
        eq(dev.scheduler.core.TargetType.NONE), eq(0L), any());
  }

  @Test void set_invalidValue_rejects() {
    assertThrows(IllegalArgumentException.class,
        () -> new RuntimeConfigService(new FakeRepo(), null).set("loop.scan-delay-ms", "abc", "alice"));
  }

  // ---- Task 4: scheduler.alert.* hot keys + getDouble ----

  @Test void alertKeys_areKnown() {
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_DELAY));
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_OPEN_SAMPLES));
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_RECOVER_SAMPLES));
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_HISTORY_KEEP_DAYS));
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_AUDIT_TAMPER));
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_DLQ_DEPTH));
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_DELIVERY_FAILED));
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_FAILURE_RATE));
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_P95_LATENCY_MS));
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_WORKER_OFFLINE));
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_TASK_FAILURE_RATE));
    assertTrue(RuntimeConfigKeys.isKnown(RuntimeConfigKeys.ALERT_DAG_RUN_FAILED));
  }

  @Test void alertKeys_defaultOf() {
    assertEquals("0.05", RuntimeConfigKeys.defaultOf(RuntimeConfigKeys.ALERT_FAILURE_RATE));
    assertEquals("5000", RuntimeConfigKeys.defaultOf(RuntimeConfigKeys.ALERT_P95_LATENCY_MS));
    assertEquals("3", RuntimeConfigKeys.defaultOf(RuntimeConfigKeys.ALERT_OPEN_SAMPLES));
    assertEquals("7", RuntimeConfigKeys.defaultOf(RuntimeConfigKeys.ALERT_HISTORY_KEEP_DAYS));
  }

  @Test void getDouble_usesDbValue() {
    FakeRepo repo = new FakeRepo();
    repo.upsert(RuntimeConfigKeys.ALERT_FAILURE_RATE, "0.05", "a");
    assertEquals(0.05,
        new RuntimeConfigService(repo, null).getDouble(RuntimeConfigKeys.ALERT_FAILURE_RATE, 0.1));
  }

  @Test void getDouble_invalidValue_fallsBack() {
    FakeRepo repo = new FakeRepo();
    repo.upsert(RuntimeConfigKeys.ALERT_FAILURE_RATE, "abc", "a");
    assertEquals(0.1,
        new RuntimeConfigService(repo, null).getDouble(RuntimeConfigKeys.ALERT_FAILURE_RATE, 0.1));
  }

  @Test void getDouble_missing_fallsBack() {
    assertEquals(0.1,
        new RuntimeConfigService(new FakeRepo(), null).getDouble(RuntimeConfigKeys.ALERT_FAILURE_RATE, 0.1));
  }

  @Test void set_alertRate_valid_persists() {
    FakeRepo repo = new FakeRepo();
    AuditRecorder auditor = mock(AuditRecorder.class);
    new RuntimeConfigService(repo, auditor).set(RuntimeConfigKeys.ALERT_FAILURE_RATE, "0.2", "alice");
    assertEquals("0.2", repo.find(RuntimeConfigKeys.ALERT_FAILURE_RATE).orElseThrow().value());
  }

  @Test void set_alertRate_invalidValue_rejects() {
    assertThrows(IllegalArgumentException.class,
        () -> new RuntimeConfigService(new FakeRepo(), null)
            .set(RuntimeConfigKeys.ALERT_FAILURE_RATE, "abc", "alice"));
  }

  @Test void set_alertIntegerKey_invalidValue_rejects() {
    assertThrows(IllegalArgumentException.class,
        () -> new RuntimeConfigService(new FakeRepo(), null)
            .set(RuntimeConfigKeys.ALERT_OPEN_SAMPLES, "abc", "alice"));
  }

  @Test void set_alertIntegerKey_valid_persists() {
    FakeRepo repo = new FakeRepo();
    AuditRecorder auditor = mock(AuditRecorder.class);
    new RuntimeConfigService(repo, auditor).set(RuntimeConfigKeys.ALERT_OPEN_SAMPLES, "3", "alice");
    assertEquals("3", repo.find(RuntimeConfigKeys.ALERT_OPEN_SAMPLES).orElseThrow().value());
  }
}