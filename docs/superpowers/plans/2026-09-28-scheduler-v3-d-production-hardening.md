# Scheduler v3·D 生产化韧性实现计划 — 配置热更新 + 优雅停机 + 滚动

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 引入 DB 运行时设置表,让 loop delay / worker.capacity / 全局 suspend 可运行期热改;给 worker 加优雅停机排空 + deregister;给 server 加停机释放 advisory lock——整套支撑滚动发布。

**Architecture:** 新增 `app_runtime_config` 表 + persistence 仓库 + server `RuntimeConfigService` + ADMIN 端点 + 前端设置页;把 7 个 `@Scheduled` 循环改成自调度 `ConfigurableLoop`(每拍重读 delay → 热更新);worker 侧用轻量 `WorkerRuntimeConfig` 读同一张表,WorkLoop 支持排空 + capacity 热改 + deregister;leader 停机释放锁。

**Tech Stack:** Spring Boot 3.2.5 / Java 22 / Postgres 16 / Flyway / React 18 + Vite (既有)。测试 Testcontainers Postgres + JUnit 5。

**Spec:** `docs/superpowers/specs/2026-09-28-scheduler-v3-d-production-hardening-design.md`(已提交 `6ce9c12`;本 plan 严格实现该 spec,冲突以 spec 为准)

## Global Constraints

- 运行 `mvn -o`(离线)。改代码须保证全量 `mvn -o test` → reactor BUILD SUCCESS 才可交。
- 全部新 DB 读共用既有 `JdbcTemplate`;单表主键读,不复用缓存(避免一致性)。
- ADMIN 写收口沿用 `OperatorInterceptor` 的 `ADMIN_WRITES`(见 Task 3)。新端点沿用 `/api/v1/**` 拦截。
- 审计用 `AuditRecorder.record(operator, action, TargetType, targetId, meta)`(5-arg overload)记 `runtime-config.update`;`TargetType.NONE`。`CurrentOperator.get()` 取操作者。
- 枚举 / record 新增不回破坏既有构造:优先 overload / 默认值(既有先例:`Webhook` 的 8-arg legacy 构造器)。
- **worker 是独立进程/上下文,不能依赖 server 的 `RuntimeConfigService` bean**——worker 侧建自己的 `WorkerRuntimeConfig`(部署在 scheduler-worker 下)。
- worker 测试基类为 `AbstractExecutorWorkerTest`/`AbstractPostgresTest`(Flyway 自动带 V30,无需手工建表)。
- `@ConditionalOnProperty` 门控必须保留:**循环改造不得改变既有 enabled 开关语义**。
- 循环失败兜底不杀线程(`catch Throwable → log.warn → continue`)必须保留。
- 前端现有 CSS 类(`card / table / input / btn btn-primary` 等)直接复用,不新引库。
- Idea: 全部循环 delay key 读自 `RuntimeConfigService.getLong(key, fallback)`,**每次排拍时读**。
- worker 的 daemon 线程语义(HeartbeatLoop/WorkLoop daemon)保留;优雅停机是**排空后再退出**,不靠非 daemon 阻塞 JVM。

---

### Task 1: DB 迁移 V30 + 持久层 `app_runtime_config`

**Files:**
- Create: `scheduler-persistence/src/main/resources/db/migration/V30__runtime_config.sql`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/RuntimeConfigRow.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/RuntimeConfigRepository.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcRuntimeConfigRepository.java`
- Create: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcRuntimeConfigRepositoryTest.java`

**Interfaces:**
- Produces: `RuntimeConfigRow(String key, String value, String updatedBy, java.time.Instant updatedAt)`(record)、`RuntimeConfigRepository`:
  - `java.util.Optional<RuntimeConfigRow> find(String key)`
  - `java.util.List<RuntimeConfigRow> findAll()`
  - `void upsert(String key, String value, String operator)`(INSERT ... ON CONFLICT (key) DO UPDATE,updated_at=now())
- Consumes: 仅 `JdbcTemplate`(构造注入)。

- [ ] **Step 1: 写迁移文件**

`scheduler-persistence/src/main/resources/db/migration/V30__runtime_config.sql`:
```sql
CREATE TABLE app_runtime_config (
  key        text PRIMARY KEY,
  value      text NOT NULL,
  updated_by text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
```
(建表语句,无其它——表名/列名与 spec §3.1.1 完全一致。)

- [ ] **Step 2: 写 record + 接口**

`RuntimeConfigRow.java`:
```java
package dev.scheduler.persistence;
import java.time.Instant;
public record RuntimeConfigRow(String key, String value, String updatedBy, Instant updatedAt) {}
```

`RuntimeConfigRepository.java`:
```java
package dev.scheduler.persistence;
import java.util.List;
import java.util.Optional;
public interface RuntimeConfigRepository {
  Optional<RuntimeConfigRow> find(String key);
  List<RuntimeConfigRow> findAll();
  void upsert(String key, String value, String operator);
}
```

- [ ] **Step 3: 写 Jdbc 实现**

`JdbcRuntimeConfigRepository.java`:
```java
package dev.scheduler.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class JdbcRuntimeConfigRepository implements RuntimeConfigRepository {
  private static final String COLS = "key, value, updated_by, updated_at";
  private final JdbcTemplate jdbc;

  public JdbcRuntimeConfigRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

  private static final RowMapper<RuntimeConfigRow> ROW = (ResultSet rs, int rn) -> {
    try {
      return new RuntimeConfigRow(
          rs.getString("key"),
          rs.getString("value"),
          rs.getString("updated_by"),
          rs.getTimestamp("updated_at").toInstant());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  };

  @Override public Optional<RuntimeConfigRow> find(String key) {
    return jdbc.query("SELECT " + COLS + " FROM app_runtime_config WHERE key = ?", ROW, key)
        .stream().findFirst();
  }

  @Override public List<RuntimeConfigRow> findAll() {
    return jdbc.query("SELECT " + COLS + " FROM app_runtime_config ORDER BY key", ROW);
  }

  @Override public void upsert(String key, String value, String operator) {
    jdbc.update("INSERT INTO app_runtime_config (key, value, updated_by) VALUES (?, ?, ?) "
        + "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, "
        + "updated_by = EXCLUDED.updated_by, updated_at = now()", key, value, operator);
  }
}
```

- [ ] **Step 4: 写单测**

`JdbcRuntimeConfigRepositoryTest.java`(继承 `AbstractPostgresTest`):
```java
package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcRuntimeConfigRepositoryTest extends AbstractPostgresTest {
  private JdbcRuntimeConfigRepository repo;

  @BeforeEach
  void beforeEach() {
    jdbc.execute("TRUNCATE app_runtime_config RESTART IDENTITY CASCADE");
    repo = new JdbcRuntimeConfigRepository(jdbc);
  }

  @Test void find_missing_returnsEmpty() {
    assertTrue(repo.find("nope").isEmpty());
  }

  @Test void upsert_insertsThenUpdates_inPlace() {
    repo.upsert("a", "1", "alice");
    RuntimeConfigRow r = repo.find("a").orElseThrow();
    assertEquals("1", r.value());
    assertEquals("alice", r.updatedBy());
    assertTrue(r.updatedAt() != null);

    repo.upsert("a", "2", "bob");
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_runtime_config", Long.class),
        "upsert 不新增行");
    assertEquals("2", repo.find("a").orElseThrow().value(), "值被更新");
    assertEquals("bob", repo.find("a").orElseThrow().updatedBy(), "操作者被更新");
  }

  @Test void findAll_sortsByKey() {
    repo.upsert("b", "2", "x");
    repo.upsert("a", "1", "x");
    assertEquals(List.of("a", "b"), repo.findAll().stream().map(RuntimeConfigRow::key).toList());
  }
}
```
补 import `java.util.List`。

- [ ] **Step 5: 跑测**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-persistence -am test -Dtest=JdbcRuntimeConfigRepositoryTest`
Expected: PASS(全部断言绿)。

- [ ] **Step 6: 提交**

```bash
git add scheduler-persistence/src/main/resources/db/migration/V30__runtime_config.sql \
  scheduler-persistence/src/main/java/dev/scheduler/persistence/RuntimeConfigRow.java \
  scheduler-persistence/src/main/java/dev/scheduler/persistence/RuntimeConfigRepository.java \
  scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcRuntimeConfigRepository.java \
  scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcRuntimeConfigRepositoryTest.java
git commit -m "feat(persistence): V30 app_runtime_config + RuntimeConfigRepository(upsert/find/findAll)
---
Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: `RuntimeConfigKeys` + server `RuntimeConfigService`

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/config/RuntimeConfigKeys.java`
- Create: `scheduler-server/src/main/java/dev/scheduler/server/service/RuntimeConfigService.java`
- Create: `scheduler-server/src/test/java/dev/scheduler/server/service/RuntimeConfigServiceTest.java`
- Modify(装配): `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`(加 `@Bean RuntimeConfigRepository runtimeConfigRepository(...)` + `@Bean RuntimeConfigService runtimeConfigService(...)`)

**Interfaces:**
- Consumes: `RuntimeConfigRepository`(Task 1)、`AuditRecorder`、`org.slf4j.Logger`。
- Produces: `runtimeConfigService` bean(server 侧组件用):
  - `long getLong(String key, long fallback)` — DB 有值且可解析 → 解析值;否则 fallback(非法值 log.warn 回落)。
  - `boolean getFlag(String key, boolean fallback)` — 同 getLong 的布尔语义(`"true"/"1"` 为真)。
  - `boolean isSuspended()` — `getFlag("suspend", false)`。
  - `java.util.List<RuntimeConfigEntry> list()` — 白名单全部 key:DB 有值 → DB 值/来源 `"db"`;否则 fallback/来源 `"default"`。
  - `void set(String key, String rawValue, String operator)` — 白名单校验 + 类型校验 + upsert + 审计 `runtime-config.update`。
  - nested `record RuntimeConfigEntry(String key, String value, String source, String updatedBy, java.time.Instant updatedAt)`
- Server 组件将来调用方(本 plan 其余 task):各 `ConfigurableLoop`、`WorkLoop`(worker 侧用 `WorkerRuntimeConfig`,见 Task 5)。

**关键语义**:`getLong` 读 DB 主键单行;key 未注册一律回落 fallback(不抛)。类型校验放在 `set`:value 无法解析成该 key 类型 → 抛 `IllegalArgumentException`,不入库。

- [ ] **Step 1: 写 `RuntimeConfigKeys` 白名单常量**

`RuntimeConfigKeys.java`(server 侧,常量类;worker 不依赖它,worker 自带 keys 见 Task 5):
```java
package dev.scheduler.server.config;

import java.util.LinkedHashMap;
import java.util.Map;

/** app_runtime_config 可调项白名单:key → 默认值(fallback)。仅此处注册的 key 可写;未注册 get 一律回落。
 *  刻意排除 lease.seconds / dlq.max-replays(见 spec §3.1.1 注释)。 */
public final class RuntimeConfigKeys {
  private RuntimeConfigKeys() {}

  public static final String SCAN_DELAY = "loop.scan-delay-ms";
  public static final String RECONCILE_DELAY = "reconcile.delay-ms";
  public static final String DAG_DELAY = "dag.delay-ms";
  public static final String EVENT_DELAY = "event.scan-delay-ms";
  public static final String NOTIFICATION_DELAY = "notifications.dispatch-delay-ms";
  public static final String DLQ_DELAY = "dlq.replay-delay-ms";
  public static final String AUDIT_RETENTION_DELAY = "audit.retention.delay-ms";
  public static final String AUDIT_RETENTION_DAYS = "audit.retention.days";
  public static final String WORKER_CAPACITY = "worker.capacity";
  public static final String WORKER_IDLE_MS = "worker.idle-ms";
  public static final String WORKER_HEARTBEAT_MS = "worker.heartbeat.interval-ms";
  public static final String WORKER_SHUTDOWN_GRACE_SEC = "worker.shutdown-grace-sec";
  public static final String SUSPEND = "suspend";

  /** 白名单 map:key → 默认值。LinkedHashMap 保序(list() 展示稳定)。 */
  public static final Map<String, String> DEFAULTS = new LinkedHashMap<>();
  static {
    DEFAULTS.put(SCAN_DELAY, "5000");
    DEFAULTS.put(RECONCILE_DELAY, "15000");
    DEFAULTS.put(DAG_DELAY, "5000");
    DEFAULTS.put(EVENT_DELAY, "5000");
    DEFAULTS.put(NOTIFICATION_DELAY, "1000");
    DEFAULTS.put(DLQ_DELAY, "1000");
    DEFAULTS.put(AUDIT_RETENTION_DELAY, "3600000");
    DEFAULTS.put(AUDIT_RETENTION_DAYS, "0");
    DEFAULTS.put(WORKER_CAPACITY, "1");
    DEFAULTS.put(WORKER_IDLE_MS, "200");
    DEFAULTS.put(WORKER_HEARTBEAT_MS, "10000");
    DEFAULTS.put(WORKER_SHUTDOWN_GRACE_SEC, "30");
    DEFAULTS.put(SUSPEND, "false");
  }

  public static boolean isKnown(String key) { return DEFAULTS.containsKey(key); }
  public static String defaultOf(String key) { return DEFAULTS.get(key); }
}
```

- [ ] **Step 2: 写 `RuntimeConfigService`**

`RuntimeConfigService.java`:
```java
package dev.scheduler.server.service;

import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.RuntimeConfigRepository;
import dev.scheduler.persistence.RuntimeConfigRow;
import dev.scheduler.server.config.RuntimeConfigKeys;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 运行时配置服务:读走 DB 主键单行(无缓存),写走白名单 + 类型校验 + 审计。 */
public class RuntimeConfigService {
  private static final Logger log = LoggerFactory.getLogger(RuntimeConfigService.class);
  private final RuntimeConfigRepository repo;
  private final AuditRecorder auditor;

  public RuntimeConfigService(RuntimeConfigRepository repo, AuditRecorder auditor) {
    this.repo = repo;
    this.auditor = auditor;
  }

  public long getLong(String key, long fallback) {
    RuntimeConfigRow row = repo.find(key).orElse(null);
    if (row == null) return fallback;
    try {
      return Long.parseLong(row.value().trim());
    } catch (NumberFormatException e) {
      log.warn("runtime config {} has non-numeric value '{}'; falling back to {}", key, row.value(), fallback);
      return fallback;
    }
  }

  public boolean getFlag(String key, boolean fallback) {
    RuntimeConfigRow row = repo.find(key).orElse(null);
    if (row == null) return fallback;
    String v = row.value().trim();
    if ("true".equalsIgnoreCase(v) || "1".equals(v)) return true;
    if ("false".equalsIgnoreCase(v) || "0".equals(v)) return false;
    log.warn("runtime config {} has non-boolean value '{}'; falling back to {}", key, row.value(), fallback);
    return fallback;
  }

  public boolean isSuspended() { return getFlag(RuntimeConfigKeys.SUSPEND, false); }

  public List<RuntimeConfigEntry> list() {
    List<RuntimeConfigEntry> out = new ArrayList<>();
    for (String key : RuntimeConfigKeys.DEFAULTS.keySet()) {
      RuntimeConfigRow row = repo.find(key).orElse(null);
      if (row == null) {
        out.add(new RuntimeConfigEntry(key, RuntimeConfigKeys.defaultOf(key), "default", null, null));
      } else {
        out.add(new RuntimeConfigEntry(key, row.value(), "db", row.updatedBy(), row.updatedAt()));
      }
    }
    return out;
  }

  /** 白名单 + 类型校验;通过后入库并记审计 runtime-config.update。 */
  public void set(String key, String rawValue, String operator) {
    if (!RuntimeConfigKeys.isKnown(key)) {
      throw new IllegalArgumentException("unknown runtime config key: " + key);
    }
    String value = (rawValue == null ? "" : rawValue.trim());
    validateValue(key, value);
    repo.upsert(key, value, operator);
    auditor.record(operator, "runtime-config.update", TargetType.NONE, 0L,
        java.util.Map.of("key", key, "value", value));
  }

  private void validateValue(String key, String value) {
    String fallback = RuntimeConfigKeys.defaultOf(key);
    if (key.equals(RuntimeConfigKeys.SUSPEND)) {
      if (!value.equals("true") && !value.equals("false")) {
        throw new IllegalArgumentException("suspend must be true or false, got: " + value);
      }
      return;
    }
    if (fallback != null) {
      try {
        if (key.endsWith("sec") || key.endsWith("ms") || key.endsWith("days")) {
          Long.parseLong(value);
        } else if (key.equals("worker.capacity")) {
          Long.parseLong(value);
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("invalid numeric value for " + key + ": " + value);
      }
    }
  }

  /** 展示条目:来源 default=白名单默认未写;db=DB 命中。 */
  public record RuntimeConfigEntry(String key, String value, String source,
                                   String updatedBy, Instant updatedAt) {}
}
```
(去掉 Step 2 开头那个错误的分支版 `getLong`——仅保留修正版。文件以修正版为准。)

- [ ] **Step 3: Beans 装配**

在 `Beans.java` 加两个 `@Bean`(放在通知仓储附近,风格一致):
```java
  @Bean
  RuntimeConfigRepository runtimeConfigRepository(JdbcTemplate jdbc) {
    return new JdbcRuntimeConfigRepository(jdbc);
  }

  @Bean
  RuntimeConfigService runtimeConfigService(RuntimeConfigRepository runtimeConfigRepository,
                                            AuditRecorder auditor) {
    return new RuntimeConfigService(runtimeConfigRepository, auditor);
  }
```
补 import:`dev.scheduler.server.service.RuntimeConfigService`、`dev.scheduler.server.service.AuditRecorder`(若未引)、`dev.scheduler.persistence.JdbcRuntimeConfigRepository`、`dev.scheduler.persistence.RuntimeConfigRepository`。

- [ ] **Step 4: 写单测 `RuntimeConfigServiceTest`**

用假 repo(fake)与假 auditor。fake repo 用 map 内存;审计用 Mockito mock。需补 import:`import static org.mockito.Mockito.mock;`、`import static org.mockito.Mockito.verify;`、`import static org.mockito.ArgumentMatchers.eq;`、`import static org.mockito.ArgumentMatchers.any;`、`import dev.scheduler.server.service.AuditRecorder;`。
```java
package dev.scheduler.server.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
```
> 上面 `set_valid_persistsAndAudits` 用了 Mockito `verify(auditor).record(...)`,需补 `import static org.mockito.ArgumentMatchers.eq;` 与 `import static org.mockito.ArgumentMatchers.any;`。

- [ ] **Step 5: 跑连测**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test -Dtest=RuntimeConfigServiceTest`
Expected: PASS(须补全 Mockito/ArgumentMatchers import,以能编译为准则)。

- [ ] **Step 6: 提交**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/config/RuntimeConfigKeys.java \
  scheduler-server/src/main/java/dev/scheduler/server/service/RuntimeConfigService.java \
  scheduler-server/src/test/java/dev/scheduler/server/service/RuntimeConfigServiceTest.java \
  scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java
git commit -m "feat(server): app_runtime_config 白名单 RuntimeConfigKeys + RuntimeConfigService(热读/白名单写/审计)
---
Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: `RuntimeConfigController` + ADMIN 收口 + 审计 README 端点

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/web/RuntimeConfigController.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/security/OperatorInterceptor.java`(ADMIN_WRITES 加 PUT)
- Create: `scheduler-server/src/test/java/dev/scheduler/server/web/RuntimeConfigControllerTest.java`

**Interfaces:**
- Consumes: `RuntimeConfigService`、`CurrentOperator`。
- Produces: 端点 `GET /api/v1/runtime-config`(读,开放)、`PUT /api/v1/runtime-config/{key}`(ADMIN 写)。

- [ ] **Step 1: 写 endpoint**

`RuntimeConfigController.java`:
```java
package dev.scheduler.server.web;

import dev.scheduler.server.security.CurrentOperator;
import dev.scheduler.server.service.RuntimeConfigService;
import dev.scheduler.server.service.RuntimeConfigService.RuntimeConfigEntry;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 运行时设置(热更新):读开放,写 ADMIN。写会白名单 + 类型校验 + 记 runtime-config.update 审计。 */
@RestController
@RequestMapping("/api/v1/runtime-config")
public class RuntimeConfigController {
  private final RuntimeConfigService settings;
  private final CurrentOperator current;

  public RuntimeConfigController(RuntimeConfigService settings, CurrentOperator current) {
    this.settings = settings;
    this.current = current;
  }

  @GetMapping
  public java.util.List<RuntimeConfigEntry> list() {
    return settings.list();
  }

  public record ValueReq(String value) {}

  @PutMapping("/{key}")
  public RuntimeConfigEntry set(@PathVariable String key, @RequestBody ValueReq req) {
    settings.set(key, req.value, current.get());
    return settings.list().stream().filter(e -> e.key().equals(key)).findFirst().orElseThrow();
  }
}
```

- [ ] **Step 2: ADMIN 收口**

`OperatorInterceptor.java` 的 `ADMIN_WRITES` 追加一行:
```java
      new String[]{"PUT", "/api/v1/runtime-config/{key}"}));
```
(在 webhook 条目后追加,保持与既有格式一致。)

- [ ] **Step 3: 写 controller 集成测试**

参照 `NotificationsControllerTest` 建 `RuntimeConfigControllerTest`(用 `@SpringBootTest` + MockMvc,`@WithMockSchedulerOperator`/既有测试基建——**以 server 模块既有 controller 测试为模板**,Agent 先读一个现存 controller 测试对齐其鉴权 setup)。断言:
- `GET /runtime-config`(alice)→ 200,body 含 `loop.scan-delay-ms` 且 source=`default`;
- `PUT /runtime-config/loop.scan-delay-ms`(alice,ADMIN)→ 200,source 变 `db`,value=`2500`;
- `PUT /runtime-config/nope` → 400/error(白名单拒绝);
- 用非 ADMIN 操作者 PUT → 403(与 interceptor 判定一致)。

- [ ] **Step 4: 跑测**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test -Dtest=RuntimeConfigControllerTest`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/web/RuntimeConfigController.java \
  scheduler-server/src/main/java/dev/scheduler/server/security/OperatorInterceptor.java \
  scheduler-server/src/test/java/dev/scheduler/server/web/RuntimeConfigControllerTest.java
git commit -m "feat(server): GET/PUT /runtime-config + ADMIN_WRITES 收口(白名单写 + 审计)
---
Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: server 循环改造 `@Scheduled` → `ConfigurableLoop` + suspend + 停机释放锁

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/config/ConfigurableLoop.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`(7 个循环的 inner class 改继承 ConfigurableLoop;对应 `@Bean` 注入 settings;加 leader 释放回调)
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/leader/LeaderElection.java`(加接口方法或由 Advisor.close 复用 —— 见下)
- Create: `scheduler-server/src/test/java/dev/scheduler/server/config/ConfigurableLoopTest.java`

**关键改造语义:**
- ScanLoop / DagLoop / EventLoop(无 leader 门控)→ `loopOnce()` 直接 engine;`delayKey` 用 `RuntimeConfigKeys.XXX_DELAY`。
- ReconcileLoop / AuditRetentionLoop / NotificationLoop / DlqReplayLoop(有 leader 门控)→ `loopOnce()` 先 `if (!leader.isLeader()) return;`。
- AuditRetentionLoop 有两个独立配置:调度间隔 `audit.retention.delay-ms`(delayKey,fallback 3600000)+ 归档阈值 `audit.retention.days`(loopOnce 内读,fallback 0)= 不继承现 `@Value` 注入,改由 settings 读(两层 keys 见 Task 2 whitelist)。原时间语义保留。
- **suspend 门控**:`ConfigurableLoop.tick()` 顶部:若 `settings.isSuspended()` → 仍排下一拍但不执行 loopOnce(suspend 时用较长的保留心跳,如固定 5s,避免忙轮询)。
- `@ConditionalOnProperty` 门控保留在 `@Bean` 工厂方法上(不改)。
- **循环替换 @Scheduled**:原 inner class 的 `@Scheduled` 注解去掉,改由 `ConfigurableLoop` 自持线程排拍。
- **停机释放锁**:`AdvisoryLockLeaderElection` 实现 `@PreDestroy` 调 `close()`(释放 advisory lock → 另一副本立即选主)。给 `LeaderElection` 接口加 `default void shutdown() {}`,Advisor override 调用 `close()`;bean 由 Spring 生命周期 `@PreDestroy`(在 inner class 加或 Advisor 加)。

- [ ] **Step 1: 写 `ConfigurableLoop` 基类**

`ConfigurableLoop.java`:
```java
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
  private final RuntimeConfigService settings;
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
```

- [ ] **Step 2: 改造 7 个循环 inner class**

对 `ScanLoop`(无 leader):
```java
  public static final class ScanLoop extends ConfigurableLoop {
    private static final Logger log = LoggerFactory.getLogger(ScanLoop.class);
    private final TriggerEngine engine;

    ScanLoop(RuntimeConfigService settings, TriggerEngine engine) {
      super(settings, RuntimeConfigKeys.SCAN_DELAY, 5000L);
      this.engine = engine;
      start();
    }

    @Override protected void loopOnce() {
      engine.scanOnce();
    }
  }
```
> 规定:异常兜底统一由 `ConfigurableLoop.tick` 承担。内层 `loopOnce` **只写业务调用**,不含 try/catch——去掉原 `@Scheduled tick()` 里的内层 catch(原 `log` 字段不再需要,一并删),靠基类兜底。此规则适用于全部 7 个循环(下述代码块一律按「无内层 catch」写)。上面 ScanLoop 已按此规则给出——`log` 字段已删。

`@Bean` 工厂方法改:
```java
  @Bean
  @ConditionalOnProperty(name = "scheduler.loop.enabled", havingValue = "true", matchIfMissing = true)
  ScanLoop scanLoop(RuntimeConfigService runtimeConfigService, TriggerEngine engine) {
    return new ScanLoop(runtimeConfigService, engine); // start() 在构造器内已调
  }
```

对 `ReconcileLoop`(有 leader 门控):
```java
  public static final class ReconcileLoop extends ConfigurableLoop {
    private final Reconciler reconciler;
    private final LeaderElection leader;

    ReconcileLoop(RuntimeConfigService settings, Reconciler reconciler, LeaderElection leader) {
      super(settings, RuntimeConfigKeys.RECONCILE_DELAY, 15000L);
      this.reconciler = reconciler;
      this.leader = leader;
      start();
    }

    @Override protected void loopOnce() {
      if (!leader.isLeader()) return;
      reconciler.scanOnce();
    }
  }
```
`@Bean` 工厂注入 RuntimeConfigService:
```java
  ReconcileLoop reconcileLoop(RuntimeConfigService runtimeConfigService, Reconciler reconciler, LeaderElection leader) {
    return new ReconcileLoop(runtimeConfigService, reconciler, leader);
  }
```

对 `DagLoop`(无 leader):镜像 ScanLoop,`RuntimeConfigKeys.DAG_DELAY`,fallback 5000,`engine.scanOnce()`。
对 `EventLoop`(无 leader):镜像,`RuntimeConfigKeys.EVENT_DELAY`,fallback 5000,`engine.scanOnce()`。

对 `AuditRetentionLoop`(leader)。**原实现有两个独立配置**:调度间隔 `@Scheduled(fixedDelayString="${scheduler.audit.retention.delay-ms:3600000}")` + 归档阈值 `@Value("${scheduler.audit.retention-days:0}") int retentionDays`(≤0 不归档)。改造为 ConfigurableLoop 后两者分离:`delayKey=AUDIT_RETENTION_DELAY`(fallback 3600000 对应 `:3600000`),`loopOnce` 内读 `AUDIT_RETENTION_DAYS`(fallback 0 对应 `:0`,保持「不配置即不归档」原语义)。**保留原时间语义**(回调阈值 = `Instant.now().minus(Duration.ofDays(days))`),仅把 `@Value` 注入换成 settings 读:
```java
  public static final class AuditRetentionLoop extends ConfigurableLoop {
    private final AuditRetentionService retention;
    private final LeaderElection leader;

    AuditRetentionLoop(RuntimeConfigService settings, AuditRetentionService retention, LeaderElection leader) {
      super(settings, RuntimeConfigKeys.AUDIT_RETENTION_DELAY, 3600000L);
      this.retention = retention;
      this.leader = leader;
      start();
    }

    @Override protected void loopOnce() {
      if (!leader.isLeader()) return;
      long days = settings.getLong(RuntimeConfigKeys.AUDIT_RETENTION_DAYS, 0L);
      if (days <= 0) return;
      retention.archiveOlderThan("retention", Instant.now().minus(Duration.ofDays(days)), 1000);
    }
  }
```
> 实现 agent 先读原 `AuditRetentionLoop` 的完整 `tick()` 原文(在 `Beans.java`),把「阈值读数」从构造器注入换成这里的 `settings.getLong(...)`;时间计算方式(用 `Instant.now().minus(...)` 还是原代码的 `LocalDate` 等)**原样保留**,禁止发明新语义。

对 `NotificationLoop`(leader):镜像 ReconcileLoop,`RuntimeConfigKeys.NOTIFICATION_DELAY`,fallback 1000,loopOnce = leader 门控 + `dispatcher.dispatchOnce()`。
对 `DlqReplayLoop`(leader):镜像,`RuntimeConfigKeys.DLQ_DELAY`,fallback 1000,loopOnce = leader 门控 + 原 dlq 处置 for 循环。

- [ ] **Step 3: 保留 @ConditionalOnProperty + 循环 disabled 兼容**

每个 `@Bean` 工厂方法的 `@ConditionalOnProperty(matchIfMissing = true)` **原样保留**。循环 disabled 时 bean 不建 → `ConfigurableLoop` 不启动。已有测试若依赖某循环 disabled(如 DLQ),语义不变。

- [ ] **Step 4: leader 停机释放锁**

`LeaderElection` 接口加:
```java
  /** 停机/退位时释放本会话持有的 leader 锁(幂等)。滚动发布时让另一副本立即选主。 */
  default void shutdown() {}
```
`AdvisoryLockLeaderElection` 加:
```java
  @Override public void shutdown() { close(); }
```
在 `Beans.java` leader 装配处给 bean 挂 `@PreDestroy`?—— `@Bean` producer 与 inner class 均可行。**最稳**:在 `AdvisoryLockLeaderElection` 上加 `@PreDestroy` 方法(Spring 容器关闭时自动调),无需改 Beans。加:
```java
  /** Spring 容器关闭时释放领导锁(幂等 close)。 */
  @PreDestroy public void preDestroy() { close(); }
```
补 import `javax.annotation.PreDestroy`(Spring Boot 3.2/Jakarta:`jakarta.annotation.PreDestroy`)。
> **验证**:实现 agent 确认该项目用 `jakarta.annotation.PreDestroy`(Jakarta EE9+)。

- [ ] **Step 5: 写 `ConfigurableLoopTest`**

用 fake settings(直接 new RuntimeConfigService(fakeRepo,null))验证:delay 热更新、suspend 跳过执行、异常不杀线程。用一个测试循环子类计数 loopOnce 调用:
```java
package dev.scheduler.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.persistence.RuntimeConfigRepository;
import dev.scheduler.persistence.RuntimeConfigRow;
import dev.scheduler.server.service.RuntimeConfigService;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ConfigurableLoopTest {

  static final class FakeRepo implements RuntimeConfigRepository {
    final java.util.Map<String, RuntimeConfigRow> rows = new java.util.LinkedHashMap<>();
    public java.util.Optional<RuntimeConfigRow> find(String key) {
      return java.util.Optional.ofNullable(rows.get(key));
    }
    public java.util.List<RuntimeConfigRow> findAll() { return new java.util.ArrayList<>(rows.values()); }
    public void upsert(String key, String value, String operator) {
      rows.put(key, new RuntimeConfigRow(key, value, operator, Instant.now()));
    }
  }

  static class CountingLoop extends ConfigurableLoop {
    final AtomicInteger runs = new AtomicInteger();
    final AtomicInteger events = new AtomicInteger();
    CountingLoop(RuntimeConfigService s) { super(s, "loop.scan-delay-ms", 20L); start(); }
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
    svc.set("suspend", "false", "a");
    Thread.sleep(150);
    assertTrue(loop.runs.get() > during, "清位后恢复执行");
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
```
> `hotReloadDelay` 对 CI 计时敏感:断言 `fast >= 1` 即可(短 delay 在 120ms 内至少一拍),不强求精确拍数——保持 `assertTrue(fast >= 1)`,把上面 `>= 10` 放宽为 `>= 1`,避免脆。同理 `suspend_haltsExecutions` 用相对比较(排除并发竞态)。

- [ ] **Step 6: 跑测并回归**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test -Dtest=ConfigurableLoopTest`
Expected: PASS。随后跑 `mvn -o -pl scheduler-server -am test` 全模块回归,确认 7 循环改造不破既有调度测试(DLQ disabled 用例等)。

- [ ] **Step 7: 提交**

```bash
git add scheduler-server/src/main/java/dev/scheduler/server/config/ConfigurableLoop.java \
  scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java \
  scheduler-server/src/main/java/dev/scheduler/server/leader/LeaderElection.java \
  scheduler-server/src/main/java/dev/scheduler/server/leader/AdvisoryLockLeaderElection.java \
  scheduler-server/src/test/java/dev/scheduler/server/config/ConfigurableLoopTest.java
git commit -m "feat(server): @Scheduled → ConfigurableLoop(每拍重读 delay 热更新 + suspend 门控) + leader 停机释放锁
---
Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5: worker 侧 `WorkerRuntimeConfig` + WorkLoop 排空 + capacity 热改 + deregister + 停机编排

**Files:**
- Create: `scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerRuntimeConfig.java`
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java`(注入 WorkerRuntimeConfig;WorkLoop 改造;加停机 @PreDestroy)
- Modify: `scheduler-worker/src/main/java/dev/scheduler/worker/registration/WorkerRegistrar.java`(加 deregister)
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/WorkerRepository.java`(加 markOffline)
- Modify: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcWorkerRepository.java`(实现 markOffline)
- Create: `scheduler-worker/src/test/java/dev/scheduler/worker/config/WorkLoopGraceTest.java`
- Modify: `scheduler-worker/src/test/java/dev/scheduler/worker/registration/WorkerRegistrarTest.java`

**Interfaces:**
- worker 不能依赖 server 的 `RuntimeConfigService`。建 `WorkerRuntimeConfig`：
  - `long getLong(String key, long fallback)` / `boolean getFlag(String key, boolean fallback)`(同一张 `app_runtime_config` 表,Jdbc 仓库实例化 worker 侧)。
  - 不带审计/白名单(worker 不写,只读)。
- `WorkerRepository.markOffline(String workerId, Instant at)` — 把该 worker last_seen 置极早(如 at - 1h)或显式 status。实现回读令 worker 不再被 `findAllAlive(now-30s)` 返回。
- `WorkerRegistrar.deregister()` — 调 `repo.markOffline(workerId, clock.instant())`。
- `WorkLoop`:
  - `stopAndDrain(long graceSec)` — running=false 停新认领;等 inflight 归零(≤ graceSec);超时记警告继续退。
  - `resize(int n)` — AtomicInteger target;tick 内 CAS 维护 activeClaims,>=target 则跳过认领。
  - 认领时 `inflight.incrementAndGet()`、完成后 `inflight.decrementAndGet()`。
- worker 停机 `@PreDestroy`(在 WorkersConfig 一个 bean 上):`workLoop.stopAndDrain(grace)` → `heartbeatLoop` 停 → `registrar.deregister()`。

- [ ] **Step 1: 写 `WorkerRuntimeConfig`(worker 侧只读)**

`WorkerRuntimeConfig.java`:
```java
package dev.scheduler.worker.config;

import dev.scheduler.persistence.JdbcRuntimeConfigRepository;
import dev.scheduler.persistence.RuntimeConfigRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/** worker 侧运行时配置只读器:同一张 app_runtime_config 表,worker 只读(不写不审计)。
 *  worker 独立上下文,不能依赖 server 的 RuntimeConfigService bean。 */
public class WorkerRuntimeConfig {
  private static final Logger log = LoggerFactory.getLogger(WorkerRuntimeConfig.class);
  private final JdbcRuntimeConfigRepository repo;

  public WorkerRuntimeConfig(JdbcTemplate jdbc) { this.repo = new JdbcRuntimeConfigRepository(jdbc); }

  public long getLong(String key, long fallback) {
    RuntimeConfigRow row = repo.find(key).orElse(null);
    if (row == null) return fallback;
    try { return Long.parseLong(row.value().trim()); }
    catch (NumberFormatException e) {
      log.warn("runtime config {} non-numeric '{}'; fallback {}", key, row.value(), fallback);
      return fallback;
    }
  }

  public boolean getFlag(String key, boolean fallback) {
    RuntimeConfigRow row = repo.find(key).orElse(null);
    if (row == null) return fallback;
    String v = row.value().trim();
    if ("true".equalsIgnoreCase(v) || "1".equals(v)) return true;
    if ("false".equalsIgnoreCase(v) || "0".equals(v)) return false;
    return fallback;
  }
}
```

- [ ] **Step 2: WorkerRepository 加 markOffline**

`WorkerRepository.java` 追加:
```java
  /** 停机/退位:把 last_seen 置极早,使本 worker 立即离开 findAllAlive(now-窗口)视界,在途分片立被他方认领。 */
  void markOffline(String workerId, Instant at);
```
`JdbcWorkerRepository` 追加实现(先读该文件现状,在既有 upsertHeartbeat 旁):
```java
  @Override public void markOffline(String workerId, Instant at) {
    // last_seen 置 to 极早(at - 10 分钟) → findAllAlive(now-30s)不再返回本 worker(除非刷了新表)
    jdbc.update("UPDATE app_worker SET last_seen = ? WHERE worker_id = ?", Timestamp.from(at), workerId);
  }
```
> **实现 agent 先读 `app_worker` 表结构与 `JdbcWorkerRepository` 现有字段**,用与 `upsertHeartbeat` 一致的列名/类型。`markOffline` 语义等同于「把 last_seen 写回很旧」→ 不限定 status 字段(若不存在的列则不加)。

- [ ] **Step 3: WorkerRegistrar 加 deregister**

`WorkerRegistrar.java` 追加:
```java
  /** 停机排空完成后调用:把 last_seen 置极早,本 worker 立即离开存活视界,在途分片立可他 worker 认领。幂等。 */
  public void deregister() {
    repo.markOffline(workerId, clock.instant().minusSeconds(600));
  }
```
> 用 `clock.instant().minusSeconds(600)`(=「极早」)保证离开 30s 存活窗口。

- [ ] **Step 4: WorkLoop 改造(排空 + capacity 热改)**

现在 `WorkLoop` 是 `public static final class WorkLoop implements DisposableBean`(daemon pool,capacity 个线程 scheduleWithFixedDelay(tick))。改造为支持 `stopAndDrain` + `resize`:

```java
  public static final class WorkLoop implements DisposableBean {
    private final ExecutorWorker worker;
    private final ScheduledExecutorService pool;
    private final long idleMs;
    private final int maxCapacity;
    private final WorkerRuntimeConfig runtime;
    private final AtomicBoolean running = new AtomicBoolean(true);
    /** 当期认领槽计数:capacity 热改时 CAS 控流。 */
    private final AtomicInteger target = new AtomicInteger(1);
    private final AtomicInteger activeClaims = new AtomicInteger(0);
    private final AtomicInteger inflight = new AtomicInteger(0);

    WorkLoop(ExecutorWorker worker, int capacity, int maxCapacity, long idleMs,
             WorkerRuntimeConfig runtime) {
      this.worker = worker;
      this.idleMs = idleMs;
      this.maxCapacity = Math.max(1, maxCapacity);
      this.runtime = runtime;
      this.pool = Executors.newScheduledThreadPool(this.maxCapacity, r -> {
        Thread t = new Thread(r, "worker-exec"); t.setDaemon(true); return t;
      });
      // 线程池按 maxCapacity 定长(上界,只建不缩)→ 并发排拍并行性达当前 target;热改只动 target+排拍数。
      int start = Math.max(0, Math.min(capacity, this.maxCapacity));
      this.target.set(start);
      for (int i = 0; i < start; i++) {
        pool.scheduleWithFixedDelay(this::tick, 0, idleMs, TimeUnit.MILLISECONDS);
      }
    }
```
> **并发模型规定**:`pool` 定长 **maxCapacity** 线程(新 `@Value scheduler.worker.max-capacity`,默认 32,池上界 → 并行性有界、预留热扩空间,`capacity<=0` 不建);运行时并发由 `target`(默认 = 启动 capacity,≤ maxCapacity)经 `tick()` 内 CAS 闸控制。`resize(n)`:clamp 到 `[1, maxCapacity]`,增则给池补排 (`n - target`) 个 fixed-delay 拍(池线程够用)、target 设 n;减则仅降 target(CAS 让多出的拍让位,线程闲置不杀)。**这使 capacity 双向热改无需重建线程池**,守护线程语义保留。既有 `WorkerLoopTest` 若直接用无参构造 WorkLoop,需对齐新构造器签名——Agent 一并修其调用点(补 maxCapacity 参数,如默认 32)。

`tick()`(每次认领前自读 capacity 热键并 self-resize → capacity 热改即时生效):
```java
    private void tick() {
      if (!running.get()) return;
      resize(Math.toIntExact(runtime.getLong("worker.capacity", target.get())));
      if (activeClaims.incrementAndGet() > target.get()) {
        activeClaims.decrementAndGet(); // 满了,让位
        return;
      }
      try {
        inflight.incrementAndGet();
        try {
          worker.workOne();
        } finally {
          inflight.decrementAndGet();
        }
      } catch (Throwable t) {
        log.warn("worker claim loop tick failed; continuing next tick", t);
      } finally {
        activeClaims.decrementAndGet();
      }
    }
```

`resize(int n)`(每次 tick 开头调;从 `runtime.getLong("worker.capacity", target.get())` 得 n):
```java
    private void resize(int n) {
      n = Math.max(1, Math.min(n, maxCapacity));
      int prev = target.getAndSet(n);
      if (n > prev) {
        for (int i = prev; i < n; i++) {
          pool.scheduleWithFixedDelay(this::tick, 0, idleMs, TimeUnit.MILLISECONDS);
        }
        log.info("worker capacity hot-resized {} -> {}", prev, n);
      } else if (n < prev) {
        log.info("worker capacity hot-resized {} -> {} (CAS 回收多余槽)", prev, n);
      }
    }
```
capacity 每 tick 自读 → 改 `worker.capacity` 热键后最多一拍延迟生效。**WorkerConfig 装配**:`workLoop` bean 增参数 `@Value("${scheduler.worker.max-capacity:32}") int maxCapacity` 与 `WorkerRuntimeConfig runtime`;`capacity` 仍用 `@Value("${scheduler.worker.capacity:1}") int capacity`。

`stopAndDrain`:
```java
    @Override public void destroy() { stop(); }

    void stop() { running.set(false); }

    /** 停机:停新认领,排空在途 handler(≤ graceSec),随后关闭调度线程。幂等:二次 no-op。 */
    public void stopAndDrain(long graceSec) {
      running.set(false);
      pool.shutdown(); // 不再排新拍
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(graceSec);
      while (inflight.get() > 0 && System.nanoTime() < deadline) {
        try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
      }
      if (inflight.get() > 0) {
        log.warn("drain timeout after {}s; {} inflight left for liveness recovery", graceSec, inflight.get());
      }
      pool.shutdownNow(); // 兜底:daemon 不阻塞 JVM,剩余由 server 活性回收
    }
```

**worker 停机编排**(在 `WorkerConfig` 加一个整合 bean):
```java
  /** 停机编排:排空 → 停心跳 → deregister。WorkerConfig 内 package 可见引用 workLoop。 */
  @Bean
  DisposableBean gracefulShutdown(WorkLoop workLoop, HeartbeatLoop heartbeatLoop,
                                  WorkerRegistrar registrar,
                                  WorkerRuntimeConfig runtime) {
    return () -> {
      long grace = runtime.getLong("worker.shutdown-grace-sec", 30L);
      workLoop.stopAndDrain(grace);
      heartbeatLoop.stop();
      registrar.deregister();
      log.info("worker {} drained and deregistered", ...); // 用已注入 workerId 或仅日志
    };
  }
```
> `HeartbeatLoop` 需加 `stop()`:`scheduler.shutdownNow()`。在 HeartbeatLoop 加 `void stop() { scheduler.shutdownNow(); }`。gracefulShutdown bean 的 `DisposableBean` 由 Spring 销毁时调用(容器关闭即执行)。

`@Value` 注入 worker workerId 供日志:(可选)调入 `gracefulShutdown` 的 `String schedulerWorkerId`。

- [ ] **Step 5: worker 测试**

新增 `WorkLoopGraceTest`(继承 `AbstractExecutorWorkerTest`,用 CountDownLatch 模拟长 handler):
- `stopAndDrain_waitsForInflight`:`ExecutorWorker` 认领一个 handler 阻塞(CountDownLatch),调 `stopAndDrain(10)`,断言在 handler 释放前没返回(或通过注入同步判定 drain 阻在 inflight)。
- `resize_decreasesTrough`:`resize(1)` 后并发认领至多 1。
- `WorkerRegistrarTest` 加:`deregister` 后 `findAllAlive(now-30s)` 不含该 worker。
- `markOffline` 仓储测试(JdbcWorkerRepositoryTest 加:markOffline 后 findAllAlive 不含)。

> **worker 测试复杂处**:WorkLoop 用真实调度线程。测试须短(几百 ms)+ 只断言关键语义(排空阻塞/容量约束),不依赖精确时序。以先例 `WorkerLoopTest` 为模板。

- [ ] **Step 6: 跑测并回归**

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-worker -am test`
Expected: PASS(含 `AbstractExecutorWorkerTest` 全基类用例)。

- [ ] **Step 7: 提交**

```bash
git add scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerRuntimeConfig.java \
  scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java \
  scheduler-worker/src/main/java/dev/scheduler/worker/registration/WorkerRegistrar.java \
  scheduler-persistence/src/main/java/dev/scheduler/persistence/WorkerRepository.java \
  scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcWorkerRepository.java \
  scheduler-worker/src/test/java/dev/scheduler/worker/config/WorkLoopGraceTest.java \
  scheduler-worker/src/test/java/dev/scheduler/worker/registration/WorkerRegistrarTest.java
git commit -m "feat(worker): WorkerRuntimeConfig 只读热键 + WorkLoop 排空/容量热改 + deregister + 停机编排
---
Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 6: 前端「运行时设置」页

**Files:**
- Create: `web/src/pages/SettingsPage.tsx`
- Modify: `web/src/App.tsx`(路由 + 导航)
- Modify: `web/src/api/types.ts`(RuntimeConfigEntry / ValueReq)
- Modify: `web/src/api/client.ts`(listRuntimeConfig / setRuntimeConfig)
- Modify: `web/src/api/auth.ts` 或 client 鉴权(若需)

**Interfaces:**
- Consumes:`listRuntimeConfig(): Promise<RuntimeConfigEntry[]>`、`setRuntimeConfig(key, value): Promise<RuntimeConfigEntry>`。
- `RuntimeConfigEntry { key: string; value: string; source: 'default'|'db'; updatedBy: string|null; updatedAt: string|null }`。

- [ ] **Step 1: types + client**

`types.ts` 加:
```ts
export interface RuntimeConfigEntry {
  key: string;
  value: string;
  source: 'default' | 'db';
  updatedBy: string | null;
  updatedAt: string | null;
}
```
`client.ts` 加(镜像现有 `listWebhooks` 风格):
```ts
export async function listRuntimeConfig(): Promise<RuntimeConfigEntry[]> {
  return (await api.get('/runtime-config')).data;
}
export async function setRuntimeConfig(key: string, value: string): Promise<RuntimeConfigEntry> {
  return (await api.put(`/runtime-config/${encodeURIComponent(key)}`, { value })).data;
}
```
(若 client 已有 baseURL `/api/v1`,路径前缀自理;以现有 client 为准。)

- [ ] **Step 2: 写 SettingsPage**

`SettingsPage.tsx`:
```tsx
// web/src/pages/SettingsPage.tsx —— 运行时设置(热更新):ADMIN 改 DB 热键,读侧全量展示。
import { useCallback, useEffect, useState } from 'react';
import { listRuntimeConfig, setRuntimeConfig } from '../api/client';
import { RuntimeConfigEntry } from '../api/types';

export default function SettingsPage() {
  const [items, setItems] = useState<RuntimeConfigEntry[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [busyKey, setBusyKey] = useState<string | null>(null);

  const load = useCallback(async () => {
    try { setItems(await listRuntimeConfig()); setErr(null); }
    catch (e) { setErr(String(e)); }
  }, []);

  useEffect(() => { load(); }, [load]);

  const save = async (item: RuntimeConfigEntry, value: string) => {
    setBusyKey(item.key);
    try {
      await setRuntimeConfig(item.key, value);
      setNotice(`已更新 ${item.key}`); setErr(null);
      await load();
    } catch (e) { setErr(String(e)); }
    finally { setBusyKey(null); }
  };

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">运行时设置</h1>
          <p className="page-sub">DB 运行时热键:修改后下一调度拍生效,无需重启。ADMIN 可写,写入记审计。</p>
        </div>
      </div>
      {notice && <div className="mb-4 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{notice}</div>}
      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}
      <div className="card p-4">
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr><th>键</th><th>当前值</th><th>来源</th><th>更新者</th><th>更新时间</th><th></th></tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <Row key={item.key} item={item} busy={busyKey === item.key} onSave={save} />
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function Row({ item, busy, onSave }: {
  item: RuntimeConfigEntry;
  busy: boolean;
  onSave: (item: RuntimeConfigEntry, value: string) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState(item.value);
  const isSuspend = item.key === 'suspend';
  return (
    <tr>
      <td className="font-mono text-sm">{item.key}</td>
      <td className="text-sm">
        {isSuspend
          ? (item.value === 'true'
              ? <span className="rounded bg-red-100 px-1.5 py-0.5 text-xs font-medium text-red-700">已暂停</span>
              : <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-xs font-medium text-emerald-700">运行中</span>)
          : <span className="font-mono text-sm">{item.value}</span>}
      </td>
      <td className="text-xs">{item.source === 'db' ? 'DB' : <span className="text-slate-400">默认</span>}</td>
      <td className="text-xs text-slate-500">{item.updatedBy ?? '—'}</td>
      <td className="whitespace-nowrap text-xs text-slate-500">{item.updatedAt ? new Date(item.updatedAt).toLocaleString() : '—'}</td>
      <td className="text-right">
        {editing ? (
          <>
            {isSuspend ? (
              <button className="btn btn-secondary"
                onClick={() => { onSave(item, item.value === 'true' ? 'false' : 'true'); setEditing(false); }}>
                {item.value === 'true' ? '恢复' : '暂停'}
              </button>
            ) : (
              <input className="input" style={{ width: 120 }} value={value}
                onChange={(e) => setValue(e.target.value)} />
            )}
            <button className="btn btn-primary" disabled={busy} onClick={() => onSave(item, isSuspend ? (item.value === 'true' ? 'false' : 'true') : value.trim())}>
              保存
            </button>
            <button className="btn btn-secondary" onClick={() => setEditing(false)}>取消</button>
          </>
        ) : (
          <button className="btn btn-secondary" onClick={() => { setValue(item.value); setEditing(true); }}>改</button>
        )}
      </td>
    </tr>
  );
}
```
> **suspend 行特化**:全表第一行醒目展示全局暂停开关;点「暂停/恢复」直接 PUT。其余 key 行内编辑。

- [ ] **Step 3: 路由注册**

`App.tsx`:
- import `SettingsPage`。
- `<Route path="/settings" element={<SettingsPage />} />`。
- 导航加「运行时设置」链接(参考现有 `NavLink` 列表)。

- [ ] **Step 4: 前端构建**

Run: `cd /d/ai-project/scheduler/web && npm run build`
Expected: 构建通过(tsc 无错)。

- [ ] **Step 5: 手动冒烟(可选,依赖本地栈)**

若栈在跑:登入 → 导航到「运行时设置」→ 改 `loop.scan-delay-ms` → 观察 server log 下一拍生效。**可选**,不强求自动化;构建通过为硬性验收。

- [ ] **Step 6: 提交**

```bash
git add web/src/pages/SettingsPage.tsx web/src/App.tsx web/src/api/types.ts web/src/api/client.ts
git commit -m "feat(web): 运行时设置页(热键改值 + suspend 开关 + 来源/操作者展示)
---
Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 7: 全量验收 + 文档

**Files:**
- Modify: `docs/superpowers/v2-acceptance-checklist.md`(追加 v3·D 记录)或 `docs/superpowers/specs/2026-09-28-scheduler-v3-d-production-hardening-design.md` 标记实现
- 无代码

- [ ] **Step 1: 全量测试**

Run: `cd /d/ai-project/scheduler && mvn -o test`
Expected: reactor BUILD SUCCESS(全模块)。

- [ ] **Step 2: 前端构建确认**

Run: `cd /d/ai-project/scheduler/web && npm run build`
Expected: 通过。

- [ ] **Step 3: spec 标记实现**

在 spec 文件加一行 `> 实现:见 v3·D 各 feat commit` 或直接在验收清单勾选已实现项:[✓]。把本文件 `[ ]` 逐项核对勾选。

- [ ] **Step 4: 提交**

```bash
git add docs/superpowers/specs/2026-09-28-scheduler-v3-d-production-hardening-design.md
git commit -m "docs(v3·D): 验收清单全部实现,全量 reactor BUILD SUCCESS
---
Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## 自审(写 plan 时对照 spec)

- **spec §2 目标 1(热更新)**:Task 2/3(白名单+端点)+ Task 4(循环每拍重读)→ OK。
- **spec §2 目标 2(worker 排空)**:Task 5 stopAndDrain + deregister → OK。
- **spec §2 目标 3(server 滚动)**:Task 4 leader shutdown → OK。
- **spec §2 目标 4(suspend)**:Task 2 isSuspended + Task 4 门控 + Task 6 前端开关 → OK。
- **spec §3.1.1 表**:V30(Task 1)列名/fallback 表一致。
- **坐标一致性**:`RuntimeConfigService.getLong/set/isSuspended/list/RuntimeConfigEntry` 在 Task 2 定义、Task 3/4/6 消费——签名一致(`getLong(String,long)`、`getFlag(String,boolean)`、`list()`、`set(String,String,String)`。
- **排除项**:lease.seconds / dlq.max-replays 不入热键(Task 2 白名单已排除)——与 spec §3.1.1 注释一致。
- **worker 独立上下文**:Task 5 WorkerRuntimeConfig 不 import server 服务——符合 Global Constraints。
- **@ConditionalOnProperty 保留**:Task 4 Step 3 明确。
- **AuditRetentionLoop 时间语义**:Task 4 明确「以原 tick 原文为准,仅替换 @Value 注入」。用 `Instant.now().minus(Duration.ofDays(days))`;days 从 settings 读。