# 操作审计日志 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 Scheduler 的全部 operator 主动写操作(3 控制器共 15 个写端点)追加 append-only 操作审计,提供过滤分页只读 API `GET /api/v1/audits` 与前端第 6 页「审计」。

**Architecture:** 新增 `app_audit` 表(V10 迁移)+ 核心模型(`TargetType` 枚举、`AuditEntry` record)+ 持久层 `AuditRepository`/`JdbcAuditRepository`。三个写控制器构造函数注入小 `AuditRecorder`(server 层,封装「X-Operator 缺省归 anonymous + Jackson 序列化 meta + 非阻断落库」),在每个写 handler **操作成功落库后**显式调用。只读 API 复用既有 `Page`/`Paging`,`occurred_at DESC`。前端第 6 个侧边栏入口「审计」,只读表格 + 过滤 + 分页(镜像 DlqPage 脚手架)。

**Tech Stack:** 与现状一致(Java 17、Spring Boot、PostgreSQL 16、Testcontainers、Flyway V10、Vite/React/Tailwind)。**无新增依赖**——persistence 模块无 Jackson,故 `AuditEntry.meta` 以**原始 JSON 文本 String** 贯穿持久层与读 API(序列化只在 server 层 AuditRecorder 用既有 Spring ObjectMapper),前端 `JSON.parse` 展示。

**Spec:** `docs/superpowers/specs/2026-09-17-scheduler-audit-log-design.md`

## Global Constraints

- **append-only**:`app_audit` 只插入、不更新不删除;单条 INSERT,无需幂等键。
- **DB 权威时钟**:`occurred_at` 用 `NOT NULL DEFAULT now()`(DB now,与 active/retry 判窗一致)。
- **operator 语义**:服务端无认证;身份 = 可选 `@RequestHeader("X-Operator")`,缺省/空 → `'anonymous'`,不阻断既有 curl/前端。
- **审计非阻断**:`AuditRecorder` 捕获所有异常只 `log.warn`,不抛、不改用户操作结果;审计 insert 与操作不同事务,操作先提交、审计后追加。
- **捕获方式**:显式 `AuditRecorder`(控制器内调用),**非 AOP**。
- **meta 语义**:紧凑**后态**字段(非 before/after diff);空载荷用 `{}` 而非 NULL(仅 repository 接受 null 表示「无 meta」)。
- **只记 operator 主动动作**:系统驱动状态迁移(认领/重试/回收/失败/父聚合)已由 `execution_shard_outcome` 覆盖,不重复记。
- **只读 API 契约**:`GET /api/v1/audits` 返回 `Page<AuditEntry>`,排序 `occurred_at DESC, id DESC`;过滤参数 `operator`(子串 ILIKE)/`action`(等值)/`targetType`(等值)/`targetId`(等值)/`from`/`to`(ISO-8601)/`limit`/`offset`(`Paging.of` 归一化)。
- **action / target_type 固定词汇**:见 Spec §4(如 `task.create`,target_type 取值 `task`/`execution`/`shard`/`dag`/`dag_run`)。
- **UI**:审计页为**第 6 个侧边栏入口**(任务/执行/DLQ/工作流/指标/审计),只读表格 + 顶部过滤 + 分页。
- 构建/测试离线:`mvn -o`;persistence 测试直接沿 `AbstractPostgresTest`;server web 测试沿 `ApiIntegrationTest`(真 PG + MockMvc + `scheduler.loop.enabled=false`)。
- 本仓风格:构造器注入、`JdbcRepository extends JdbcTemplate`、`where()`/`filterArgs()` 片段、surgical 改动、提交粒度偏小。

---

### Task 1: V10 迁移 + 核心模型 + 持久层(JdbcAuditRepository)

**Files:**
- Create: `scheduler-persistence/src/main/resources/db/migration/V10__audit.sql`
- Create: `scheduler-core/src/main/java/dev/scheduler/core/TargetType.java`
- Create: `scheduler-core/src/main/java/dev/scheduler/core/AuditEntry.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/AuditRepository.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcAuditRepository.java`
- Test: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcAuditRepositoryTest.java`

**Interfaces:**
- Consumes: `AbstractPostgresTest`(既有持久层测试基类,Flyway + Testcontainers + `protected static JdbcTemplate jdbc`)。
- Produces:
  - `dev.scheduler.core.TargetType` enum:`TASK`/`EXECUTION`/`SHARD`/`DAG`/`DAG_RUN`,字段 `String db()`(小写列值)。
  - `dev.scheduler.core.AuditEntry` record:`(long id, Instant occurredAt, String operator, String action, String targetType, long targetId, String meta, String source)`,`meta` 为原始 JSON 文本或 null。
  - `dev.scheduler.persistence.AuditRepository` 接口:`record(String operator, String action, TargetType targetType, long targetId, String metaJson, String source)`(void);`findPage(String operator, String action, String targetType, Long targetId, Instant from, Instant to, int limit, int offset) → List<AuditEntry>`;`count(同过滤参数,无分页) → long`。
  - `JdbcAuditRepository` 实现。

- [ ] **Step 1: 写 V10 迁移**

```sql
-- V10: 操作审计日志(仅 operator 主动动作;系统驱动的状态迁移由 execution_shard_outcome 覆盖)。
CREATE TABLE app_audit (
  id BIGSERIAL PRIMARY KEY,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- DB 权威时钟
  operator VARCHAR(64) NOT NULL,                  -- X-Operator,缺省 'anonymous'
  action VARCHAR(64) NOT NULL,                    -- 如 task.create
  target_type VARCHAR(16) NOT NULL,               -- task|execution|shard|dag|dag_run
  target_id BIGINT NOT NULL,
  meta JSONB NULL,                                -- 紧凑后态载荷(JSON 文本;null=无)
  source VARCHAR(255) NULL                        -- 请求方标识(保留列,当前写端传 null)
);

CREATE INDEX idx_audit_target   ON app_audit (target_type, target_id, occurred_at DESC);
CREATE INDEX idx_audit_operator ON app_audit (operator, occurred_at DESC);
```

- [ ] **Step 2: 写核心模型 TargetType + AuditEntry**

```java
// scheduler-core/src/main/java/dev/scheduler/core/TargetType.java
package dev.scheduler.core;

/** 审计目标类型:与 app_audit.target_type 列值一一对应。 */
public enum TargetType {
  TASK("task"), EXECUTION("execution"), SHARD("shard"), DAG("dag"), DAG_RUN("dag_run");
  private final String db;
  TargetType(String db) { this.db = db; }
  public String db() { return db; }
}
```

```java
// scheduler-core/src/main/java/dev/scheduler/core/AuditEntry.java
package dev.scheduler.core;

import java.time.Instant;

/**
 * 一条操作审计记录(append-only 读模型)。meta 为原始 JSON 文本或 null:写端由 server 层
 * AuditRecorder 用 Jackson 序列化,持久层不依赖 Jackson,读 API 原样透出、前端 JSON.parse 展示。
 */
public record AuditEntry(
    long id,
    Instant occurredAt,
    String operator,
    String action,
    String targetType, // TargetType.db(),如 "task"
    long targetId,
    String meta,       // JSON 文本或 null
    String source) {}
```

- [ ] **Step 3: 写 AuditRepository 接口**

```java
// scheduler-persistence/src/main/java/dev/scheduler/persistence/AuditRepository.java
package dev.scheduler.persistence;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.core.TargetType;
import java.time.Instant;
import java.util.List;

/** 操作审计 append-only 仓储(只增不删不更)。 */
public interface AuditRepository {
  /** 追加一条审计;occurred_at 由 DB now() 回填。metaJson 为 JSON 文本或 null。 */
  void record(String operator, String action, TargetType targetType, long targetId,
              String metaJson, String source);

  /** 过滤 + 分页,按 occurred_at DESC, id DESC 排序;全部过滤参数可为 null/空(不过滤)。 */
  List<AuditEntry> findPage(String operator, String action, String targetType,
                            Long targetId, Instant from, Instant to, int limit, int offset);

  /** 与 findPage 相同过滤条件的 count(供 Page.total)。 */
  long count(String operator, String action, String targetType,
             Long targetId, Instant from, Instant to);
}
```

- [ ] **Step 4: 写 JdbcAuditRepository(镜像 JdbcTaskRepository 的 where/filterArgs 风格)**

```java
// scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcAuditRepository.java
package dev.scheduler.persistence;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.core.TargetType;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcAuditRepository implements AuditRepository {
  private final JdbcTemplate jdbc;
  public JdbcAuditRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

  @Override
  public void record(String operator, String action, TargetType targetType,
                     long targetId, String metaJson, String source) {
    // ?::json 把文本转 JSONB;metaJson 为 null 时 NULL::json = NULL。append-only 单 INSERT,不需幂等键。
    jdbc.update("""
        INSERT INTO app_audit (operator, action, target_type, target_id, meta, source)
        VALUES (?, ?, ?, ?, ?::json, ?)""",
        operator, action, targetType.db(), targetId, metaJson, source);
  }

  /** WHERE 片段(与 JdbcTaskRepository)配套 {@link #filterArgs}。operator 子串 ILIKE,其余等值。 */
  private String where(String operator, String action, String targetType,
                       Long targetId, Instant from, Instant to) {
    StringBuilder w = new StringBuilder();
    if (operator != null && !operator.isBlank()) w.append(" AND operator ILIKE ?");
    if (action != null && !action.isBlank()) w.append(" AND action = ?");
    if (targetType != null && !targetType.isBlank()) w.append(" AND target_type = ?");
    if (targetId != null) w.append(" AND target_id = ?");
    if (from != null) w.append(" AND occurred_at >= ?");
    if (to != null) w.append(" AND occurred_at <= ?");
    return w.toString();
  }

  private List<Object> filterArgs(String operator, String action, String targetType,
                                  Long targetId, Instant from, Instant to) {
    List<Object> a = new ArrayList<>();
    if (operator != null && !operator.isBlank()) a.add("%" + operator.trim() + "%");
    if (action != null && !action.isBlank()) a.add(action.trim());
    if (targetType != null && !targetType.isBlank()) a.add(targetType.trim());
    if (targetId != null) a.add(targetId);
    if (from != null) a.add(Timestamp.from(from));
    if (to != null) a.add(Timestamp.from(to));
    return a;
  }

  @Override
  public List<AuditEntry> findPage(String operator, String action, String targetType,
                                   Long targetId, Instant from, Instant to,
                                   int limit, int offset) {
    List<Object> a = filterArgs(operator, action, targetType, targetId, from, to);
    a.add(limit); a.add(offset);
    return jdbc.query("SELECT id, occurred_at, operator, action, target_type, target_id, meta, source"
            + " FROM app_audit WHERE 1=1" + where(operator, action, targetType, targetId, from, to)
            + " ORDER BY occurred_at DESC, id DESC LIMIT ? OFFSET ?",
        (rs, i) -> new AuditEntry(
            rs.getLong("id"), rs.getTimestamp("occurred_at").toInstant(),
            rs.getString("operator"), rs.getString("action"), rs.getString("target_type"),
            rs.getLong("target_id"), rs.getString("meta"), rs.getString("source")),
        a.toArray());
  }

  @Override
  public long count(String operator, String action, String targetType,
                    Long targetId, Instant from, Instant to) {
    Long c = jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE 1=1"
            + where(operator, action, targetType, targetId, from, to),
        Long.class, filterArgs(operator, action, targetType, targetId, from, to).toArray());
    return c == null ? 0 : c;
  }
}
```

- [ ] **Step 5: 运行 Task 1 编译,确认尚无测试时通过(Flyway 会跑 V10)**

运行:`cd /d/ai-project/scheduler && mvn -o -pl scheduler-persistence -am test -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false`
预期:BUILD SUCCESS,迁移链 V1..V10 一致(若 V10 SQL 有错,这里即炸)。

- [ ] **Step 6: 写 JdbcAuditRepositoryTest**

```java
// scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcAuditRepositoryTest.java
package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.core.TargetType;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcAuditRepositoryTest extends AbstractPostgresTest {
  private AuditRepository audits;

  @BeforeEach
  void setUp() {
    jdbc.execute("TRUNCATE app_audit RESTART IDENTITY CASCADE");
    audits = new JdbcAuditRepository(jdbc);
  }

  @Test
  void record_appendsSingleRow_occurredAtFromDbNowMetaJson() {
    audits.record("ops", "task.create", TargetType.TASK, 7L, "{\"name\":\"x\"}", "cli");
    List<Map<String, Object>> rows = jdbc.queryForList(
        "SELECT operator, action, target_type, target_id, meta, source, occurred_at FROM app_audit");
    assertEquals(1, rows.size());
    Map<String, Object> r = rows.get(0);
    assertEquals("ops", r.get("operator"));
    assertEquals("task.create", r.get("action"));
    assertEquals("task", r.get("target_type"));
    assertEquals(7L, r.get("target_id"));
    assertTrue(r.get("meta") != null, "meta 应落库:" + r.get("meta"));
    assertEquals("cli", r.get("source"));
    assertTrue(r.get("occurred_at") instanceof Timestamp, "occurred_at 应由 DB now() 回填");
  }

  @Test
  void record_nullMetaAndSource_storeSqlNull() {
    audits.record("a", "task.pause", TargetType.TASK, 1L, null, null);
    assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE operator='a'", Long.class));
    assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE meta IS NOT NULL", Long.class));
    assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM app_audit WHERE source IS NOT NULL", Long.class));
  }

  @Test
  void findPage_filtersAndPagination_descByOccurredAt() {
    // 同 tick 插入:occurred_at=now() 相同,以 id DESC 稳定平局(后插者在先)。
    audits.record("alice", "task.create", TargetType.TASK, 1L, "{\"name\":\"a\"}", null);
    audits.record("alice", "task.update", TargetType.TASK, 1L, null, null);
    audits.record("bob",   "task.create", TargetType.TASK, 2L, null, null);
    audits.record("alice", "dag.create",  TargetType.DAG,  9L, null, null); // id 最大 → 倒序首位

    assertEquals(4, audits.count(null, null, null, null, null, null));
    assertEquals(4, audits.findPage(null, null, null, null, null, null, 100, 0).size());

    // operator 子串(ILIKE)
    assertEquals(3, audits.count("ali", null, null, null, null, null));
    // action 等值
    assertEquals(2, audits.count(null, "task.create", null, null, null, null));
    // targetType 等值
    assertEquals(1, audits.count(null, null, "dag", null, null, null));
    // targetId 等值
    assertEquals(1, audits.count(null, null, null, 9L, null, null));
    // 组合精确定位 (task, 1)
    assertEquals(2, audits.count(null, null, "task", 1L, null, null));
    // 时间窗(occurred_at >= from AND <= to)
    assertEquals(4, audits.count(null, null, null, null,
        Instant.parse("2000-01-01T00:00:00Z"), Instant.parse("2200-01-01T00:00:00Z")));
    assertEquals(0, audits.count(null, null, null, null,
        Instant.parse("2200-01-01T00:00:00Z"), Instant.parse("2300-01-01T00:00:00Z")));

    // 分页 + 倒序:limit=2 → 最近 2 条(id DESC 平局 → dag.create, 再 bob 的 task.create)
    List<AuditEntry> p2 = audits.findPage(null, null, null, null, null, null, 2, 0);
    assertEquals(2, p2.size());
    assertEquals("dag.create", p2.get(0).action());
    assertEquals(9L, p2.get(0).targetId());
    assertEquals("task.create", p2.get(1).action());
    assertEquals(2L, p2.get(1).targetId());
    // offset=2 → 剩最早 2 条
    List<AuditEntry> pOff = audits.findPage(null, null, null, null, null, null, 100, 2);
    assertEquals(2, pOff.size());
    assertEquals("task.create", pOff.get(0).action());
    assertEquals(1L, pOff.get(0).targetId());
    assertEquals("task.update", pOff.get(1).action());
  }
}
```

- [ ] **Step 7: 运行 Task 1 测试确认绿**

运行:`cd /d/ai-project/scheduler && mvn -o -pl scheduler-persistence -am test -Dtest=JdbcAuditRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
预期:3 个测试 PASS。

- [ ] **Step 8: 提交**

```bash
cd /d/ai-project/scheduler && git add -A && git commit -m "feat(audit): V10 app_audit 迁移 + TargetType/AuditEntry 模型 + JdbcAuditRepository(过滤分页只读)"
```

---

### Task 2: AuditRecorder + AuditController + Bean 装配 + 读 API 测试

**Files:**
- Create: `scheduler-server/src/main/java/dev/scheduler/server/service/AuditRecorder.java`
- Create: `scheduler-server/src/main/java/dev/scheduler/server/web/AuditController.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`(加 2 个 @Bean)
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`(加读 API 用例)

**Interfaces:**
- Consumes: Task 1 的 `AuditRepository`(注成 bean)、Spring Boot 自动配置的 `ObjectMapper`。既有 `Page`/`Paging`(同在 `dev.scheduler.server.web` 包)。
- Produces:
  - `dev.scheduler.server.service.AuditRecorder`:`record(String operator, String action, TargetType target, long targetId, Map<String,Object> meta)`(source 缺省 overload,透传 null)。
  - `dev.scheduler.server.web.AuditController`:`GET /api/v1/audits` → `Page<AuditEntry>`。

- [ ] **Step 1: 写 AuditRecorder(非阻断 + anonymous 兜底 + meta 序列化)**

```java
// scheduler-server/src/main/java/dev/scheduler/server/service/AuditRecorder.java
package dev.scheduler.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.AuditRepository;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 操作审计记录器:控制器写 handler 在操作成功落库后调用,追加一条审计。
 * <ul>
 *   <li>operator 缺省/空 → 'anonymous'(服务端无认证,语义在 X-Operator 头)。</li>
 *   <li>meta 为 Map,经 ObjectMapper 序列化为 JSON 文本后交由持久层落 JSONB(persistence 无 Jackson)。</li>
 *   <li>审计非阻断:序列化或 insert 失败只 log.warn,不抛、不改变用户操作结果(操作已先提交)。</li>
 * </ul>
 */
public class AuditRecorder {
  private static final Logger log = LoggerFactory.getLogger(AuditRecorder.class);
  private final AuditRepository audits;
  private final ObjectMapper json;

  public AuditRecorder(AuditRepository audits, ObjectMapper json) {
    this.audits = audits;
    this.json = json;
  }

  /** source 为 null(保留列,未启用来源追踪)。 */
  public void record(String operator, String action, TargetType target, long targetId, Map<String, Object> meta) {
    record(operator, action, target, targetId, meta, null);
  }

  public void record(String operator, String action, TargetType target, long targetId,
                     Map<String, Object> meta, String source) {
    String who = (operator == null || operator.isBlank()) ? "anonymous" : operator;
    String metaJson = null;
    if (meta != null) {
      try {
        metaJson = json.writeValueAsString(meta);
      } catch (JsonProcessingException e) {
        log.warn("audit meta serialization failed; recording without meta: {} {} {}", who, action, targetId, e);
      }
    }
    try {
      audits.record(who, action, target, targetId, metaJson, source);
    } catch (RuntimeException e) {
      log.warn("audit record failed (non-blocking): {} {} target={} id={}", who, action, target.db(), targetId, e);
    }
  }
}
```

- [ ] **Step 2: 写 AuditController(只读 API)**

```java
// scheduler-server/src/main/java/dev/scheduler/server/web/AuditController.java
package dev.scheduler.server.web;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.persistence.AuditRepository;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** 审计只读 API:GET /api/v1/audits 过滤 + 分页,occurred_at DESC(镜像 TaskController.list 信封)。 */
@RestController
@RequestMapping("/api/v1/audits")
public class AuditController {
  private final AuditRepository audits;
  public AuditController(AuditRepository audits) { this.audits = audits; }

  @GetMapping
  public Page<AuditEntry> list(
      @RequestParam(required = false) String operator,
      @RequestParam(required = false) String action,
      @RequestParam(required = false) String targetType,
      @RequestParam(required = false) Long targetId,
      @RequestParam(required = false) String from,
      @RequestParam(required = false) String to,
      @RequestParam(required = false) Integer limit,
      @RequestParam(required = false) Integer offset) {
    Paging p = Paging.of(limit, offset);
    Instant fromT = parseInstant(from);
    Instant toT = parseInstant(to);
    return new Page<>(audits.findPage(operator, action, targetType, targetId, fromT, toT,
            p.limit(), p.offset()),
        audits.count(operator, action, targetType, targetId, fromT, toT),
        p.offset(), p.limit());
  }

  private static Instant parseInstant(String s) {
    if (s == null || s.isBlank()) return null;
    try {
      return Instant.parse(s); // ISO-8601 带偏移
    } catch (DateTimeParseException e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "invalid `from`/`to` (expected ISO-8601 with offset): " + s);
    }
  }
}
```

- [ ] **Step 3: Beans.java 装配 auditRepository + auditRecorder**

在既有仓储 @Bean 附近(`taskRepository` 之后)插入两个 @Bean;并补 imports。

```java
// scheduler-server/.../config/Beans.java 新增方法与 imports
@Bean
AuditRepository auditRepository(JdbcTemplate jdbc) {
  return new JdbcAuditRepository(jdbc);
}

@Bean
AuditRecorder auditRecorder(AuditRepository audits, ObjectMapper json) {
  return new AuditRecorder(audits, json);
}
```

新增 imports(顶部 import 区):
```java
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.scheduler.persistence.AuditRepository;
import dev.scheduler.persistence.JdbcAuditRepository;
import dev.scheduler.server.service.AuditRecorder;
```

- [ ] **Step 4: 运行编译,确认 AuditRecorder/AuditController/Bean 无错**

运行:`cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false`
预期:BUILD SUCCESS(尚无 server 侧新测试)。

- [ ] **Step 5: ApiIntegrationTest 加读 API 用例(直种子读端,不依赖写端)**

在既有任务分页用例(如 `tasksList_nameSubstringAndPausedFilterPagination`)之后加两个 @Test。文件顶部已 `import java.time.Instant`。

```java
/** 审计读 API:GET /api/v1/audits 过滤 + 分页信封,occurred_at DESC。写端见 taskWriteActions_areAudited 等用例;此处直种子读端。 */
@Test
void auditReadApi_filtersAndPagination() throws Exception {
  long t1 = postTask("audit-seed-a"); // 取真实 task id 作 targetId
  long t2 = postTask("audit-seed-b");
  jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta, source)"
          + " VALUES ('alice','task.create','task',?,'{\"name\":\"audit-seed-a\"}','cli')", t1);
  jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta)"
          + " VALUES ('alice','task.update','task',?, null)", t1);
  jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, meta)"
          + " VALUES ('bob','task.create','task',?, '{}')", t2);

  // 全量信封
  mvc.perform(get("/api/v1/audits"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.total").value(3))
      .andExpect(jsonPath("$['items']", hasSize(3)))
      .andExpect(jsonPath("$.limit").value(100))
      .andExpect(jsonPath("$.offset").value(0));
  // operator 子串
  mvc.perform(get("/api/v1/audits").param("operator", "ali"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.total").value(2));
  // action 等值
  mvc.perform(get("/api/v1/audits").param("action", "task.update"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.total").value(1));
  // targetId 等值
  mvc.perform(get("/api/v1/audits").param("targetId", String.valueOf(t1)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.total").value(2));
  // 分页:limit=1 offset=2 → 剩 1 条,total 恒 3
  mvc.perform(get("/api/v1/audits").param("limit", "1").param("offset", "2"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$['items']", hasSize(1)))
      .andExpect(jsonPath("$.total").value(3));
}

/** 审计读 API:occurred_at 时间窗过滤(from/to)。窗口用 ±1h 规避 JVM/容器时钟偏斜(见 ExecutorWorkerTest 教训)。 */
@Test
void auditReadApi_timeWindow() throws Exception {
  jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, occurred_at)"
      + " VALUES ('old','t','task',1, now() - interval '2 days')");
  jdbc.update("INSERT INTO app_audit (operator, action, target_type, target_id, occurred_at)"
      + " VALUES ('new','t','task',2, now())");
  // from=1h 前 → 只含 'new'(now 行)
  mvc.perform(get("/api/v1/audits").param("from", Instant.now().minusSeconds(3600).toString()))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.total").value(1))
      .andExpect(jsonPath("$['items'][0].operator").value("new"));
  // to=1h 前 → 只含 'old'(2 天前行)
  mvc.perform(get("/api/v1/audits").param("to", Instant.now().minusSeconds(3600).toString()))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.total").value(1))
      .andExpect(jsonPath("$['items'][0].operator").value("old"));
}
```

- [ ] **Step 6: 运行 ApiIntegrationTest 确认绿**

运行:`cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test -Dtest=ApiIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false`
预期:既有 + 新增用例全 PASS(若某既有用例因 V10 表干扰失败——不会,truncate 未含 app_audit,且为 append-only 不影响既有断言;留意观察)。

- [ ] **Step 7: 提交**

```bash
cd /d/ai-project/scheduler && git add -A && git commit -m "feat(audit): AuditRecorder(非阻断)+ AuditController GET /api/v1/audits + Beans 装配读 API 测试"
```

---

### Task 3: 三个控制器 15 个写端点接 operator + Auditor.record

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/TaskController.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/ExecutionController.java`
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/web/DagController.java`
- Test: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`(加 3 个写端审计用例)

**Interfaces:**
- Consumes: Task 2 的 `AuditRecorder` bean(三个控制器构造器各加一参)、`TargetType`.
- Produces: 全部 15 个写端点在操作成功落库后追加意图审计(action 词汇见下表)。

| action | endpoint | target | targetId | meta |
|---|---|---|---|---|
| `task.create` | POST /tasks | TASK | 新建 id | taskMeta(Task) |
| `task.update` | PUT /tasks/{id} | TASK | id | taskMeta(Task) |
| `task.pause` | POST /tasks/{id}/pause | TASK | id | `{}` |
| `task.resume` | POST /tasks/{id}/resume | TASK | id | `{}` |
| `task.delete` | DELETE /tasks/{id} | TASK | id | `{}` |
| `task.trigger` | POST /tasks/{id}/trigger | TASK | id | `{}` |
| `execution.rerun` | POST /executions/{id}/rerun | EXECUTION | 新建 id | `{}` |
| `execution.cancel` | POST /executions/{id}/cancel | EXECUTION | id | `{}`(仅实际发生取消时) |
| `shard.requeue` | POST /executions/shards/{shardId}/requeue | SHARD | shardId | `{}` |
| `dag.create` | POST /dags | DAG | 新建 id | `{name}` |
| `dag.pause` / `dag.resume` | POST /dags/{id}/pause、/resume | DAG | id | `{}` |
| `dag.trigger` | POST /dags/{id}/trigger | DAG | id | `{}` |
| `dag_run.cancel` | POST /dags/runs/{runId}/cancel | DAG_RUN | runId | `{}`(仅实际发生取消时) |
| `dag_node.rerun` | POST /dags/runs/{runId}/nodes/{nodeId}/rerun | DAG_RUN | runId | `{nodeId}` |

**规则**:每个被改的写 handler 方法签名加 `@RequestHeader(value = "X-Operator", required = false) String operator`;在**操作成功落库后、return 前**调 `auditor.record(...)`。幂等短路(已取消)与冲突/404 分支**不记录**(动作未实际发生)。

- [ ] **Step 1: TaskController — 注入 + taskMeta 辅助 + 6 个写端点**

```java
// 头部新增 imports
import dev.scheduler.core.TargetType;
import dev.scheduler.server.service.AuditRecorder;
import java.util.Map; // 若已无则加(用于 Map.of() 空载荷)
```

```java
// 字段与构造器:加 auditor
private final AuditRecorder auditor;
public TaskController(TaskRepository tasks, ShardRepository shards,
                      AvailableHandlerRefs availableRefs, AuditRecorder auditor) {
  this.tasks = tasks;
  this.shards = shards;
  this.availableRefs = availableRefs;
  this.auditor = auditor;
}
```

```java
// 新增私有辅助:审计 meta = 操作后已确认的 Task 后态紧凑字段(非 diff)。
private java.util.Map<String, Object> taskMeta(Task t) {
  java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
  m.put("name", t.name());
  m.put("handlerRef", t.handlerRef());
  m.put("cron", t.cron());
  m.put("shardCount", t.shardCount());
  m.put("timeoutSeconds", t.timeoutSeconds());
  m.put("maxRetries", t.maxRetries());
  m.put("backoffMs", t.backoffMs());
  m.put("retryMode", t.retryMode());
  m.put("retryCapMs", t.retryCapMs());
  m.put("retryBudgetMs", t.retryBudgetMs());
  m.put("enabled", t.enabled());
  m.put("paused", t.paused());
  return m;
}
```

```java
// create:签名加 operator;落库后 record
@PostMapping
public ResponseEntity<Task> create(
    @RequestHeader(value = "X-Operator", required = false) String operator,
    @RequestBody CreateTaskRequest req) {
  // ...既有校验不变...
  Task created = tasks.create(new Task(...));
  auditor.record(operator, "task.create", TargetType.TASK, created.id(), taskMeta(created));
  return ResponseEntity.status(HttpStatus.CREATED).body(created);
}
```

```java
// update:签名加 operator;findById 后 record 后态
@PutMapping("/{id}")
public Task update(@RequestHeader(value = "X-Operator", required = false) String operator,
                   @PathVariable long id, @RequestBody UpdateTaskRequest req) {
  // ...既有校验不变...
  if (!tasks.update(id, updated)) throw notFound("task " + id);
  Task saved = tasks.findById(id).orElseThrow(() -> notFound("task " + id));
  auditor.record(operator, "task.update", TargetType.TASK, saved.id(), taskMeta(saved));
  return saved;
}
```

```java
// pause / resume / delete:各自签名加 operator;落库后 record
@PostMapping("/{id}/pause")
public Task pause(@RequestHeader(value = "X-Operator", required = false) String operator,
                  @PathVariable long id) {
  requireTask(id);
  tasks.setPaused(id, true);
  auditor.record(operator, "task.pause", TargetType.TASK, id, Map.of());
  return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
}

@PostMapping("/{id}/resume")
public Task resume(@RequestHeader(value = "X-Operator", required = false) String operator,
                   @PathVariable long id) {
  requireTask(id);
  tasks.setPaused(id, false);
  auditor.record(operator, "task.resume", TargetType.TASK, id, Map.of());
  return tasks.findById(id).orElseThrow(() -> notFound("task " + id));
}

@DeleteMapping("/{id}")
public ResponseEntity<Void> delete(@RequestHeader(value = "X-Operator", required = false) String operator,
                                   @PathVariable long id) {
  // ...既有 blockers 校验/删除不变;删除成功(未 404)后 record,再 return
  if (blockers.isEmpty() && tasks.delete(id)) {
    auditor.record(operator, "task.delete", TargetType.TASK, id, Map.of());
    return ResponseEntity.noContent().build();
  }
  // 保持既有 delete 方法剩余逻辑(冲突/404 分支原样,不 record)
  ...
}
```

```java
// trigger:拉出 run 局部变量,record 后返回
@PostMapping("/{id}/trigger")
public ResponseEntity<Execution> trigger(@RequestHeader(value = "X-Operator", required = false) String operator,
                                         @PathVariable long id) {
  Execution run = manualRun(id, UUID.randomUUID().toString());
  auditor.record(operator, "task.trigger", TargetType.TASK, id, Map.of());
  return ResponseEntity.status(HttpStatus.CREATED).body(run);
}
```

- [ ] **Step 2: ExecutionController — 注入 + rerun/cancel/requeue**

```java
// 头部新增 imports
import dev.scheduler.core.TargetType;
import dev.scheduler.server.service.AuditRecorder;
import java.util.Map; // 若已无则加
// 构造器加 auditor
private final AuditRecorder auditor;
public ExecutionController(ExecutionRepository executions, ShardRepository shards,
                           JdbcTemplate jdbc, AuditRecorder auditor) {
  ...
  this.auditor = auditor;
}
```

```java
// rerun:签名加 operator;新建轮后 record(目标 = 新 execution id)
@PostMapping("/{id}/rerun")
public ResponseEntity<Execution> rerun(@RequestHeader(value = "X-Operator", required = false) String operator,
                                       @PathVariable long id) {
  // ...既有校验/查源不变...
  Execution created = shards.createParentWithShards(
      source.taskId(), key, source.shardCount(), source.args(), source.id());
  auditor.record(operator, "execution.rerun", TargetType.EXECUTION, created.id(), Map.of());
  return ResponseEntity.status(HttpStatus.CREATED).body(created);
}
```

```java
// requeue:签名加 operator;重排后 record(目标 = shardId)
@PostMapping("/shards/{shardId}/requeue")
public ResponseEntity<Shard> requeue(@RequestHeader(value = "X-Operator", required = false) String operator,
                                     @PathVariable long shardId) {
  // ...既有校验不变...
  shards.requeueShard(shardId);
  auditor.record(operator, "shard.requeue", TargetType.SHARD, shardId, Map.of());
  return ResponseEntity.ok(shards.findShard(shardId).orElseThrow(() -> notFound("shard " + shardId)));
}
```

```java
// cancel:签名加 operator;仅在实际取消发生的两分支各 record 一次(幂等 CANCELED 短路返回、409 分支不记)
@PostMapping("/{id}/cancel")
public ResponseEntity<Execution> cancel(@RequestHeader(value = "X-Operator", required = false) String operator,
                                        @PathVariable long id) {
  Execution e = executions.findById(id).orElseThrow(() -> notFound("execution " + id));
  if (e.status() == ExecutionStatus.CANCELED) return ResponseEntity.ok(e); // 幂等:不记
  if (e.status() != ExecutionStatus.DUE) {
    throw new ResponseStatusException(HttpStatus.CONFLICT, ...); // 409:不记
  }
  if (shards.hasRunningShard(id)) {
    shards.requestCancelParent(id);
    auditor.record(operator, "execution.cancel", TargetType.EXECUTION, id, Map.of());
    return ResponseEntity.accepted().body(derived(id));
  }
  shards.cancelParentImmediate(id);
  auditor.record(operator, "execution.cancel", TargetType.EXECUTION, id, Map.of());
  return ResponseEntity.ok(derived(id));
}
```

- [ ] **Step 3: DagController — 注入 + create/pause/resume/trigger/cancel/rerunNode**

```java
// 头部新增 imports
import dev.scheduler.core.TargetType;
import dev.scheduler.server.service.AuditRecorder;
import java.util.Map;
// 构造器加 auditor
private final AuditRecorder auditor;
public DagController(DagRepository dags, ShardRepository shards,
                     DagEngine dagEngine, AuditRecorder auditor) {
  this.dags = dags;
  this.shards = shards;
  this.dagEngine = dagEngine;
  this.query = new DagQueryService(dags, shards);
  this.auditor = auditor;
}
```

```java
// create:签名加 operator;record 后返回
@PostMapping
public ResponseEntity<Dag> create(@RequestHeader(value = "X-Operator", required = false) String operator,
                                  @RequestBody CreateDagRequest req) {
  // ...既有校验不变...
  Dag created = dags.createDag(req.name(), req.description(), req.cron(), nodes, edges);
  auditor.record(operator, "dag.create", TargetType.DAG, created.id(), Map.of("name", created.name()));
  return ResponseEntity.status(HttpStatus.CREATED).body(created);
}
```

```java
// pause / resume / trigger:拆分一行的既有写法,各自加 operator + record
@PostMapping("/{id}/pause")
public Dag pause(@RequestHeader(value = "X-Operator", required = false) String operator, @PathVariable long id) {
  requireDag(id);
  dags.setPaused(id, true);
  auditor.record(operator, "dag.pause", TargetType.DAG, id, Map.of());
  return byId(id);
}

@PostMapping("/{id}/resume")
public Dag resume(@RequestHeader(value = "X-Operator", required = false) String operator, @PathVariable long id) {
  requireDag(id);
  dags.setPaused(id, false);
  auditor.record(operator, "dag.resume", TargetType.DAG, id, Map.of());
  return byId(id);
}

@PostMapping("/{id}/trigger")
public ResponseEntity<DagRun> trigger(@RequestHeader(value = "X-Operator", required = false) String operator,
                                      @PathVariable long id) {
  requireDag(id);
  DagRun run = dags.createManualRun(id);
  auditor.record(operator, "dag.trigger", TargetType.DAG, id, Map.of());
  return ResponseEntity.status(HttpStatus.CREATED).body(run);
}
```

```java
// cancel:签名加 operator;在 finalizeRun 之后记录一次(仅实际发生取消;幂等/409 分支不记)
@PostMapping("/runs/{runId}/cancel")
public ResponseEntity<RunDetail> cancel(@RequestHeader(value = "X-Operator", required = false) String operator,
                                        @PathVariable long runId) {
  // ...既有前置(幂等返回 / 409 分支)与级联逻辑不变...
  dags.finalizeRun(runId, DagRunStatus.CANCELED, "cancelled by operator");
  auditor.record(operator, "dag_run.cancel", TargetType.DAG_RUN, runId, Map.of());
  return ResponseEntity.ok(query.runDetail(runId).orElseThrow());
}
```

```java
// rerunNode:签名加 operator;回绕后 record(targetId=runId, meta={nodeId})
@PostMapping("/runs/{runId}/nodes/{nodeId}/rerun")
public ResponseEntity<DagRunNode> rerunNode(
    @RequestHeader(value = "X-Operator", required = false) String operator,
    @PathVariable long runId, @PathVariable long nodeId) {
  // ...既有前置校验不变...
  DagRunNode node = dagEngine.rerunNode(runId, nodeId);
  auditor.record(operator, "dag_node.rerun", TargetType.DAG_RUN, runId, Map.of("nodeId", nodeId));
  return ResponseEntity.ok(node);
}
```

- [ ] **Step 4: 编译确认三控制器 + 样式无损**

运行:`cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false`
预期:BUILD SUCCESS,无新破坏(既有 ApiIntegrationTest 尚缺写端断言,先不动其预期)。

- [ ] **Step 5: ApiIntegrationTest 加写端审计用例(3 个 @Test)**

新增 imports:`import dev.scheduler.core.ExecutionStatus;`(若未引入,用于 cancel 用例断言)、`import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;`(TaskController.delete 已用,若未引入)。

```java
/** 审计写端:TaskController create/update/pause/resume/trigger/delete;X-Operator 缺省记 anonymous;meta 为后态。 */
@Test
void taskWriteActions_areAudited() throws Exception {
  // create with X-Operator → operator 为该头,meta 含后态 name
  long id = objectMapper.readTree(mvc.perform(post("/api/v1/tasks").header("X-Operator", "alice")
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"name\":\"audited-task\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
              + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
      .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
  mvc.perform(get("/api/v1/audits").param("operator", "alice").param("action", "task.create")
          .param("targetId", String.valueOf(id)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.total").value(1))
      .andExpect(jsonPath("$['items'][0].targetType").value("task"))
      .andExpect(jsonPath("$['items'][0].meta").value(org.hamcrest.Matchers.containsString("audited-task")));

  // update → task.update,targetId=id,meta 为后态(改名后)
  mvc.perform(put("/api/v1/tasks/" + id).header("X-Operator", "alice")
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"name\":\"renamed-audit\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
              + "\"cron\":\"0 */6 * * * *\",\"shardCount\":1}"))
      .andExpect(status().isOk());
  mvc.perform(get("/api/v1/audits").param("action", "task.update").param("targetId", String.valueOf(id)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.total").value(1))
      .andExpect(jsonPath("$['items'][0].meta").value(org.hamcrest.Matchers.containsString("renamed-audit")));

  // 无 X-Operator 头 → operator 兜底 'anonymous'
  mvc.perform(post("/api/v1/tasks/" + id + "/pause")).andExpect(status().isOk());
  mvc.perform(post("/api/v1/tasks/" + id + "/resume")).andExpect(status().isOk());
  mvc.perform(get("/api/v1/audits").param("action", "task.pause"))
      .andExpect(status().isOk()).andExpect(jsonPath("$['items'][0].operator").value("anonymous"));
  mvc.perform(get("/api/v1/audits").param("action", "task.resume"))
      .andExpect(status().isOk()).andExpect(jsonPath("$['items'][0].operator").value("anonymous"));

  // trigger → task.trigger(目标 = 任务 id)
  mvc.perform(post("/api/v1/tasks/" + id + "/trigger").header("X-Operator", "alice"))
      .andExpect(status().isCreated());
  mvc.perform(get("/api/v1/audits").param("action", "task.trigger").param("targetId", String.valueOf(id)))
      .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));

  // 该 id 已被 trigger(有 execution)→ delete 409(不记 action);换个未触发的任务验 delete
  mvc.perform(delete("/api/v1/tasks/" + id)).andExpect(status().isConflict());
  long delId = objectMapper.readTree(mvc.perform(post("/api/v1/tasks").header("X-Operator", "bob")
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"name\":\"audit-del\",\"kind\":\"cron\",\"handlerRef\":\"demo\","
              + "\"cron\":\"" + CRON + "\",\"shardCount\":1}"))
      .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asLong();
  mvc.perform(delete("/api/v1/tasks/" + delId).header("X-Operator", "bob")).andExpect(status().isNoContent());
  mvc.perform(get("/api/v1/audits").param("action", "task.delete").param("targetId", String.valueOf(delId)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$['items'][0].operator").value("bob"));
}

/** 审计写端:ExecutionController rerun/cancel + shard.requeue(目标均为操作者主动动作)。 */
@Test
void executionWriteActions_areAudited() throws Exception {
  long id = postTask("audit-exec");

  // rerun(确定性终结源轮后):execution.rerun,target = 新轮 id
  long execId = triggerParent(id);
  for (Long sid : jdbc.queryForList("SELECT id FROM execution_shard WHERE execution_id=?", Long.class, execId))
    jdbc.update("UPDATE execution_shard SET status='SUCCESS', finished_at=now() WHERE id=?", sid);
  jdbc.update("UPDATE execution SET status='SUCCESS', finished_at=now() WHERE id=? AND status='DUE'", execId);
  long newId = objectMapper.readTree(mvc.perform(post("/api/v1/executions/" + execId + "/rerun")
          .header("X-Operator", "carol")).andExpect(status().isCreated()).andReturn()
      .getResponse().getContentAsString()).get("id").asLong();
  mvc.perform(get("/api/v1/audits").param("action", "execution.rerun").param("targetId", String.valueOf(newId)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$['items'][0].operator").value("carol"));

  // requeue FAILED+dead_letter shard → shard.requeue
  Shard dlq = seedDeadLetter(id);
  mvc.perform(post("/api/v1/executions/shards/" + dlq.id() + "/requeue").header("X-Operator", "carol"))
      .andExpect(status().isOk());
  mvc.perform(get("/api/v1/audits").param("action", "shard.requeue").param("targetId", String.valueOf(dlq.id())))
      .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));

  // cancel(无 RUNNING shard → 直取消)→ execution.cancel
  long cancelTarget = triggerParent(id);
  mvc.perform(post("/api/v1/executions/" + cancelTarget + "/cancel").header("X-Operator", "carol"))
      .andExpect(status().isOk());
  mvc.perform(get("/api/v1/audits").param("action", "execution.cancel").param("targetId", String.valueOf(cancelTarget)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$['items'][0].operator").value("carol"));
}

/** 审计写端:DagController create/pause/resume/trigger + dag_run.cancel + dag_node.rerun(meta 含 nodeId)。 */
@Test
void dagWriteActions_areAudited() throws Exception {
  long t = postTask("audit-dag-task");
  long dagId = postDag("audit-dag", new long[]{t}, new String[]{"A"}, new String[][]{});
  // postDag 无 X-Operator → anonymous
  mvc.perform(get("/api/v1/audits").param("action", "dag.create").param("targetId", String.valueOf(dagId)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$['items'][0].operator").value("anonymous"))
      .andExpect(jsonPath("$['items'][0].meta").value(org.hamcrest.Matchers.containsString("audit-dag")));

  // pause / resume
  mvc.perform(post("/api/v1/dags/" + dagId + "/pause")).andExpect(status().isOk());
  mvc.perform(get("/api/v1/audits").param("action", "dag.pause")).andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));
  mvc.perform(post("/api/v1/dags/" + dagId + "/resume")).andExpect(status().isOk());
  mvc.perform(get("/api/v1/audits").param("action", "dag.resume")).andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));

  // trigger → dag.trigger
  long runId = triggerDag(dagId);
  mvc.perform(get("/api/v1/audits").param("action", "dag.trigger").param("targetId", String.valueOf(dagId)))
      .andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));

  // dag_run.cancel(scanOnce 后 A RUNNING → cancel 全节点级联终态)→ 记录一次
  dagEngine.scanOnce();
  mvc.perform(post("/api/v1/dags/runs/" + runId + "/cancel").header("X-Operator", "dave"))
      .andExpect(status().isOk());
  mvc.perform(get("/api/v1/audits").param("action", "dag_run.cancel").param("targetId", String.valueOf(runId)))
      .andExpect(status().isOk()).andExpect(jsonPath("$['items'][0].operator").value("dave"));

  // dag_node.rerun:终态节点可重跑(rerunNode 仅要求节点 isTerminal();A 已 CANCELED)→ targetId=runId, meta 含 nodeId
  long aId = dagNodeId(runId, "A");
  mvc.perform(post("/api/v1/dags/runs/" + runId + "/nodes/" + aId + "/rerun").header("X-Operator", "dave"))
      .andExpect(status().isOk());
  mvc.perform(get("/api/v1/audits").param("action", "dag_node.rerun").param("targetId", String.valueOf(runId)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$['items'][0].operator").value("dave"))
      .andExpect(jsonPath("$['items'][0].meta").value(org.hamcrest.Matchers.containsString("\"nodeId\":" + aId)));
}
```

- [ ] **Step 6: 运行 ApiIntegrationTest 确认全绿**

运行:`cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test -Dtest=ApiIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false`
预期:既有全部用例 + 新增 3 个写端审计用例 PASS。若 `dagWriteActions_areAudited` 中 CANCELED 节点随 dag 取消重被引擎特殊处理导致 rerun 非 200,按「Rulings」记一条修正(把 A 改为经 SUCCESS 终态后 rerun,复用既有 `dagNodeRerun_terminalRestartsAndDerivesAgain` 已完成同一动作)。

- [ ] **Step 7: 提交**

```bash
cd /d/ai-project/scheduler && git add -A && git commit -m "feat(audit): Task/Execution/Dag 三控制器 15 写端点接 X-Operator + AuditRecorder + 集成断言"
```

---

### Task 4: 前端 — types / client / 审计页 / 侧边栏第 6 入口

**Files:**
- Modify: `web/src/api/types.ts`(加 `AuditEntry`)
- Modify: `web/src/api/client.ts`(加 `ListAuditsParams` + `listAudits`)
- Create: `web/src/pages/AuditPage.tsx`
- Modify: `web/src/App.tsx`(NAV 第 6 项 + Route)

**Interfaces:**
- Consumes: 后端 `Page<AuditEntry>` 契约(token: `items/total/offset/limit`,`AuditEntry`: `id/occurredAt/operator/action/targetType/targetId/meta/source`,`meta` 为原始 JSON 文本)。既有 `qstr`/`req`/`Page<T>`/`Pager`/`useInterval` 组件。
- Produces: `AuditEntry` 类型、`listAudits(params)` client、`AuditPage`。

- [ ] **Step 1: types.ts 加 AuditEntry**

```ts
// web/src/api/types.ts 追加(meta 为后端原样透出的 JSON 文本,展示时 JSON.parse)
export interface AuditEntry {
  id: number;
  occurredAt: string;
  operator: string;
  action: string;
  targetType: string;
  targetId: number;
  meta: string | null;
  source: string | null;
}
```

- [ ] **Step 2: client.ts 加 listAudits**

```ts
// imports 的 type 列表加 AuditEntry
import type { AuditEntry, /* ...既有... */ } from './types';

// ---- 审计(只读) ----
export interface ListAuditsParams {
  operator?: string;
  action?: string;
  targetType?: string;
  targetId?: number;
  from?: string; // ISO-8601 with offset
  to?: string;
  limit?: number;
  offset?: number;
}
export const listAudits = (p: ListAuditsParams = {}): Promise<Page<AuditEntry>> =>
  req<Page<AuditEntry>>(`/api/v1/audits${qstr(p)}`);
```

- [ ] **Step 3: 写 AuditPage.tsx(镜像 DlqPage 的过滤 + 分页 + 轮询脚手架)**

```tsx
// web/src/pages/AuditPage.tsx
import { useCallback, useEffect, useState } from 'react';
import { listAudits } from '../api/client';
import { useInterval } from '../lib/useInterval';
import { AuditEntry } from '../api/types';
import Pager from '../components/Pager';

const PAGE_SIZE = 20;
const ACTIONS = [
  'task.create', 'task.update', 'task.pause', 'task.resume', 'task.delete', 'task.trigger',
  'execution.rerun', 'execution.cancel', 'shard.requeue',
  'dag.create', 'dag.pause', 'dag.resume', 'dag.trigger', 'dag_run.cancel', 'dag_node.rerun',
];
const TARGET_TYPES = ['task', 'execution', 'shard', 'dag', 'dag_run'];

const fmt = (t: string) => (t ? new Date(t).toLocaleString() : '—');
/** meta 是后端透出的 JSON 文本,解析为紧凑摘要展示。 */
const metaSummary = (m: string | null) => {
  if (!m) return '—';
  try {
    const o = JSON.parse(m);
    const keys = Object.keys(o);
    return keys.length === 0
      ? '{}'
      : keys.map((k) => `${k}=${JSON.stringify(o[k])}`).join(' · ');
  } catch {
    return m;
  }
};

export default function AuditPage() {
  const [rows, setRows] = useState<AuditEntry[]>([]);
  const [total, setTotal] = useState(0);
  const [operator, setOperator] = useState('');
  const [action, setAction] = useState('');
  const [targetType, setTargetType] = useState('');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [offset, setOffset] = useState(0);
  const [err, setErr] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const page = await listAudits({
        operator: operator === '' ? undefined : operator,
        action: action === '' ? undefined : action,
        targetType: targetType === '' ? undefined : targetType,
        from: from === '' ? undefined : from,
        to: to === '' ? undefined : to,
        limit: PAGE_SIZE,
        offset,
      });
      setRows(page.items);
      setTotal(page.total);
      setErr(null);
    } catch (e) { setErr(String(e)); }
  }, [operator, action, targetType, from, to, offset]);

  useEffect(() => { load(); }, [load]);
  useInterval(load, 5000);

  return (
    <div>
      <div className="page-head">
        <div>
          <h1 className="page-title">审计</h1>
          <p className="page-sub">操作者主动动作的 append-only 审计;系统驱动的状态迁移不重复记录</p>
        </div>
        {total > 0 && <div className="text-sm text-slate-500">{total} 条</div>}
      </div>

      {err && <div className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{err}</div>}

      <div className="toolbar">
        <label className="field">
          <span className="label">操作者</span>
          <input className="input" placeholder="子串匹配" value={operator}
            onChange={(e) => { setOperator(e.target.value); setOffset(0); }} />
        </label>
        <label className="field">
          <span className="label">动作</span>
          <select className="input" value={action}
            onChange={(e) => { setAction(e.target.value); setOffset(0); }}>
            <option value="">全部</option>
            {ACTIONS.map((a) => <option key={a} value={a}>{a}</option>)}
          </select>
        </label>
        <label className="field">
          <span className="label">目标类型</span>
          <select className="input" value={targetType}
            onChange={(e) => { setTargetType(e.target.value); setOffset(0); }}>
            <option value="">全部</option>
            {TARGET_TYPES.map((t2) => <option key={t2} value={t2}>{t2}</option>)}
          </select>
        </label>
        <label className="field">
          <span className="label">开始</span>
          <input className="input" type="datetime-local" value={from}
            onChange={(e) => { setFrom(e.target.value); setOffset(0); }} />
        </label>
        <label className="field">
          <span className="label">结束</span>
          <input className="input" type="datetime-local" value={to}
            onChange={(e) => { setTo(e.target.value); setOffset(0); }} />
        </label>
      </div>

      {rows.length === 0 ? (
        <div className="card p-10 text-center text-sm text-slate-400">暂无操作审计记录</div>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>时间</th>
                <th>操作者</th>
                <th>动作</th>
                <th>目标</th>
                <th>meta</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.id}>
                  <td className="text-xs text-slate-500">{fmt(r.occurredAt)}</td>
                  <td className="font-mono text-xs">{r.operator}</td>
                  <td><span className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700">{r.action}</span></td>
                  <td className="font-mono text-xs">{r.targetType}:{r.targetId}</td>
                  <td className="break-words font-mono text-xs text-slate-600">{metaSummary(r.meta)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <Pager total={total} offset={offset} limit={PAGE_SIZE} onPage={setOffset} />
    </div>
  );
}
```

- [ ] **Step 4: App.tsx 加第 6 导航 + 路由**

```tsx
import AuditPage from './pages/AuditPage';

const NAV = [
  { to: '/tasks', label: '任务', icon: '▤' },
  { to: '/executions', label: '执行', icon: '↻' },
  { to: '/dlq', label: 'DLQ', icon: '⚠' },
  { to: '/dags', label: '工作流', icon: '⌗' },
  { to: '/metrics', label: '指标', icon: '▦' },
  { to: '/audits', label: '审计', icon: '≡' }, // 第 6 入口
];

// Routes 内追加
<Route path="/audits" element={<AuditPage />} />
```

- [ ] **Step 5: 前端类型检查 + 构建**

运行:`cd /d/ai-project/scheduler/web && npx tsc --noEmit`
预期:无 TS 错误。随后若需完整构建:`npm run build`(确认 Tailwind/路由产物正常)。

- [ ] **Step 6: 提交**

```bash
cd /d/ai-project/scheduler && git add -A && git commit -m "feat(web): 审计页 + listAudits client + 侧边栏第 6 入口"
```

---

### Task 5: 全量构建 + 回归验证

**Files:**(无代码改动,仅验证)
- 验证:scheduler 根模块全量离线构建 + 全测试。

**Interfaces:**
- Consumes:Task 1-4 全部改动。

- [ ] **Step 1: 离线安装到本地仓库(跳过测试,快速暴露编译断点)**

运行:`cd /d/ai-project/scheduler && mvn -o -T1C install -DskipTests`
预期:BUILD SUCCESS(四模块全过——core/persistence/server/worker,含新 migration 编译)。

- [ ] **Step 2: 全量测试(回归)**

运行:`cd /d/ai-project/scheduler && mvn -o -T1C test`
预期:BUILD SUCCESS,全模块测试绿(含新 V10 Flyway 迁移在每块 Testcontainers 库上跑,既有用例不受 append-only 表影响)。

- [ ] **Step 3: 前端构建复核(若有未跑,须在此跑)**

运行:`cd /d/ai-project/scheduler/web && npm run build`
预期:产物生成无错(第 6 页路由已含)。

- [ ] **Step 4: 提交(若前面有未提交的回归修正)或确认工作树干净**

运行:`cd /d/ai-project/scheduler && git status`
预期:工作树干净(或仅含最后一条验证性提交)。

---

## Self-Review

**Spec 覆盖核对(逐节↔任务):** §1 数据模型(V10)↔ T1;§2 捕获点 AuditRecorder ↔ T2;§3 语义 → Global Constraints(DB now、非阻断、append-only);§4 action 清单 + meta 载荷 ↔ T3 的 15 端点明细表;§5 读 API 参数清单 + Page ↔ T2;§6 UI 第 6 入口 ↔ T4。全部覆盖。

**Placeholder 扫描:** 无 TBD/TODO;每个任务给出完整 SQL/类/方法/测试,数值与签名与既有实况(TaskController list 信封、ExecutionController rerun/cancel/requeue、DagController create/trigger/cancel/rerunNode、我之前读过的真实签名)对齐;无「类似 Task N」「补充处理」式占位。Task 3 的 DagController.create/pause/trigger/cancel/rerunNode 与 ExecutionController 均给了精确改法;delete 的既有冲突/404 分支以「保持原样、不 record」交代,非省略。

**类型一致性核对:** `TargetType.db()`/`AuditEntry` 全字段在 T1→T2→T3 签名完全一致(`${targetType}` 传 `TargetType`,`meta` 传 `String`(== 序列化后 JSON);`AuditController` 把 `targetType` 的 String 直传 repo(与 DB 列值一致));前端 `AuditEntry` 与后端字段一一对应;`listAudits` 参数与后端 @RequestParam 同名。

**已裁决/已知取舍(供执行时 reference):**
- `meta` 以原始 JSON 文本 String 贯穿持久层与读 API(而非持久层引入 Jackson 反序列化为 Map);前端 `metaSummary` 用 `JSON.parse`。忠实于「无新增依赖」全局约束。
- `execution.cancel`/`dag_run.cancel` 只在**实际发生取消**的分支记录一次(幂等短路/409 不记)。
- 写端 `source` 一律传 null(保留列但未启用来源追踪;Spec §3 允许「忽略传 null」)。
- `dag_node.rerun` 目标为 `dag_run`、targetId=`runId`、meta={nodeId}(Spec §4 明确)。
- 若 `dagWriteActions_areAudited` 里 CANCELED 节点重跑被引擎拒绝(理论上 `isTerminal()` 含 CANCELED,应通过),执行者改经 SUCCESS 终态后 rerun 即可(既有 `dagNodeRerun_terminalRestartsAndDerivesAgain` 已证该路径);此为 test-only 修正,不动实现。