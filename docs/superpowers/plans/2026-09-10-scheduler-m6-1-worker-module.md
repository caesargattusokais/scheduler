# M6.1 分布式 worker 面实现计划 —— worker 模块骨架 + 注册心跳

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建成可独立启动的 `scheduler-worker` 执行进程：从 `scheduler-server` 迁移 execute/handler/retry 三包到 worker（`dev.scheduler.worker.*`），新增 `worker` 表 + 心跳注册，使 worker 进程能向共享 Postgres 注册并心跳上报自己服务的 handler refs；server（控制面）临时经依赖 worker 继续跑执行（每步全绿），并具备"存活 refs 视图"读取能力。

**Architecture:** 控制面/执行面按 spec 分进程。本里程碑做地基：把本就属执行面的代码物理搬进 worker 模块（server 临时依赖 worker 保持行为不变），加 `worker` 注册表（DB 当注册表），worker 启动即注册 + 周期心跳。worker 进程已可跑执行认领环（DB 原子认领天然支持多进程分摊）。隔离的最终解耦（server 摘掉执行装配）留待 M6.3。

**Tech Stack:** Java 17 / Spring Boot 3.2.5 / PostgreSQL 16 / Flyway（迁移归属 persistence 模块）/ JUnit 5 + Testcontainers（persistence 侧）/ Maven reactor。

**Spec:** `docs/superpowers/specs/2026-09-10-scheduler-m6-distributed-worker-design.md`（M6.1 行 + §1/§2）。

## Global Constraints

- 控制面 `scheduler-server` 进程**不 import、不运行任何 `ExecutionHandler`**（最终态）；本里程碑允许 server 临时依赖 `scheduler-worker` 复用 `dev.scheduler.worker.execute.ExecutorWorker` 以保持行为不变 —— 但 handler 类型（`dev.scheduler.worker.handler.*`）不得再被 server import。M6.3 摘除后此临时耦合消失。
- **迁移 single-owner**：Flyway 迁移由 `scheduler-persistence` 拥有（测试基座 `AbstractPostgresTest` 用其 resources 跑迁移）。server 开 `spring.flyway.enabled: true`；**worker 开 `spring.flyway.enabled: false`**，不抢跑迁移。
- **DB 是 broker + 结果后端 + worker 注册表**，零新中间件。
- 每一任务末的验证：reactor 或指定模块 `mvn -o ... test` 保持绿（现全绿）；每次迁移类改动不改行为（server 集成测试 `ApiIntegrationTest` 仍跑通执行链）。
- 包迁移规则：`dev.scheduler.server.execute|handler|retry` → `dev.scheduler.worker.execute|handler|retry`，类名/方法签名/行为一律不变，只改包声明与引用。

---

### Task 1: 建 `scheduler-worker` 模块 + 根 pom 注册

**Files:**
- Create: `scheduler-worker/pom.xml`
- Modify: `pom.xml`（根，`<modules>` 加 `scheduler-worker`）

**Interfaces:**
- Consumes: 无（本任务仅脚手架）。
- Produces: 新 Maven 模块 `dev.scheduler:scheduler-worker`，后续任务把三包移入其 `src/main/java`。

- [ ] **Step 1:** 写 `scheduler-worker/pom.xml`（镜像 server pom 依赖子集，去掉 web starter；`parent` 指向根 `scheduler`）：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>dev.scheduler</groupId>
    <artifactId>scheduler</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </parent>
  <artifactId>scheduler-worker</artifactId>
  <dependencies>
    <dependency>
      <groupId>dev.scheduler</groupId>
      <artifactId>scheduler-persistence</artifactId>
      <version>${project.version}</version>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-jdbc</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-registry-prometheus</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <version>5.10.2</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>ch.qos.logback</groupId>
      <artifactId>logback-classic</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>postgresql</artifactId>
      <scope>runtime</scope>
    </dependency>
  </dependencies>
</project>
```

- [ ] **Step 2:** 根 `pom.xml` 的 `<modules>` 增补，使 `-am` reactor 能带上 worker：

```xml
    <module>scheduler-worker</module>
```

- [ ] **Step 3:** 验证构建通过（空模块无 src）：

Run: `cd /d/ai-project/scheduler && mvn -q -o -pl scheduler-worker -am validate`
Expected: exit 0，无报错。

- [ ] **Step 4:** Commit。

```bash
git add scheduler-worker/pom.xml pom.xml
git commit -m "build(m6): 新增 scheduler-worker 模块骨架，注册入根 reactor"

Co-Authored-By: Claude Code <noreply@anthropic.com>
```

---

### Task 2: 迁移 execute/handler/retry 三包到 worker + 移测试

**Files:**
- Move (git mv) `scheduler-server/src/main/java/dev/scheduler/server/execute/ExecutorWorker.java` → `scheduler-worker/src/main/java/dev/scheduler/worker/execute/ExecutorWorker.java`
- Move `scheduler-server/src/main/java/dev/scheduler/server/handler/*.java` → `scheduler-worker/src/main/java/dev/scheduler/worker/handler/*.java`（`CancellationToken, ExecutionHandler, HandlerContext, HandlerRegistry, MapHandlerRegistry, DemoHandler`）
- Move `scheduler-server/src/main/java/dev/scheduler/server/retry/*.java` → `scheduler-worker/src/main/java/dev/scheduler/worker/retry/*.java`（`FailureResolver, RetryPolicy`）
- Move tests:
  - `scheduler-server/src/test/java/dev/scheduler/server/handler/HandlerRegistryTest.java` → `scheduler-worker/src/test/java/dev/scheduler/worker/handler/HandlerRegistryTest.java`
  - `scheduler-server/src/test/java/dev/scheduler/server/retry/*.java` → `scheduler-worker/src/test/java/dev/scheduler/worker/retry/*.java`
  - `scheduler-server/src/test/java/dev/scheduler/server/execute/*.java` → `scheduler-worker/src/test/java/dev/scheduler/worker/execute/*.java`（`AbstractExecutorWorkerTest, ExecutorWorkerTest` 及它们引用的任何 fixture，一并迁移）
- Modify: `scheduler-server/pom.xml`（加 scheduler-worker 依赖）
- Modify: `scheduler-server/.../config/Beans.java`（引用改为 `dev.scheduler.worker.*`）
- Modify: 凡 server 侧 import 这四类的地方（`Reconciler` 等）→ `dev.scheduler.worker.*`

**Interfaces:**
- Consumes: Task 1 的模块。
- Produces: worker 模块持有 `dev.scheduler.worker.handler.HandlerRegistry`（含 `refs()`）、`ExecutorWorker(TaskRepository, ShardRepository, HandlerRegistry, String, FailureResolver, Clock)`、`FailureResolver(ShardRepository, RetryPolicy, Clock)`、`new RetryPolicy()`——签名与迁移前完全一致。

- [ ] **Step 1:** 用 `git mv` 迁移 java 文件到目标路径，保留 git 历史。
- [ ] **Step 2:** 改迁入文件的 `package dev.scheduler.server.execute|handler|retry;` → `package dev.scheduler.worker.execute|handler|retry;`，并把包内互相 import 也改为新包。
- [ ] **Step 3:** 改 server 侧引用：`Beans.java`（import + 用到 `DemoHandler/HandlerRegistry/MapHandlerRegistry/ExecutorWorker/FailureResolver/RetryPolicy` 的 bean 方法）、`Reconciler`（import `FailureResolver`）、其他 `grep` 出的引用，一律 `dev.scheduler.worker.*`。
- [ ] **Step 4:** `scheduler-server/pom.xml` `<dependencies>` 追加（server 临时依赖 worker 以复用执行侧，保持行为不变）：

```xml
    <dependency>
      <groupId>dev.scheduler</groupId>
      <artifactId>scheduler-worker</artifactId>
      <version>${project.version}</version>
    </dependency>
```

- [ ] **Step 5:** 编译 + 全量测试验证（重点：server 仍靠 worker 依赖跑执行环，`ApiIntegrationTest` 全链仍绿）：

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test`
Expected: BUILD SUCCESS（含 `ApiIntegrationTest`）；无 `dev.scheduler.server.{execute,handler,retry}` 残留引用（`grep -rn "dev.scheduler.server.execute\|server.handler\|server.retry" scheduler-server/src` 为空）。

- [ ] **Step 6:** Commit。

```bash
git add -A
git commit -m "refactor(m6): 迁移 execute/handler/retry 三包到 scheduler-worker(dev.scheduler.worker.*)，server 临时依赖 worker 保持行为不变

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: `worker` 表迁移 + WorkerRegistration + WorkerRepository + JdbcWorkerRepository + 测试

**Files:**
- Create: `scheduler-persistence/src/main/resources/db/migration/V5__worker.sql`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/WorkerRegistration.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/WorkerRepository.java`
- Create: `scheduler-persistence/src/main/java/dev/scheduler/persistence/JdbcWorkerRepository.java`
- Create: `scheduler-persistence/src/test/java/dev/scheduler/persistence/JdbcWorkerRepositoryTest.java`

**Interfaces:**
- Consumes: `AbstractPostgresTest`（persistence 测试基座，跑 resources 里的 V1..V5 迁移）、`JdbcTemplate`。
- Produces:
  - `record WorkerRegistration(String id, List<String> refs, Instant lastSeen, String status)`
  - `interface WorkerRepository { void upsertHeartbeat(WorkerRegistration w); List<WorkerRegistration> findAllAlive(Instant lastSeenAtLeast); }`
  - `JdbcWorkerRepository(JdbcTemplate)` 实现（upsert 用 `INSERT ... ON CONFLICT (id) DO UPDATE`；refs 以逗号拼接存 `text`，读出 split，因为 ref 为不含逗号的标识符）。

- [ ] **Step 1: 写迁移 `V5__worker.sql`**

```sql
-- M6: worker 注册表(DB 当注册表)。refs = 该 worker 服务的 handler refs(逗号拼接 text)。
-- last_seen 心跳时间;status ALIVE/DEAD/DRAINING。
CREATE TABLE worker (
  id         varchar     NOT NULL PRIMARY KEY,
  refs       text        NOT NULL,
  last_seen  timestamptz NOT NULL,
  status     varchar     NOT NULL
);
```

- [ ] **Step 2: 写失败测试 `JdbcWorkerRepositoryTest extends AbstractPostgresTest`**

```java
package dev.scheduler.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcWorkerRepositoryTest extends AbstractPostgresTest {
  private final JdbcWorkerRepository repo = new JdbcWorkerRepository(jdbc);
  private final Instant BASE = Instant.parse("2026-09-10T00:00:00Z");

  @BeforeEach void clean() {
    jdbc.update("TRUNCATE worker");
  }

  @Test
  void upsertHeartbeat_createsThenUpdatesSingleRow() {
    repo.upsertHeartbeat(new WorkerRegistration("w1", List.of("demo"), BASE, "ALIVE"));
    repo.upsertHeartbeat(new WorkerRegistration("w1", List.of("demo", "ping"), BASE.plusSeconds(5), "ALIVE"));
    assertEquals(List.of("w1"), jdbc.queryForList("SELECT id FROM worker", String.class));
    assertEquals("demo,ping",
        jdbc.queryForObject("SELECT refs FROM worker WHERE id='w1'", String.class));
  }

  @Test
  void findAllAlive_filtersByStatusAndFreshness() {
    repo.upsertHeartbeat(new WorkerRegistration("fresh", List.of("demo"), BASE.plusSeconds(60), "ALIVE"));
    repo.upsertHeartbeat(new WorkerRegistration("stale", List.of("old"), BASE.minusSeconds(60), "ALIVE"));
    repo.upsertHeartbeat(new WorkerRegistration("dead", List.of("x"), BASE.plusSeconds(60), "DEAD"));
    List<WorkerRegistration> alive = repo.findAllAlive(BASE);
    assertEquals(List.of("fresh"), alive.stream().map(WorkerRegistration::id).toList());
    assertEquals(List.of("demo"), alive.get(0).refs());
  }
}
```

- [ ] **Step 3: 运行确认失败**：

Run: `cd /d/ai-project/scheduler && mvn -q -o -pl scheduler-persistence -am test -Dtest=JdbcWorkerRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL——`WorkerRegistration/WorkerRepository/JdbcWorkerRepository` 未定义。

- [ ] **Step 4: 实现 `WorkerRegistration` + `WorkerRepository` + `JdbcWorkerRepository`**

```java
// WorkerRegistration.java
package dev.scheduler.persistence;
import java.time.Instant;
import java.util.List;
public record WorkerRegistration(String id, List<String> refs, Instant lastSeen, String status) {}
```

```java
// WorkerRepository.java
package dev.scheduler.persistence;
import java.time.Instant;
import java.util.List;
public interface WorkerRepository {
  void upsertHeartbeat(WorkerRegistration w);
  List<WorkerRegistration> findAllAlive(Instant lastSeenAtLeast);
}
```

```java
// JdbcWorkerRepository.java
package dev.scheduler.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class JdbcWorkerRepository implements WorkerRepository {
  private final JdbcTemplate jdbc;

  public JdbcWorkerRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  private static final RowMapper<WorkerRegistration> MAP = (rs, i) -> from(rs);

  private static WorkerRegistration from(ResultSet rs) throws SQLException {
    String refs = rs.getString("refs");
    return new WorkerRegistration(
        rs.getString("id"),
        refs == null || refs.isBlank() ? List.of() : Arrays.asList(refs.split(",")),
        rs.getTimestamp("last_seen").toInstant(),
        rs.getString("status"));
  }

  @Override
  public void upsertHeartbeat(WorkerRegistration w) {
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES (?, ?, ?, ?) "
        + "ON CONFLICT (id) DO UPDATE SET refs=EXCLUDED.refs, "
        + "last_seen=EXCLUDED.last_seen, status=EXCLUDED.status",
        w.id(), String.join(",", w.refs()), w.lastSeen(), w.status());
  }

  @Override
  public List<WorkerRegistration> findAllAlive(Instant lastSeenAtLeast) {
    return jdbc.query("SELECT id, refs, last_seen, status FROM worker "
        + "WHERE status='ALIVE' AND last_seen >= ?", MAP, lastSeenAtLeast);
  }
}
```

- [ ] **Step 5: 运行确认通过**：

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-persistence -am test -Dtest=JdbcWorkerRepositoryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS（2 个用例）。

- [ ] **Step 6:** 跑 persistence 全量测试确认 V5 迁移未破坏 SchemaSmokeTest 等（迁移已并入 `AbstractPostgresTest` 的数据源）：

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-persistence -am test`
Expected: 全绿。

- [ ] **Step 7: Commit。**

```bash
git add scheduler-persistence
git commit -m "feat(persistence): worker 注册表——V5 迁移 + WorkerRepository/Jdbc 实现

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: WorkerApplication + WorkerConfig（装配 execute/handler/retry + 心跳注册）

**Files:**
- Create: `scheduler-worker/src/main/java/dev/scheduler/worker/WorkerApplication.java`（主入口）
- Create: `scheduler-worker/src/main/java/dev/scheduler/worker/config/WorkerConfig.java`（装配 execute/handler/retry + 心跳）
- Create: `scheduler-worker/src/main/java/dev/scheduler/worker/registration/WorkerRegistrar.java`
- Create: `scheduler-worker/src/main/resources/application.yml`
- Create: `scheduler-worker/src/test/java/dev/scheduler/worker/registration/WorkerRegistrarTest.java`

**Interfaces:**
- Consumes: `WorkerRepository`（Task 3）、迁移后的 `ExecutorWorker/FailureResolver/RetryPolicy/HandlerRegistry/ExecutionHandler/HandlerContext`（Task 2）、`JdbcTaskRepository/JdbcShardRepository`（persistence）。
- Produces:
  - `WorkerRegistrar(WorkerRepository, HandlerRegistry, String workerId, Clock)` + `void heartbeat()`——取 `registry.refs()` 即时上报，状态 `ALIVE`。
  - 可独立 `java -cp` 启动的 worker 进程，启动即注册、每 `scheduler.heartbeat.interval-ms` 心跳；`server.port=8081` 暴露 actuator。

- [ ] **Step 1: 写失败测试 `WorkerRegistrarTest`（Fake repo，无 DB 需 boot）**

```java
package dev.scheduler.worker.registration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.scheduler.persistence.WorkerRegistration;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.server.handler.HandlerRegistry; // 占位——迁移后改为 dev.scheduler.worker.handler.HandlerRegistry
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkerRegistrarTest {
  private static final class Fake implements WorkerRepository {
    final List<WorkerRegistration> rows = new ArrayList<>();
    @Override public void upsertHeartbeat(WorkerRegistration w) { rows.add(w); }
    @Override public List<WorkerRegistration> findAllAlive(Instant since) { return List.of(); }
  }

  @Test
  void heartbeat_reportsRegistryRefsAsAliveWorker() {
    Fake fake = new Fake();
    var registry = new MapHandlerRegistry(List.of(() -> new DemoHandler()));
    var clock = Clock.fixed(Instant.parse("2026-09-10T00:00:00Z"), ZoneOffset.UTC);
    new WorkerRegistrar(fake, registry, "w1", clock).heartbeat();

    WorkerRegistration w = fake.rows.get(0);
    assertEquals("w1", w.id());
    assertEquals(List.of("demo"), w.refs());
    assertEquals("ALIVE", w.status());
    assertEquals(clock.instant(), w.lastSeen());
  }
}
```

- [ ] **Step 2:** 运行确认失败（`WorkerRegistrar` 未定义 —— 注意 import 占位需在 Step 3 改为真实 worker 包路径）。

Run: `cd /d/ai-project/scheduler && mvn -q -o -pl scheduler-worker -am test -Dtest=WorkerRegistrarTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL。

- [ ] **Step 3: 实现 `WorkerRegistrar`（import 用真实 `dev.scheduler.worker.handler.*`）**

```java
package dev.scheduler.worker.registration;

import dev.scheduler.persistence.WorkerRegistration;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.worker.handler.HandlerRegistry;
import java.time.Clock;

/** 向共享 DB 注册本 worker 并周期心跳:上报服务的 handler refs + 活性。 */
public class WorkerRegistrar {
  private final WorkerRepository repo;
  private final HandlerRegistry registry;
  private final String workerId;
  private final Clock clock;

  public WorkerRegistrar(WorkerRepository repo, HandlerRegistry registry, String workerId, Clock clock) {
    this.repo = repo;
    this.registry = registry;
    this.workerId = workerId;
    this.clock = clock;
  }

  public void heartbeat() {
    repo.upsertHeartbeat(new WorkerRegistration(workerId, registry.refs(), clock.instant(), "ALIVE"));
  }
}
```

- [ ] **Step 4: 实现 `WorkerApplication` + `WorkerConfig` + `application.yml`**

```java
// WorkerApplication.java
package dev.scheduler.worker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** 执行节点引导入口:装配 handler/executor/retry + 心跳注册。扫描 dev.scheduler.worker.*。 */
@SpringBootApplication
@EnableScheduling
public class WorkerApplication {
  public static void main(String[] args) {
    SpringApplication.run(WorkerApplication.class, args);
  }
}
```

```java
// config/WorkerConfig.java —— 镜像 server Beans 的执行侧装配(需 worker 的仓库 bean)
package dev.scheduler.worker.config;

import dev.scheduler.persistence.JdbcShardRepository;
import dev.scheduler.persistence.JdbcTaskRepository;
import dev.scheduler.persistence.JdbcWorkerRepository;
import dev.scheduler.persistence.ShardRepository;
import dev.scheduler.persistence.TaskRepository;
import dev.scheduler.persistence.WorkerRepository;
import dev.scheduler.worker.execute.ExecutorWorker;
import dev.scheduler.worker.handler.DemoHandler;
import dev.scheduler.worker.handler.ExecutionHandler;
import dev.scheduler.worker.handler.HandlerRegistry;
import dev.scheduler.worker.handler.MapHandlerRegistry;
import dev.scheduler.worker.registration.WorkerRegistrar;
import dev.scheduler.worker.retry.FailureResolver;
import dev.scheduler.worker.retry.RetryPolicy;
import java.time.Clock;
import java.util.List;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
public class WorkerConfig {
  // ---- 仓库 bean(worker 独立上下文,不能依赖 server Beans) ----
  @Bean TaskRepository taskRepository(JdbcTemplate jdbc) { return new JdbcTaskRepository(jdbc); }
  @Bean ShardRepository shardRepository(JdbcTemplate jdbc) { return new JdbcShardRepository(jdbc); }
  @Bean WorkerRepository workerRepository(JdbcTemplate jdbc) { return new JdbcWorkerRepository(jdbc); }
  @Bean Clock clock() { return Clock.systemUTC(); }

  @Bean ExecutionHandler demoHandler() { return new DemoHandler(); }

  @Bean
  HandlerRegistry handlerRegistry(List<ExecutionHandler> handlers) {
    List<Supplier<ExecutionHandler>> suppliers =
        handlers.stream().map(h -> (Supplier<ExecutionHandler>) () -> h).toList();
    return new MapHandlerRegistry(suppliers);
  }

  @Bean RetryPolicy retryPolicy() { return new RetryPolicy(); }
  @Bean FailureResolver failureResolver(ShardRepository shards, RetryPolicy retryPolicy, Clock clock) {
    return new FailureResolver(shards, retryPolicy, clock);
  }

  @Bean
  String schedulerWorkerId(@Value("${scheduler.worker-id:}") String configured) {
    return configured.isBlank() ? "worker@" + java.util.UUID.randomUUID() : configured;
  }

  @Bean
  ExecutorWorker executorWorker(TaskRepository tasks, ShardRepository shards,
                                HandlerRegistry handlers, String schedulerWorkerId,
                                FailureResolver failureResolver, Clock clock) {
    return new ExecutorWorker(tasks, shards, handlers, schedulerWorkerId, failureResolver, clock);
  }

  @Bean
  WorkerRegistrar workerRegistrar(WorkerRepository repo, HandlerRegistry registry,
                                  String schedulerWorkerId, Clock clock) {
    return new WorkerRegistrar(repo, registry, schedulerWorkerId, clock);
  }

  /** 启动即注册一次,随后按 interval 周期心跳。 */
  @Bean
  ApplicationRunner registerOnStart(WorkerRegistrar registrar) {
    return args -> registrar.heartbeat();
  }

  /** 执行认领环:共享 DB 原子认领,支持多 worker 并发分摊。 */
  @Bean
  WorkLoop workLoop(ExecutorWorker worker) {
    return new WorkLoop(worker);
  }

  /** 心跳环。 */
  @Bean
  HeartbeatLoop heartbeatLoop(WorkerRegistrar registrar) {
    return new HeartbeatLoop(registrar);
  }

  public static final class WorkLoop {
    private final ExecutorWorker worker;
    WorkLoop(ExecutorWorker worker) { this.worker = worker; }
    @Scheduled(fixedDelayString = "${scheduler.loop.work-delay-ms:100}")
    public void tick() { worker.workOne(); }
  }

  public static final class HeartbeatLoop {
    private final WorkerRegistrar registrar;
    HeartbeatLoop(WorkerRegistrar registrar) { this.registrar = registrar; }
    @Scheduled(fixedDelayString = "${scheduler.heartbeat.interval-ms:10000}")
    public void tick() { registrar.heartbeat(); }
  }
}
```

```yaml
# application.yml
spring:
  application:
    name: scheduler-worker
  datasource:
    url: ${DB_URL:jdbc:postgresql://localhost:5432/scheduler}
    username: ${DB_USER:scheduler}
    password: ${DB_PASSWORD:scheduler}
  flyway:
    enabled: false

server:
  port: 8081

scheduler:
  worker-id: ${SCHEDULER_WORKER_ID:}
  loop:
    work-delay-ms: 100
  heartbeat:
    interval-ms: 10000

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
```

- [ ] **Step 5:** 更新 `WorkerRegistrarTest` 的 import 占位为真实 `dev.scheduler.worker.handler.{HandlerRegistry, MapHandlerRegistry, DemoHandler}`，跑通过：

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-worker test`
Expected: `WorkerRegistrarTest` PASS；reactor 编译无错。

- [ ] **Step 6:** 全 reactor 编译 + server 测试（确认 server 临时依赖 worker 后整体仍绿）：

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test`
Expected: BUILD SUCCESS。

- [ ] **Step 7:** 运行验证（worker 进程可启动并注册心跳；DB 用 5433 实测容器）——注意本 worker 进程是长驻的,验证后停掉:

```bash
cd /d/ai-project/scheduler/scheduler-worker && mvn -q -o dependency:build-classpath -Dmdep.outputFile=cp-test.txt -DincludeScope=test
DB_URL=jdbc:postgresql://localhost:5433/scheduler DB_USER=scheduler DB_PASSWORD=scheduler java -cp "target/classes;$(cat cp-test.txt)" dev.scheduler.worker.WorkerApplication &
# 等几秒后:
psql -h localhost -p 5433 -U scheduler -d scheduler -c "SELECT id, refs, status, (now()-last_seen) AS age FROM worker;"
# 应看到一行 ALIVE worker;再等 >10s 后 age 回落(<2s)证明心跳在推进。kill 该 java 进程。
```

- [ ] **Step 8: Commit。**

```bash
git add scheduler-worker
git commit -m "feat(worker): WorkerApplication + WorkerConfig——装配 execute/handler/retry + 启动注册与周期心跳;server.port=8081

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5: server 存活-refs 读视图（workerRepository bean + 集成断言）

**Files:**
- Modify: `scheduler-server/src/main/java/dev/scheduler/server/config/Beans.java`（加 `workerRepository` bean）
- Modify: `scheduler-server/src/test/java/dev/scheduler/server/web/ApiIntegrationTest.java`（加一条断言：写入 worker 后可读出存活 refs）

**Interfaces:**
- Consumes: `JdbcWorkerRepository` / `WorkerRepository`（持久化，经 Task 3）。
- Produces: 控制面能读共享 DB 的 worker 注册表（`findAllAlive`），供 M6.2 建/改任务时的存活-ref 校验。

- [ ] **Step 1: `Beans.java` 加 bean（server 上下文需要 worker 表读写以做存活-refs 视图）：**

```java
  @Bean
  WorkerRepository workerRepository(JdbcTemplate jdbc) {
    return new JdbcWorkerRepository(jdbc);
  }
```
（补 `import dev.scheduler.persistence.{WorkerRepository, JdbcWorkerRepository};`）

- [ ] **Step 2: `ApiIntegrationTest` 加集成断言（沿用其既有 worker/DB 辅助）：**

```java
  /** M6:控制面应能从共享 DB 读存活 worker 注册视图(refs)。 */
  @Test
  void readsLiveWorkerRegistryView() throws Exception {
    Instant base = Instant.parse("2026-09-10T00:00:00Z");
    jdbc.update("INSERT INTO worker(id, refs, last_seen, status) "
        + "VALUES('w1','demo',?,'ALIVE')", base);
    var live = workerRepository.findAllAlive(base.minusSeconds(10));
    assertEquals(List.of("w1"), live.stream().map(WorkerRegistration::id).toList());
    assertEquals(List.of("demo"), live.get(0).refs());
  }
```
（在测试类里注入/构造 `JdbcWorkerRepository`，等价于其既有 `shards`/`jdbc` 用法；`Instant`/`List` 已有 import 则复用，缺则补。）

- [ ] **Step 3:** 跑 server 全量（含 ApiIntegrationTest 集成，验证 server 上下文带 workerRepository bean 后仍绿）：

Run: `cd /d/ai-project/scheduler && mvn -o -pl scheduler-server -am test`
Expected: BUILD SUCCESS，新断言 PASS。

- [ ] **Step 4: Commit。**

```bash
git add scheduler-server
git commit -m "feat(server): workerRepository bean——控制面读共享 DB 的存活 refs 视图(供 M6.2 校验)

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 6: 端到端运行验证 + README 更新

**Files:**
- Modify: `README.md`（模块布局 + worker 启动方式 + DB 注册表说明）

**Interfaces:**
- Consumes: Task 1–5 全部。
- Produces: 可运行的「1 控制 + 1 worker」双进程;README 记录 worker 独立启动路径。

- [ ] **Step 1:** 停掉此前所有 worker 验证进程,唯留后端 server(8080)。启动**一个 worker 进程**(Task 4 Step 7 同款命令)。

- [ ] **Step 2:** 确认控制面(8080)正常、worker(8081) actuator health UP;`psql` 查 `worker` 表出现一行 ALIVE,`(now()-last_seen)` 心跳推进 <2s。

- [ ] **Step 3:** 触发一个任务,确认 worker 独立认领并跑通(前端/API 触发一 cron 任务,或查 `execution_shard` 被该 worker claim 到 RUNNING→终态)。这兼作 M6.4 前「多进程分摊可行」的预演。

- [ ] **Step 4:** README 增补:模块图加 `scheduler-worker`;「运行」节加 worker start 命令(env DB_URL 等同 server,`WorkerApplication` 主类,cp-test 生成同 server);注明 worker 需先由 server/迁移建表(`spring.flyway.enabled=false`)。

- [ ] **Step 5:** 停 worker 进程(保留 server 与本回合已验证的 DB/前端即可);提交 README:

```bash
git add README.md
git commit -m "docs(m6): README 增补 scheduler-worker 模块与独立启动方式

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

- [ ] **Step 6:** 全 reactor 最终绿:

Run: `cd /d/ai-project/scheduler && mvn -o test`
Expected: BUILD SUCCESS,全量测试(含 persistence V5、server 集成、worker 心跳单测)绿。

---

## Self-Review

- **Spec 覆盖**：M6.1 行验收（worker 进程启动 ✓ Task4/6、注册表写入 ✓ Task3/4、worker 行 upsert ✓ Task3/4、server 读存活-refs 视图 ✓ Task5）全落地。
- **占位符扫描**：无 TBD/TODO；新的 4 个 java 文件 + sql + yml + pom 均有完整代码或精确 git mv 命令；唯一"占位"是 Task4 Step1 测试里刻意标注、Step5 要求更正的 import 路径（已显式说明,非漏写）。
- **类型一致性**：`WorkerRegistration(id, refs, lastSeen, status)`、`WorkerRepository.upsertHeartbeat/findAllAlive`、`WorkerRegistrar(..., Clock).heartbeat()`、`ExecutorWorker`/`FailureResolver`/`RetryPolicy`/`HandlerRegistry.refs()` 签名在 Task3/4/5 间一致；`registry.refs()` 返回 `List<String>` 与 `WorkerRegistration.refs` 一致。
- **迁移纪律**：Task4 明确 worker `flyway.enabled=false`(Global Constraint);`HandlerRegistryTest` 移到 worker 后其 `MapHandlerRegistry` 新增 refs 单测仍随 worker 测试跑,受 Task2 Step5 全量验证覆盖。