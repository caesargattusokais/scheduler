# Scheduler v3·D — 生产化韧性:配置热更新 + 优雅停机 + 滚动

> **规划路径**:v3 迭代第一期(v3 无单一总纲,按子项推进;本 spec 即 D 子项设计)
> **方向**:生产化韧性(用户选定)
> **更新日期**:2026-09-28
> **范围**:配置热更新(DB 运行时设置表)+ 优雅停机(server + worker)+ 滚动发布支撑。三件事一起做(用户确认)。热键覆盖 `loop delay` 7 个 + `worker.capacity` + 全局 suspend 开关(用户确认全做)。

---

## 1. 背景与问题

现状锚点(设计依据,均已核实源码):

| 关注点 | 现状 | 生产化缺口 |
|---|---|---|
| 循环调度周期 | 7 个 server 循环均为 `@Scheduled(fixedDelayString = "${...}")` | delay 在 bean 初始化**绑死一次**,运行期改配置不生效 → 改调优项必须重启 |
| worker 停机 | WorkLoop / HeartbeatLoop 为自持 daemon 线程池,`destroy()` 里 `shutdownNow()` | 停机是**暴力掐断**,在途分片直接丢,靠 server 侧 60s 活性窗口延迟回收 |
| worker 下线 | WorkerRegistrar 仅 `heartbeat()`(upsert ALIVE) | 无下线/排空语义,在途执行无排空路径 |
| server 滚动 | `AdvisoryLockLeaderElection.close()` 已能释放 advisory lock | 停机时刻未主动释放 → 滚动时另一副本须等锁 |

**核心矛盾**:① `@Scheduled(fixedDelayString)` 无法运行期改变 delay —— 这是「热更新」无法落地的根,不做循环改造则永远改配置要重启;② daemon + `shutdownNow` 停机语义与「滚动发布不丢在途」冲突。

---

## 2. 目标(可验收)

1. **配置热更新**:`loop delay`(7 个)、`worker.capacity`、全局 `suspend` 开关,均可通过 ADMIN 端点运行期修改,并经 `app_audit` 留痕;下次对应拍子生效,无需重启。
2. **优雅停机(worker)**:收到停机信号 → 停新认领 → 排空在途(≤ grace 上限)→ `deregister` 让在途分片立即可被其它 worker 认领 → 停心跳。在途执行不被丢。
3. **优雅停机(server)**:停机时刻释放 advisory lock → 另一副本立即选主,滚动不空等。
4. **全局 suspend**:置位后所有循环停拍(排空);清位后恢复。用于运维期整体停顿。

**非目标(明确排除)**:
- 不做回滚历史(每一次 set 都有 DB 行 + 审计,但本期不做「回复到某历史值」UI/能力)。
- 不做多集群/多 region(C 子项,架构级,另行推进)。
- 不做能力开关的灰度发布。

---

## 3. 设计

### 3.1 运行时配置新子系统

#### 3.1.1 新表 `app_runtime_config`(V30 迁移)

```sql
CREATE TABLE app_runtime_config (
  key        text PRIMARY KEY,
  value      text NOT NULL,
  updated_by text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
```

**可选 key 白名单**(仅注册过的 key 可写;未注册 key 的 get 一律回落 fallback):

| key | fallback(= 现状 `@Value` 默认) | 消费方 |
|---|---|---|
| `loop.scan-delay-ms` | 5000 | ScanLoop |
| `reconcile.delay-ms` | 15000 | ReconcileLoop |
| `dag.delay-ms` | 5000 | DagLoop |
| `event.scan-delay-ms` | 5000 | EventLoop |
| `notifications.dispatch-delay-ms` | 1000 | NotificationLoop |
| `dlq.replay-delay-ms` | 1000 | DlqReplayLoop |
| `audit.retention.days` | 30 | AuditRetentionLoop(现由 `@Value` 注入)|
| `worker.capacity` | 1 | WorkLoop(worker 侧)|
| `worker.idle-ms` | 200 | WorkLoop(worker 侧)|
| `worker.heartbeat.interval-ms` | 10000 | HeartbeatLoop(worker 侧)|
| `worker.shutdown-grace-sec` | 30 | WorkLoop 排空上限(worker 侧)|
| `suspend` | false | 全部循环(全局暂停开关)|

> **刻意排除在热键外**:`lease.seconds` / `dlq.max-replays` / `audit.retention` 等 —— lease 与活性判定强耦合、热改更易诱发误判(现配 `-D` 启动参数可控,保持重启级);`dlq.max-replays` 已有每任务 `dlq_max_replays` 独立控制,无需全局热键。`audit.retention.days` 保留在表但注释为「低优先,可使可启」,见 3.2 改造时确认。

#### 3.1.2 持久层

- `RuntimeConfigRepository` 接口 + `JdbcRuntimeConfigRepository`:
  - `Optional<RuntimeConfigRow> find(String key)` / `List<RuntimeConfigRow> findAll()`
  - `upsert(String key, String value, String operator)`(INSERT ... ON CONFLICT (key) DO UPDATE)
  - record `RuntimeConfigRow(String key, String value, String updatedBy, Instant updatedAt)`
- 放 `scheduler-persistence` 模块,worker 依赖它(worker 独立上下文也要读)。

#### 3.1.3 server 服务层

- `RuntimeConfigService`(server):
  - 持有 key→fallback 映射(`RuntimeConfigKeys` 常量类,白名单 + 默认值)。
  - `long getLong(String key)` / `long getLongFallback(String key)` / `boolean getFlag(String key)` / `boolean isSuspended()`
    - 读路径:**DB 无值 → 回落 fallback**;DB 有值 → 解析(非法值日志警告并回落 fallback,不抛)。
    - 每次调用实时读 DB(单行主键查询,量小,无需缓存 —— 避免缓存一致性复杂度)。
  - `void set(String key, String value, String operator)`:
    - 白名单校验(未注册 key 拒绝);
    - 类型校验(value 需可解析成该 key 的类型);
    - 调 `RuntimeConfigRepository.upsert`;
    - 记审计 `runtime-config.update`(detail: key=K value=V → fallback=F 或旧值)。
  - `List<RuntimeConfigEntry>` list：DB 现存行 + 未注册过但白名单内未写过的 key 也列出(值为 fallback,标记「默认」),前端可见全部可调项。

#### 3.1.4 端点(ADMIN 收口)

- `GET /runtime-config` — 列全部可调项(值 + 来源 default/db + 操作者 + 时间)。
- `PUT /runtime-config/{key}` — body `{ "value": "..." }`;白名单/类型校验;`runtime-config.update` 审计。
- 放 `RuntimeConfigController`,沿用现有 `OperatorInterceptor` 收口 + 审计拦截链。

#### 3.1.5 前端「运行时设置」页

- 新 `SettingsPage.tsx`:表格列出全部 key(名称/当前值/来源/操作者/时间),ADMIN 行内可改;顶部全局 `suspend` 开关醒目展示。
- 路由注册进现有导航。

### 3.2 循环改造:`@Scheduled` → `ConfigurableLoop`

**引入 `ConfigurableLoop` 抽象基类**(server 侧):

```java
public abstract class ConfigurableLoop implements DisposableBean {
  private final ScheduledExecutorService pool;      // 自持 daemon 调度线程
  private final RuntimeConfigService settings;
  private final String delayKey;
  protected final long fallbackMs;
  private volatile boolean running = true;

  protected abstract void loopOnce();               // 子类实现:原有 tick 逻辑 + leader 门控

  private void schedule() {
    pool.schedule(this::tick, settings.getLong(delayKey, fallbackMs), MILLISECONDS);
  }
  private void tick() {
    if (!running) return;
    try { loopOnce(); } catch (Throwable t) { log.warn("loop tick failed; continuing", t); }
    schedule();                                       // 每次排下一拍时重读 delay → 热更新生效
  }
  void start() { schedule(); }
  @Override public void destroy() { running = false; pool.shutdownNow(); }
}
```

- 7 个循环各自改为继承 `ConfigurableLoop`:
  - ScanLoop、DagLoop、EventLoop(无 leader 门控)→ `loopOnce()` 直接调 engine。
  - ReconcileLoop、AuditRetentionLoop、NotificationLoop、DlqReplayLoop(有 leader 门控)→ `loopOnce()` 里先 `if (!leader.isLeader()) return;`。
- **delay 每拍重读** → 改 DB 值后下一拍生效,热更新落地。
- **`destroy()` 用 `running=false` + `shutdownNow()`**:`shutdownNow` 是 daemon 末位兜底;拍子本身不做长事务,排空靠 worker 侧。

**全局 suspend 门控**:
- `ConfigurableLoop.tick()` 顶部加 `if (settings.isSuspended()) { schedule(); return; }`(仍在排拍,但不执行 loopOnce → 停拍不停止调度线程,清位即恢复);或直接 `if (suspended) return;` 看实现取舍 —— 采用**继续排拍但跳过执行**,恢复即下一拍。用较长间隔兜底,避免 suspend 时忙轮询。

### 3.3 worker 优雅停机 + 排空

#### 3.3.1 `WorkLoop` 改造

现有:
```java
public static final class WorkLoop implements DisposableBean {
  private final ScheduledExecutorService pool;  // capacity 个守护线程,各 scheduleWithFixedDelay(tick)
  ...
}
```

新增能力(保持 `capacity` 个并发语义):

**a. 排空 + 停机 `stopAndDrain()`**:
1. `running=false` → 现有排定 tick 见 `running` 后直接返回,不再认领新分片;
2. 记录在途 handler 计数(认领时 +1 的并发信号量 `inflight`);
3. 等 `inflight` 归 0(每 500ms 轮询,≤ `worker.shutdown-grace-sec` 上限,可配);
4. grace 到点仍未归零 → 记警告并继续退出(在途由 server 活性回收兜底,不无限阻塞 JVM);
5. 之后 HeartbeatLoop 停 + `deregister()`。

**b. capacity 热更新**(用户确认做):
- `WorkLoop` 维护 `AtomicInteger capacity`;提供 `resize(int n)`:
  - n > 当前:补排 n-c 个认领拍(每拍一个线程);
  - n < 当前:仅降低目标,`tick` 内按「全局目标 capacity」判断是否让位(用一个 `AtomicInteger target`,tick 抢不到 slot 即 skip)——简单做法:每个 tick 用 `CAS` 维护一个 `activeClaims` 计数,`>= target` 则该 tick 跳过认领。
- `RuntimeConfigService` 的值变化 → 由 worker 端监听(worker 有自己的 settings bean,循环自身 tick 时读取并 `resize` 同步)。

> 说明:worker 是独立进程/上下文,不能依赖 server 的 `RuntimeConfigService` bean。设计 `RuntimeConfigKeys`/`JdbcRuntimeConfigRepository` 放 persistence,worker 也能构造自己的 settings 读取器(轻量封装 `WorkerRuntimeConfig`),这才让 worker 侧热键(worker.capacity / idle-ms / heartbeat / lease / shutdown-grace)可行。

#### 3.3.2 `WorkerRegistrar.deregister()`

- 新增 `deregister()`:把本 worker 的 last_seen 写回很早就(如 now - 10 分钟/或 status 'OFFLINE' 语义)→ `findAllAlive(lastSeenAtLeast=now-30s)` 不再返回本 worker → **在途分片立即被其它 worker 认领**,不等 60s 回收窗口。
- 复用 `repo.upsertHeartbeat`(带很旧 last_seen)或新增 `repo.markOffline(workerId, instant)`。倾向新增显式 `markOffline`,语义清晰。
- 停心跳前调用,幂等。

#### 3.3.3 停机编排

- Spring `@PreDestroy`(bean 级)+ `Runtime.getRuntime().addShutdownHook`(进程级兜底,捕获 SIGTERM):
  1. WorkLoop.stopAndDrain();
  2. HeartbeatLoop 停;
  3. WorkerRegistrar.deregister();
  4. 退出。

### 3.4 server 滚动退位

- `AdvisoryLockLeaderElection` 已有 `close()`,补一个 `shutdownCallback` 或由循环 stop 里调用 `leader.close()`:
  - 停机时刻释放 advisory lock → 另一副本立即选主(advisory lock 会话级,close 即释放,滚动不空等)。

---

## 4. 测试策略

### 4.1 持久层(scheduler-persistence)
- `JdbcRuntimeConfigRepositoryTest`:upsert 新建/更新(幂等,不新增行)、find 命中/未命中、findAll 排序、updated_by/updated_at 回读。

### 4.2 server 单元/集成
- `RuntimeConfigServiceTest`:白名单拒绝、类型校验非法回落 fallback、get 无 DB 回落 fallback、set 记审计(用假审计 recorder)。
- `ConfigurableLoopTest`:delay 热更新(改 settings 后下一拍用新值)、suspend 跳过执行、tick 抛异常不杀线程。
- `RuntimeConfigControllerTest`(ADMIN):GET 白名单全列、PUT 合法/非法/越权。
- 回归:现有 7 循环相关测试(TriggerEngine/Reconciler/DagEngine/EventEngine/Notification/Dlq)确认循环改造不破坏调度行为。

### 4.3 worker
- `WorkLoopTest`:排空(在途完成才返回)、grace 超时兜底、capacity resize 增/减认领、`deregister` 后 findAllAlive 不含本 worker。
- `WorkerRuntimeConfigTest`:worker 侧热键读取回落。

### 4.4 前端
- SettingsPage 构建通过;走 ADIM 真实端点改值 + service log 验证热生效。

### 4.5 全量基准
- `mvn -o test` → reactor BUILD SUCCESS。

---

## 5. 验收清单(对照 §2)

- [ ] `app_runtime_config` 表 + 仓库 + service + ADMIN 端点 + 前端页齐备,白名单/类型校验 + 审计留痕。
- [ ] 7 个 server 循环改 `ConfigurableLoop`,`delay` 每拍重读 → 改 DB 值下一拍生效。
- [ ] worker.capacity 热键增/减认领拍有效。
- [ ] 全局 suspend:置位全循环停拍,清位恢复。
- [ ] worker 停机排空:在途执行不被丢;`deregister` 后立即可被认领。
- [ ] server 停机释放 advisory lock,另一副本立即选主。
- [ ] 全量 `mvn -o test` BUILD SUCCESS。

---

## 6. 依赖 / 顺序

1. **表 + 持久层**(3.1.1/3.1.2)→ 2. **server service + 白名单 + 端点 + 前端**(3.1.3-3.1.5)→ 3. **server 循环改造 + suspend**(3.2)→ 4. **worker 停排 + capacity + deregister**(3.3)→ 5. **server 滚动退位**(3.4)。
2. worker 侧先建 `WorkerRuntimeConfig` 读取器(persistence 已含仓库),再改 WorkLoop。
3. 循环改造与优雅停机共用一个 `ConfigurableLoop`,前后紧耦合,单 plan 一次交付。