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

  /** 停机退位:markOffline 把 last_seen 置极早,worker 立即离开 findAllAlive(now-30s) 视界(行仍保留)。 */
  @Test
  void markOffline_excludesWorkerFromAliveWindow() {
    repo.upsertHeartbeat(new WorkerRegistration("w1", List.of("demo"), BASE.plusSeconds(60), "ALIVE"));
    repo.markOffline("w1", BASE.minusSeconds(600));
    assertTrue(repo.findAllAlive(BASE.minusSeconds(30)).stream().noneMatch(w -> w.id().equals("w1")),
        "markOffline 后 w1 离开 30s 存活视界");
    assertEquals("w1", jdbc.queryForObject("SELECT id FROM worker", String.class),
        "行保留,仅离开存活视界");
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

  /** 死行清理:last_seen 早于阈值即删;活行(last_seen >= 阈值)保留。 */
  @Test
  void purgeStale_removesOld_keepsRecent() {
    repo.upsertHeartbeat(new WorkerRegistration("old", List.of("a"), BASE.minusSeconds(60), "ALIVE"));
    repo.upsertHeartbeat(new WorkerRegistration("recent", List.of("b"), BASE.plusSeconds(10), "ALIVE"));
    repo.purgeStale(BASE.minusSeconds(30));
    assertEquals(List.of("recent"), jdbc.queryForList("SELECT id FROM worker", String.class));
  }

  /** E2-B:isAlive 锚 DB 时钟(now()-窗),非 JVM — abandon 安全前门须与 stuck 判定同源,防双跑。 */
  @Test
  void isAlive_judgesByDbClockFreshness() {
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES ('w-fresh','', now() - interval '5 seconds', 'ALIVE')");
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES ('w-stale','', now() - interval '120 seconds', 'ALIVE')");
    jdbc.update("INSERT INTO worker (id, refs, last_seen, status) VALUES ('w-dead','', now(), 'DEAD')");
    assertTrue(repo.isAlive("w-fresh", 30), "fresh ALIVE(last_seen 5s<窗30s,真活)→ alive");
    assertTrue(!repo.isAlive("w-stale", 30), "last_seen 120s 超窗 → 非 alive");
    assertTrue(!repo.isAlive("w-dead", 30), "DEAD 态 → 非 alive");
    assertTrue(!repo.isAlive("w-missing", 30), "不存在 → 非 alive");
  }
}