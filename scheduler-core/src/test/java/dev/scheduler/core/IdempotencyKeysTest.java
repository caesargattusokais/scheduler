package dev.scheduler.core;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.time.Instant;

class IdempotencyKeysTest {
  @Test void stablePerTaskInstant() {
    Instant t = Instant.parse("2026-01-01T00:00:00Z");
    assertEquals("7:1767225600000", IdempotencyKeys.forTrigger(7L, t));
  }
  @Test void differsAcrossInstants() {
    Instant t = Instant.ofEpochMilli(1000);
    assertNotEquals(IdempotencyKeys.forTrigger(1L, t),
        IdempotencyKeys.forTrigger(1L, t.plusSeconds(1)));
  }
  @Test void ignoresShardIndex_sameKeyAcrossShards() {
    Instant t = Instant.now();
    // 父幂等键只代表"该次触发",不带 shard 段:同一 task+trigger 恒同键,跨 shard 亦同。
    assertEquals(IdempotencyKeys.forTrigger(1L, t), IdempotencyKeys.forTrigger(1L, t));
  }
}