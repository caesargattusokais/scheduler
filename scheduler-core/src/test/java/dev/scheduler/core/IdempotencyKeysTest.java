package dev.scheduler.core;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.time.Instant;

class IdempotencyKeysTest {
  @Test void stablePerTaskInstantShard() {
    Instant t = Instant.parse("2026-01-01T00:00:00Z");
    assertEquals("7:1767225600000:2", IdempotencyKeys.forTrigger(7L, t, 2));
  }
  @Test void differsAcrossShards() {
    Instant t = Instant.now();
    assertNotEquals(IdempotencyKeys.forTrigger(1L, t, 0), IdempotencyKeys.forTrigger(1L, t, 1));
  }
}
