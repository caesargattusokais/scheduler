package dev.scheduler.worker.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class DemoHandlerTest {
  @Test void ref_isDemo() { assertEquals("demo", new DemoHandler().ref()); }

  @Test void resultPayload_reportsDeliveryCoordinate() {
    var first = HandlerContext.of(42, 7, 3, 5, "data", "args"); // attempt=1 首次投递
    String p1 = new DemoHandler().resultPayload(first);
    assertTrue(p1.contains("\"idempotencyKey\":\"42:3\""), "幂等键 = executionId:shardIndex");
    assertTrue(p1.contains("\"attempt\":1"));
    assertTrue(p1.contains("\"redelivery\":false"));

    var retry = new HandlerContext(42, 7, 3, 5, "data", "args", "w", new CancellationToken(false), null, 2);
    String p2 = new DemoHandler().resultPayload(retry);
    assertTrue(p2.contains("\"attempt\":2"));
    assertTrue(p2.contains("\"redelivery\":true"), "重投递 attempt=2 → redelivery=true");
  }
}