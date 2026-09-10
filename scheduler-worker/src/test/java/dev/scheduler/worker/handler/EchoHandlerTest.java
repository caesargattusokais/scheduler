package dev.scheduler.worker.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;

class EchoHandlerTest {
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T10:00:00Z"), ZoneOffset.UTC);

  @Test void ref_isEcho() { assertEquals("echo", new EchoHandler(CLOCK).ref()); }

  @Test void resultPayload_recordsWorkerShardAndEcho() {
    var ctx = new HandlerContext(1L, 2L, 3, 5, "data", "hello", "worker-a", new CancellationToken(false));
    String p = new EchoHandler(CLOCK).resultPayload(ctx);
    assertTrue(p.contains("\"workerId\":\"worker-a\""));
    assertTrue(p.contains("\"shardIndex\":3"));
    assertTrue(p.contains("\"echoed\":\"hello\""));
    assertTrue(p.contains("\"at\":\"2026-01-01T10:00:00Z\""));
  }

  @Test void handle_onCancelRequested_throwsCancellation() {
    var ctx = new HandlerContext(1L, 2L, 0, 1, null, null, "worker-a", new CancellationToken(true));
    assertThrows(CancellationException.class, () -> new EchoHandler(CLOCK).handle(ctx));
  }
}