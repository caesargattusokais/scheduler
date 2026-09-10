package dev.scheduler.server.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class HandlerRegistryTest {
  @Test void resolvesByRefAndFailsFastWhenMissing() {
    var r = new MapHandlerRegistry(List.of(DemoHandler::new));
    assertEquals("demo", r.get("demo").ref());
    assertThrows(IllegalArgumentException.class, () -> r.get("nope"));
  }

  @Test void refsListsEveryRegisteredHandler() {
    var r = new MapHandlerRegistry(List.of(DemoHandler::new));
    assertEquals(List.of("demo"), r.refs());
  }
}
