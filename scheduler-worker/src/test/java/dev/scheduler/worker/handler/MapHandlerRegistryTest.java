package dev.scheduler.worker.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class MapHandlerRegistryTest {
  @Test void resolvesByRefAndFailsFastWhenMissing() {
    var r = new MapHandlerRegistry(List.of(DemoHandler::new));
    assertEquals("demo", r.get("demo").ref());
    assertThrows(IllegalArgumentException.class, () -> r.get("nope"));
  }

  @Test void refsListsEveryRegisteredHandler() {
    var r = new MapHandlerRegistry(List.of(DemoHandler::new));
    assertEquals(List.of("demo"), r.refs());
  }

  @Test
  void duplicateRef_failsFast() {
    assertThrows(IllegalStateException.class, () -> new MapHandlerRegistry(List.of(
        (Supplier<ExecutionHandler>) DemoHandler::new,
        (Supplier<ExecutionHandler>) DemoHandler::new)));
  }
}
