package dev.scheduler.worker.handler;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Supplier;

public class MapHandlerRegistry implements HandlerRegistry {
  private final Map<String, ExecutionHandler> handlers;

  public MapHandlerRegistry(List<Supplier<ExecutionHandler>> suppliers) {
    Map<String, ExecutionHandler> built = new HashMap<>();
    for (ExecutionHandler h : suppliers.stream().map(Supplier::get).toList()) {
      if (built.putIfAbsent(h.ref(), h) != null) {
        throw new IllegalStateException("duplicate handler ref: " + h.ref());
      }
    }
    handlers = built;
  }

  @Override public ExecutionHandler get(String ref) {
    ExecutionHandler h = handlers.get(ref);
    if (h == null) throw new IllegalArgumentException("handler not registered: " + ref);
    return h;
  }

  @Override public List<String> refs() {
    return handlers.keySet().stream().sorted().toList();
  }
}
