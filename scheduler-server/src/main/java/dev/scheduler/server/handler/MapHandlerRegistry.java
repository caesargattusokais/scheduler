package dev.scheduler.server.handler;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MapHandlerRegistry implements HandlerRegistry {
  private final Map<String, ExecutionHandler> handlers;

  public MapHandlerRegistry(List<Supplier<ExecutionHandler>> suppliers) {
    handlers = suppliers.stream().map(Supplier::get)
        .collect(Collectors.toMap(ExecutionHandler::ref, h -> h, (a, b) -> a));
  }

  @Override public ExecutionHandler get(String ref) {
    ExecutionHandler h = handlers.get(ref);
    if (h == null) throw new IllegalArgumentException("handler not registered: " + ref);
    return h;
  }
}