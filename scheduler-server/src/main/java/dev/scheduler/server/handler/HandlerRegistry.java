package dev.scheduler.server.handler;

public interface HandlerRegistry {
  ExecutionHandler get(String ref);
}
