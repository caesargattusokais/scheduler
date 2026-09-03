package dev.scheduler.server.handler;

public interface ExecutionHandler {
  String ref();

  void handle(HandlerContext ctx);
}