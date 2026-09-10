package dev.scheduler.worker.handler;

public interface ExecutionHandler {
  String ref();

  void handle(HandlerContext ctx);
}
