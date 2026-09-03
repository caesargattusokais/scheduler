package dev.scheduler.server.handler;

public class DemoHandler implements ExecutionHandler {
  @Override public String ref() { return "demo"; }

  @Override public void handle(HandlerContext ctx) {
    // no-op demo handler
  }
}