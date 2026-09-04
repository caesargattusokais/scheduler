package dev.scheduler.server.handler;

import java.util.concurrent.CancellationException;

public class DemoHandler implements ExecutionHandler {
  @Override public String ref() { return "demo"; }

  @Override public void handle(HandlerContext ctx) {
    if (ctx.token() != null && ctx.token().isCancelRequested()) {
      // 协作取消:token 置位即抛取消信号,worker 单独捕获映射 CANCELED(不落入重试/死信)。
      throw new CancellationException("demo cancelled");
    }
    // no-op demo handler
  }
}