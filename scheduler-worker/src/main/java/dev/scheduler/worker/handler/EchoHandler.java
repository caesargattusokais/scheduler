package dev.scheduler.worker.handler;

import java.time.Clock;
import java.util.concurrent.CancellationException;

/** M6.4:第二个真实 handler。协作取消感知(demo 同款);resultPayload 记录「哪个 worker 执行了哪个分片」+
 *  回显 args,作为多 worker 分摊的观测仪表,经 T2 通路落 execution_shard.result_payload。 */
public class EchoHandler implements ExecutionHandler {
  private final Clock clock;

  public EchoHandler(Clock clock) { this.clock = clock; }

  @Override public String ref() { return "echo"; }

  @Override public void handle(HandlerContext ctx) {
    if (ctx.token() != null && ctx.token().isCancelRequested()) {
      throw new CancellationException("echo cancelled");
    }
  }

  @Override public String resultPayload(HandlerContext ctx) {
    return "{\"echoed\":\"" + safe(ctx.args()) + "\",\"workerId\":\"" + ctx.workerId()
        + "\",\"shardIndex\":" + ctx.shardIndex() + ",\"at\":\"" + clock.instant().toString() + "\"}";
  }

  private static String safe(String s) { return s == null ? "" : s; }
}
