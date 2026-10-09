package dev.scheduler.worker.handler;

import java.time.Clock;
import java.util.concurrent.CancellationException;

/** 用户新加的 demo handler(ref=myjob):M6.4 同款结构,协作取消感知;resultPayload 带独立
 *  job 标记以便在 result_payload 里与本机 echo 类 handler 区分。仅依赖 core/persistence,不碰控制面。 */
public class MyJobHandler implements ExecutionHandler {
  private final Clock clock;

  public MyJobHandler(Clock clock) { this.clock = clock; }

  @Override public String ref() { return "myjob"; }

  @Override public void handle(HandlerContext ctx) {
    if (ctx.token() != null && ctx.token().isCancelRequested()) {
      throw new CancellationException("myjob cancelled");
    }
    ctx.touch(); // E3 契约示范:推进边界上报(本 handler 即时返回,touch 对短跑无实际效果)。
  }

  @Override public String resultPayload(HandlerContext ctx) {
    return "{\"job\":\"myjob\",\"workerId\":\"" + ctx.workerId() + "\",\"shardIndex\":"
        + ctx.shardIndex() + ",\"echoed\":\"" + safe(ctx.args()) + "\",\"at\":\""
        + clock.instant().toString() + "\"}";
  }

  private static String safe(String s) { return s == null ? "" : s; }
}