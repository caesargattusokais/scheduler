package dev.scheduler.worker.handler;

public interface ExecutionHandler {
  String ref();

  void handle(HandlerContext ctx);

  /** 运行后续写回 execution_shard.result_payload 的可选结果;默认空(绝大多数 handler 不产出)。 */
  default String resultPayload(HandlerContext ctx) { return ""; }
}
