package dev.scheduler.server.handler;

public record HandlerContext(long executionId, String args, CancellationToken token) {

  /** 便捷构造:默认未取消 token,供手工/既有测试在不关心协作取消时使用。 */
  public static HandlerContext of(long executionId, String args) {
    return new HandlerContext(executionId, args, new CancellationToken(false));
  }
}