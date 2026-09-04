package dev.scheduler.server.handler;

/** 一次分片运行的上下文:父 execution + 本分片坐标 + 父 args + 协作取消令牌。 */
public record HandlerContext(long executionId, long shardId, int shardIndex, int shardCount,
                             String shardData, String args, CancellationToken token) {

  /** 便捷构造:默认未取消 token,供手工/既有测试在不关心协作取消时使用。 */
  public static HandlerContext of(long executionId, long shardId, int shardIndex, int shardCount,
                                  String shardData, String args) {
    return new HandlerContext(executionId, shardId, shardIndex, shardCount, shardData, args,
        new CancellationToken(false));
  }
}