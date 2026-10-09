package dev.scheduler.worker.handler;

/** 一次分片运行的上下文:父 execution + 本分片坐标 + 父 args + 协作取消令牌 + E3 进度心跳。
 *  {@link #touch()} 把「仍在推进」上报给 worker 续租线程合并 flush({@code progress==null} 时 no-op)。 */
public record HandlerContext(long executionId, long shardId, int shardIndex, int shardCount,
                             String shardData, String args, String workerId, CancellationToken token,
                             ProgressSignal progress) {

  /** 便捷构造:默认未取消 token、无 workerId、无进度心跳,供手工/既有测试在不关心协作取消/进度时使用。 */
  public static HandlerContext of(long executionId, long shardId, int shardIndex, int shardCount,
                                  String shardData, String args) {
    return new HandlerContext(executionId, shardId, shardIndex, shardCount, shardData, args, null,
        new CancellationToken(false), null);
  }

  /** E3 进度上报:置位进度信号(重交由 worker 周期合并落库);progress==null 时 no-op。 */
  public void touch() {
    if (progress != null) progress.touch();
  }
}
