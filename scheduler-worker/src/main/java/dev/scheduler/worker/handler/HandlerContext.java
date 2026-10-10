package dev.scheduler.worker.handler;

/** 一次分片运行的上下文:父 execution + 本分片坐标 + 父 args + 协作取消令牌 + E3 进度心跳。
 *  {@link #touch()} 把「仍在推进」上报给 worker 续租线程合并 flush({@code progress==null} 时 no-op)。
 *  E3#2 投递坐标:{@link #attempt()} 为本次投递的在途 attempt(1-based,1=首次,>1=重投递),
 *  {@link #idempotencyKey()} 为本分片逻辑单元的稳定标识,{@link #redelivery()} 即 attempt>1。 */
public record HandlerContext(long executionId, long shardId, int shardIndex, int shardCount,
                             String shardData, String args, String workerId, CancellationToken token,
                             ProgressSignal progress, int attempt) {

  /** 便捷构造:默认未取消 token、无 workerId、无进度心跳、attempt=1(首次投递),供手工/既有测试在不关心
   *  协作取消/进度/投递坐标时使用。 */
  public static HandlerContext of(long executionId, long shardId, int shardIndex, int shardCount,
                                  String shardData, String args) {
    return new HandlerContext(executionId, shardId, shardIndex, shardCount, shardData, args, null,
        new CancellationToken(false), null, 1);
  }

  /** E3 进度上报:置位进度信号(重交由 worker 周期合并落库);progress==null 时 no-op。 */
  public void touch() {
    if (progress != null) progress.touch();
  }

  /** E3#2 稳定幂等键:本分片逻辑单元的标识,跨 attempt/重认领不变(= executionId : shardIndex)。 */
  public String idempotencyKey() {
    return executionId + ":" + shardIndex;
  }

  /** E3#2 是否重投递(attempt > 1;attempt == 1 即首次投递)。 */
  public boolean redelivery() {
    return attempt > 1;
  }
}
