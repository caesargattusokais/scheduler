package dev.scheduler.worker.handler;

/**
 * 协作取消令牌:无状态弹桶,仅携带本次 handler 运行开始时从 DB 读到的 {@code cancel_requested}。
 * worker 每次构造,handler 在运行中自行检查并协作退出(抛 {@link java.util.concurrent.CancellationException})。
 */
public record CancellationToken(boolean cancelRequested) {
  public boolean isCancelRequested() {
    return cancelRequested;
  }
}