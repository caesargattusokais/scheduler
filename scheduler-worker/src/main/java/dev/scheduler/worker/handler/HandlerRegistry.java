package dev.scheduler.worker.handler;

public interface HandlerRegistry {
  ExecutionHandler get(String ref);

  /** 全量已注册 handler 的 ref,供任务表单枚举校验用。 */
  java.util.List<String> refs();
}
