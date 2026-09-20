package dev.scheduler.core;

/** 审计目标类型:与 app_audit.target_type 列值一一对应。NONE = 不针对具体资源(控制面自身的 access.denied / audit.archive)。 */
public enum TargetType {
  TASK("task"), EXECUTION("execution"), SHARD("shard"), DAG("dag"), DAG_RUN("dag_run"), NONE("none");
  private final String db;
  TargetType(String db) { this.db = db; }
  public String db() { return db; }
}