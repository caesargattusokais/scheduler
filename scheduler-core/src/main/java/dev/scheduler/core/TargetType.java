package dev.scheduler.core;

/** 审计目标类型:与 app_audit.target_type 列值一一对应。 */
public enum TargetType {
  TASK("task"), EXECUTION("execution"), SHARD("shard"), DAG("dag"), DAG_RUN("dag_run");
  private final String db;
  TargetType(String db) { this.db = db; }
  public String db() { return db; }
}