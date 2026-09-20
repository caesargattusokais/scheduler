package dev.scheduler.core;

/** 操作者角色(级序:OPERATOR < ADMIN)。用于写端点的操作者授权判定;与 app_operator.role 列值一一对应。 */
public enum OperatorRole {
  OPERATOR, ADMIN;

  /** 级序 rank:OPERATOR=1, ADMIN=2。判定"当前角色是否满足所需角色"用 rank() 不小于所需求值。 */
  public int rank() {
    return this == ADMIN ? 2 : 1;
  }
}