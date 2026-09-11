package dev.scheduler.server.web;

/**
 * 分页参数归一化:所有列表接口统一 limit 默认 100 上限 500(向下裁剪到 1),offset 默认 0 最低 0。
 * 控制器仅此一处校验,其余接口共用。
 */
public record Paging(int limit, int offset) {
  public static Paging of(Integer limit, Integer offset) {
    int lim = (limit == null) ? 100 : Math.min(Math.max(limit, 1), 500);
    int off = (offset == null) ? 0 : Math.max(offset, 0);
    return new Paging(lim, off);
  }
}