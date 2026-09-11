package dev.scheduler.server.web;

import java.util.List;

/**
 * 统一列表返回包裹 spec:所有列表接口返回 {@code {items,total,offset,limit}}。
 * {@code offset}/{@code limit} 回显实际用于本页的值;{@code total} 为过滤后的全量计数(不含分页)。
 */
public record Page<T>(List<T> items, long total, int offset, int limit) {}