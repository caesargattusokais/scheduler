package dev.scheduler.persistence;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.core.TargetType;
import java.time.Instant;
import java.util.List;

/** 操作审计 append-only 仓储(只增不删不更)。 */
public interface AuditRepository {
  /** 追加一条审计(带 diff 与 before 快照 JSON 文本);occurred_at 由 DB now() 回填。
   *  diffJson 为 before/after diff 的 JSON 文本或 null;beforeJson 为操作前全量 Task 快照的 JSON 文本或 null。 */
  void record(String operator, String action, TargetType targetType, long targetId,
              String metaJson, String diffJson, String beforeJson, String source);

  /** 无 diff/before 的追加(委托 8-arg,both null)——既有调用点与旧测试零改动。 */
  default void record(String operator, String action, TargetType targetType, long targetId,
                      String metaJson, String source) {
    record(operator, action, targetType, targetId, metaJson, null, null, source);
  }

  /** 过滤 + 分页,按 occurred_at DESC, id DESC 排序;全部过滤参数可为 null/空(不过滤)。
   *  hasDiff=true 仅纳入有实际变更的行(diff 非 NULL 且非空对象 `{}`);diffField 非空白按
   *  JSONB 顶层键存在过滤(改过该字段)。 */
  List<AuditEntry> findPage(String operator, String action, String targetType,
                            Long targetId, Instant from, Instant to,
                            Boolean hasDiff, String diffField, int limit, int offset);

  /** 与 findPage 相同过滤条件的 count(供 Page.total)。 */
  long count(String operator, String action, String targetType,
             Long targetId, Instant from, Instant to, Boolean hasDiff, String diffField);
}