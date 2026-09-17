package dev.scheduler.persistence;

import dev.scheduler.core.AuditEntry;
import dev.scheduler.core.TargetType;
import java.time.Instant;
import java.util.List;

/** 操作审计 append-only 仓储(只增不删不更)。 */
public interface AuditRepository {
  /** 追加一条审计;occurred_at 由 DB now() 回填。metaJson 为 JSON 文本或 null。 */
  void record(String operator, String action, TargetType targetType, long targetId,
              String metaJson, String source);

  /** 过滤 + 分页,按 occurred_at DESC, id DESC 排序;全部过滤参数可为 null/空(不过滤)。 */
  List<AuditEntry> findPage(String operator, String action, String targetType,
                            Long targetId, Instant from, Instant to, int limit, int offset);

  /** 与 findPage 相同过滤条件的 count(供 Page.total)。 */
  long count(String operator, String action, String targetType,
             Long targetId, Instant from, Instant to);
}