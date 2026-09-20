package dev.scheduler.server.service;

import dev.scheduler.core.TargetType;
import dev.scheduler.persistence.AuditRepository;
import java.time.Instant;
import java.util.Map;
import org.springframework.stereotype.Service;

/** 审计保留/归档:把 occurred_at < cutoff 的旧行归档删除(即删即重链,DB 事务原子),
 *  并追记一条 target=NONE 的 audit.archive 审计。返回本次实际归档行数。 */
@Service
public class AuditRetentionService {
  private final AuditRepository audits;
  private final AuditRecorder recorder;

  public AuditRetentionService(AuditRepository audits, AuditRecorder recorder) {
    this.audits = audits;
    this.recorder = recorder;
  }

  /** 归档 cutoff 之前的行,至多 limit 条(按 id 升序)。归档删除不透支取证链(integrity() 仍 verified)。 */
  public long archiveOlderThan(String operator, Instant cutoff, int limit) {
    long archived = audits.archiveOlderThan(cutoff, limit);
    recorder.record(operator, "audit.archive", TargetType.NONE, 0L,
        Map.of("archived", archived, "olderThan", cutoff.toString()));
    return archived;
  }
}