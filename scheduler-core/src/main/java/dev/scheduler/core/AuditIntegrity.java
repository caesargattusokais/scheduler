package dev.scheduler.core;

/**
 * 审计取证链完整性结果(total=表全量,chained=已入链行数,firstTamperedId=首个自哈希或链衔接异常行 id,无则 null)。
 * verified 仅在 全量行均已入链 且 无任何篡改 时成立。
 */
public record AuditIntegrity(long totalRecords, long chainedRecords, Long firstTamperedId) {
  /** Jackson 以 isXxx() 命名为 property 'verified'。 */
  public boolean isVerified() { return firstTamperedId == null && chainedRecords == totalRecords; }
}