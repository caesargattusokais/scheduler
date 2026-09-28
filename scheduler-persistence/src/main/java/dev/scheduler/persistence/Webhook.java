package dev.scheduler.persistence;

import java.time.Instant;
import java.util.List;

/** app_webhook 行映射:投递端点订阅。kinds 为空 → 订阅全部 event kind。
 *  scopeMode:任务维度过滤语义 —— 'ALL'(不按任务,现状)/ 'INCLUDE'(白名单:仅选中任务/DAG 命中才投)
 *  / 'EXCLUDE'(黑名单:排除选中,其余都投);selectedTaskIds/selectedDagIds 为选中任务/DAG 的 id 集。 */
public record Webhook(
    long id,
    String url,
    String secret,
    List<String> kinds,
    boolean enabled,
    int maxAttempts,
    long backoffMs,
    Instant createdAt,
    String scopeMode,
    List<Long> selectedTaskIds,
    List<Long> selectedDagIds) {

  /** 既有 8 参构造(旧调用点/测试 fixture 沿用):scopeMode 缺省 'ALL'(订阅全部任务),任务/DAG 选择为空。 */
  public Webhook(long id, String url, String secret, List<String> kinds, boolean enabled,
                 int maxAttempts, long backoffMs, Instant createdAt) {
    this(id, url, secret, kinds, enabled, maxAttempts, backoffMs, createdAt,
        "ALL", List.of(), List.of());
  }
}