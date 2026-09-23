package dev.scheduler.persistence;

import java.util.List;

/** app_webhook 订阅仓储:通知投递器轮询的端点来源 + 通知管理 API 的 CRUD。 */
public interface WebhookRepository {
  List<Webhook> list();

  long create(String url, String secret, List<String> kinds, boolean enabled, int maxAttempts, long backoffMs);

  /** 更新;返回是否真的更新到(id 存在)。 */
  boolean update(long id, String url, String secret, List<String> kinds, boolean enabled,
                 int maxAttempts, long backoffMs);

  /** 删除;返回是否真的删到(id 存在)。 */
  boolean delete(long id);
}