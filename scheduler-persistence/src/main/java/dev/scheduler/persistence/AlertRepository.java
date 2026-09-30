package dev.scheduler.persistence;

import dev.scheduler.core.AlertEpisode;
import dev.scheduler.core.AlertEpisodeView;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** 告警事件(alert episode)读模型仓储。 */
public interface AlertRepository {
  /** 按 key 找活跃行(PENDING/OPEN),即 WHERE key=? AND status<>'RESOLVED'。 */
  Optional<AlertEpisode> findActiveByKey(String key);

  /** 全量活跃行(PENDING/OPEN),供引擎 reconcile 逐键判定(无 join,与 findActiveByKey 同映射)。 */
  List<AlertEpisode> findAllActive();

  /** 插入并返回含自增 id 的行(id 由 DB 回填)。同 key 已存在活跃(PENDING/OPEN)行时插入将抛
   *  DuplicateKeyException(alert_episode_active_key_uq partial unique 生效)。 */
  AlertEpisode insert(AlertEpisode e);

  /** 按 id 覆盖 mutable 列,updated_at 由 SQL now() 落。 */
  void update(AlertEpisode e);

  /** 仅删活跃(PENDING/OPEN)行——PENDING 未确认回落的 DELETE 用。 */
  void deleteByKey(String key);

  /** 删除 resolved_at < cutoff 的 RESOLVED 行,返回删除行数。 */
  long pruneResolved(Instant cutoff);

  /** 活跃 OPEN 的告警列。 */
  List<AlertEpisodeView> findActive();

  /** RESOLVED 历史列,按 opened_at DESC。 */
  List<AlertEpisodeView> findResolved(int limit, int offset);

  /** findResolved 同过滤(resolved)的全量计数。 */
  long countResolved();
}
