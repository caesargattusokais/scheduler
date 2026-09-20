package dev.scheduler.persistence;

import dev.scheduler.core.OperatorEntry;
import dev.scheduler.core.OperatorRole;
import java.util.List;
import java.util.Optional;

/** 操作者目录仓储:写端点授权拦截器在此读白名单/角色;操作者管理端点在此增删。 */
public interface OperatorRepository {
  /** name 对应登记且活跃的操作者角色;未登记或停用 → empty(拦截器据以 403)。 */
  Optional<OperatorRole> roleOf(String name);

  /** 全量目录(按 name 升序)。 */
  List<OperatorEntry> list();

  /** 登记或更新(name → role/active 幂等 upsert);供引导属性与管理端。 */
  void upsert(String name, OperatorRole role, boolean active);

  /** 停用(active=false)并保留行,维系已写审计的可读性;name 不存在则为 no-op。 */
  void deactivate(String name);

  /** 取 name 对应活跃操作者的口令哈希(未登记/停用/无密 → empty)。 */
  Optional<String> activePasswordHash(String name);

  /** 设置/重置口令哈希(bcrypt 文本;name 须已登记,upsert 语义)。 */
  void setPassword(String name, String bcryptHash);

  /** 尚未设口令(password_hash IS NULL)的操作者 name 列表(按 name 升序);供默认口令引导。 */
  List<String> namesWithoutPassword();
}