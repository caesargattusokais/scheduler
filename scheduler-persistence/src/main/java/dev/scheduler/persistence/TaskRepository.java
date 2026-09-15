package dev.scheduler.persistence;
import dev.scheduler.core.Task;
import java.util.List;
import java.util.Optional;

public interface TaskRepository {
  Task create(Task t);
  Optional<Task> findById(long id);
  List<Task> findCronEnabled();
  /** 游标分批:返回 id>afterId 的 enabled+cron 任务,至多 limit 行;afterId=0 从头。配合 §4 扫描分批。 */
  List<Task> findCronEnabledPage(long afterId, int limit);
  List<Task> findAll();
  /** 列表:name 子串(ILIKE)、paused 过滤 + limit/offset 分页(ORDER BY id)。 */
  List<Task> findPage(String name, Boolean paused, int limit, int offset);
  /** 与 findPage 同过滤条件的全量计数(不含分页)。 */
  long count(String name, Boolean paused);
  boolean update(long id, Task t);
  void setPaused(long id, boolean paused);
  /** 该任务的执行轮数(含全部 execution),用于删除前置检查。 */
  long executionCount(long taskId);
  /** 被 DAG 节点 / 运行快照引用的总数(task_id 外键触及 app_dag_node 与 dag_run_node)。 */
  long dagReferenceCount(long taskId);
  /** 物理删除;仅当无任何 execution 且未被任何 DAG 节点/运行引用时成功,返回是否删除。 */
  boolean delete(long id);
}
