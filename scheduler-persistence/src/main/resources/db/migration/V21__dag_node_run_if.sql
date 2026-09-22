-- 1b DAG 条件分支:节点可选 join 条件。
-- run_if='all_success'(默认):全部上游成功后执行,任一失败/跳过→被跳过(保持旧行为)。
-- run_if='any_success':任一上游成功即执行(OR-join);仅当全部上游终态且无任一成功→被跳过。
ALTER TABLE app_dag_node ADD COLUMN run_if TEXT NOT NULL DEFAULT 'all_success';