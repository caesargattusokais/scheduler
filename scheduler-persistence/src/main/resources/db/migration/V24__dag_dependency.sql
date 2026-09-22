-- V24: 跨 DAG 依赖(事件链)。app_dag 可声明依赖某个上游 DAG;当该上游 DAG 的一次 run 达到终态 SUCCESS,
-- 引擎为每个启用、非暂停的下游 DAG 幂等新建一次 run(键 dep:{上游runId}),独占触发(设了依赖则 cron 须为空)。
ALTER TABLE app_dag ADD COLUMN depends_on_dag_id BIGINT REFERENCES app_dag(id);