-- V29: 通知订阅任务维度。投递器据此收/放,避免「订阅了事件类型就全任务轰炸」:
--   scope_mode 'ALL'     → 不按任务过滤(现状默认);
--   scope_mode 'INCLUDE' → 白名单:仅选中任务/其所在 DAG 命中终态才投;
--   scope_mode 'EXCLUDE' → 黑名单:排除选中任务/DAG,其余都投。
-- selected_task_ids/selected_dag_ids 存选中任务/DAG 的 id(bigint[])。既有行默认 ALL → 向后兼容。
ALTER TABLE app_webhook
  ADD COLUMN scope_mode        text    NOT NULL DEFAULT 'ALL',
  ADD COLUMN selected_task_ids bigint[] NOT NULL DEFAULT '{}',
  ADD COLUMN selected_dag_ids  bigint[] NOT NULL DEFAULT '{}';