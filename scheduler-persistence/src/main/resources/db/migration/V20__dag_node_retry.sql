-- 1a 单节点重试:定义侧每节点可配重试预算,运行侧按 attempt 计数。
-- 默认 node_max_retries=0(不重试)保持旧行为;仅显式配置的节点启用重试。
ALTER TABLE app_dag_node ADD COLUMN node_max_retries INT NOT NULL DEFAULT 0;
ALTER TABLE app_dag_node ADD COLUMN node_backoff_ms BIGINT NOT NULL DEFAULT 5000;

ALTER TABLE dag_run_node ADD COLUMN attempt INT NOT NULL DEFAULT 0;
ALTER TABLE dag_run_node ADD COLUMN next_retry_at TIMESTAMPTZ;