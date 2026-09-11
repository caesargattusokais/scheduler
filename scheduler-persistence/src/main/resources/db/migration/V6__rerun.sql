-- V6: 重跑溯源。execution.rerun_of 指向被本轮重跑的源轮执行 id(普通触发/cron/dag 为 NULL)。
ALTER TABLE execution ADD COLUMN rerun_of BIGINT REFERENCES execution(id);
CREATE INDEX idx_execution_rerun_of ON execution(rerun_of);