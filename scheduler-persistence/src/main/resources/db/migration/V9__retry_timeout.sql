-- 重试策略化 + 执行超时纵深(运行时深化子项目 1):
-- app_task 加退避模式/封顶/整轮预算三新列;execution_shard 加整轮重试预算到期点(首次进重试置 now()+W)。
-- timeout_seconds 为既有列,semantics 变更(0=不超时、>0=强制执行),不含在本迁移列变更内。
ALTER TABLE app_task
  ADD COLUMN retry_mode     VARCHAR(16) NOT NULL DEFAULT 'exponential',
  ADD COLUMN retry_cap_ms   BIGINT NULL,
  ADD COLUMN retry_budget_ms BIGINT NULL;
ALTER TABLE execution_shard
  ADD COLUMN retry_budget_until TIMESTAMPTZ NULL;