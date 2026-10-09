-- V32: E3 progress-heartbeat —— 区分「慢而健康」vs「真卡住」。
-- execution_shard.last_progress_at:handler 最近一次 touch() 落库的时间(NULL=从未 touch)。
--   由 DB now() 单一时钟盖章,每续租周期合并 flush 一次(见 ShardRepository.recordProgress)。
-- app_task.progress_timeout_seconds:进度陈旧预算(NULL/0=不启用 progress 监控,退化 E2B 现状)。
ALTER TABLE execution_shard ADD COLUMN last_progress_at timestamptz;
ALTER TABLE app_task       ADD COLUMN progress_timeout_seconds int;
