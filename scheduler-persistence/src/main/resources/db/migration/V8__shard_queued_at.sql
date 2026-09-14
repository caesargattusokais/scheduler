-- shard 的「入队时刻」:指标「DUE 最深队龄」据此量真正的排队时长。
-- 此前用父 execution.created_at 近似,重试/重排(FAILED→DUE)会沿用最初创建时间导致队龄虚高。
-- queued_at 在每次进入 DUE 时被据点(创建默认 now(),重试/requeue 显式 now())。
ALTER TABLE execution_shard ADD COLUMN queued_at TIMESTAMPTZ NOT NULL DEFAULT now();