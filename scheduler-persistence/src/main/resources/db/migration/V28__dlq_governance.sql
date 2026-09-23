-- 3 方向·DLQ 治理强化:每任务自动重放上限 + 分片重放计数。
-- dlq_max_replays:该任务死信分片的自动重放额度(0=不自动重放,DEFAULT 0);超额分片由 DLQ 重放循环永久弃。
-- replay_count:分片累计被 requeue 重放次数(手动与自动重放均计入,供循环判定是否仍低于上限)。
ALTER TABLE app_task ADD COLUMN dlq_max_replays INT NOT NULL DEFAULT 0;
ALTER TABLE execution_shard ADD COLUMN replay_count INT NOT NULL DEFAULT 0;