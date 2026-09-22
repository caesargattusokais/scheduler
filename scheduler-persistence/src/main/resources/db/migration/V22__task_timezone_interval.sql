-- 3a 时区/间隔触发器:任务可选时区与秒级间隔触发。
-- timezone 仅解释 cron 用(默认 UTC);interval_seconds 非空 = 间隔触发(相对 UTC 的 epoch 对齐边界,与 task 时区无关),否则走 cron。
-- 二者互斥:建/改任务时校验「恰具其一」(cron 或 interval_seconds)。
ALTER TABLE app_task ADD COLUMN timezone TEXT NOT NULL DEFAULT 'UTC';
ALTER TABLE app_task ADD COLUMN interval_seconds INTEGER;