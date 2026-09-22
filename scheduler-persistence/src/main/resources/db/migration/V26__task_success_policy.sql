-- 3c 分片聚合/部分成功:任务级成功聚合策略,默认 NULL/NONE = 严格 all-or-nothing(任一失败片 → 父失败)。
-- success_policy_type ∈ (NULL, 'NONE', 'RATIO_PERCENT', 'MIN_SUCCESS', 'MAX_FAILURES');
-- success_policy_value 语义随类型:占比%(1-99) / 最少成功片数 / 最多容忍失败数。
ALTER TABLE app_task ADD COLUMN success_policy_type VARCHAR(20);
ALTER TABLE app_task ADD COLUMN success_policy_value INTEGER;