-- V33: reconcile decision(对账决策留痕)append-only 表。
-- phase  timeout_reclaim / orphan_reclaim / parent_aggregate / near_miss;
-- target_type shard(分片) / execution(父);outcome REQUEUED/DEAD_LETTER(回收)或父终态(汇聚)或 SKIPPED(near-miss)。
CREATE TABLE reconcile_decision (
  id          bigserial PRIMARY KEY,
  created_at  timestamptz NOT NULL DEFAULT now(),
  phase       text NOT NULL,
  target_type text NOT NULL,
  target_id   bigint NOT NULL,
  task_id     bigint,
  reason      text NOT NULL,
  outcome     text,
  from_status text,
  to_status   text,
  attempt     int,
  detail      text
);
-- 1h 窗口计数 + 活动视图倒序分页。
CREATE INDEX reconcile_decision_created_at_idx ON reconcile_decision (created_at);
-- 取证定位。
CREATE INDEX reconcile_decision_target_id_idx ON reconcile_decision (target_id);