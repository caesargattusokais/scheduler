-- V23: 入站事件 outbox + 事件触发时钟。POST /api/v1/events 写入 PENDING 行(dedupe_key 唯一防重放),
-- leader 门控 EventEngine 周期扫描:把事件 route_key 匹配到订阅该路由的启用任务(event_routes 非空),
-- 为每条事件创建一个父 execution(dedupe 幂等),回填 task_id/execution_id 后置 DISPATCHED。
CREATE TABLE app_task_event (
  id            bigserial PRIMARY KEY,
  route_key     text NOT NULL,
  payload_json  jsonb NOT NULL DEFAULT '{}'::jsonb,
  dedupe_key    text NOT NULL UNIQUE,
  status        text NOT NULL DEFAULT 'PENDING',   -- PENDING(待分派) / DISPATCHED(已投递)
  task_id       bigint,                            -- 分派后回填所触发的任务(id)
  execution_id  bigint,                            -- 分派后回填所创建的父 execution(id)
  created_at    timestamptz NOT NULL DEFAULT now(),
  dispatched_at timestamptz
);
CREATE INDEX app_task_event_pending_idx ON app_task_event (status);
CREATE INDEX app_task_event_route_idx ON app_task_event (route_key);

-- 3b 事件触发时钟:event_routes 非空 = 事件触发(与 cron/intervalSeconds 三选一互斥)。
ALTER TABLE app_task ADD COLUMN event_routes text[];