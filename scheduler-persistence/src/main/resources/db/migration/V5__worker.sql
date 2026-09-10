-- M6: worker 注册表(DB 当注册表)。refs = 该 worker 服务的 handler refs(逗号拼接 text)。
-- last_seen 心跳时间;status ALIVE/DEAD/DRAINING。
CREATE TABLE worker (
  id         varchar     NOT NULL PRIMARY KEY,
  refs       text        NOT NULL,
  last_seen  timestamptz NOT NULL,
  status     varchar     NOT NULL
);