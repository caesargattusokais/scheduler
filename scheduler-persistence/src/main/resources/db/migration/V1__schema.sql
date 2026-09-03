CREATE TABLE app_task (
  id            BIGSERIAL PRIMARY KEY,
  name          TEXT NOT NULL,
  kind          TEXT NOT NULL DEFAULT 'cron',
  handler_ref   TEXT NOT NULL,
  cron          TEXT,
  shard_count            INT  NOT NULL DEFAULT 1,
  timeout_seconds        INT  NOT NULL DEFAULT 300,
  max_retries            INT  NOT NULL DEFAULT 0,
  backoff_ms             BIGINT NOT NULL DEFAULT 1000,
  retryable_failure_pattern TEXT,
  max_active_concurrent  INT  NOT NULL DEFAULT 8,
  enabled       BOOLEAN NOT NULL DEFAULT true,
  paused        BOOLEAN NOT NULL DEFAULT false,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE execution (
  id              BIGSERIAL PRIMARY KEY,
  task_id         BIGINT NOT NULL REFERENCES app_task(id),
  status          TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  args            TEXT,
  shard_index     INT NOT NULL DEFAULT 0,
  shard_count     INT NOT NULL DEFAULT 1,
  attempt         INT NOT NULL DEFAULT 0,
  worker_id       TEXT,
  lease_until     TIMESTAMPTZ,
  next_retry_at   TIMESTAMPTZ,
  started_at      TIMESTAMPTZ,
  finished_at     TIMESTAMPTZ,
  result_payload  TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_execution_task_status ON execution(task_id, status);
CREATE INDEX idx_execution_status ON execution(status);

CREATE TABLE execution_outcome (
  id           BIGSERIAL PRIMARY KEY,
  execution_id BIGINT NOT NULL REFERENCES execution(id),
  status       TEXT NOT NULL,
  at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  detail       TEXT
);
CREATE INDEX idx_outcome_execution ON execution_outcome(execution_id);
