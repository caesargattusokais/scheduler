CREATE TABLE execution_shard (
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  execution_id      BIGINT NOT NULL REFERENCES execution(id) ON DELETE CASCADE,
  shard_index       INT NOT NULL,
  shard_data        TEXT,
  status            TEXT NOT NULL DEFAULT 'DUE',
  worker_id         TEXT,
  lease_until       TIMESTAMPTZ,
  attempt           INT NOT NULL DEFAULT 0,
  next_retry_at     TIMESTAMPTZ,
  cancel_requested  BOOLEAN NOT NULL DEFAULT false,
  dead_letter       BOOLEAN NOT NULL DEFAULT false,
  started_at        TIMESTAMPTZ,
  finished_at       TIMESTAMPTZ,
  result_payload    TEXT,
  UNIQUE (execution_id, shard_index)
);
CREATE INDEX idx_shard_exec_status ON execution_shard (execution_id, status);

CREATE TABLE execution_shard_outcome (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  shard_id   BIGINT NOT NULL REFERENCES execution_shard(id) ON DELETE CASCADE,
  status     TEXT NOT NULL,
  detail     TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_shard_outcome_shard ON execution_shard_outcome (shard_id);
