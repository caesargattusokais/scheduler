CREATE TABLE app_dag (
  id          BIGSERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  description TEXT,
  cron        TEXT,
  enabled     BOOLEAN NOT NULL DEFAULT true,
  paused      BOOLEAN NOT NULL DEFAULT false,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE app_dag_node (
  id          BIGSERIAL PRIMARY KEY,
  dag_id      BIGINT NOT NULL REFERENCES app_dag(id) ON DELETE CASCADE,
  node_key    TEXT NOT NULL,
  task_id     BIGINT NOT NULL REFERENCES app_task(id),
  sort_order  INT NOT NULL DEFAULT 0,
  UNIQUE (dag_id, node_key)
);

CREATE TABLE dag_edge (
  id          BIGSERIAL PRIMARY KEY,
  dag_id      BIGINT NOT NULL REFERENCES app_dag(id) ON DELETE CASCADE,
  from_node_id BIGINT NOT NULL REFERENCES app_dag_node(id) ON DELETE CASCADE,
  to_node_id   BIGINT NOT NULL REFERENCES app_dag_node(id) ON DELETE CASCADE,
  UNIQUE (dag_id, from_node_id, to_node_id),
  CHECK (from_node_id <> to_node_id)
);

CREATE TABLE dag_run (
  id              BIGSERIAL PRIMARY KEY,
  dag_id          BIGINT NOT NULL REFERENCES app_dag(id),
  idempotency_key TEXT NOT NULL UNIQUE,
  status          TEXT NOT NULL DEFAULT 'PENDING',
  trigger_reason  TEXT NOT NULL,
  cancel_requested BOOLEAN NOT NULL DEFAULT false,
  finished_at     TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_dag_run_dag_status ON dag_run (dag_id, status);

CREATE TABLE dag_run_node (
  id            BIGSERIAL PRIMARY KEY,
  dag_run_id    BIGINT NOT NULL REFERENCES dag_run(id) ON DELETE CASCADE,
  node_key      TEXT NOT NULL,
  task_id       BIGINT NOT NULL REFERENCES app_task(id),
  execution_id  BIGINT REFERENCES execution(id),
  status        TEXT NOT NULL DEFAULT 'PENDING',
  sort_order    INT NOT NULL DEFAULT 0,
  detail        TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at   TIMESTAMPTZ,
  UNIQUE (dag_run_id, node_key)
);
CREATE INDEX idx_dag_run_node_run_status ON dag_run_node (dag_run_id, status);

CREATE TABLE dag_run_outcome (
  id          BIGSERIAL PRIMARY KEY,
  dag_run_id  BIGINT NOT NULL REFERENCES dag_run(id) ON DELETE CASCADE,
  status      TEXT NOT NULL,
  detail      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE dag_run_node_outcome (
  id          BIGSERIAL PRIMARY KEY,
  node_id     BIGINT NOT NULL REFERENCES dag_run_node(id) ON DELETE CASCADE,
  status      TEXT NOT NULL,
  detail      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
