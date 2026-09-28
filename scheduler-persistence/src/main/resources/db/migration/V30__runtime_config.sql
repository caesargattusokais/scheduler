CREATE TABLE app_runtime_config (
  key        text PRIMARY KEY,
  value      text NOT NULL,
  updated_by text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);