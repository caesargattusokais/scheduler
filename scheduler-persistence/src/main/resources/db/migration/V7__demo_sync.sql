-- V7: 真实业务同步演示表。仅建 DDL(源表 + 目标表);数据种子不再放在迁移里,改由 server 端
-- 按需的运行时 seeder(scheduler.demo.sync-seed-rows)在启动后播种——避免每次全新 Testcontainers
-- 测试库都重灌千万行拖慢整个测试套件。种子逻辑见 dev.scheduler.server.config.Beans#seedDemoSync。
CREATE TABLE IF NOT EXISTS demo_sync_source (
  id          BIGINT PRIMARY KEY,
  batch_no    BIGINT              NOT NULL DEFAULT 0,
  acct_no     VARCHAR(32)         NOT NULL,
  amount      NUMERIC(20, 2)      NOT NULL,
  quantity    INTEGER             NOT NULL,
  rate        DOUBLE PRECISION    NOT NULL,
  active      BOOLEAN             NOT NULL,
  category    VARCHAR(16)         NOT NULL,
  email       VARCHAR(128),
  region      CHAR(2),
  memo        TEXT,
  check_flag  INTEGER             NOT NULL,
  cfg_json    JSONB               NOT NULL DEFAULT '{}'::jsonb,
  created_at  TIMESTAMPTZ         NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ         NOT NULL DEFAULT now(),
  synced_at   TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS demo_sync_target (LIKE demo_sync_source INCLUDING ALL);

CREATE INDEX IF NOT EXISTS idx_sync_source_synced ON demo_sync_source(synced_at);
CREATE INDEX IF NOT EXISTS idx_sync_source_category ON demo_sync_source(category);