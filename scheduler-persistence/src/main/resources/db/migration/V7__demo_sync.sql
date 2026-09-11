-- V7: 真实业务同步演示表。建表 + 源表一次性灌 1000 万行(种子,仅当源为空,幂等)。
-- 供 ref=sync 的 worker handler 做「数据同步」:把源表各主键区间批量 INSERT 到目标表,验证
-- 调度的分片并行 + 父汇本能撑住千万级批处理。
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

-- 仅当源表为空时播种(幂等;重复启动不再重灌)。约 1000 万行。
INSERT INTO demo_sync_source
  (id, batch_no, acct_no, amount, quantity, rate, active, category, email, region, memo, check_flag, cfg_json, created_at, updated_at)
SELECT gs,
       (gs / 10000)::bigint,
       'acct-' || lpad((gs % 500000)::text, 8, '0'),
       (gs % 2000000 + 1)::numeric / 100,
        gs % 5000,
      (gs % 1000)::double precision / 10,
      (gs % 2)::int = 1,
      chr(65 + (gs % 6)) || '-cat',
      'user' || (gs % 800000)::text || '@corp.example.com',
      CASE gs % 4 WHEN 0 THEN 'CN' WHEN 1 THEN 'US' WHEN 2 THEN 'EU' ELSE 'JP' END,
      'row-' || gs,
       gs % 7,
      jsonb_build_object('seq', gs, 'mod97', gs % 97),
      now(), now()
FROM generate_series(1, 10000000) AS gs
WHERE NOT EXISTS (SELECT 1 FROM demo_sync_source LIMIT 1)
ON CONFLICT (id) DO NOTHING;