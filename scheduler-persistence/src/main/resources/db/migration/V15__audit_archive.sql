-- 审计保留/归档:按留存策略把 expired(row occurred_at < cutoff)行移动到归档表,并从 app_audit 删除。
-- 删除前先 COPY 再 DELETE(同事务、以 archived_at 批次为界),随后重链剩余行,使取证链在归档后仍 verified。
-- 归档行保留其归档时刻的 prev_hash/chunk_hash(冻结快照),不跨归档边界校验。
CREATE TABLE app_audit_archive (
  id bigint NOT NULL,
  operator text,
  action text,
  target_type text,
  target_id bigint,
  meta jsonb,
  diff jsonb,
  before_meta jsonb,
  source text,
  occurred_at timestamptz,
  prev_hash text,
  chunk_hash text,
  archived_at timestamptz NOT NULL DEFAULT now()
);

-- 重链中间段:按 id 升序重算剩余全部行 prev_hash/chunk_hash(首存行 prev_hash=NULL)。
-- 与 V13 存量回填同源(scheduler_audit_key/chunk),归档删除后调用以保持完整性校验通过。
CREATE OR REPLACE FUNCTION scheduler_audit_rechain() RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
  r record; prev text := NULL; c text; n bigint := 0;
BEGIN
  FOR r IN
    SELECT id, operator, action, target_type, target_id, occurred_at, meta, source, diff, before_meta
    FROM app_audit ORDER BY id
  LOOP
    c := scheduler_audit_chunk(prev, r.operator, r.action, r.target_type, r.target_id,
      r.occurred_at, r.meta, r.source, r.diff, r.before_meta);
    UPDATE app_audit SET prev_hash = prev, chunk_hash = c WHERE id = r.id;
    prev := c; n := n + 1;
  END LOOP;
  RETURN n;
END $$;