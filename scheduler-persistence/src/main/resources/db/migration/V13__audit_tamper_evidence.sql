-- 审计取证链(tamper-evidence):每行存 prev_hash(链) + chunk_hash,chunk = sha256(行数据键 ‖ prev_hash)。
-- 哈希全程在 DB 侧计算(单一实现源):插入、存量回填、完整性校验共用下面两个 helper 函数,杜绝 Java/SQL 字节漂移。
CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE app_audit ADD COLUMN prev_hash text;
ALTER TABLE app_audit ADD COLUMN chunk_hash text;

-- 行数据键:固定列序拼接,列间以 | 分隔(operator/action/target_type 为短码,数据可变字段为 JSONB 规范文本)。
-- occurred_at 用 epoch 毫秒整数(而非 ::text),规避读端会话时区差异导致的文本漂移。
CREATE OR REPLACE FUNCTION scheduler_audit_key(
  p_prev text, p_operator text, p_action text, p_target_type text,
  p_target_id bigint, p_occurred_at timestamptz,
  p_meta jsonb, p_source text, p_diff jsonb, p_before jsonb)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT coalesce(p_prev,'') || '|' || coalesce(p_operator,'') || '|' || coalesce(p_action,'')
    || '|' || coalesce(p_target_type,'') || '|' || coalesce(p_target_id::text,'')
    || '|' || coalesce((extract(epoch from p_occurred_at)*1000)::bigint::text,'')
    || '|' || coalesce(p_meta::text,'') || '|' || coalesce(p_source,'')
    || '|' || coalesce(p_diff::text,'') || '|' || coalesce(p_before::text,'')
$$;

CREATE OR REPLACE FUNCTION scheduler_audit_chunk(
  p_prev text, p_operator text, p_action text, p_target_type text,
  p_target_id bigint, p_occurred_at timestamptz,
  p_meta jsonb, p_source text, p_diff jsonb, p_before jsonb)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT encode(digest(scheduler_audit_key(
    p_prev,p_operator,p_action,p_target_type,p_target_id,p_occurred_at,p_meta,p_source,p_diff,p_before), 'sha256'), 'hex')
$$;

-- 存量回填:id 升序补齐链,使链从第 1 行起不中断;首行 prev_hash=NULL。
DO $$
DECLARE
  r record; prev text := NULL; c text;
BEGIN
  FOR r IN
    SELECT id, operator, action, target_type, target_id, occurred_at, meta, source, diff, before_meta
    FROM app_audit ORDER BY id
  LOOP
    c := scheduler_audit_chunk(prev, r.operator, r.action, r.target_type, r.target_id,
      r.occurred_at, r.meta, r.source, r.diff, r.before_meta);
    UPDATE app_audit SET prev_hash = prev, chunk_hash = c WHERE id = r.id;
    prev := c;
  END LOOP;
END $$;