-- V25: 1c DAG 版本化——run 封印整份定义。app_dag 显式 version(编辑即 ++);dag_run/dag_run_node/dag_run_edge
-- 在 createRun 时封印 version + 节点行为(run_if/node_max_retries/node_backoff_ms) + 边。DagEngine 推进只读封印快照,
-- 之后定义怎么改都不影响在飞/历史 run(定义表保持单版原地改)。
ALTER TABLE app_dag ADD COLUMN version INT NOT NULL DEFAULT 1;

ALTER TABLE dag_run ADD COLUMN dag_version INT;

ALTER TABLE dag_run_node ADD COLUMN run_if TEXT,
  ADD COLUMN node_max_retries INT NOT NULL DEFAULT 0,
  ADD COLUMN node_backoff_ms BIGINT NOT NULL DEFAULT 5000;

CREATE TABLE dag_run_edge (
  id BIGSERIAL PRIMARY KEY,
  dag_run_id BIGINT NOT NULL REFERENCES dag_run(id),
  from_node_id BIGINT NOT NULL REFERENCES dag_run_node(id),
  to_node_id   BIGINT NOT NULL REFERENCES dag_run_node(id),
  UNIQUE (dag_run_id, from_node_id, to_node_id)
);

-- 历史回填(版本化前定义不可变 → 当前定义值即旧 run 实际所用,回填保历史语义):
-- 行为列按 (dag_run.dag_id, node_key) 从当前 app_dag_node 归位。
UPDATE dag_run_node drn
  SET run_if = COALESCE(adn.run_if, 'all_success'),
      node_max_retries = COALESCE(adn.node_max_retries, 0),
      node_backoff_ms = COALESCE(adn.node_backoff_ms, 5000)
  FROM dag_run dr, app_dag_node adn
  WHERE drn.dag_run_id = dr.id AND adn.dag_id = dr.dag_id AND adn.node_key = drn.node_key;

-- 边按 (dag.dag_id, node_key) 归位到各 run 内的对应 node。
INSERT INTO dag_run_edge (dag_run_id, from_node_id, to_node_id)
SELECT dr.id, frn.id, trn.id
FROM dag_run dr
JOIN app_dag_node fan  ON fan.dag_id  = dr.dag_id
JOIN dag_edge    de    ON de.from_node_id = fan.id
JOIN app_dag_node tan  ON tan.id     = de.to_node_id
JOIN dag_run_node frn  ON frn.dag_run_id = dr.id AND frn.node_key = fan.node_key
JOIN dag_run_node trn  ON trn.dag_run_id = dr.id AND trn.node_key = tan.node_key;