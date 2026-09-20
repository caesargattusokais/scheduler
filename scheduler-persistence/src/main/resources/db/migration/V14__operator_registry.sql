-- 操作者目录:白名单 + 角色(OPERATOR/ADMIN)。这是"已声明身份上的授权策略"——X-Operator 由调用方自报,
-- 本表约束"只有登记且活跃的操作者能执行写端操作,敏感写(DELETE/取消/触发 DAG/操作者管理)需 ADMIN"。
-- 认证(密码/令牌)不在本增量范围;目录引导数据由运行时属性(OperatorBootstrap)幂等写入,故迁移不落地种子。
CREATE TABLE app_operator (
  name text PRIMARY KEY,
  role text NOT NULL CONSTRAINT app_operator_role CHECK (role IN ('OPERATOR', 'ADMIN')),
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);