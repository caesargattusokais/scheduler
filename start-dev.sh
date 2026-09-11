#!/usr/bin/env bash
# 本地一键启动 scheduler 分布式栈(M6.3 后:server 控制面 + 独立 worker)。
# 用法: bash start-dev.sh [server|worker]    # 不带参数=同时拉起两者
# 依赖: 已 mvn package(有 target/classes)与 dependency:build-classpath 生成的 cp 文件;
#        若 cp 文件缺失会自动生成。
set -euo pipefail
cd "$(dirname "$0")"

# --- DB 连接(可用环境变量覆盖;默认本机 postgres/scheduler) ---
DB_URL="${DB_URL:-jdbc:postgresql://localhost:5432/scheduler}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres123}"

ensure_cp() {
  local mod="$1"; local out="/tmp/${mod}-cp.txt"
  if [[ ! -f "$out" ]]; then
    echo "[start-dev] 生成 ${mod} classpath..." >&2
    (cd "$mod" && mvn -q -o dependency:build-classpath -Dmdep.outputFile="$out") >&2
  fi
  printf '%s' "$out"
}

boot_server() {
  local cp; cp="scheduler-server/target/classes;$(cat "$(ensure_cp scheduler-server)")"
  echo "[start-dev] 启动 server 控制面 :8080 ..."
  DB_URL="$DB_URL" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" \
  SERVER_PORT=8080 java -cp "$cp" dev.scheduler.server.SchedulerApplication
}

boot_worker() {
  local cp; cp="scheduler-worker/target/classes;$(cat "$(ensure_cp scheduler-worker)")"
  echo "[start-dev] 启动 worker :8081 (ref: demo) ..."
  DB_URL="$DB_URL" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" \
  java -cp "$cp" dev.scheduler.worker.WorkerApplication
}

case "${1:-all}" in
  server) boot_server ;;
  worker) boot_worker ;;
  *)
    boot_server & pws=$!
    boot_worker & pww=$!
    trap 'kill $pws $pww 2>/dev/null' EXIT
    echo "[start-dev] server=$pws worker=$pww; 控制台: http://localhost:5173/tasks (前端请另跑 cd web && npm run dev)"
    wait -n; wait
    ;;
esac