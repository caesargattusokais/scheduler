# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

分布式业务任务调度平台 (distributed business task scheduler) — an "SchedulerX-like" scheduler focusing on correctness: DB-claim dispatch, reliability triad (timeout/lease-orphan-retry/DLQ), sharding, and DAG workflows, with a React console. Docs and most domain code are **bilingual (Chinese + English)** — match the surrounding language when editing. Design docs are authoritative: work is driven milestone-by-milestone through `docs/superpowers/specs/` (design) + `docs/superpowers/plans/` (implementation), each on a feature branch with whole-branch review before merge.

- Java 21, Spring Boot 3.2.5, PostgreSQL 16, Spring JDBC (hand-written SQL — **no JPA/Hibernate**), Micrometer/Prometheus, Flyway migrations.
- Frontend: React 19 + TypeScript + Vite 6 + Tailwind 4, in `web/` (ships via Vite to :5173 with proxies `/api` + `/actuator` → :8080).

## Modules

| Module | Role |
|--------|------|
| `scheduler-core` | Pure domain (no deps): state machines, idempotency keys, DAG/execution/shard models. Testable without a DB. |
| `scheduler-persistence` | `Jdbc*Repository` implementations (one per aggregate), Flyway migrations (`V1`–`V33`), retry policy + failure resolver. |
| `scheduler-server` | Spring Boot **control plane** :8080 — REST API (`/api/v1`), TriggerEngine/DagEngine/EventEngine, Reconciler, AlertEngine, notifications, audit, Prometheus meters. Does **not** execute handlers; only validates `handlerRef` against the live worker registry. |
| `scheduler-worker` | Independent **executor** process :8081+ — DB-driven claim loop, heartbeat/liveness, lease renewal, runs `Handler`s. Shares the DB with server (workers must not run Flyway; server builds schema). |
| `web` | React console — routes `/tasks`, `/executions`, `/events`, `/dlq`, `/stuck`, `/reconcile`, `/dags`, `/metrics`, `/alerts`, `/audits`, `/operators` (admin-gated), `/notifications`, `/settings`, `/login`, `/force-password`. |

**Module dependency:** core → persistence → server, and persistence → worker. Worker declares `scheduler-server` as **test-only** (to drive Reconciler in liveness regression tests).

## Commands

Run from repo root (Windows + Git Bash; Maven multi-module, parent `pom` — use `-pl` to target a module).

```bash
mvn -q compile            # compile all modules
mvn -q test-compile       # compile tests
mvn test                  # run tests for all modules (persistence/server/worker need Docker — see below)
mvn test -pl scheduler-server -Dtest=ApiIntegrationTest test   # single test class
mvn spring-boot:run -pl scheduler-server        # run control plane :8080
mvn spring-boot:run -pl scheduler-worker        # run worker (:8081)
bash start-dev.sh          # boot server + worker together (needs prior `mvn package`); also has `[server|worker]`
```

**Tests require Docker.** `scheduler-core` tests are pure JUnit (no DB). All `persistence`/`server`/`worker` tests use Testcontainers (`postgres:16-alpine`) via `AbstractPostgresTest`. If Docker isn't running, those tests fail to start containers. The `api.version=1.40` system property is passed to Surefire by the root pom (Windows Testcontainers needs the newer Docker API min); don't remove it.

Frontend (from `web/`):
```bash
npm install && npm run dev      # dev server :5173, proxies /api & /actuator to :8080
npm run build                   # tsc -b && vite build (type-checks then bundles)
npm run preview                 # preview built dist
```

`start-dev.sh` defaults DB to `localhost:5432` (user `postgres`), seeds a demo `demo_sync_source` table if `SCHEDULER_DEMO_SYNC_SEED_ROWS>0`. Server env overrides: `DB_URL`/`DB_USER`/`DB_PASSWORD`, `SCHEDULER_WORKER_ID`, `SCHEDULER_LOOP_SCAN_DELAY_MS`, `SCHEDULER_WORKER_STALE_AFTER_SECONDS`, `SCHEDULER_RECONCILE_DELAY_MS`. Worker env: `SCHEDULER_LOOP_WORK_DELAY_MS`, `SCHEDULER_LEASE_SECONDS`, `SCHEDULER_LEASE_RENEW_SECONDS`.

## Architecture — the big picture

**Two surfaces, deliberately separated:** CLI-ish REST for human ops (low-frequency, `/api/v1`), and **DB claim/publish** for the execution hot path. Executors never talk to the API; the scheduler never tells a worker to "go". Dispatch = writing a `DUE` execution/shard row; a worker pulls it with an atomic claim.

**Single authority = the DB.** State machines (`ExecutionTransitions`, `DagRunNodeTransitions`, etc.) encode valid transitions; claims/lease/reclaim are **atomic conditional UPDATEs (CAS)** whose affected-row count is the concurrency verdict. The **Reconciler** (leader-only) derives the "should-be" state declaratively from trigger/lease/retry plans and CASes the DB toward it — it never relays events forward. The `execution_outcome`/audit log is appends-only and records what happened; it is **not** the reconciler's source of truth.

**Six design disciplines** (the design doc §7, worth internalizing before touching scheduling logic):
1. Claim is atomic (DB CAS, exactly-once).
2. Lease (liveness, handles crash) and timeout (duration, cooperative cancel) are separate — never conflated.
3. Every reconciler mutation is one CAS SQL.
4. A sharded run's **parent state is derived only by the reconciler** (shard workers must never race to finalize the parent).
5. Workflows are deliberately minimal — no auto-rollback, no whole-batch auto-retry, no DAG timeout; failure pauses a batch for human action (rerun the failed node or mark FAILED).
6. Control plane ≠ execution plane.

**Leader election** = PostgreSQL **advisory lock** (`AdvisoryLockLeaderElection`, session-scoped, via a dedicated connection). Only the leader runs global scans (Trigger/Dag/Event engines, Reconciler, working of alerts/notifications/AuditRetention/DlqReplay). Standbys idle — no estrangement, no transaction overlap. Loops are `ConfigurableLoop` instances defined in `scheduler-server/.../config/Beans.java`, all fixed-period, externally configurable via runtime-config hot keys.

**Worker liveness contract:** the authoritative source is the DB `worker` table row — an `ALIVE` status whose `last_seen` is refreshed on a ~10s heartbeat, staying a few seconds behind `now()`; `/actuator/health` (HTTP) is an extra probe, not the source. `scheduler_worker_active` metric and handler-ref validation use the same `last_seen ≤ 30s` window. Note the reclaim path reads the configurable `scheduler.worker.stale-after-seconds` instead, and that property is **effectively capped at 60s**: the worker purges its own row once `last_seen` is older than 60s, and `findExpiredRunning` treats a *missing* row as dead — so raising it past 60s does not extend the reclaim window. Controller `/api/v1/tasks` create/update validates `handlerRef` against **live** worker refs (`AvailableHandlerRefs`) — a task referencing a dead worker is rejected.

**Metrics:** global lazy gauges that read the DB at scrape time (no per-task startup snapshots): `scheduler_active_runs`, `scheduler_due_queue_max_age_seconds`, `scheduler_dag_runs_active`, `scheduler_dlq_depth`, `scheduler_worker_active`, plus execution SLI meters (`scheduler_execution_{completed,failed,failure_rate,latency_p*}`) and the E3 reconcile family (`scheduler_reconcile_{timeout_reclaimed,orphan_reclaimed,aggregated,near_miss}_1h`, `scheduler_reconcile_backlog`, `scheduler_reconcile_backlog_max_age_seconds`) near the end of `Beans.java`. (A `scheduler_reconcile_reclaimed_1h` aggregate was dropped — it is `timeout_reclaimed + orphan_reclaimed`, derivable in PromQL.) Console `/metrics` renders 15 gauge cards + execution-SLO cards (the SLO block reads `/api/v1/metrics/executions`, not Prometheus). Meters are defined once in `Beans.java`.

**Sharding:** one `execution` (parent, `shardCount=N`) expands to N `execution_shard` rows, each a mini-execution with the full lifecycle (claim/lease/retry/orphan/timeout/DLQ). `idempotencyKey = executionId + shardIndex`. Parent finalizes only via reconciler derivation; `shardFailPolicy` is `FAIL_FAST` (default) or `COMPLETE_ALL`.

## REST API conventions

Prefix `/api/v1`; all errors through `ApiExceptionHandler` (4xx business, incl. 404/409; 5xx server). **All list endpoints return `Page<T> = { items, total, offset, limit }`** with common `limit` (default 100, clamp 1–500) and `offset` query params — `total` is the filtered full count (frontend renders pagination/counters from it). Backend pagination lives in `web/Page.java` + `Paging.java`. OpenAPI schema is snapshot at `docs/openapi/api-docs.json`; a `docs`-adjacent API table lives in `README.md` — keep both in sync when changing endpoints.

Endpoint families: `/tasks`, `/executions` (incl. `/rerun`, `/cancel`, `/dlq`, `/shards/{id}/requeue`, new `/stuck` view + `/shards/{id}/abandon`), `/dags` (incl. `/runs/...` + single-node `/rerun`), `/alerts`, `/audit`, `/operators`, `/notifications`, `/runtime-config`, `/events`, `/auth`, `/handlers` (available refs), `/metrics`, `/reconcile` (`/backlog` + `/activity`).

## Notable subsystems (recent milestones, high magnetic surface)

- **Stuck-alive + force-abandon** (episode e2b): `StuckShard` view (`findStuckAliveRunning`/`countStuckAliveRunning`), global alert rule, and a **liveness-gated** `POST /shards/{id}/abandon`. The gate anchor is a single DB-clock read (spec "单一时钟") so the JVM clock can't skew and double-run — the ALIVE gate checks `lease_until` against that one DB timestamp.
- **Idempotency / exactly-once** (e1): `IdempotencyKeys`, unique constraints, and `idempotencyKey` re-derivation so double-trigger/reclaim cannot create two rows.
- **Strong operator auth** + audit (v3-d era): `scheduler.operators` bootstrap (`alice:ADMIN,...`) seeds the directory idempotently; `scheduler.operators.default-password` bootstraps BCrypt passwords. `OperatorInterceptor` guards write endpoints by role. `AuditRecorder` + retention.
- **Notifications**: event `fire` → outbox (`app_notification`) → leader `NotificationLoop` HMAC-signs and delivers to subscribed webhooks (`app_webhook`), with backoff/retry and INCLUDE/EXCLUDE task/DAG scoping.

## Development workflow

Follow the existing SDD rhythm rather than improvising: milestone docs live under `docs/superpowers/` (21 specs + 25 plans); `.superpowers/sdd/<milestone>/` holds per-task progress.md + review diffs. Each milestone lands on its own feature branch, is implemented task-by-task with review, then whole-branch re-reviewed before merge to `main`. When asked for observability/reliability changes, the design docs are the source of truth — read the relevant spec before implementing, and update `README.md` (run table), the API table, and `docs/openapi/api-docs.json` when endpoints or env vars change.

`web/node_modules`, `web/dist` and `web/tsconfig.tsbuildinfo` are **git-ignored** (root `.gitignore`), so the frontend needs an `npm install` before `npm run build` on a fresh clone.