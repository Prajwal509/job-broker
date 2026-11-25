# Northwind Job Broker

A simulated GPU job broker: it queues jobs from teams, places them on nodes,
starts and stops nodes to control cost, and reports cost per team. State is in
SQLite, time is a simulated clock, and the cloud provider is a seeded fake.

## Quick start

```sh
python3.11 -m venv .venv && source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest -q                       # ~3 s, includes two full trace replays
python -m broker.trace replay --trace data/01a_trace.csv --policy fair --seed 42 --out out
python -m broker.trace compare --trace data/01a_trace.csv --seeds 20 --out out   # ~40 s
export ADMIN_TOKEN=dev-secret && python -m uvicorn --factory api:create_app --port 8000
```

Docker: `cp .env.example .env && docker compose up --build`.

## Layout

| File | Role |
|---|---|
| `api.py` | FastAPI HTTP layer (`create_app` factory) |
| `broker/broker.py` | All state changes: submit, cancel, complete, scheduling, reaper, event loop |
| `broker/scheduler.py` | Queue order only (fair or fifo) |
| `broker/provider.py` | Fake cloud: bursty outages, 4-8 min boot, 3% never ready |
| `broker/store.py` | SQLite tables: jobs, nodes, ledger, meta |
| `broker/billing.py` | Cost report from the occupancy ledger |
| `broker/trace.py` | Trace cleaning, replay and policy comparison |

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/jobs` | Submit a job; same `team` + `idempotency_key` returns the same job (200 instead of 201) |
| GET | `/jobs/{id}` | Read job state |
| POST | `/jobs/{id}/cancel` | Cancel a queued or running job |
| POST | `/jobs/{id}/complete` | A running job reports it ended: `{"outcome": "succeeded" \| "failed"}` |
| GET | `/queue` | Queued jobs in scheduling order |
| GET | `/nodes` | Nodes, tenant and free GPUs |
| GET | `/cost?from=&to=&team=` | Cost per team for an ISO8601 UTC window |
| POST | `/admin/advance` | Advance simulated time (`X-Admin-Token`; max 7 days per call) |
| GET | `/healthz`, `/readyz` | Liveness / readiness |
| GET | `/metrics` | Prometheus text metrics |

```sh
curl -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"team":"vision","gpus":1,"estimated_minutes":30,"priority":"normal","idempotency_key":"v1"}'
curl -X POST localhost:8000/admin/advance -H 'X-Admin-Token: dev-secret' \
  -H 'Content-Type: application/json' -d '{"minutes":8}'
curl -X POST localhost:8000/jobs/job-000001/complete -H 'Content-Type: application/json' \
  -d '{"outcome":"succeeded"}'
curl 'localhost:8000/cost?from=1970-01-01T00:00:00Z&to=1970-01-02T00:00:00Z'
```

Errors are JSON: `{"error": "validation_error" | "conflict" | "not_found" | ..., "detail": ...}`.

## Key decisions and assumptions

- **Security first:** a node runs jobs of one team at a time. It can move to
  another team only when its last job has ended (assumed wiped between tenants).
- **Placement:** a queued job goes to a ready node of its own team (or a free
  one) with enough free GPUs, tightest fit first. A node that is booting is
  reserved for the team that caused the launch.
- **Launching:** a team gets a new node only for jobs that will not fit on the
  nodes already booting for it, checked job by job (a job must fit on one node).
  The node is sized for the team's whole queue (capped at gpu-l), because with a
  10-node cap, small nodes would cap total capacity.
- **Fairness (`fair`, default):** priority first; a job waiting over 60 min moves
  up one tier. Within a tier, the team with the fewest GPU-hours in the last 24 h
  goes first, then teams take turns, FIFO within a team. `fifo` is the alternative.
- **High priority** changes queue order only. It never pre-empts running jobs.
- **Idle cost** goes to the team that last used the node: its demand caused
  the launch. Nodes that never ran a job (e.g. failed launches) are
  `unattributed_cost`, visible as provider waste.
- **Idle reaper:** a ready node idle for more than `IDLE_TIMEOUT_MINUTES` is
  terminated, but never before it is 10 minutes old (billing minimum).
- **Provider failures:** per-node-type exponential backoff (1 min doubling to
  30 min) with seeded jitter; a size in backoff falls back to a bigger size.
  Never-ready launches are terminated at 15 min and their jobs stay queued.
- **Completion:** HTTP jobs end via `/complete` or `/cancel`. Replay jobs end at
  `actual_minutes`; the scheduler only ever sees `estimated_minutes`.
- **Restart:** jobs, nodes, the cost ledger, the simulated clock, backoff state and
  the fake provider's RNG state are in SQLite, so a restart continues exactly.
- **"Start within 10 minutes"** is measured, not guaranteed (no warm pool).

## Stakeholder tradeoffs

- ML engineers: aging and team turn-taking stop one team from starving others.
  The 10-minute wish is reported per team, and it is not met for vision (see below).
- Finance: every dollar of the provider bill is assigned to a team or to
  `unattributed_cost`, with idle cost shown separately.
- Security: hard one-team-per-node rule, even though it costs packing efficiency.
- Platform team: retries, backoff and the reaper are automatic; alerts in
  [docs/RUNBOOK.md](docs/RUNBOOK.md) are tickets except a stalled scheduler.

## Trace replay

See [docs/REPLAY.md](docs/REPLAY.md). Results are deterministic for a given seed.
Over 20 provider seeds, fair and fifo cost the same within noise (~$3,440 vs
~$3,420 per week, ~34% idle). Fair cuts the light `platform` team's p90 wait
from ~47 to ~21 min. Neither policy helps vision (p50 ~33 min): the limit is
capacity (10 nodes, one team per node), not queue order.

## Cut and next

Cut: interruptible nodes, pre-emption, warm pools, per-team authentication,
team quotas, multi-region placement.

Next: a warm gpu-s pool or interruptible nodes for vision's small jobs,
Postgres for concurrent production state, team auth and quotas, a real provider
integration. Interruptible nodes would need a revocation event with 2-minute
notice that requeues the node's jobs at the front with their original submit time.

## AI tools

- GitHub Copilot: code generation and tests.
- Claude: task analysis, prompt support, then a code review that found bugs
  (HTTP jobs never finished, non-deterministic replay, extra launches, launch
  decisions ignoring that a job needs one node) and the fixes and simplification.
