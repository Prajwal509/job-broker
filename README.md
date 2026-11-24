# Northwind Job Broker

A deterministic, simulated GPU job broker with SQLite persistence, cost
attribution, trace replay, and a FastAPI control plane.

## Quick start

```sh
python3.11 -m venv .venv && source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest -q
python -m broker.trace replay --trace data/01a_trace.csv --policy fair --seed 42 --out out
export ADMIN_TOKEN=dev-secret && python -m uvicorn api:app --host 0.0.0.0 --port 8000
```

Docker: `cp .env.example .env && docker compose up --build`.

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/jobs` | Submit or idempotently retry a job |
| GET | `/jobs/{id}` | Read job state |
| POST | `/jobs/{id}/cancel` | Cancel a queued/running job |
| GET | `/queue` | Scheduling-ordered queue |
| GET | `/nodes` | Nodes and free capacity |
| GET | `/cost?team=&from=&to=` | ISO8601 UTC cost report |
| POST | `/admin/advance` | Advance simulated time with admin token |
| GET | `/healthz`, `/readyz` | Liveness/readiness |
| GET | `/metrics` | Prometheus text metrics |

```sh
curl -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"team":"vision","gpus":1,"estimated_minutes":30,"priority":"normal","idempotency_key":"v1"}'
curl localhost:8000/nodes
curl -X POST localhost:8000/admin/advance -H 'X-Admin-Token: dev-secret' \
  -H 'Content-Type: application/json' -d '{"minutes":8}'
```

## Key decisions and assumptions

- A node has one tenant while running; it may be re-tenanted only after its last
  running job ends.
- Fair priority uses rolling GPU-hours, team round-robin, FIFO, and 60-minute
  aging; FIFO has neither fairness nor aging.
- High priority changes queue order only and never preempts running jobs.
- Idle cost is attributed to the node's current/last tenant; untethered failed
  launches remain platform cost.
- The scheduler uses the smallest fitting node, steps up for large aggregate
  team demand, and counts launching GPU capacity.
- An idle ready node is reaped after the timeout but not before it is 10 minutes
  old or if its tenant queue can fit.
- Capacity outages use seeded exponential backoff; launch failures terminate
  after 15 simulated minutes and leave jobs queued.
- Starting within 10 minutes is a soft 10-minute SLO metric, not a cost-blind
  warm-pool guarantee.

## Stakeholder tradeoffs

- ML users get aging and fair team rotation before permanent heavy-team
  starvation.
- Finance gets minimum-billing, idle-cost attribution, and reconciled reports.
- Security gets a hard one-tenant-per-node invariant.
- Platform operations gets self-healing retry/reaper behavior and narrow alert
  boundaries.

## Trace replay

See [docs/REPLAY.md](docs/REPLAY.md). For seed 42, fair cost was $3,363.87
versus FIFO $3,371.44; both had 32-33% idle cost and reached 10 nodes.

## Cut and next

Cut: interruptible nodes, preemption, cross-size bin-packing, warm pools,
per-team authentication, and multi-region placement.

Next: Postgres for concurrent production state, auth and team quotas,
distributed workers, Kubernetes/ECS deployment, and real provider
integration. Interruptible nodes would need 2-minute eviction handling and
requeueing; the sketch is intentionally excluded.

## AI tools

- GitHub Copilot: code generation and tests.
- Claude: task analysis and prompt support.
- Start time: TBD
- Finish time: TBD
- Hours worked, including AI time: TBD
