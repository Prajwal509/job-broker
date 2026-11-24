# Job Broker API

Run the API with simulated time only:

```sh
pip install -r requirements.txt
export ADMIN_TOKEN=dev-secret
uvicorn api:app --reload
```

| Method | Path | Purpose |
|---|---|---|
| POST | `/jobs` | Submit a job; returns 200 for an idempotent retry |
| GET | `/jobs/{id}` | Read job status and timestamps |
| POST | `/jobs/{id}/cancel` | Cancel a queued or running job |
| GET | `/queue` | List queued jobs in scheduling order |
| GET | `/nodes` | List node state, tenant, free GPUs, and idle time |
| GET | `/cost?team=&from=&to=` | Report cost for an ISO8601 UTC window |
| POST | `/admin/advance` | Advance simulated time; requires `X-Admin-Token` |
| GET | `/healthz`, `/readyz` | Liveness and database readiness |
| GET | `/metrics` | Prometheus text metrics |

Submit and inspect a job:

```sh
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"team":"vision","gpus":1,"estimated_minutes":30,"priority":"normal","idempotency_key":"vision-001"}'
curl http://localhost:8000/jobs/JOB_ID
curl http://localhost:8000/queue
curl http://localhost:8000/nodes
```

Advance the simulator and query cost:

```sh
curl -X POST http://localhost:8000/admin/advance \
  -H 'Content-Type: application/json' \
  -H 'X-Admin-Token: dev-secret' \
  -d '{"minutes":8}'
curl 'http://localhost:8000/cost?from=1970-01-01T00:00:00Z&to=1970-01-01T01:00:00Z'
```

The admin endpoint accepts exactly one of `minutes` (relative) or `to`
(absolute simulated seconds). Cost windows must include a UTC offset. When
`team` is provided, only the matching entry in `teams` is returned; aggregate
totals still cover the whole requested window. Validation errors use HTTP 422,
idempotency conflicts and terminal-job cancellation use 409, and missing jobs
use 404. Set a non-empty `ADMIN_TOKEN` to enable simulator advancement.
