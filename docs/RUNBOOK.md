# Runbook

## What to watch

- `oldest_queued_job_age_seconds`: ticket at 30 minutes, page if capacity is
  free and the scheduler remains stalled.
- `spend_rate_usd_per_hour`: page when it stays above an agreed spend limit
  (an alert threshold set in Prometheus/Alertmanager, e.g. $60/h for 30 min;
  the app itself has no cost cap).
- `queue_depth{priority="*"}` and node states: page only for sustained overload
  with free capacity.
- `idle_nodes` and idle cost: ticket above 25%.
- Nodes at `MAX_NODES`: ticket after 1 hour.
- Capacity outage: ticket after 30 minutes.

## First response

**Provider outage**
1. Check `/metrics` for `launch_failures_total`.
2. Confirm backoff state and wait for the next retry; do not restart unless
   state is corrupted.
3. Ticket capacity outage after 30 minutes.

**Runaway cost**
1. Set `MAX_NODES=0` (no new launches; the reaper still removes idle nodes), then restart.
2. Check `/nodes` and `/cost`.
3. Restore the prior safe configuration after confirming the idle reaper works.

**Database locked or corrupt**
1. Stop the broker container (it runs as a single instance) and any backup job.
2. Check filesystem permissions and free space.
3. Restore `job-broker.db` and WAL/SHM files from a consistent backup; never
   reuse orphaned WAL files across a different database.

**Restart**
1. Stop writes.
2. Keep `/data` mounted and preserve SQLite sidecars.
3. Start the same image tag, verify `/healthz` (process up), then `/readyz` (database reachable).
4. Check simulated clock and node state remain restart-safe.

Backoff and jitter contain provider bursts, the reaper removes idle nodes,
and broker state/RNG are persisted so normal restarts do not require paging.
