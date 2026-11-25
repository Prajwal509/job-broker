# Design notes

## Event loop

Nothing sleeps. `Broker.advance_to(t)` finds the next event before `t` (a node
becomes ready, a launch fails, a job ends, a node's idle deadline, a launch
retry), moves the clock there, processes it, re-runs the scheduler, and saves
everything in one SQLite transaction. A week of trace replays in about a second.

## Restart-safe state

Jobs, nodes and ledger segments are SQLite tables (WAL, `synchronous=NORMAL`).
The `meta` table holds the simulated clock, schema version, launch backoff
state, launch failure count and the fake provider's RNG/outage state. It is
written once at the end of every transaction, so after a restart the broker
continues exactly as if it had not stopped (tested).

## Scheduling

1. **Order** the queue (`scheduler.py`, see Fairness).
2. **Place** each job, in order, on a ready node whose tenant is its team or
   nobody, with enough free GPUs. Prefer the team's own node, then the tightest fit.
3. **Release** nodes that booted for a team that has nothing left to run, and
   place again.
4. **Launch**: for each team, pack its queued jobs one by one into the nodes
   already booting for it. If some do not fit, launch one node for that team,
   sized for the team's whole queue (gpu-l if nothing smaller fits). Teams take
   turns, one launch each per round, until nothing is left or 10 nodes are alive.
   A booting node is reserved for its team, so another team cannot take it.

## Cost attribution

The ledger has one row per interval in which a node's tenant and GPUs in use did
not change. For each interval, cost = seconds × hourly price / 3600.
`direct` = cost × GPUs used / node GPUs and goes to the tenant. The rest is
`idle`: unused GPUs, boot time, and idle time before the reaper. It goes to the
node's current or last tenant, because that team's demand launched the node.
Nodes that never ran any job go to `unattributed_cost`. The 10-minute minimum
shortfall is booked as idle when the node is terminated. Reports clip intervals
to `[from, to)`. A test checks that the ledger total equals the provider bill
computed from nodes alone.

## Fairness

`fair` (default): order by effective priority, where a job waiting more than 60
simulated minutes moves up one tier (anti-starvation). Within a tier, teams with
fewer GPU-seconds used in the last 24 hours go first, teams take turns
(round-robin) so one big team cannot fill a whole pass, and jobs are FIFO within
a team. `fifo`: priority, then submission time, no aging or usage. Ties break
on job id, which is sequential, so runs are reproducible.

High priority affects ordering only. Running work is never pre-empted, which
avoids wasted partial jobs and keeps tenant isolation simple.
