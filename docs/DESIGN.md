# Design notes

## Restart-safe state

Jobs, nodes, and occupancy ledger segments are stored in SQLite transactions with
WAL and `synchronous=NORMAL`. The `meta` table stores the simulated clock,
schema version, node ID counter, retry state, and fake-provider RNG/outage state.
On restart, alive provider nodes are reconstructed from non-terminated node rows;
event times are derived from persisted job and node timestamps.

## Cost attribution

Running-job GPU-seconds are billed directly to the job's team in proportion to
the node capacity used. Boot, unused GPU capacity, and idle time go to the
segment's current or last tenant: that team's demand caused the node launch.
Nodes that never ran a job are instead charged to the separate `platform`
bucket, so failed launches and unused capacity without a benefiting tenant
remain visible as platform waste.

Shortfalls against the ten-minute minimum are booked as idle cost at node
termination. Reports clip elapsed segments to `[from, to)` and include that
shortfall only when termination falls inside the window. Costs are rounded and
allocated in cents so team totals plus platform cost reconcile to provider
billed cost exactly.

## Scheduling fairness

The default `fair` policy orders by effective priority, then puts teams with
less GPU usage in the rolling 24-hour window first. Usage is measured in
GPU-seconds, equivalent in ordering to GPU-hours. Ties are served FIFO within
each team and teams are round-robined within a priority tier so a large team
cannot consume the whole scheduling pass. A queued job moves up one priority
tier after waiting more than 60 simulated minutes; promotion is capped at high.

`FAIRNESS_POLICY=fifo` keeps priority ordering but uses pure FIFO within each
tier, without per-team usage or aging. High priority affects ordering only:
running work is never preempted, avoiding wasted partial jobs and preserving
simple tenant isolation.
