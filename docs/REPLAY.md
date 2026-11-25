# Trace Replay

```sh
python -m broker.trace replay --trace data/01a_trace.csv --policy fair|fifo --seed 42 --out out
python -m broker.trace compare --trace data/01a_trace.csv --seeds 20 --out out
```

The replay uses the real Broker, SQLite store, fake provider and simulated
clock. Jobs end after `actual_minutes`; the scheduler sees only
`estimated_minutes`. After the last submission it keeps running until every job
has ended and every node is reaped, so all cost is counted. The same seed always
gives the same result (tested). `--seed` changes only the provider's random
outages and boot times.

## Data issues and decisions

1. Mixed UTC offsets (`+01:00`, `-07:00`) were converted to UTC.
2. Rows were not in time order; they are sorted by normalised time.
3. 14 exact duplicate rows were dropped. Each row's idempotency key is a hash of
   its seven normalised columns. Similar rows with different `actual_minutes`
   (e.g. the 2025-03-05 06:10 vision burst) were kept as real load.
4. 2 rows asked for 3 and 16 GPUs, which no job can request; they were rejected.
5. 8 jobs of 600-2,880 minutes were kept and flagged as outliers.
6. Failed and cancelled rows are replayed to their `actual_minutes` and count
   for cost and wait time.

478 jobs are replayed.

## fair vs fifo over 20 seeds (`out/compare-20-seeds.json`)

| Metric | fair mean | fifo mean | fair lower in |
|---|---:|---:|---:|
| Total cost / week | $3,442 | $3,416 | 9 / 20 seeds |
| Idle % | 34.6% | 34.0% | 9 / 20 |
| platform p90 wait | 20.6 min | 47.4 min | 14 / 20 |
| vision p50 wait | 33.1 min | 33.5 min | 7 / 20 |
| vision p90 wait | 104.0 min | 94.9 min | 1 / 20 |
| speech p90 wait | 23.1 min | 21.2 min | 2 / 20 |
| nlp p90 wait | 12.5 min | 12.2 min | 8 / 20 |
| forecast p90 wait | 8.4 min | 8.5 min | 8 / 20 |

## Seed 42 detail (`out/*-42-*.json`)

| Policy | Total cost | Idle % | Unattributed | Nodes launched | Launch failures | Peak nodes | Max queue |
|---|---:|---:|---:|---:|---:|---:|---:|
| fair | $3,411.23 | 33.96% | $33.68 | 147 | 79 | 10 | 150 |
| fifo | $3,233.13 | 30.13% | $29.04 | 153 | 71 | 10 | 150 |

The $178 gap on this seed looks large, but over 20 seeds the two policies swap
places (fair is cheaper in 9 of 20). One seed is not enough to compare cost.

Idle timeout sensitivity (seed 42, one run each, indicative only):

| Idle timeout | fair cost | fifo cost |
|---:|---:|---:|
| 5 min | $3,375.80 | $3,385.29 |
| 10 min | $3,411.23 | $3,233.13 |
| 20 min | $3,536.14 | $3,493.84 |

20 minutes is consistently the most expensive: idle nodes wait longer before
they are reaped.

## Analysis

- **Cost does not depend on the policy.** Both are ~$3,400/week with ~34% idle.
  For fair, seed 42, the $1,158 of idle cost splits into $929 of unused GPUs on
  nodes that are running jobs, $102 of boot time and failed launches, and $127
  of empty nodes waiting for the reaper. So most of the waste is packing: one
  team per node, and nodes sized for a queue that drains before they fill.
  Tuning the reaper can only win back a small part of it.
- **Fair does what it is for:** the light, bursty `platform` team waits much
  less (p90 47 → 21 min) and no team is starved. The cost is a little extra wait
  for the heavy teams (vision and speech p90).
- **Vision is the real problem under both policies:** 255 of 478 jobs, p50 ~33
  min, only ~40% start within 10 minutes. The fleet is at its 10-node cap with a
  queue of ~150 jobs, so this is a capacity limit, not an ordering one. The
  next levers are capacity levers: a small warm pool of gpu-s nodes for vision,
  interruptible nodes (35% price) for low-priority work, or a higher node cap.

I would ship `fair`: same cost, better protection for small teams, and the
10-minute target is tracked as a metric rather than promised.
