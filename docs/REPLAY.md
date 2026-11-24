# Trace Replay

Replay command:

```sh
python -m broker.trace replay --trace data/01a_trace.csv --policy fair|fifo --seed 42 --out out/
```

The replay uses the real Broker, SQLite store, fake provider, and seeded
simulated clock. It submits cleaned rows and ends jobs after `actual_minutes`.
The scheduler receives only `estimated_minutes`.

## Data issues and decisions

1. Mixed UTC offsets were parsed and converted to UTC.
2. Rows were sorted by normalized submitted timestamp.
3. Fourteen exact duplicate rows were dropped. Each duplicate also received a
   deterministic idempotency key derived from its normalized seven-column row.
   Rows that were similar but had distinct `actual_minutes`, including the
   2025-03-05 06:10 vision burst, were kept as legitimate stress cases.
4. Two rows requested unsupported GPU counts and were rejected/reportedly
   excluded.
5. Eight jobs of 600-2880 minutes were retained and flagged as cost outliers.
6. Failed and cancelled rows were replayed with the supplied outcome at
   `actual_minutes`; these outcomes affect lifecycle, cost, and wait reporting.

## Results for seed 42

| Policy | Jobs | Total cost | Idle cost | Idle % | Nodes | Failures | Peak | Max queue |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| fair | 478 | $3,363.87 | $1,106.66 | 32.90% | 143 | 84 | 10 | 151 |
| fifo | 478 | $3,371.44 | $1,114.26 | 33.05% | 143 | 84 | 10 | 151 |

Per-team wait percentiles and costs are in
[fair replay JSON](../out/fair-42-10m.json) and
[FIFO replay JSON](../out/fifo-42-10m.json).

## Fair-policy idle timeout sensitivity

| Idle timeout | Total cost | Idle cost | Idle % | Nodes | Launch failures |
|---:|---:|---:|---:|---:|---:|
| 5 min | $3,419.78 | $1,172.43 | 34.28% | 155 | 85 |
| 10 min | $3,363.87 | $1,106.66 | 32.90% | 143 | 84 |
| 20 min | $3,580.34 | $1,331.18 | 37.18% | 125 | 77 |

Fair differed little from FIFO on this seed; both reached the 10-node cap and
had the same queue depth.

I would ship fair: it gives lower measured cost and avoids giving persistent
heavy demand permanent queue priority.

Fair's 10-minute sensitivity had the lowest cost and idle percentage among the
three tested values.

The sensitivity is indicative, not an optimization result.

Caveat: these results use one synthetic trace and one seed.
