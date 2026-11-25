import argparse
import csv
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .billing import cost_report
from .broker import Broker, BrokerConfig
from .models import JobStatus
from .store import Store

FIELDS = [
    "submitted_at", "team", "gpus", "estimated_minutes",
    "actual_minutes", "priority", "outcome",
]
OUTLIER_MINUTES = 600


@dataclass
class CleaningReport:
    rows: list[dict] = field(default_factory=list)
    duplicate_rows: int = 0
    rejected_gpus: list[dict] = field(default_factory=list)
    outlier_rows: list[dict] = field(default_factory=list)


def _utc_timestamp(value: str) -> tuple[str, int]:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a UTC offset")
    utc = parsed.astimezone(timezone.utc)
    return utc.isoformat().replace("+00:00", "Z"), int(utc.timestamp())


def clean_trace(path: str | Path) -> CleaningReport:
    """Normalise offsets to UTC, drop exact duplicates and unsupported GPU counts,
    flag very long jobs, and sort by submission time."""
    report = CleaningReport()
    seen: set[str] = set()
    with Path(path).open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != FIELDS:
            raise ValueError(f"trace columns must be {FIELDS}")
        for raw in reader:
            row = dict(raw)
            row["submitted_at"], row["submitted_epoch"] = _utc_timestamp(raw["submitted_at"])
            for name in ("gpus", "estimated_minutes", "actual_minutes"):
                row[name] = int(row[name])
            identity = "\x1f".join(str(row[f]) for f in FIELDS)
            if identity in seen:
                report.duplicate_rows += 1
                continue
            seen.add(identity)
            row["key"] = hashlib.sha256(identity.encode()).hexdigest()
            if row["gpus"] not in {1, 2, 4, 8}:
                report.rejected_gpus.append(row)
                continue
            if row["actual_minutes"] >= OUTLIER_MINUTES:
                report.outlier_rows.append(row)
            report.rows.append(row)
    report.rows.sort(key=lambda row: (row["submitted_epoch"], row["team"]))
    return report


def _percentile(values: list[float], percentile: float) -> float:
    """Linear interpolation between closest ranks."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def _peak_alive_nodes(broker: Broker) -> int:
    changes = []
    for node in broker.nodes.values():
        changes.append((node.launched_at, 1))
        if node.terminated_at is not None:
            changes.append((node.terminated_at, -1))
    alive = peak = 0
    for _, delta in sorted(changes):  # at equal times, terminations (-1) come first
        alive += delta
        peak = max(peak, alive)
    return peak


def replay_trace(
    cleaning: CleaningReport,
    policy: str,
    seed: int,
    db_path: str | Path,
    idle_timeout_minutes: int = 10,
) -> dict:
    db = Path(db_path)
    for path in (db, Path(f"{db}-wal"), Path(f"{db}-shm")):
        path.unlink(missing_ok=True)

    config = BrokerConfig(
        db_path=str(db),
        seed=seed,
        fairness_policy=policy,
        idle_timeout_minutes=idle_timeout_minutes,
    )
    broker = Broker(Store(config.db_path), config=config)
    max_queue_depth = 0
    for row in cleaning.rows:
        broker.advance_to(row["submitted_epoch"])
        broker.submit(
            team=row["team"],
            gpus=row["gpus"],
            estimated_minutes=row["estimated_minutes"],
            priority=row["priority"],
            idempotency_key=row["key"],
            actual_minutes=row["actual_minutes"],
            outcome=row["outcome"],
        )
        # The queue only grows on submit, so sampling here finds the peak.
        max_queue_depth = max(max_queue_depth, len(broker.queue()))

    # Run until every job has ended and every node has been reaped.
    while broker.alive_node_count() or any(
        j.status in (JobStatus.queued, JobStatus.running) for j in broker.jobs.values()
    ):
        broker.advance_to(broker.clock.now + 3600)

    start = min((row["submitted_epoch"] for row in cleaning.rows), default=0)
    report = cost_report(broker.store, start, broker.clock.now + 1, broker.clock.now)

    waits: dict[str, list[float]] = {}
    for job in broker.jobs.values():
        waits.setdefault(job.team, []).append((job.started_at - job.submitted_at) / 60
                                              if job.started_at is not None else 0.0)
    wait_metrics = {
        team: {
            "jobs": len(values),
            "p50": round(_percentile(values, 0.50), 2),
            "p90": round(_percentile(values, 0.90), 2),
            "p99": round(_percentile(values, 0.99), 2),
            "started_within_10m_percent": round(100 * sum(v <= 10 for v in values) / len(values), 2),
        }
        for team, values in sorted(waits.items())
    }
    result = {
        "policy": policy,
        "seed": seed,
        "idle_timeout_minutes": idle_timeout_minutes,
        "jobs": len(broker.jobs),
        "wait_minutes": wait_metrics,
        "cost": {
            "total_cost": round(report.total_cost, 2),
            "idle_cost": round(report.idle_cost, 2),
            "idle_percent": round(report.idle_percent, 2),
            "unattributed_cost": round(report.unattributed_cost, 2),
            "teams": {
                team: {
                    "gpu_hours": round(cost.gpu_hours, 2),
                    "direct_cost": round(cost.direct_cost, 2),
                    "idle_cost": round(cost.idle_cost, 2),
                    "total_cost": round(cost.total_cost, 2),
                }
                for team, cost in sorted(report.teams.items())
            },
        },
        "nodes_launched": len(broker.nodes),
        "launch_failures": broker.launch_failures,
        "peak_nodes": _peak_alive_nodes(broker),
        "max_queue_depth": max_queue_depth,
        "cleaning": {
            "duplicate_rows": cleaning.duplicate_rows,
            "rejected_gpus": len(cleaning.rejected_gpus),
            "outlier_rows": len(cleaning.outlier_rows),
        },
    }
    broker.store.close()
    return result


def compare_policies(cleaning: CleaningReport, seeds: int, db_path: str | Path) -> dict:
    """Replay fair and fifo with the same provider seeds. One seed is one random
    sequence of outages and boot times, so a single run can mislead."""
    summary: dict = {"seeds": seeds}
    runs = {
        policy: [replay_trace(cleaning, policy, seed, db_path) for seed in range(1, seeds + 1)]
        for policy in ("fair", "fifo")
    }

    def metric(run: dict, name: str) -> float:
        if name in ("total_cost", "idle_percent"):
            return run["cost"][name]
        team, stat = name.split(":")
        return run["wait_minutes"][team][stat]

    teams = sorted(runs["fair"][0]["wait_minutes"])
    names = ["total_cost", "idle_percent"] + [f"{t}:{s}" for t in teams for s in ("p50", "p90")]
    for name in names:
        fair = [metric(r, name) for r in runs["fair"]]
        fifo = [metric(r, name) for r in runs["fifo"]]
        summary[name] = {
            "fair_mean": round(sum(fair) / seeds, 2),
            "fifo_mean": round(sum(fifo) / seeds, 2),
            "fair_lower_in_seeds": sum(a < b for a, b in zip(fair, fifo)),
        }
    return summary


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay the Job Broker trace")
    sub = parser.add_subparsers(dest="command", required=True)
    replay = sub.add_parser("replay")
    replay.add_argument("--trace", required=True)
    replay.add_argument("--policy", choices=("fair", "fifo"), required=True)
    replay.add_argument("--seed", type=int, default=42)
    replay.add_argument("--out", required=True)
    replay.add_argument("--idle_timeout_minutes", type=int, default=10)
    compare = sub.add_parser("compare", help="fair vs fifo over many provider seeds")
    compare.add_argument("--trace", required=True)
    compare.add_argument("--seeds", type=int, default=20)
    compare.add_argument("--out", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    cleaning = clean_trace(args.trace)
    if args.command == "compare":
        name = f"compare-{args.seeds}-seeds"
        result = compare_policies(cleaning, args.seeds, out / f"{name}.db")
    else:
        name = f"{args.policy}-{args.seed}-{args.idle_timeout_minutes}m"
        result = replay_trace(
            cleaning, args.policy, args.seed, out / f"{name}.db", args.idle_timeout_minutes
        )
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    (out / f"{name}.json").write_text(text)
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
