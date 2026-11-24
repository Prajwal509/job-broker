import argparse
import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .billing import Billing
from .broker import Broker, BrokerConfig
from .models import JobStatus, NodeState
from .store import Store


@dataclass
class CleaningReport:
    rows: list[dict]
    duplicate_rows: int = 0
    rejected_gpus: list[dict] | None = None
    outlier_rows: list[dict] | None = None

    def __post_init__(self) -> None:
        self.rejected_gpus = self.rejected_gpus or []
        self.outlier_rows = self.outlier_rows or []


def _utc_timestamp(value: str) -> tuple[str, int]:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a UTC offset")
    utc = parsed.astimezone(timezone.utc)
    return utc.isoformat().replace("+00:00", "Z"), int(utc.timestamp())


def clean_trace(path: str | Path) -> CleaningReport:
    report = CleaningReport(rows=[], rejected_gpus=[], outlier_rows=[])
    seen: set[str] = set()
    fields = [
        "submitted_at",
        "team",
        "gpus",
        "estimated_minutes",
        "actual_minutes",
        "priority",
        "outcome",
    ]
    with Path(path).open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != fields:
            raise ValueError("trace columns do not match the required 7 columns")
        for raw in reader:
            normalized_at, epoch = _utc_timestamp(raw["submitted_at"])
            row = dict(raw)
            row["submitted_at"] = normalized_at
            row["submitted_epoch"] = epoch
            row["gpus"] = int(row["gpus"])
            row["estimated_minutes"] = int(row["estimated_minutes"])
            row["actual_minutes"] = int(row["actual_minutes"])

            identity = "\x1f".join(
                str(row[field]) for field in fields
            )
            if identity in seen:
                report.duplicate_rows += 1
                continue
            seen.add(identity)

            if row["gpus"] not in {1, 2, 4, 8}:
                report.rejected_gpus.append(row)
                continue
            if row["actual_minutes"] >= 600:
                report.outlier_rows.append(row)
            report.rows.append(row)
    report.rows.sort(key=lambda row: (row["submitted_epoch"], row["team"]))
    return report


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = index - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def replay_trace(
    trace: str | Path,
    policy: str,
    seed: int,
    db_path: str | Path,
    idle_timeout_minutes: int = 10,
) -> dict:
    cleaning = clean_trace(trace)
    db = Path(db_path)
    if db.exists():
        db.unlink()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(db) + suffix)
        if sidecar.exists():
            sidecar.unlink()

    config = BrokerConfig(
        db_path=str(db),
        seed=seed,
        fairness_policy=policy,
        idle_timeout_minutes=idle_timeout_minutes,
    )
    broker = Broker(Store(config.db_path), config=config)
    peak_nodes = 0
    max_queue_depth = 0
    end_time = broker.clock.now

    for row in cleaning.rows:
        timestamp = row["submitted_epoch"]
        broker.advance_to(timestamp)
        digest = hashlib.sha256(
            "\x1f".join(str(row[field]) for field in (
                "submitted_at", "team", "gpus", "estimated_minutes",
                "actual_minutes", "priority", "outcome",
            )).encode()
        ).hexdigest()
        broker.submit(
            team=row["team"],
            gpus=row["gpus"],
            estimated_minutes=row["estimated_minutes"],
            priority=row["priority"],
            idempotency_key=digest,
            actual_minutes=row["actual_minutes"],
            outcome=row["outcome"],
        )
        end_time = max(end_time, timestamp + row["actual_minutes"] * 60)
        peak_nodes = max(
            peak_nodes,
            sum(node.state != NodeState.terminated for node in broker.nodes.values()),
        )
        max_queue_depth = max(
            max_queue_depth,
            sum(job.status == JobStatus.queued for job in broker.jobs.values()),
        )

    broker.advance_to(end_time)
    report = Billing(broker.store).report(
        min((row["submitted_epoch"] for row in cleaning.rows), default=end_time),
        end_time + 1,
        end_time,
    )

    wait_by_team: dict[str, list[float]] = {}
    for job in broker.jobs.values():
        if job.started_at is not None:
            wait_by_team.setdefault(job.team, []).append(
                (job.started_at - job.submitted_at) / 60
            )
    wait_metrics = {}
    for team, waits in sorted(wait_by_team.items()):
        wait_metrics[team] = {
            "p50": round(_percentile(waits, 0.50), 2),
            "p90": round(_percentile(waits, 0.90), 2),
            "p99": round(_percentile(waits, 0.99), 2),
            "started_within_10m_percent": round(
                100 * sum(wait <= 10 for wait in waits) / len(waits), 2
            ),
        }

    team_cost = {}
    for team, cost in sorted(report.teams.items()):
        team_cost[team] = {
            "gpu_hours": round(cost.gpu_hours, 4),
            "direct_cost": round(cost.direct_cost, 2),
            "idle_cost": round(cost.idle_cost, 2),
            "total_cost": round(cost.total_cost, 2),
        }
    result = {
        "policy": policy,
        "seed": seed,
        "idle_timeout_minutes": idle_timeout_minutes,
        "wait_time": wait_metrics,
        "jobs_counted": sum(
            1 for job in broker.jobs.values()
            if job.status in {
                JobStatus.succeeded,
                JobStatus.failed,
                JobStatus.cancelled,
                JobStatus.running,
            }
        ),
        "cost": {
            "total_cost": round(report.total_cost, 2),
            "idle_cost": round(report.idle_cost, 2),
            "idle_percent": round(report.idle_percent, 2),
            "platform_cost": round(report.platform_cost, 2),
            "teams": team_cost,
        },
        "nodes_launched": len(broker.nodes),
        "launch_failures": broker.provider.launch_failures,
        "peak_nodes": peak_nodes,
        "max_queue_depth": max_queue_depth,
        "cleaning": {
            "duplicate_rows": cleaning.duplicate_rows,
            "rejected_gpus": len(cleaning.rejected_gpus),
            "outlier_rows": len(cleaning.outlier_rows),
        },
    }
    broker.store.close()
    return result


def _print_cleaning(path: str, cleaning: CleaningReport) -> None:
    print(f"Trace cleaning: {path}")
    print("1. UTC offsets normalized; mixed offsets converted to UTC")
    print(f"2. Sorted {len(cleaning.rows)} accepted rows by submitted_at")
    print(f"3. Exact duplicate rows dropped: {cleaning.duplicate_rows}")
    print("   Similar burst rows with distinct actual_minutes were kept")
    print(
        f"4. Rejected unsupported GPU rows: {len(cleaning.rejected_gpus)}"
    )
    print(f"5. Long-job outliers (>=600 min): {len(cleaning.outlier_rows)}")
    print("6. Failed/cancelled outcomes retained and replayed at actual_minutes")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay the Job Broker trace")
    sub = parser.add_subparsers(dest="command", required=True)
    replay = sub.add_parser("replay")
    replay.add_argument("--trace", required=True)
    replay.add_argument("--policy", choices=("fair", "fifo"), required=True)
    replay.add_argument("--seed", type=int, default=42)
    replay.add_argument("--out", required=True)
    replay.add_argument("--idle_timeout_minutes", type=int, default=10)
    args = parser.parse_args(list(argv) if argv is not None else None)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    cleaning = clean_trace(args.trace)
    _print_cleaning(args.trace, cleaning)
    result = replay_trace(
        args.trace,
        args.policy,
        args.seed,
        out / f"{args.policy}-{args.seed}-{args.idle_timeout_minutes}m.db",
        args.idle_timeout_minutes,
    )
    output = out / (
        f"{args.policy}-{args.seed}-{args.idle_timeout_minutes}m.json"
    )
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
