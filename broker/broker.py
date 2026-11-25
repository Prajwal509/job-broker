import json
import logging
import os
import random
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator

from .clock import SimClock
from .models import (
    MINIMUM_BILLING_SECONDS,
    NODE_GPUS,
    NODE_TYPES,
    TERMINAL_STATUSES,
    Job,
    JobStatus,
    Node,
    NodeState,
    Priority,
)
from .provider import CapacityUnavailable, FakeProvider
from .scheduler import Scheduler
from .store import Store

event_logger = logging.getLogger("job_broker.events")

USAGE_WINDOW_SECONDS = 24 * 60 * 60  # the fair policy looks at the last 24h of GPU use


class ValidationError(ValueError):
    pass


class ConflictError(Exception):
    pass


class IllegalTransitionError(Exception):
    pass


class KeyLookupError(LookupError):
    pass


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class BrokerConfig:
    db_path: str = field(default_factory=lambda: _env("DB_PATH", "job-broker.db"))
    idle_timeout_minutes: int = field(
        default_factory=lambda: int(_env("IDLE_TIMEOUT_MINUTES", "10"))
    )
    max_nodes: int = field(default_factory=lambda: int(_env("MAX_NODES", "10")))
    seed: int = field(default_factory=lambda: int(_env("SEED", "42")))
    fairness_policy: str = field(default_factory=lambda: _env("FAIRNESS_POLICY", "fair"))
    admin_token: str = field(default_factory=lambda: _env("ADMIN_TOKEN", ""))


class Broker:
    """All state changes go through this class.

    Time only moves through advance_to(), which jumps from one simulated event
    to the next (node ready, launch failed, job ends, idle deadline, retry due).
    """

    def __init__(
        self,
        store: Store | None = None,
        clock: SimClock | None = None,
        provider: FakeProvider | None = None,
        config: BrokerConfig | None = None,
    ) -> None:
        self.config = config or BrokerConfig()
        self.store = store or Store(self.config.db_path)
        self.clock = clock or SimClock(self.store.get_meta("clock", 0))
        self.provider = provider or FakeProvider(self.config.seed)
        self.scheduler = Scheduler(self.config.fairness_policy)
        self.jobs: dict[str, Job] = {j.id: j for j in self.store.jobs()}
        self.nodes: dict[str, Node] = {n.id: n for n in self.store.nodes()}
        runtime = self.store.get_meta("runtime", {})
        # node type -> {"next": earliest retry time, "failures": consecutive failures}
        self.backoff: dict[str, dict[str, int]] = runtime.get("backoff", {})
        self.launch_failures: int = runtime.get("launch_failures", 0)
        if "provider" in runtime:
            self.provider.restore_state(runtime["provider"])

    # ---------- persistence and logging ----------

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """One SQLite transaction. Runtime state is saved once, at the outermost level."""
        outermost = not self.store.conn.in_transaction
        with self.store.transaction():
            yield
            if outermost:
                self.store.set_meta(
                    {
                        "clock": self.clock.now,
                        "runtime": {
                            "backoff": self.backoff,
                            "launch_failures": self.launch_failures,
                            "provider": self.provider.export_state(),
                        },
                    }
                )

    def _log_event(
        self, event: str, job_id: str | None = None, node_id: str | None = None
    ) -> None:
        event_logger.info(
            json.dumps(
                {"time": self.clock.now, "job_id": job_id, "node_id": node_id, "event": event},
                separators=(",", ":"),
            )
        )

    # ---------- public API ----------

    def submit(
        self,
        team: str,
        gpus: int,
        estimated_minutes: int,
        priority: str,
        idempotency_key: str,
        actual_minutes: int | None = None,
        outcome: str = "succeeded",
    ) -> Job:
        """Queue a job. The same (team, idempotency_key) returns the existing job.

        actual_minutes/outcome are only used by the simulator (trace replay).
        Jobs submitted over HTTP end when finish_job() or cancel() is called.
        """
        if not isinstance(team, str) or not team.strip():
            raise ValidationError("team must be a non-empty string")
        team = team.strip()
        if isinstance(gpus, bool) or gpus not in {1, 2, 4, 8}:
            raise ValidationError("gpus must be one of 1, 2, 4, 8")
        if isinstance(estimated_minutes, bool) or estimated_minutes <= 0:
            raise ValidationError("estimated_minutes must be positive")
        if priority not in {"low", "normal", "high"}:
            raise ValidationError("invalid priority")
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ValidationError("idempotency_key is required")

        existing = self.store.get_job_by_key(team, idempotency_key)
        if existing is not None:
            same = (existing.gpus, existing.estimated_minutes, existing.priority.value)
            if same != (gpus, estimated_minutes, priority):
                raise ConflictError("idempotency key was used with a different payload")
            return self.jobs[existing.id]

        job = Job(
            # Sequential ids keep scheduling tie-breaks, and so replays, deterministic.
            id=f"job-{len(self.jobs) + 1:06d}",
            team=team,
            gpus=gpus,
            estimated_minutes=estimated_minutes,
            priority=Priority(priority),
            idempotency_key=idempotency_key,
            submitted_at=self.clock.now,
            actual_minutes=actual_minutes,
            outcome=outcome,
        )
        with self._transaction():
            self.jobs[job.id] = job
            self.store.save_job(job)
            self._log_event("job_submitted", job_id=job.id)
            self.schedule()
        return job

    def get_job(self, job_id: str) -> Job:
        job = self.jobs.get(job_id)
        if job is None:
            raise KeyLookupError(job_id)
        return job

    def cancel(self, job_id: str) -> Job:
        job = self.get_job(job_id)
        if job.status in TERMINAL_STATUSES:
            raise ConflictError("terminal job cannot be cancelled")
        with self._transaction():
            self._end_job(job, JobStatus.cancelled)
            self.schedule()
        return job

    def finish_job(self, job_id: str, outcome: str = "succeeded") -> Job:
        """A running job reports that it ended (POST /jobs/{id}/complete)."""
        job = self.get_job(job_id)
        if job.status != JobStatus.running:
            raise IllegalTransitionError("only running jobs can finish")
        if outcome not in {"succeeded", "failed"}:
            raise ValidationError("outcome must be succeeded or failed")
        with self._transaction():
            self._end_job(job, JobStatus(outcome))
            self.schedule()
        return job

    def queue(self) -> list[Job]:
        """Queued jobs in the order the scheduler will try to place them."""
        return self.scheduler.queue_order(
            list(self.jobs.values()), self._rolling_usage(), self.clock.now
        )

    def free_gpus(self, node: Node) -> int:
        used = sum(
            j.gpus for j in self.jobs.values()
            if j.status == JobStatus.running and j.node_id == node.id
        )
        return NODE_GPUS[node.type] - used

    def alive_node_count(self) -> int:
        return sum(n.state != NodeState.terminated for n in self.nodes.values())

    def advance_to(self, timestamp: int) -> None:
        """Move simulated time forward, processing every event on the way."""
        if timestamp < self.clock.now:
            raise ValueError("cannot move simulated clock backwards")
        while (next_time := self._next_event_time(timestamp)) is not None:
            self.clock.advance_to(next_time)
            self._process_due_events()
        self.clock.advance_to(timestamp)
        self._process_due_events()

    # ---------- simulated events ----------

    def _reap_deadline(self, node: Node) -> int:
        # Idle for more than N minutes, and never before the 10-minute billing minimum.
        return max(
            node.idle_since + self.config.idle_timeout_minutes * 60 + 1,
            node.launched_at + MINIMUM_BILLING_SECONDS,
        )

    def _next_event_time(self, until: int) -> int | None:
        times: list[int] = []
        for node in self.nodes.values():
            if node.state == NodeState.launching:
                times.append(node.ready_at if node.ready_at is not None else node.failure_at)
            elif node.state == NodeState.ready and node.idle_since is not None:
                times.append(self._reap_deadline(node))
        times += [
            j.ends_at for j in self.jobs.values()
            if j.status == JobStatus.running and j.ends_at is not None
        ]
        times += [retry["next"] for retry in self.backoff.values()]
        due = [t for t in times if self.clock.now < t <= until]
        return min(due, default=None)

    def _process_due_events(self) -> None:
        now = self.clock.now
        with self._transaction():
            for node in list(self.nodes.values()):
                if node.state != NodeState.launching:
                    continue
                if node.failure_at is not None and node.failure_at <= now:
                    self.launch_failures += 1
                    self._terminate_node(node, event="node_launch_failed")
                elif node.ready_at is not None and node.ready_at <= now:
                    node.state = NodeState.ready
                    node.idle_since = now
                    self.store.save_node(node)
                    self._log_event("node_ready", node_id=node.id)

            for job in list(self.jobs.values()):
                if job.status == JobStatus.running and job.ends_at is not None and job.ends_at <= now:
                    self._end_job(job, JobStatus(job.outcome))

            self._run_reaper()
            self.schedule()

    def _run_reaper(self) -> None:
        for node in list(self.nodes.values()):
            if (
                node.state == NodeState.ready
                and node.idle_since is not None
                and self.clock.now >= self._reap_deadline(node)
            ):
                self._terminate_node(node)

    # ---------- job and node state changes ----------

    def _end_job(self, job: Job, status: JobStatus) -> None:
        node = self.nodes.get(job.node_id) if job.node_id else None
        job.status = status
        job.outcome = status.value
        job.finished_at = self.clock.now
        job.node_id = None
        self.store.save_job(job)
        self._log_event(f"job_{status.value}", job_id=job.id, node_id=node.id if node else None)
        if node is None:
            return
        if self.free_gpus(node) == NODE_GPUS[node.type]:
            # Last job gone: start the idle timer and let any team use the node.
            node.idle_since = self.clock.now
            node.tenant = None
            self.store.save_node(node)
        self._sync_ledger(node)

    def _start_job(self, job: Job, node: Node) -> None:
        node.tenant = job.team
        node.last_tenant = job.team
        node.idle_since = None
        job.status = JobStatus.running
        job.started_at = self.clock.now
        job.node_id = node.id
        if job.actual_minutes is not None:
            job.ends_at = self.clock.now + job.actual_minutes * 60
        self.store.save_job(job)
        self.store.save_node(node)
        self._sync_ledger(node)
        self._log_event("job_started", job_id=job.id, node_id=node.id)

    def _terminate_node(self, node: Node, event: str = "node_terminated") -> None:
        node.state = NodeState.terminated
        node.terminated_at = self.clock.now
        self.provider.terminate_node(node.id)
        self.store.save_node(node)
        self._sync_ledger(node)
        self._log_event(event, node_id=node.id)

    def _sync_ledger(self, node: Node) -> None:
        self.store.sync_ledger_segment(
            node_id=node.id,
            timestamp=self.clock.now,
            tenant=node.tenant or node.last_tenant,
            gpus_used=NODE_GPUS[node.type] - self.free_gpus(node),
            active=node.state != NodeState.terminated,
        )

    # ---------- scheduling ----------

    def schedule(self) -> None:
        """Place queued jobs on ready nodes, then launch nodes for what is left."""
        with self._transaction():
            ordered = self.queue()
            self._place_jobs(ordered)
            # A node that booted for a team with nothing left to run is opened to all.
            if self._release_unneeded_reservations(ordered):
                self._place_jobs(ordered)
            self._launch_for_uncovered_jobs(ordered)

    def _place_jobs(self, ordered: list[Job]) -> None:
        free = {
            n.id: self.free_gpus(n)
            for n in self.nodes.values() if n.state == NodeState.ready
        }
        for job in ordered:
            if job.status != JobStatus.queued:
                continue
            candidates = [
                n for n in self.nodes.values()
                if n.id in free and n.tenant in (None, job.team) and free[n.id] >= job.gpus
            ]
            if candidates:
                # Prefer the team's own node, then the tightest fit (best fit).
                node = min(candidates, key=lambda n: (n.tenant != job.team, free[n.id]))
                free[node.id] -= job.gpus
                self._start_job(job, node)

    def _release_unneeded_reservations(self, ordered: list[Job]) -> bool:
        """A node booted for a team stays reserved for it while that team has a
        queued job that fits. Otherwise the empty node is opened to every team."""
        released = False
        for node in self.nodes.values():
            if node.state != NodeState.ready or node.tenant is None:
                continue
            if self.free_gpus(node) != NODE_GPUS[node.type]:
                continue
            if not any(
                j.team == node.tenant and j.status == JobStatus.queued
                and j.gpus <= NODE_GPUS[node.type]
                for j in ordered
            ):
                node.tenant = None
                self.store.save_node(node)
                released = True
        return released

    def _launch_for_uncovered_jobs(self, ordered: list[Job]) -> None:
        teams = list(dict.fromkeys(j.team for j in ordered if j.status == JobStatus.queued))
        while self.alive_node_count() < self.config.max_nodes:
            launched = False
            for team in teams:  # at most one launch per team per round, in queue order
                if self.alive_node_count() >= self.config.max_nodes:
                    break
                if not self._uncovered_jobs(team, ordered):
                    continue
                team_queue = [j for j in ordered if j.team == team and j.status == JobStatus.queued]
                node_type = self._type_outside_backoff(self._node_type_for(team_queue))
                if node_type and self._launch_one(node_type, team):
                    launched = True
            if not launched:
                return

    def _uncovered_jobs(self, team: str, ordered: list[Job]) -> list[Job]:
        """Queued jobs of a team that will not fit on nodes already booting for it.

        Every queued job that fits on a ready node was placed already, so only
        booting nodes count. Jobs are packed one at a time because a job must
        fit on a single node: two half-free nodes are not one free node.
        """
        remaining = [
            NODE_GPUS[n.type] for n in self.nodes.values()
            if n.state == NodeState.launching and n.tenant == team
        ]
        uncovered = []
        for job in ordered:
            if job.team != team or job.status != JobStatus.queued:
                continue
            slot = next((i for i, free in enumerate(remaining) if free >= job.gpus), None)
            if slot is None:
                uncovered.append(job)
            else:
                remaining[slot] -= job.gpus
        return uncovered

    @staticmethod
    def _node_type_for(jobs: list[Job]) -> str:
        """Smallest node type that fits the team's whole queue, else the largest.

        Sizing for the whole queue (not one job) matters because of the 10-node
        cap: ten 1-GPU nodes would cap the fleet at 10 GPUs.
        """
        demand = sum(j.gpus for j in jobs)
        return next((t for t in NODE_TYPES if NODE_GPUS[t] >= demand), NODE_TYPES[-1])

    def _type_outside_backoff(self, node_type: str) -> str | None:
        """If a node type is in launch backoff, fall back to the next bigger type."""
        for candidate in NODE_TYPES[NODE_TYPES.index(node_type):]:
            retry = self.backoff.get(candidate)
            if retry is None or retry["next"] <= self.clock.now:
                return candidate
        return None

    def _launch_one(self, node_type: str, tenant: str | None = None) -> Node | None:
        now = self.clock.now
        with self._transaction():
            try:
                launch = self.provider.launch_node(node_type, now)
            except CapacityUnavailable:
                # Exponential backoff per node type (1, 2, 4 ... max 30 min) + seeded jitter.
                failures = self.backoff.get(node_type, {}).get("failures", 0) + 1
                delay = min(60 * 2 ** min(failures - 1, 5), 30 * 60)
                delay += random.Random(self.config.seed + failures).randint(0, 60)
                self.backoff[node_type] = {"next": now + delay, "failures": failures}
                self.launch_failures += 1
                self._log_event("launch_capacity_unavailable")
                return None

            self.backoff.pop(node_type, None)
            node = Node(
                id=launch.node_id,
                type=node_type,
                state=NodeState.launching,
                launched_at=now,
                ready_at=launch.ready_at,
                failure_at=launch.failure_at,
                tenant=tenant,  # reserved for the team whose demand launched it
                last_tenant=tenant,
            )
            self.nodes[node.id] = node
            self.store.save_node(node)
            self._sync_ledger(node)
            self._log_event("node_launch_started", node_id=node.id)
            return node

    def _rolling_usage(self) -> dict[str, float]:
        """GPU-seconds each team used in the last 24 simulated hours."""
        usage: dict[str, float] = {}
        now = self.clock.now
        cutoff = now - USAGE_WINDOW_SECONDS
        for job in self.jobs.values():
            if job.started_at is None:
                continue
            end = job.finished_at if job.finished_at is not None else now
            seconds = min(end, now) - max(job.started_at, cutoff)
            if seconds > 0:
                usage[job.team] = usage.get(job.team, 0.0) + job.gpus * seconds
        return usage
