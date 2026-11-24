import os
import random
import uuid
from dataclasses import dataclass

from .clock import SimClock
from .models import Job, JobStatus, Node, NodeState, Priority
from .provider import CapacityUnavailable, FakeProvider
from .scheduler import Scheduler
from .store import Store


class ValidationError(ValueError):
    pass


class ConflictError(Exception):
    pass


class IllegalTransitionError(Exception):
    pass


class KeyLookupError(LookupError):
    pass


@dataclass
class BrokerConfig:
    db_path: str = os.environ.get("DB_PATH", "job-broker.db")
    idle_timeout_minutes: int = int(os.environ.get("IDLE_TIMEOUT_MINUTES", "10"))
    max_nodes: int = int(os.environ.get("MAX_NODES", "10"))
    seed: int = int(os.environ.get("SEED", "42"))
    fairness_policy: str = os.environ.get("FAIRNESS_POLICY", "fair")


class Broker:
    NODE_TYPES = ("gpu-s", "gpu-m", "gpu-l")
    NODE_GPUS = {"gpu-s": 1, "gpu-m": 4, "gpu-l": 8}

    def __init__(
        self,
        store: Store | None = None,
        clock: SimClock | None = None,
        provider: FakeProvider | None = None,
        config: BrokerConfig | None = None,
    ) -> None:
        self.config = config or BrokerConfig()
        self.store = store or Store(self.config.db_path)
        self.clock = clock or SimClock(int(self.store.get_meta("clock", 0)))
        self.provider = provider or FakeProvider(self.config.seed, self.clock)
        self.provider.set_clock(self.clock)
        self.scheduler = Scheduler(self.config.fairness_policy)
        self.backoff: dict[str, dict[str, int]] = {}
        self.retry_count: dict[str, int] = {t: 0 for t in self.NODE_TYPES}
        self.next_reaper = self.clock.now
        self.jobs: dict[str, Job] = {j.id: j for j in self.store.jobs()}
        self.nodes: dict[str, Node] = {n.id: n for n in self.store.nodes()}

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
        if not isinstance(team, str) or not team.strip():
            raise ValidationError("team must be a non-empty string")
        team = team.strip()
        if gpus not in {1, 2, 4, 8}:
            raise ValidationError("gpus must be one of 1, 2, 4, 8")
        if estimated_minutes <= 0:
            raise ValidationError("estimated_minutes must be positive")
        if priority not in {"low", "normal", "high"}:
            raise ValidationError("invalid priority")
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise ValidationError("idempotency_key is required")

        existing = self.store.get_job_by_key(team, idempotency_key)
        payload = (team, gpus, estimated_minutes, priority)
        if existing is not None:
            existing_payload = (
                existing.team,
                existing.gpus,
                existing.estimated_minutes,
                existing.priority.value,
            )
            if payload != existing_payload:
                raise ConflictError("idempotency key has a different payload")
            return existing

        job = Job(
            id=str(uuid.uuid4()),
            team=team,
            gpus=gpus,
            estimated_minutes=estimated_minutes,
            priority=Priority(priority),
            idempotency_key=idempotency_key,
            submitted_at=self.clock.now,
            actual_minutes=actual_minutes,
            outcome=outcome,
        )
        if actual_minutes is not None:
            job.ends_at = self.clock.now + actual_minutes * 60
        self.jobs[job.id] = job
        self.store.save_job(job)
        self.store.set_meta("clock", self.clock.now)
        self.schedule()
        return job

    def get_job(self, job_id: str) -> Job:
        job = self.jobs.get(job_id)
        if job is None:
            raise KeyLookupError(job_id)
        return job

    def cancel(self, job_id: str, now: int | None = None) -> Job:
        job = self.jobs.get(job_id)
        if job is None:
            raise KeyLookupError(job_id)
        if job.status in {
            JobStatus.succeeded,
            JobStatus.failed,
            JobStatus.cancelled,
        }:
            raise ConflictError("terminal job cannot be cancelled")
        now = self.clock.now if now is None else now
        job.status = JobStatus.cancelled
        job.finished_at = now
        job.ends_at = now
        job.outcome = "cancelled"
        job.node_id = None
        self.store.save_job(job)
        self.schedule()
        return job

    def finish_job(
        self, job_id: str, outcome: str = "succeeded", now: int | None = None
    ) -> Job:
        job = self.jobs.get(job_id)
        if job is None:
            raise KeyLookupError(job_id)
        if job.status != JobStatus.running:
            raise IllegalTransitionError("only running jobs can finish")
        if outcome not in {"succeeded", "failed", "cancelled"}:
            raise ValidationError("invalid job outcome")
        now = self.clock.now if now is None else now
        job.status = JobStatus(outcome)
        job.finished_at = now
        job.ends_at = now
        job.outcome = outcome
        job.node_id = None
        self.store.save_job(job)
        self.schedule()
        return job

    def advance_to(self, timestamp: int) -> None:
        if timestamp < self.clock.now:
            raise ValueError("cannot move simulated clock backwards")
        target = timestamp
        while True:
            event_times: list[int] = []

            for node in self.nodes.values():
                if node.state == NodeState.launching and node.ready_at is not None:
                    event_times.append(node.ready_at)
                if node.state == NodeState.launching and node.failure_at is not None:
                    event_times.append(node.failure_at)

            for job in self.jobs.values():
                if job.status == JobStatus.running and job.ends_at is not None:
                    event_times.append(job.ends_at)

            for type_name, retry in self.backoff.items():
                if retry["next"] > 0:
                    event_times.append(retry["next"])

            if self.next_reaper <= target:
                event_times.append(self.next_reaper)

            due = [t for t in event_times if t > self.clock.now and t <= target]
            if not due:
                break
            self.clock.advance_to(min(due))
            self._process_due_events()
            self.store.set_meta("clock", self.clock.now)

        self.clock.advance_to(target)
        self.next_reaper = min(self.next_reaper or target, target)
        self._process_due_events()
        self.schedule()
        self.store.set_meta("clock", self.clock.now)

    def _process_due_events(self) -> None:
        now = self.clock.now

        for node in list(self.nodes.values()):
            if (
                node.state == NodeState.launching
                and node.failure_at is not None
                and node.failure_at <= now
            ):
                self._terminate_node(node, now)
            elif (
                node.state == NodeState.launching
                and node.ready_at is not None
                and node.ready_at <= now
            ):
                node.state = NodeState.ready
                node.failure_at = None
                self.store.save_node(node)

        for job in list(self.jobs.values()):
            if (
                job.status == JobStatus.running
                and job.ends_at is not None
                and job.ends_at <= now
            ):
                if job.outcome == "cancelled":
                    job.status = JobStatus.cancelled
                elif job.outcome == "failed":
                    job.status = JobStatus.failed
                else:
                    job.status = JobStatus.succeeded
                job.finished_at = now
                job.node_id = None
                self.store.save_job(job)

        if now >= self.next_reaper:
            self._run_reaper(now)
            self.next_reaper = now + 60

        retry_types = [
            t
            for t, retry in self.backoff.items()
            if retry["next"] > 0 and retry["next"] <= now
        ]
        for type_name in retry_types:
            retry = self.backoff[type_name]
            retry["next"] = 0
            self._launch_one(type_name, now)

        self.schedule()

    def schedule(self) -> None:
        usage = self._rolling_usage()
        ordered = self.scheduler.queue_order(list(self.jobs.values()), usage, self.clock.now)

        for job in ordered:
            candidates = [
                node
                for node in self.nodes.values()
                if node.state == NodeState.ready
                and node.tenant in (None, job.team)
                and self._free_gpus(node) >= job.gpus
            ]
            if candidates:
                node = min(
                    candidates,
                    key=lambda n: (
                        n.tenant != job.team,
                        self._free_gpus(n),
                    ),
                )
                node.tenant = job.team
                job.status = JobStatus.running
                job.started_at = self.clock.now
                job.node_id = node.id
                if job.actual_minutes is not None:
                    job.ends_at = self.clock.now + job.actual_minutes * 60
                self.store.save_job(job)
                self.store.save_node(node)

        for job in ordered:
            if job.status != JobStatus.queued:
                continue
            if self._launch_duplicate_would_fit(job):
                continue
            if sum(
                1
                for node in self.nodes.values()
                if node.state in (NodeState.launching, NodeState.ready)
            ) >= self.config.max_nodes:
                continue

            node_type = self._smallest_type_for(job)
            team_demand = sum(
                j.gpus
                for j in ordered
                if j.team == job.team and j.status == JobStatus.queued
            )
            if team_demand >= 8:
                node_type = "gpu-l"
            elif team_demand >= 4:
                node_type = "gpu-m"
            if job.gpus > self.NODE_GPUS[node_type]:
                continue

            retry = self.backoff.get(node_type)
            if retry and retry["next"] > self.clock.now:
                for larger in self.NODE_TYPES[self.NODE_TYPES.index(node_type) + 1 :]:
                    larger_retry = self.backoff.get(larger)
                    if not larger_retry or larger_retry["next"] <= self.clock.now:
                        node_type = larger
                        break
                else:
                    continue
            self._launch_one(node_type, self.clock.now)

    def _launch_duplicate_would_fit(self, job: Job) -> bool:
        for node in self.nodes.values():
            if (
                node.state == NodeState.launching
                and node.tenant == job.team
                and self.NODE_GPUS[node.type] >= job.gpus
            ):
                return True
        return False

    def _smallest_type_for(self, job: Job) -> str:
        if job.gpus <= 1:
            return "gpu-s"
        if job.gpus <= 4:
            return "gpu-m"
        return "gpu-l"

    def _launch_one(self, node_type: str, now: int) -> Node | None:
        try:
            node_id = self.provider.launch_node(node_type, now)
            details = self.provider.launch_details(node_id, node_type, now)
        except CapacityUnavailable:
            failures = self.retry_count[node_type] + 1
            self.retry_count[node_type] = failures
            base = min(60 * (2 ** min(failures - 1, 6)), 30 * 60)
            jitter = random.Random(self.config.seed + failures).randint(0, 60)
            next_retry = now + base + jitter
            self.backoff[node_type] = {
                "next": next_retry,
                "failures": failures,
            }
            self.provider.launch_failures += 1
            return None

        node = Node(
            id=node_id,
            type=node_type,
            state=NodeState.launching,
            launched_at=now,
            ready_at=details.ready_at,
            tenant=self._tenant_for_new_node(),
            failure_at=details.failure_at,
        )
        queued = [
            j
            for j in self.jobs.values()
            if j.status == JobStatus.queued
        ]
        if queued:
            ordered = self.scheduler.queue_order(queued, self._rolling_usage(), now)
            node.tenant = ordered[0].team
        self.retry_count[node_type] = 0
        self.backoff.pop(node_type, None)
        self.nodes[node.id] = node
        self.store.save_node(node)
        return node

    def _tenant_for_new_node(self) -> str | None:
        return None

    def _free_gpus(self, node: Node) -> int:
        used = sum(
            j.gpus
            for j in self.jobs.values()
            if j.status == JobStatus.running and j.node_id == node.id
        )
        return self.NODE_GPUS[node.type] - used

    def _terminate_node(self, node: Node, now: int) -> None:
        node.state = NodeState.terminated
        node.terminated_at = now
        self.provider.terminate_node(node.id)
        self.store.save_node(node)

    def _run_reaper(self, now: int) -> None:
        timeout = self.config.idle_timeout_minutes * 60
        for node in list(self.nodes.values()):
            if node.state != NodeState.ready or self._free_gpus(node) != self.NODE_GPUS[node.type]:
                continue
            if node.ready_at is None or now - node.ready_at < timeout:
                continue
            if now - node.launched_at < 600:
                continue
            tenant_queued = any(
                j.status == JobStatus.queued
                and j.team == node.tenant
                and j.gpus <= self._free_gpus(node)
                for j in self.jobs.values()
            )
            if not tenant_queued:
                self._terminate_node(node, now)

    def _rolling_usage(self) -> dict[str, float]:
        usage: dict[str, float] = {}
        cutoff = self.clock.now - 24 * 60 * 60
        for job in self.jobs.values():
            if job.started_at is not None and job.started_at >= cutoff:
                usage[job.team] = usage.get(job.team, 0.0) + job.gpus
        return usage
