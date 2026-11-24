import json
import logging
import os
import random
import uuid
from dataclasses import dataclass

from .clock import SimClock
from .models import Job, JobStatus, Node, NodeState, Priority
from .provider import CapacityUnavailable, FakeProvider
from .scheduler import Scheduler
from .store import Store
from .billing import Billing

event_logger = logging.getLogger("job_broker.events")


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
    admin_token: str = os.environ.get("ADMIN_TOKEN", "")
    daily_budget_usd: float | None = (
        float(os.environ["DAILY_BUDGET_USD"])
        if os.environ.get("DAILY_BUDGET_USD")
        else None
    )


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
        self.jobs: dict[str, Job] = {j.id: j for j in self.store.jobs()}
        self.nodes: dict[str, Node] = {n.id: n for n in self.store.nodes()}
        runtime = self.store.get_meta("runtime", {})
        self.backoff: dict[str, dict[str, int]] = runtime.get("backoff", {})
        self.retry_count: dict[str, int] = {
            t: 0 for t in self.NODE_TYPES
        } | runtime.get("retry_count", {})
        self.next_reaper = int(runtime.get("next_reaper", self.clock.now))
        provider_state = runtime.get("provider")
        if provider_state and hasattr(self.provider, "restore_state"):
            self.provider.restore_state(provider_state)
        if hasattr(self.provider, "alive"):
            self.provider.alive = {
                node.id
                for node in self.nodes.values()
                if node.state != NodeState.terminated
            }
        stored_counter = int(self.store.get_meta("node_id_counter", 0))
        existing_counter = max(
            (
                int(node_id.removeprefix("node-"))
                for node_id in self.nodes
                if node_id.startswith("node-") and node_id.removeprefix("node-").isdigit()
            ),
            default=0,
        )
        self.node_id_counter = max(stored_counter, existing_counter)
        if hasattr(self.provider, "last_node_number"):
            self.provider.last_node_number = self.node_id_counter
        for node in self.nodes.values():
            if node.last_tenant is None:
                node.last_tenant = node.tenant
        self._persist_runtime_state()

    def _persist_runtime_state(self) -> None:
        export_state = getattr(self.provider, "export_state", None)
        provider_state = export_state() if export_state is not None else {
            "launch_failures": getattr(self.provider, "launch_failures", 0),
        }
        self.store.set_meta_values(
            {
                "clock": self.clock.now,
                "node_id_counter": self.node_id_counter,
                "runtime": {
                    "backoff": self.backoff,
                    "retry_count": self.retry_count,
                    "next_reaper": self.next_reaper,
                    "provider": provider_state,
                },
            }
        )

    def _log_event(
        self,
        event: str,
        job_id: str | None = None,
        node_id: str | None = None,
        timestamp: int | None = None,
    ) -> None:
        event_logger.info(
            json.dumps(
                {
                    "time": self.clock.now if timestamp is None else timestamp,
                    "job_id": job_id,
                    "node_id": node_id,
                    "event": event,
                },
                separators=(",", ":"),
            )
        )

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
        if isinstance(gpus, bool) or gpus not in {1, 2, 4, 8}:
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
        with self.store.transaction():
            self.jobs[job.id] = job
            self.store.save_job(job)
            self._log_event("job_submitted", job_id=job.id)
            self.schedule()
            self._persist_runtime_state()
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
        node_id = job.node_id
        with self.store.transaction():
            job.status = JobStatus.cancelled
            job.finished_at = now
            job.ends_at = now
            job.outcome = "cancelled"
            job.node_id = None
            self.store.save_job(job)
            self._mark_node_idle(node_id, now)
            self.schedule()
            self._persist_runtime_state()
            self._log_event("job_cancelled", job_id=job.id, node_id=node_id, timestamp=now)
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
        node_id = job.node_id
        with self.store.transaction():
            job.status = JobStatus(outcome)
            job.finished_at = now
            job.ends_at = now
            job.outcome = outcome
            job.node_id = None
            self.store.save_job(job)
            self._mark_node_idle(node_id, now)
            self.schedule()
            self._persist_runtime_state()
            self._log_event(
                f"job_{outcome}", job_id=job.id, node_id=node_id, timestamp=now
            )
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

            timeout = self.config.idle_timeout_minutes * 60
            for node in self.nodes.values():
                if node.state == NodeState.ready and node.idle_since is not None:
                    event_times.append(
                        max(node.idle_since + timeout + 1, node.launched_at + 600)
                    )

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
            self._persist_runtime_state()

        self.clock.advance_to(target)
        self._process_due_events()
        self._persist_runtime_state()
        self._log_event("sim_time_advanced", timestamp=target)

    def _process_due_events(self) -> None:
        with self.store.transaction():
            self._process_due_events_in_transaction()

    def _process_due_events_in_transaction(self) -> None:
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
                node.idle_since = now
                self.store.save_node(node)
                self._log_event("node_ready", node_id=node.id, timestamp=now)

        emptied_node_ids: set[str] = set()
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
                if job.node_id is not None:
                    emptied_node_ids.add(job.node_id)
                previous_node_id = job.node_id
                job.node_id = None
                self.store.save_job(job)
                self._log_event(
                    f"job_{job.status.value}",
                    job_id=job.id,
                    node_id=previous_node_id,
                    timestamp=now,
                )

        for node_id in emptied_node_ids:
            self._mark_node_idle(node_id, now)

        reaper_deadline_due = any(
            node.state == NodeState.ready
            and node.idle_since is not None
            and now >= max(
                node.idle_since + self.config.idle_timeout_minutes * 60 + 1,
                node.launched_at + 600,
            )
            for node in self.nodes.values()
        )
        if now >= self.next_reaper or reaper_deadline_due:
            self._run_reaper(now)
            self.next_reaper = now + 60

        for type_name in [
            t
            for t, retry in self.backoff.items()
            if retry["next"] > 0 and retry["next"] <= now
        ]:
            self.backoff.pop(type_name)

        self.schedule()
        self._persist_runtime_state()

    def schedule(self) -> None:
        with self.store.transaction():
            self._schedule_in_transaction()

    def _schedule_in_transaction(self) -> None:
        for node in self.nodes.values():
            if (
                node.state == NodeState.ready
                and self._free_gpus(node) == self.NODE_GPUS[node.type]
                and node.tenant is not None
            ):
                node.tenant = None
                self.store.save_node(node)

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
                node.last_tenant = job.team
                job.status = JobStatus.running
                job.started_at = self.clock.now
                job.node_id = node.id
                node.idle_since = None
                if job.actual_minutes is not None:
                    job.ends_at = self.clock.now + job.actual_minutes * 60
                self.store.save_job(job)
                self.store.save_node(node)
                self._log_event("job_started", job_id=job.id, node_id=node.id)

        teams: list[str] = []
        for job in ordered:
            if job.status == JobStatus.queued and job.team not in teams:
                teams.append(job.team)

        while (
            self._alive_node_count() < self.config.max_nodes
            and self._daily_budget_allows_launch()
        ):
            launched = False
            for team in teams:
                team_jobs = [
                    job
                    for job in ordered
                    if job.team == team and job.status == JobStatus.queued
                ]
                demand = sum(job.gpus for job in team_jobs)
                covered = sum(
                    self._free_gpus(node)
                    for node in self.nodes.values()
                    if node.state == NodeState.ready and node.tenant in (None, team)
                )
                covered += sum(
                    self.NODE_GPUS[node.type]
                    for node in self.nodes.values()
                    if node.state == NodeState.launching and node.tenant == team
                )
                if demand <= covered:
                    continue

                largest_job = max(team_jobs, key=lambda job: job.gpus)
                node_type = self._smallest_type_for(largest_job)
                if demand >= 8:
                    node_type = "gpu-l"
                elif demand >= 4 and node_type == "gpu-s":
                    node_type = "gpu-m"
                elif demand > self.NODE_GPUS[node_type]:
                    index = self.NODE_TYPES.index(node_type)
                    node_type = self.NODE_TYPES[min(index + 1, len(self.NODE_TYPES) - 1)]
                node_type = self._type_outside_backoff(node_type)
                if node_type is None:
                    continue
                if self._launch_one(node_type, self.clock.now, team) is not None:
                    launched = True
                    if self._alive_node_count() >= self.config.max_nodes:
                        break
            if not launched:
                break
        self._sync_all_segments(self.clock.now)
        self._persist_runtime_state()

    def _alive_node_count(self) -> int:
        return sum(
            node.state in (NodeState.launching, NodeState.ready)
            for node in self.nodes.values()
        )

    def _daily_budget_allows_launch(self) -> bool:
        if self.config.daily_budget_usd is None:
            return True
        day_start = self.clock.now - (self.clock.now % 86400)
        cost = Billing(self.store).report(day_start, self.clock.now + 1, self.clock.now).total_cost
        allowed = cost < self.config.daily_budget_usd
        if not allowed:
            event_logger.warning(
                json.dumps({
                    "time": self.clock.now,
                    "job_id": None,
                    "node_id": None,
                    "event": "daily_budget_exceeded",
                })
            )
        return allowed

    def _type_outside_backoff(self, node_type: str) -> str | None:
        first = self.NODE_TYPES.index(node_type)
        for candidate in self.NODE_TYPES[first:]:
            retry = self.backoff.get(candidate)
            if retry is None or retry["next"] <= self.clock.now:
                return candidate
        return None

    def _smallest_type_for(self, job: Job) -> str:
        if job.gpus <= 1:
            return "gpu-s"
        if job.gpus <= 4:
            return "gpu-m"
        return "gpu-l"

    def _launch_one(
        self, node_type: str, now: int, tenant: str | None = None
    ) -> Node | None:
        with self.store.transaction():
            return self._launch_one_in_transaction(node_type, now, tenant)

    def _launch_one_in_transaction(
        self, node_type: str, now: int, tenant: str | None
    ) -> Node | None:
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
            self._persist_runtime_state()
            self._log_event("launch_capacity_unavailable", timestamp=now)
            return None

        if node_id.startswith("node-") and node_id.removeprefix("node-").isdigit():
            self.node_id_counter = max(
                self.node_id_counter, int(node_id.removeprefix("node-"))
            )
            self.store.set_meta("node_id_counter", self.node_id_counter)

        node = Node(
            id=node_id,
            type=node_type,
            state=NodeState.launching,
            launched_at=now,
            ready_at=details.ready_at,
            tenant=tenant,
            failure_at=details.failure_at,
            last_tenant=tenant,
        )
        queued = [j for j in self.jobs.values() if j.status == JobStatus.queued]
        if tenant is None and queued:
            ordered = self.scheduler.queue_order(queued, self._rolling_usage(), now)
            node.tenant = ordered[0].team
            node.last_tenant = node.tenant
        self.retry_count[node_type] = 0
        self.backoff.pop(node_type, None)
        self.nodes[node.id] = node
        self.store.save_node(node)
        self._sync_node_segment(node, now)
        self._persist_runtime_state()
        self._log_event("node_launch_started", node_id=node.id, timestamp=now)
        return node

    def _free_gpus(self, node: Node) -> int:
        used = sum(
            j.gpus
            for j in self.jobs.values()
            if j.status == JobStatus.running and j.node_id == node.id
        )
        return self.NODE_GPUS[node.type] - used

    def _terminate_node(self, node: Node, now: int) -> None:
        launch_failed = node.failure_at is not None and node.failure_at <= now
        if launch_failed:
            self.provider.launch_failures += 1
        node.state = NodeState.terminated
        node.terminated_at = now
        self.provider.terminate_node(node.id)
        self.store.save_node(node)
        self._sync_node_segment(node, now)
        self._log_event(
            "node_launch_failed" if launch_failed else "node_terminated",
            node_id=node.id,
            timestamp=now,
        )

    def _mark_node_idle(self, node_id: str | None, now: int) -> None:
        if node_id is None:
            return
        node = self.nodes.get(node_id)
        if (
            node is not None
            and node.state == NodeState.ready
            and not any(
                job.status == JobStatus.running and job.node_id == node_id
                for job in self.jobs.values()
            )
        ):
            node.idle_since = now
            node.tenant = None
            self.store.save_node(node)
            self._sync_node_segment(node, now)

    def _sync_node_segment(self, node: Node, timestamp: int) -> None:
        used = self.NODE_GPUS[node.type] - self._free_gpus(node)
        self.store.sync_ledger_segment(
            node_id=node.id,
            timestamp=timestamp,
            tenant=node.tenant or node.last_tenant,
            gpus_used=used,
            active=node.state != NodeState.terminated,
        )

    def _sync_all_segments(self, timestamp: int) -> None:
        for node in self.nodes.values():
            self._sync_node_segment(node, timestamp)

    def _run_reaper(self, now: int) -> None:
        timeout = self.config.idle_timeout_minutes * 60
        for node in list(self.nodes.values()):
            if node.state != NodeState.ready or self._free_gpus(node) != self.NODE_GPUS[node.type]:
                continue
            if node.idle_since is None or now - node.idle_since <= timeout:
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
            if job.started_at is None:
                continue
            if job.finished_at is not None:
                end = job.finished_at
            elif job.status == JobStatus.running:
                end = self.clock.now
            else:
                continue
            duration = max(0, min(end, self.clock.now) - max(job.started_at, cutoff))
            if duration:
                usage[job.team] = (
                    usage.get(job.team, 0.0) + job.gpus * duration
                )
        return usage
