from dataclasses import dataclass
from enum import StrEnum

# Provider facts from the brief. Node types are listed smallest first.
NODE_GPUS = {"gpu-s": 1, "gpu-m": 4, "gpu-l": 8}
NODE_PRICE_PER_HOUR = {"gpu-s": 1.0, "gpu-m": 4.5, "gpu-l": 8.5}
NODE_TYPES = tuple(NODE_GPUS)
MINIMUM_BILLING_SECONDS = 10 * 60


class Priority(StrEnum):
    low = "low"
    normal = "normal"
    high = "high"


class JobStatus(StrEnum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


TERMINAL_STATUSES = {JobStatus.succeeded, JobStatus.failed, JobStatus.cancelled}


class NodeState(StrEnum):
    launching = "launching"
    ready = "ready"
    terminated = "terminated"


@dataclass
class Job:
    id: str
    team: str
    gpus: int
    estimated_minutes: int
    priority: Priority
    idempotency_key: str
    status: JobStatus = JobStatus.queued
    submitted_at: int = 0
    started_at: int | None = None
    finished_at: int | None = None
    ends_at: int | None = None  # set when a simulated job (trace replay) starts
    actual_minutes: int | None = None  # simulation only; the scheduler never reads it
    outcome: str = "succeeded"  # simulation only; how the job ends at ends_at
    node_id: str | None = None


@dataclass
class Node:
    id: str
    type: str
    state: NodeState
    launched_at: int
    ready_at: int | None = None
    terminated_at: int | None = None
    tenant: str | None = None  # team currently allowed on the node (None = free)
    failure_at: int | None = None  # set when the launch will never become ready
    idle_since: int | None = None
    last_tenant: str | None = None  # who pays for idle time after the last job


@dataclass(frozen=True)
class LedgerSegment:
    node_id: str
    start: int
    end: int | None
    tenant: str | None
    gpus_used: int
