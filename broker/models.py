from dataclasses import dataclass
from enum import StrEnum


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
    ends_at: int | None = None
    actual_minutes: int | None = None
    outcome: str = "succeeded"
    node_id: str | None = None


@dataclass
class Node:
    id: str
    type: str
    state: NodeState
    launched_at: int
    ready_at: int | None = None
    terminated_at: int | None = None
    tenant: str | None = None
    failure_at: int | None = None
