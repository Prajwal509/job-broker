import random
from dataclasses import dataclass

from .models import NODE_GPUS


class CapacityUnavailable(Exception):
    pass


@dataclass(frozen=True)
class ProviderLaunch:
    node_id: str
    ready_at: int | None  # None when the node will never become ready
    failure_at: int | None  # when the provider reports the failed launch


class FakeProvider:
    """Seeded fake cloud: bursty capacity outages, 4-8 min boot, 3% never ready."""

    OUTAGE_START_PROBABILITY = 0.08
    NEVER_READY_PROBABILITY = 0.03

    def __init__(self, seed: int = 42) -> None:
        self.rng = random.Random(seed)
        self.outage_until = 0
        self.alive: set[str] = set()
        self.last_node_number = 0

    def launch_node(self, node_type: str, now: int) -> ProviderLaunch:
        if node_type not in NODE_GPUS:
            raise ValueError(f"unknown node type {node_type}")
        # Outages come in bursts: once one starts, every launch fails until it ends.
        if now < self.outage_until:
            raise CapacityUnavailable("outage in progress")
        if self.rng.random() < self.OUTAGE_START_PROBABILITY:
            self.outage_until = now + self.rng.randint(10 * 60, 30 * 60)
            raise CapacityUnavailable("outage started")

        self.last_node_number += 1
        node_id = f"node-{self.last_node_number}"
        self.alive.add(node_id)
        ready_delay = self.rng.randint(4 * 60, 8 * 60)
        if self.rng.random() < self.NEVER_READY_PROBABILITY:
            return ProviderLaunch(node_id, ready_at=None, failure_at=now + 15 * 60)
        return ProviderLaunch(node_id, ready_at=now + ready_delay, failure_at=None)

    def terminate_node(self, node_id: str) -> None:
        self.alive.discard(node_id)

    # Saved with the broker state so a restart continues the same random sequence.
    def export_state(self) -> dict:
        return {
            "rng_state": self.rng.getstate(),
            "outage_until": self.outage_until,
            "alive": sorted(self.alive),
            "last_node_number": self.last_node_number,
        }

    def restore_state(self, state: dict) -> None:
        self.rng.setstate(_as_tuple(state["rng_state"]))
        self.outage_until = state["outage_until"]
        self.alive = set(state["alive"])
        self.last_node_number = state["last_node_number"]


def _as_tuple(value):
    """JSON turns the RNG state tuple into lists; random.setstate needs tuples."""
    if isinstance(value, list):
        return tuple(_as_tuple(item) for item in value)
    return value
