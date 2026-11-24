import random
from dataclasses import dataclass


class CapacityUnavailable(Exception):
    pass


@dataclass(frozen=True)
class ProviderLaunch:
    node_id: str
    ready_at: int
    failure_at: int | None


class FakeProvider:
    PRICES = {"gpu-s": 1.0, "gpu-m": 4.5, "gpu-l": 8.5}
    GPUS = {"gpu-s": 1, "gpu-m": 4, "gpu-l": 8}

    def __init__(self, seed: int = 42, clock=None) -> None:
        self.rng = random.Random(seed)
        self.clock = clock
        self.outage_until: int = 0
        self.alive: set[str] = set()
        self.launch_failures = 0

    def set_clock(self, clock) -> None:
        self.clock = clock

    def launch_node(self, node_type: str, now: int | None = None) -> str:
        now = self.clock.now if now is None and self.clock is not None else (now or 0)
        if now < self.outage_until:
            raise CapacityUnavailable("sticky outage")
        if self.rng.random() < 0.08:
            self.outage_until = now + self.rng.randint(10 * 60, 30 * 60)
            raise CapacityUnavailable("outage started")

        number = len(self.alive) + 1
        node_id = f"node-{number}"
        while node_id in self.alive:
            number += 1
            node_id = f"node-{number}"
        self.alive.add(node_id)
        return node_id

    def launch_details(
        self, node_id: str, node_type: str, now: int
    ) -> ProviderLaunch:
        ready_delay = self.rng.randint(4 * 60, 8 * 60)
        failure_at = None
        if self.rng.random() < 0.03:
            failure_at = now + 15 * 60
        return ProviderLaunch(node_id, now + ready_delay, failure_at)

    def terminate_node(self, node_id: str) -> None:
        self.alive.discard(node_id)

    def rng_state(self) -> tuple:
        return self.rng.getstate()

    def set_rng_state(self, state: tuple) -> None:
        self.rng.setstate(state)
