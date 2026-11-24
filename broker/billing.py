from dataclasses import dataclass

from .provider import FakeProvider
from .store import Store


@dataclass
class TeamCost:
    gpu_hours: float = 0.0
    direct_cost: float = 0.0
    idle_cost: float = 0.0

    @property
    def total_cost(self) -> float:
        return self.direct_cost + self.idle_cost


@dataclass
class CostReport:
    from_time: int
    to_time: int
    teams: dict[str, TeamCost]
    platform_cost: float
    total_cost: float
    idle_cost: float
    idle_percent: float


class Billing:
    MINIMUM_BILLING_SECONDS = 600

    def __init__(self, store: Store) -> None:
        self.store = store

    def report(self, from_time: int, to_time: int, now: int) -> CostReport:
        if to_time < from_time:
            raise ValueError("to_time must not be before from_time")

        nodes = {node.id: node for node in self.store.nodes()}
        segments = self.store.ledger_segments()
        node_used = {segment.node_id for segment in segments if segment.gpus_used > 0}
        amounts: dict[tuple[str | None, str], float] = {}
        gpu_seconds: dict[str, float] = {}

        for segment in segments:
            node = nodes[segment.node_id]
            capacity = FakeProvider.GPUS[node.type]
            if not 0 <= segment.gpus_used <= capacity:
                raise ValueError(f"invalid GPU usage for node {node.id}")
            end = segment.end if segment.end is not None else now
            start = max(segment.start, from_time)
            clipped_end = min(end, to_time, now)
            if clipped_end <= start:
                continue
            seconds = clipped_end - start
            cost = seconds * FakeProvider.PRICES[node.type] / 3600
            direct = cost * segment.gpus_used / capacity
            owner = segment.tenant if node.id in node_used else None
            self._add_amount(amounts, owner, "direct", direct)
            self._add_amount(amounts, owner, "idle", cost - direct)
            if owner is not None:
                gpu_seconds[owner] = (
                    gpu_seconds.get(owner, 0.0) + seconds * segment.gpus_used
                )

        for node in nodes.values():
            if node.terminated_at is None:
                continue
            lived = node.terminated_at - node.launched_at
            shortfall = max(0, self.MINIMUM_BILLING_SECONDS - lived)
            if shortfall and from_time <= node.terminated_at < to_time:
                owner = (
                    (node.last_tenant or self._last_segment_tenant(segments, node.id))
                    if node.id in node_used
                    else None
                )
                cost = shortfall * FakeProvider.PRICES[node.type] / 3600
                self._add_amount(amounts, owner, "idle", cost)

        provider_total = self._provider_total(
            nodes.values(), from_time, to_time, now
        )
        allocated_raw = sum(amounts.values())
        if abs(allocated_raw - provider_total) > 1e-8:
            raise RuntimeError(
                f"ledger cost {allocated_raw:.10f} does not match "
                f"provider cost {provider_total:.10f}"
            )
        cents = self._allocate_cents(amounts, round(provider_total * 100))

        teams: dict[str, TeamCost] = {}
        platform_cost = 0.0
        idle_cost = 0.0
        for (owner, kind), value in cents.items():
            dollars = value / 100
            if kind == "idle":
                idle_cost += dollars
            if owner is None:
                platform_cost += dollars
                continue
            team_cost = teams.setdefault(owner, TeamCost())
            if kind == "direct":
                team_cost.direct_cost += dollars
            else:
                team_cost.idle_cost += dollars

        for team, seconds in gpu_seconds.items():
            teams.setdefault(team, TeamCost()).gpu_hours = seconds / 3600

        total_cost = round(provider_total * 100) / 100
        return CostReport(
            from_time=from_time,
            to_time=to_time,
            teams=teams,
            platform_cost=platform_cost,
            total_cost=total_cost,
            idle_cost=idle_cost,
            idle_percent=idle_cost / total_cost * 100 if total_cost else 0.0,
        )

    @staticmethod
    def _add_amount(
        amounts: dict[tuple[str | None, str], float],
        owner: str | None,
        kind: str,
        amount: float,
    ) -> None:
        if amount:
            key = (owner, kind)
            amounts[key] = amounts.get(key, 0.0) + amount

    @staticmethod
    def _last_segment_tenant(segments, node_id: str) -> str | None:
        for segment in reversed(segments):
            if segment.node_id == node_id and segment.tenant is not None:
                return segment.tenant
        return None

    @classmethod
    def _provider_total(
        cls, nodes, from_time: int, to_time: int, now: int
    ) -> float:
        total = 0.0
        for node in nodes:
            end = node.terminated_at if node.terminated_at is not None else now
            seconds = max(0, min(end, to_time, now) - max(node.launched_at, from_time))
            total += seconds * FakeProvider.PRICES[node.type] / 3600
            if (
                node.terminated_at is not None
                and node.terminated_at < to_time
                and node.terminated_at >= from_time
            ):
                shortfall = max(
                    0, cls.MINIMUM_BILLING_SECONDS - (node.terminated_at - node.launched_at)
                )
                total += shortfall * FakeProvider.PRICES[node.type] / 3600
        return total

    @staticmethod
    def _allocate_cents(
        amounts: dict[tuple[str | None, str], float], total_cents: int
    ) -> dict[tuple[str | None, str], int]:
        cents = {key: int(amount * 100) for key, amount in amounts.items()}
        remainder = total_cents - sum(cents.values())
        ordered = sorted(
            amounts,
            key=lambda key: amounts[key] * 100 - cents[key],
            reverse=True,
        )
        for key in ordered[:remainder]:
            cents[key] += 1
        if sum(cents.values()) != total_cents:
            raise RuntimeError("could not reconcile billing to cents")
        return cents
