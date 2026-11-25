from dataclasses import dataclass, field

from .models import MINIMUM_BILLING_SECONDS, NODE_GPUS, NODE_PRICE_PER_HOUR
from .store import Store


@dataclass
class TeamCost:
    gpu_hours: float = 0.0
    direct_cost: float = 0.0  # share of the node price for the GPUs the team's jobs used
    idle_cost: float = 0.0  # unused GPUs, boot time and idle time on the team's nodes

    @property
    def total_cost(self) -> float:
        return self.direct_cost + self.idle_cost


@dataclass
class CostReport:
    from_time: int
    to_time: int
    teams: dict[str, TeamCost] = field(default_factory=dict)
    unattributed_cost: float = 0.0  # nodes that never ran a job (e.g. failed launches)
    idle_cost: float = 0.0  # all idle cost, attributed or not

    @property
    def total_cost(self) -> float:
        return sum(t.total_cost for t in self.teams.values()) + self.unattributed_cost

    @property
    def idle_percent(self) -> float:
        total = self.total_cost
        return self.idle_cost / total * 100 if total else 0.0


def cost_report(store: Store, from_time: int, to_time: int, now: int) -> CostReport:
    """Cost per team in [from_time, to_time), computed from the occupancy ledger.

    Values are not rounded; callers round for display.
    """
    if to_time < from_time:
        raise ValueError("to_time must not be before from_time")
    report = CostReport(from_time, to_time)
    nodes = {node.id: node for node in store.nodes()}
    segments = store.ledger_segments()
    # Idle time on a node that ran at least one job belongs to its (last) tenant,
    # because that team's demand launched it. Never-used nodes stay unattributed.
    used_nodes = {s.node_id for s in segments if s.gpus_used > 0}

    def add(node_id: str, tenant: str | None, direct: float, idle: float, gpu_seconds: float) -> None:
        report.idle_cost += idle
        if tenant is None or node_id not in used_nodes:
            report.unattributed_cost += direct + idle
            return
        team = report.teams.setdefault(tenant, TeamCost())
        team.direct_cost += direct
        team.idle_cost += idle
        team.gpu_hours += gpu_seconds / 3600

    for segment in segments:
        node = nodes[segment.node_id]
        start = max(segment.start, from_time)
        end = min(segment.end if segment.end is not None else now, to_time, now)
        if end <= start:
            continue
        seconds = end - start
        cost = seconds * NODE_PRICE_PER_HOUR[node.type] / 3600
        direct = cost * segment.gpus_used / NODE_GPUS[node.type]
        add(node.id, segment.tenant, direct, cost - direct, seconds * segment.gpus_used)

    # 10-minute minimum: the shortfall is billed when the node is terminated.
    for node in nodes.values():
        if node.terminated_at is None or not from_time <= node.terminated_at < to_time:
            continue
        shortfall = MINIMUM_BILLING_SECONDS - (node.terminated_at - node.launched_at)
        if shortfall > 0:
            add(node.id, node.last_tenant, 0.0, shortfall * NODE_PRICE_PER_HOUR[node.type] / 3600, 0)
    return report
