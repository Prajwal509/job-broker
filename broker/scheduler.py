from .models import Job, JobStatus, Node, NodeState, Priority


class Scheduler:
    def __init__(self, policy: str = "fair") -> None:
        self.policy = policy
        self.priority_rank = {"low": 0, "normal": 1, "high": 2}

    def queue_order(self, jobs: list[Job], usage: dict[str, float], now: int) -> list[Job]:
        queued = [j for j in jobs if j.status == JobStatus.queued]
        if self.policy == "fifo":
            return sorted(
                queued,
                key=lambda j: (self.priority_rank[j.priority.value], j.submitted_at, j.id),
            )

        def effective_priority(job: Job) -> int:
            rank = self.priority_rank[job.priority.value]
            if job.priority != Priority.high and now - job.submitted_at > 60 * 60:
                rank += 1
            return min(rank, 2)

        return sorted(
            queued,
            key=lambda j: (
                -effective_priority(j),
                usage.get(j.team, 0.0),
                j.submitted_at,
                j.id,
            ),
        )

    def place_jobs(
        self, jobs: list[Job], nodes: list[Node], now: int
    ) -> tuple[list[Job], list[Node]]:
        usage: dict[str, float] = {}
        for node in nodes:
            if node.tenant:
                usage[node.tenant] = usage.get(node.tenant, 0.0)
        for job in self.queue_order(jobs, usage, now):
            candidates = [
                n
                for n in nodes
                if n.state == NodeState.ready
                and n.tenant in (None, job.team)
                and self.free_gpus(n, jobs) >= job.gpus
            ]
            if candidates:
                node = min(candidates, key=lambda n: (n.tenant is None, self.free_gpus(n, jobs)))
                node.tenant = job.team
                job.status = JobStatus.running
                job.started_at = now
                job.node_id = node.id
        return jobs, nodes

    @staticmethod
    def free_gpus(node: Node, jobs: list[Job]) -> int:
        capacities = {"gpu-s": 1, "gpu-m": 4, "gpu-l": 8}
        running = sum(
            j.gpus
            for j in jobs
            if j.status == JobStatus.running
            and j.node_id == node.id
        )
        return capacities[node.type] - running
