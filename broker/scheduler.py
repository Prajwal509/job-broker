from .models import Job, JobStatus, Priority

PRIORITY_RANK = {Priority.low: 0, Priority.normal: 1, Priority.high: 2}
AGING_SECONDS = 60 * 60  # a job waiting longer than this moves up one priority tier


class Scheduler:
    """Decides only the order of the queue; Broker decides where jobs run.

    fifo: priority, then submission time.
    fair: priority (with aging), then teams with the fewest GPU-seconds in the
          last 24h first, round-robin between teams, FIFO within a team.
    """

    def __init__(self, policy: str = "fair") -> None:
        if policy not in ("fair", "fifo"):
            raise ValueError(f"unknown fairness policy {policy}")
        self.policy = policy

    def queue_order(self, jobs: list[Job], usage: dict[str, float], now: int) -> list[Job]:
        queued = [j for j in jobs if j.status == JobStatus.queued]
        if self.policy == "fifo":
            return sorted(queued, key=lambda j: (-PRIORITY_RANK[j.priority], j.submitted_at, j.id))

        def tier(job: Job) -> int:
            rank = PRIORITY_RANK[job.priority]
            if now - job.submitted_at > AGING_SECONDS:
                rank += 1
            return min(rank, PRIORITY_RANK[Priority.high])

        ordered: list[Job] = []
        for level in sorted({tier(j) for j in queued}, reverse=True):
            by_team: dict[str, list[Job]] = {}
            for job in sorted((j for j in queued if tier(j) == level), key=lambda j: (j.submitted_at, j.id)):
                by_team.setdefault(job.team, []).append(job)
            teams = sorted(by_team, key=lambda t: (usage.get(t, 0.0), by_team[t][0].submitted_at, t))
            # Round-robin: one job from each team in turn, lightest team first.
            while any(by_team.values()):
                for team in teams:
                    if by_team[team]:
                        ordered.append(by_team[team].pop(0))
        return ordered
