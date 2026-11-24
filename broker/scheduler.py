from .models import Job, JobStatus, Priority


class Scheduler:
    def __init__(self, policy: str = "fair") -> None:
        self.policy = policy
        self.priority_rank = {"low": 0, "normal": 1, "high": 2}

    def queue_order(self, jobs: list[Job], usage: dict[str, float], now: int) -> list[Job]:
        queued = [j for j in jobs if j.status == JobStatus.queued]
        if self.policy == "fifo":
            return sorted(
                queued,
                key=lambda j: (
                    -self.priority_rank[j.priority.value],
                    j.submitted_at,
                    j.id,
                ),
            )

        def effective_priority(job: Job) -> int:
            rank = self.priority_rank[job.priority.value]
            if job.priority != Priority.high and now - job.submitted_at > 60 * 60:
                rank += 1
            return min(rank, 2)

        tiers: dict[int, list[Job]] = {}
        for job in queued:
            tiers.setdefault(effective_priority(job), []).append(job)

        ordered: list[Job] = []
        for tier in sorted(tiers, reverse=True):
            team_jobs: dict[str, list[Job]] = {}
            for job in sorted(tiers[tier], key=lambda j: (j.submitted_at, j.id)):
                team_jobs.setdefault(job.team, []).append(job)
            teams = sorted(
                team_jobs,
                key=lambda team: (
                    usage.get(team, 0.0),
                    team_jobs[team][0].submitted_at,
                    team,
                ),
            )
            while any(team_jobs.values()):
                for team in teams:
                    if team_jobs[team]:
                        ordered.append(team_jobs[team].pop(0))
        return ordered

