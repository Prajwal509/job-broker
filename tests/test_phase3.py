import random

from broker.broker import Broker, BrokerConfig
from broker.models import Job, JobStatus, Priority
from broker.scheduler import Scheduler
from broker.store import Store


def make_broker(tmp_path, max_nodes=1):
    config = BrokerConfig(
        db_path=str(tmp_path / "jobs.db"),
        idle_timeout_minutes=10,
        max_nodes=max_nodes,
        seed=1,
        fairness_policy="fair",
    )
    broker = Broker(store=Store(config.db_path), config=config)

    class ReliableRng(random.Random):
        def __init__(self):
            super().__init__(0)

        def random(self):
            return 0.5

        def randint(self, start, _end):
            return start

    broker.provider.rng = ReliableRng()
    return broker


def test_fair_policy_prioritizes_lower_usage_team():
    jobs = [
        Job(f"heavy-{index}", "heavy", 1, 10, Priority.normal, str(index))
        for index in range(10)
    ]
    jobs.append(Job("light-1", "light", 1, 10, Priority.normal, "light"))

    ordered = Scheduler("fair").queue_order(
        jobs, {"heavy": 20_000_000, "light": 0}, now=100
    )

    assert ordered[0].team == "light"
    assert [job.team for job in ordered[:5]] == [
        "light",
        "heavy",
        "heavy",
        "heavy",
        "heavy",
    ]


def test_aged_low_job_moves_into_normal_tier():
    low = Job(
        "old-low",
        "vision",
        1,
        10,
        Priority.low,
        "low",
        submitted_at=0,
    )
    normal = Job(
        "new-normal",
        "vision",
        1,
        10,
        Priority.normal,
        "normal",
        submitted_at=3_601,
    )

    ordered = Scheduler("fair").queue_order([normal, low], {}, now=3_601)

    assert [job.id for job in ordered] == ["old-low", "new-normal"]


def test_fifo_policy_does_not_apply_aging_or_team_usage():
    low = Job(
        "old-low",
        "vision",
        1,
        10,
        Priority.low,
        "low",
        submitted_at=0,
    )
    normal = Job(
        "new-normal",
        "vision",
        1,
        10,
        Priority.normal,
        "normal",
        submitted_at=3_601,
    )

    ordered = Scheduler("fifo").queue_order(
        [low, normal], {"vision": 100_000}, now=3_601
    )

    assert [job.id for job in ordered] == ["new-normal", "old-low"]


def test_aged_low_job_runs_before_new_normal_after_capacity_frees(tmp_path):
    broker = make_broker(tmp_path)
    blocker = broker.submit(
        "blocker", 1, 90, "normal", "blocker", actual_minutes=70
    )
    broker.advance_to(8 * 60)
    assert blocker.status == JobStatus.running

    low = broker.submit("vision", 1, 10, "low", "old-low")
    broker.advance_to(61 * 60)
    normal = broker.submit("vision", 1, 10, "normal", "new-normal")
    assert low.status == JobStatus.queued
    assert normal.status == JobStatus.queued

    broker.advance_to(blocker.ends_at)

    assert low.status == JobStatus.running
    assert normal.status == JobStatus.queued


def test_high_priority_submission_does_not_preempt_running_job(tmp_path):
    broker = make_broker(tmp_path)
    running = broker.submit(
        "team-a", 1, 120, "normal", "running", actual_minutes=90
    )
    broker.advance_to(8 * 60)
    node_id = running.node_id
    assert running.status == JobStatus.running

    urgent = broker.submit("team-b", 1, 10, "high", "urgent")

    assert running.status == JobStatus.running
    assert running.node_id == node_id
    assert urgent.status == JobStatus.queued
