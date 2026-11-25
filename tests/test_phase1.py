import random

from broker.broker import (
    Broker,
    BrokerConfig,
    ConflictError,
    IllegalTransitionError,
    ValidationError,
)
from broker.models import NODE_GPUS, JobStatus, NodeState
from broker.provider import CapacityUnavailable, FakeProvider
from broker.scheduler import Scheduler
from broker.models import Job, Priority
from broker.store import Store


def make_broker(tmp_path, **overrides):
    config = BrokerConfig(
        db_path=str(tmp_path / "jobs.db"),
        idle_timeout_minutes=10,
        max_nodes=10,
        seed=42,
        fairness_policy="fair",
    )
    for name, value in overrides.items():
        setattr(config, name, value)
    return Broker(store=Store(config.db_path), config=config)


def test_idempotency(tmp_path):
    broker = make_broker(tmp_path)
    first = broker.submit("vision", 1, 30, "normal", "key-1")
    duplicate = broker.submit("vision", 1, 30, "normal", "key-1")
    assert duplicate.id == first.id
    try:
        broker.submit("vision", 2, 30, "normal", "key-1")
    except ConflictError:
        pass
    else:
        raise AssertionError("expected conflict")


def test_validation_and_lifecycle(tmp_path):
    broker = make_broker(tmp_path)
    for gpu_count, key in ((3, "bad"), (True, "boolean")):
        try:
            broker.submit("vision", gpu_count, 30, "normal", key)
        except ValidationError:
            pass
        else:
            raise AssertionError("expected validation error")

    queued = broker.submit("vision", 1, 30, "normal", "job-1")
    cancelled = broker.cancel(queued.id)
    assert cancelled.status == JobStatus.cancelled
    try:
        broker.cancel(cancelled.id)
    except ConflictError:
        pass
    else:
        raise AssertionError("terminal cancel should conflict")


def test_cancel_running_frees_gpu(tmp_path):
    broker = make_broker(tmp_path)
    job = broker.submit("vision", 1, 30, "normal", "job-1")
    broker.advance_to(8 * 60)
    assert job.status == JobStatus.running
    broker.cancel(job.id)
    node = broker.nodes[job.node_id] if job.node_id else next(iter(broker.nodes.values()))
    assert broker.free_gpus(node) == NODE_GPUS[node.type]


def test_best_fit_placement(tmp_path):
    broker = make_broker(tmp_path)
    broker.submit("vision", 1, 30, "normal", "small")
    broker.submit("vision", 1, 30, "normal", "small-2")
    broker.advance_to(10 * 60)
    nodes = [n for n in broker.nodes.values() if n.state == NodeState.ready]
    assert nodes
    running = [j for j in broker.jobs.values() if j.status == JobStatus.running]
    assert len(running) == 2


def test_max_nodes_cap(tmp_path):
    broker = make_broker(tmp_path, max_nodes=2)
    for index in range(20):
        broker.submit("vision", 1, 30, "normal", f"job-{index}")
    alive = [
        n
        for n in broker.nodes.values()
        if n.state in (NodeState.launching, NodeState.ready)
    ]
    assert len(alive) <= 2


def test_tenant_isolation(tmp_path):
    broker = make_broker(tmp_path)
    randomizer = random.Random(2025)

    class ReliableRng(random.Random):
        def __init__(self):
            super().__init__(0)

        def random(self):
            return 0.5

        def randint(self, start, _end):
            return start

    broker.provider.rng = ReliableRng()

    def assert_isolation():
        for node in broker.nodes.values():
            teams = {
                job.team
                for job in broker.jobs.values()
                if job.status == JobStatus.running and job.node_id == node.id
            }
            assert len(teams) <= 1
            if teams:
                assert node.tenant in teams

    process_events = broker._process_due_events

    def process_and_check():
        process_events()
        assert_isolation()

    broker._process_due_events = process_and_check
    teams = ("vision", "speech", "search", "ranking")
    for index in range(60):
        if index:
            broker.advance_to(
                broker.clock.now + randomizer.randint(30, 180)
            )
        broker.submit(
            randomizer.choice(teams),
            randomizer.choice((1, 2, 4, 8)),
            randomizer.randint(5, 45),
            randomizer.choice(("low", "normal", "high")),
            f"random-{index}",
            actual_minutes=randomizer.randint(3, 20),
        )
        assert_isolation()
    broker.advance_to(broker.clock.now + 2 * 60 * 60)
    assert_isolation()


def test_capacity_outage_resilience(tmp_path):
    broker = make_broker(tmp_path)

    class FailingProvider(FakeProvider):
        def launch_node(self, node_type, now):
            raise CapacityUnavailable("outage")

    broker.provider = FailingProvider()
    job = broker.submit("vision", 1, 30, "normal", "job")
    assert job.status == JobStatus.queued
    broker.advance_to(30 * 60)
    assert job.status == JobStatus.queued
    assert broker.launch_failures >= 2  # retried with backoff, never gave up
    assert broker.backoff["gpu-s"]["failures"] >= 2


def test_launch_failure_and_reaper(tmp_path):
    broker = make_broker(tmp_path)

    class ForcedLaunchFailureRng(random.Random):
        values = iter((0.5, 0.0, 0.5, 0.5))

        def __init__(self):
            super().__init__(0)

        def random(self):
            return next(self.values)

        def randint(self, start, _end):
            return start

    broker.provider.rng = ForcedLaunchFailureRng()
    job = broker.submit("vision", 1, 30, "normal", "job")
    failed_node = next(iter(broker.nodes.values()))
    assert failed_node.ready_at is None
    assert failed_node.failure_at == 15 * 60
    broker.advance_to(15 * 60)
    assert failed_node.state == NodeState.terminated
    assert failed_node.terminated_at == failed_node.failure_at
    assert job.status == JobStatus.queued
    replacements = [
        node
        for node in broker.nodes.values()
        if node.id != failed_node.id and node.state == NodeState.launching
    ]
    assert len(replacements) == 1


def test_never_ready_launch_has_no_ready_time():
    provider = FakeProvider()

    class ForcedFailureRng:
        values = iter((0.5, 0.0))  # no outage, then "never ready"

        def randint(self, _start, _end):
            return 4 * 60

        def random(self):
            return next(self.values)

    provider.rng = ForcedFailureRng()
    launch = provider.launch_node("gpu-s", 100)
    assert launch.ready_at is None
    assert launch.failure_at == 100 + 15 * 60


def test_node_ids_are_monotonic_and_terminated_rows_are_preserved(tmp_path):
    broker = make_broker(tmp_path)

    class ReliableRng(random.Random):
        def __init__(self):
            super().__init__(0)

        def random(self):
            return 0.5

        def randint(self, start, _end):
            return start

    broker.provider.rng = ReliableRng()
    first = broker._launch_one("gpu-s")
    assert first is not None
    broker.clock.advance_to(1)
    broker._terminate_node(first)

    second = broker._launch_one("gpu-s")
    assert second is not None
    assert second.id != first.id
    assert broker.store.conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0] == 2
    assert broker.store.conn.execute(
        "SELECT state FROM nodes WHERE id=?", (first.id,)
    ).fetchone()[0] == "terminated"

    store = Store(broker.config.db_path)
    restarted = Broker(store=store, config=broker.config)
    restarted.provider.rng = ReliableRng()
    third = restarted._launch_one("gpu-s")
    assert third is not None
    assert third.id not in {first.id, second.id}


def test_reaper_timeout_starts_when_last_job_finishes(tmp_path):
    broker = make_broker(tmp_path)
    job = broker.submit("vision", 1, 30, "normal", "job", actual_minutes=15)
    broker.advance_to(8 * 60)
    assert job.status == JobStatus.running
    broker.advance_to(job.ends_at)
    node = broker.nodes[job.node_id] if job.node_id else next(iter(broker.nodes.values()))
    assert job.status == JobStatus.succeeded
    idle_since = job.finished_at
    assert idle_since is not None

    broker.advance_to(idle_since + 9 * 60)
    assert node.state == NodeState.ready
    broker.advance_to(idle_since + 10 * 60 + 1)
    assert node.state == NodeState.terminated


def test_fifo_sorts_priority_descending_then_submission_time():
    jobs = [
        Job("later-high", "a", 1, 10, Priority.high, "1", submitted_at=2),
        Job("early-low", "a", 1, 10, Priority.low, "2", submitted_at=1),
        Job("early-high", "a", 1, 10, Priority.high, "3", submitted_at=1),
        Job("normal", "a", 1, 10, Priority.normal, "4", submitted_at=1),
    ]
    ordered = Scheduler("fifo").queue_order(jobs, {}, 10)
    assert [job.id for job in ordered] == [
        "early-high",
        "later-high",
        "normal",
        "early-low",
    ]


def test_empty_node_is_reused_by_another_team(tmp_path):
    broker = make_broker(tmp_path)
    first = broker.submit("team-a", 1, 30, "normal", "a", actual_minutes=15)
    launch_node = next(iter(broker.nodes.values()))
    assert launch_node.tenant == "team-a"

    broker.advance_to(8 * 60)
    assert first.status == JobStatus.running
    broker.advance_to(first.ends_at)
    assert launch_node.tenant is None

    second = broker.submit("team-b", 1, 10, "normal", "b")
    assert second.status == JobStatus.running
    assert second.node_id == launch_node.id
    assert launch_node.tenant == "team-b"


def test_due_launch_retry_is_dropped_when_queue_is_empty(tmp_path):
    broker = make_broker(tmp_path)

    class CountingFailureProvider(FakeProvider):
        calls = 0

        def launch_node(self, _node_type, _now):
            self.calls += 1
            raise CapacityUnavailable("outage")

    broker.provider = CountingFailureProvider()
    job = broker.submit("vision", 1, 30, "normal", "job")
    initial_calls = broker.provider.calls
    assert initial_calls >= 1
    broker.cancel(job.id)
    broker.advance_to(10 * 60)
    assert broker.provider.calls == initial_calls


def test_large_team_queue_launches_for_uncovered_demand(tmp_path):
    broker = make_broker(tmp_path)

    class ReliableRng(random.Random):
        def __init__(self):
            super().__init__(0)

        def random(self):
            return 0.5

        def randint(self, start, _end):
            return start

    broker.provider.rng = ReliableRng()
    for index in range(40):
        broker.submit("vision", 1, 30, "normal", f"job-{index}")

    alive_nodes = [
        node
        for node in broker.nodes.values()
        if node.state in (NodeState.launching, NodeState.ready)
    ]
    assert 2 <= len(alive_nodes) <= broker.config.max_nodes
    assert sum(NODE_GPUS[node.type] for node in alive_nodes) >= 40


def test_fair_queue_round_robins_teams():
    jobs = [
        Job("a1", "team-a", 1, 10, Priority.normal, "a1", submitted_at=1),
        Job("a2", "team-a", 1, 10, Priority.normal, "a2", submitted_at=2),
        Job("a3", "team-a", 1, 10, Priority.normal, "a3", submitted_at=3),
        Job("b1", "team-b", 1, 10, Priority.normal, "b1", submitted_at=4),
        Job("b2", "team-b", 1, 10, Priority.normal, "b2", submitted_at=5),
    ]
    ordered = Scheduler("fair").queue_order(jobs, {}, 10)
    assert [job.id for job in ordered] == ["a1", "b1", "a2", "b2", "a3"]


def test_fair_usage_is_rolling_gpu_seconds(tmp_path):
    broker = make_broker(tmp_path)
    broker.clock.now = 10_000
    running = Job(
        "running",
        "team-a",
        2,
        10,
        Priority.normal,
        "running",
        status=JobStatus.running,
        started_at=9_400,
    )
    old = Job(
        "old",
        "team-b",
        8,
        10,
        Priority.normal,
        "old",
        status=JobStatus.succeeded,
        started_at=9_000 - 24 * 60 * 60,
        finished_at=9_200 - 24 * 60 * 60,
    )
    broker.jobs = {running.id: running, old.id: old}
    assert broker._rolling_usage() == {"team-a": 1_200}


def test_illegal_transition(tmp_path):
    broker = make_broker(tmp_path)
    job = broker.submit("vision", 1, 30, "normal", "job")
    try:
        broker.finish_job(job.id)
    except IllegalTransitionError:
        pass
    else:
        raise AssertionError("queued job cannot finish")


class ReliableRng(random.Random):
    """No outages, every node ready after exactly 4 minutes."""

    def __init__(self):
        super().__init__(0)

    def random(self):
        return 0.5

    def randint(self, start, _end):
        return start


def test_completion_is_reported_by_a_call(tmp_path):
    broker = make_broker(tmp_path)
    broker.provider.rng = ReliableRng()
    job = broker.submit("vision", 1, 30, "normal", "api-job")  # no actual_minutes
    broker.advance_to(4 * 60)
    assert job.status == JobStatus.running
    node = broker.nodes[job.node_id]

    broker.finish_job(job.id, "failed")
    assert job.status == JobStatus.failed
    assert node.idle_since == broker.clock.now
    broker.advance_to(broker.clock.now + 11 * 60)
    assert node.state == NodeState.terminated


def test_booting_node_stays_reserved_for_the_team_that_launched_it(tmp_path):
    broker = make_broker(tmp_path)
    broker.provider.rng = ReliableRng()
    alpha = broker.submit("alpha", 1, 30, "normal", "a", actual_minutes=30)
    broker.advance_to(60)
    beta = broker.submit("beta", 1, 30, "high", "b", actual_minutes=30)
    assert len(broker.nodes) == 2

    broker.advance_to(4 * 60)  # alpha's node is ready; beta's is still booting
    assert alpha.status == JobStatus.running
    assert beta.status == JobStatus.queued
    broker.advance_to(5 * 60)
    assert beta.status == JobStatus.running
    assert len(broker.nodes) == 2  # no extra launch


def test_job_must_fit_on_one_node_when_deciding_to_launch(tmp_path):
    broker = make_broker(tmp_path)
    broker.provider.rng = ReliableRng()
    first = broker.submit("t", 4, 10, "normal", "m0", actual_minutes=10)
    broker.advance_to(4 * 60)
    broker.submit("t", 4, 10, "normal", "m1", actual_minutes=10)
    broker.advance_to(first.ends_at + 5 * 60)
    idle = [n for n in broker.nodes.values() if n.state == NodeState.ready]
    assert [n.type for n in idle] == ["gpu-m", "gpu-m"]
    assert all(broker.free_gpus(n) == 4 for n in idle)

    big = broker.submit("t", 8, 10, "normal", "big", actual_minutes=10)
    booting = [n for n in broker.nodes.values() if n.state == NodeState.launching]
    assert [n.type for n in booting] == ["gpu-l"]  # 4 + 4 free does not fit 8
    broker.advance_to(broker.clock.now + 4 * 60)
    assert big.status == JobStatus.running
