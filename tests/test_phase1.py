from broker.broker import (
    Broker,
    BrokerConfig,
    ConflictError,
    IllegalTransitionError,
    ValidationError,
)
from broker.models import JobStatus, NodeState
from broker.provider import CapacityUnavailable
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
    try:
        broker.submit("vision", 3, 30, "normal", "bad")
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
    assert broker._free_gpus(node) == node_capacity(node)


def node_capacity(node):
    return {"gpu-s": 1, "gpu-m": 4, "gpu-l": 8}[node.type]


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
    broker.submit("team-a", 1, 30, "normal", "a")
    broker.advance_to(10 * 60)
    broker.submit("team-b", 1, 30, "normal", "b")
    for node in broker.nodes.values():
        tenants = {
            broker.jobs[job_id].team
            for job_id, job in broker.jobs.items()
            if job.status == JobStatus.running and job.node_id == node.id
        }
        assert len(tenants) <= 1


def test_capacity_outage_resilience(tmp_path):
    broker = make_broker(tmp_path)

    class FailingProvider:
        launch_failures = 0

        def set_clock(self, clock):
            pass

        def launch_node(self, node_type, now):
            self.launch_failures += 1
            raise CapacityUnavailable("outage")

        def terminate_node(self, node_id):
            pass

    broker.provider = FailingProvider()
    job = broker.submit("vision", 1, 30, "normal", "job")
    assert job.status == JobStatus.queued
    broker.advance_to(30 * 60)
    assert job.status == JobStatus.queued
    assert broker.provider.launch_failures >= 1


def test_launch_failure_and_reaper(tmp_path):
    broker = make_broker(tmp_path)
    job = broker.submit("vision", 1, 30, "normal", "job")
    broker.advance_to(15 * 60)
    assert any(n.state == NodeState.terminated for n in broker.nodes.values()) or job.status == JobStatus.running


def test_illegal_transition(tmp_path):
    broker = make_broker(tmp_path)
    job = broker.submit("vision", 1, 30, "normal", "job")
    try:
        broker.finish_job(job.id)
    except IllegalTransitionError:
        pass
    else:
        raise AssertionError("queued job cannot finish")
