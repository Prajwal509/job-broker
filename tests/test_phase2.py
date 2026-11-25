import sqlite3

import pytest

from broker.billing import cost_report
from broker.broker import Broker, BrokerConfig
from broker.models import NODE_PRICE_PER_HOUR, JobStatus, NodeState
from broker.store import Store


def make_broker(tmp_path, name):
    config = BrokerConfig(
        db_path=str(tmp_path / name),
        idle_timeout_minutes=10,
        max_nodes=2,
        seed=1,
        fairness_policy="fair",
    )
    return Broker(store=Store(config.db_path), config=config)


def state_snapshot(broker):
    return {
        "clock": broker.clock.now,
        "jobs": broker.store.jobs(),
        "nodes": broker.store.nodes(),
        "ledger": broker.store.conn.execute(
            "SELECT node_id,start,end,tenant,gpus_used FROM ledger ORDER BY rowid"
        ).fetchall(),
        "meta": broker.store.conn.execute(
            "SELECT key,value FROM meta ORDER BY key"
        ).fetchall(),
        "provider_alive": set(broker.provider.alive),
        "provider_rng": broker.provider.rng.getstate(),
    }


def test_restart_restores_full_state_and_matches_uninterrupted_run(tmp_path):
    resumed_source = make_broker(tmp_path, "resumed-source.db")
    resumed_source.submit("vision", 1, 120, "normal", "vision", actual_minutes=90)
    resumed_source.submit("speech", 8, 120, "normal", "speech", actual_minutes=90)
    resumed_source.submit("ranking", 8, 120, "normal", "ranking", actual_minutes=90)
    resumed_source.advance_to(12 * 60)
    assert any(job.status == JobStatus.running for job in resumed_source.jobs.values())
    assert any(job.status == JobStatus.queued for job in resumed_source.jobs.values())

    before_restart = state_snapshot(resumed_source)
    for database_name in ("continuous.db", "resumed.db"):
        backup = sqlite3.connect(str(tmp_path / database_name))
        resumed_source.store.conn.backup(backup)
        backup.close()
    resumed_source.store.close()

    continuous_config = BrokerConfig(
        db_path=str(tmp_path / "continuous.db"),
        idle_timeout_minutes=10,
        max_nodes=2,
        seed=1,
        fairness_policy="fair",
    )
    continuous = Broker(
        store=Store(continuous_config.db_path),
        config=continuous_config,
    )
    restarted = Broker(
        store=Store(str(tmp_path / "resumed.db")),
        config=resumed_source.config,
    )
    assert state_snapshot(restarted) == before_restart
    active_ids = {
        node.id
        for node in restarted.nodes.values()
        if node.state != NodeState.terminated
    }
    assert restarted.provider.alive == active_ids

    continuous.advance_to(130 * 60)
    restarted.advance_to(130 * 60)
    assert state_snapshot(restarted) == state_snapshot(continuous)


def test_cost_report_reconciles_direct_and_idle_cost_in_window(tmp_path):
    broker = make_broker(tmp_path, "billing.db")
    job = broker.submit("vision", 1, 30, "normal", "job", actual_minutes=5)
    broker.advance_to(8 * 60)
    assert job.status == JobStatus.running
    started_at = job.started_at
    broker.advance_to(job.ends_at + 11 * 60)
    node = next(iter(broker.nodes.values()))
    assert node.state == NodeState.terminated

    report = cost_report(broker.store, 0, broker.clock.now + 1, broker.clock.now)
    team = report.teams["vision"]
    assert team.gpu_hours == pytest.approx(5 / 60)
    assert team.direct_cost == pytest.approx(5 / 60)  # 5 min of a $1/h node
    assert team.idle_cost > 0
    assert team.total_cost + report.unattributed_cost == pytest.approx(report.total_cost)
    assert report.total_cost == pytest.approx(provider_bill(broker))

    running_window = cost_report(broker.store, started_at, job.finished_at, broker.clock.now)
    assert running_window.teams["vision"].total_cost == pytest.approx(5 / 60)


def test_live_node_cost_accrues_through_report_time(tmp_path):
    broker = make_broker(tmp_path, "live-billing.db")
    job = broker.submit("vision", 1, 120, "normal", "job", actual_minutes=90)
    broker.advance_to(8 * 60)
    assert job.status == JobStatus.running

    report = cost_report(broker.store, 0, 100 * 60, broker.clock.now)
    assert report.total_cost == pytest.approx(broker.clock.now / 3600)
    assert report.teams["vision"].gpu_hours > 0


def test_minimum_billing_shortfall_is_booked_at_termination(tmp_path):
    broker = make_broker(tmp_path, "minimum.db")
    node = broker._launch_one("gpu-s", "vision")
    assert node is not None
    broker.clock.advance_to(300)
    broker._terminate_node(node)
    before_termination = cost_report(broker.store, 0, 300, 300)
    through_termination = cost_report(broker.store, 0, 301, 301)
    assert before_termination.total_cost == pytest.approx(300 / 3600)
    assert through_termination.total_cost == pytest.approx(600 / 3600)  # 10-min minimum
    # The node never ran a job, so nobody is charged for it.
    assert through_termination.unattributed_cost == pytest.approx(600 / 3600)
    assert through_termination.teams == {}


def provider_bill(broker):
    """What the provider charges, computed from nodes only (independent of the ledger)."""
    total = 0.0
    for node in broker.store.nodes():
        end = node.terminated_at if node.terminated_at is not None else broker.clock.now
        total += max(end - node.launched_at, 600) * NODE_PRICE_PER_HOUR[node.type] / 3600
    return total

