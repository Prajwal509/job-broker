import sqlite3

import pytest

from broker.billing import Billing
from broker.broker import Broker, BrokerConfig
from broker.models import JobStatus, NodeState
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
        "provider_rng": broker.provider.rng_state(),
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

    report = Billing(broker.store).report(0, broker.clock.now + 1, broker.clock.now)
    team = report.teams["vision"]
    assert team.gpu_hours == pytest.approx(5 / 60)
    assert team.direct_cost == pytest.approx(0.08)
    assert team.idle_cost > 0
    assert team.total_cost + report.platform_cost == report.total_cost
    assert report.idle_percent == pytest.approx(
        report.idle_cost / report.total_cost * 100
    )

    running_window = Billing(broker.store).report(
        started_at, job.finished_at, broker.clock.now
    )
    assert running_window.teams["vision"].total_cost == pytest.approx(0.08)


def test_live_node_cost_accrues_through_report_time(tmp_path):
    broker = make_broker(tmp_path, "live-billing.db")
    job = broker.submit("vision", 1, 120, "normal", "job", actual_minutes=90)
    broker.advance_to(8 * 60)
    assert job.status == JobStatus.running

    report = Billing(broker.store).report(0, 100 * 60, broker.clock.now)
    assert report.total_cost == pytest.approx(round(broker.clock.now / 3600, 2))
    assert report.teams["vision"].gpu_hours > 0


def test_minimum_billing_shortfall_is_booked_at_termination(tmp_path):
    broker = make_broker(tmp_path, "minimum.db")
    node = broker._launch_one("gpu-s", 0, "vision")
    assert node is not None
    broker._terminate_node(node, 300)
    billing = Billing(broker.store)

    before_termination = billing.report(0, 300, 300)
    through_termination = billing.report(0, 301, 301)
    assert before_termination.total_cost == pytest.approx(0.08)
    assert through_termination.total_cost == pytest.approx(0.17)
    assert through_termination.platform_cost == pytest.approx(0.17)
    assert through_termination.teams == {}


def test_store_migrates_phase1_ledger_schema(tmp_path):
    database = str(tmp_path / "legacy.db")
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE nodes (
          id TEXT PRIMARY KEY,
          type TEXT NOT NULL,
          state TEXT NOT NULL,
          launched_at INTEGER NOT NULL,
          ready_at INTEGER,
          terminated_at INTEGER,
          tenant TEXT,
          failure_at INTEGER,
          idle_since INTEGER
        );
        CREATE TABLE ledger (
          node_id TEXT NOT NULL,
          start INTEGER NOT NULL,
          end INTEGER NOT NULL,
          tenant TEXT,
          gpus_used INTEGER NOT NULL
        );
        """
    )
    connection.close()

    store = Store(database)
    columns = {
        row["name"]: row for row in store.conn.execute("PRAGMA table_info(ledger)")
    }
    assert columns["id"]["pk"] == 1
    assert columns["end"]["notnull"] == 0
    assert store.get_meta("schema_version") == Store.SCHEMA_VERSION
