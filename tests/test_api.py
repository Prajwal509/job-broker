import io
import json
import logging

from fastapi.testclient import TestClient

from api import create_app
from broker.broker import Broker, BrokerConfig
from broker.provider import ProviderLaunch
from broker.store import Store


def make_client(tmp_path):
    config = BrokerConfig(
        db_path=str(tmp_path / "api.db"),
        idle_timeout_minutes=10,
        max_nodes=2,
        seed=1,
        fairness_policy="fair",
        admin_token="test-token",
    )
    broker = Broker(store=Store(config.db_path), config=config)
    return TestClient(create_app(broker)), broker


def job_body(**updates):
    body = {
        "team": "vision",
        "gpus": 1,
        "estimated_minutes": 30,
        "priority": "normal",
        "idempotency_key": "client-1",
    }
    return body | updates


def test_job_api_validation_idempotency_and_conflict(tmp_path):
    client, _broker = make_client(tmp_path)

    invalid = client.post("/jobs", json=job_body(gpus=True))
    assert invalid.status_code == 422
    assert invalid.json()["error"] == "validation_error"

    created = client.post("/jobs", json=job_body())
    duplicate = client.post("/jobs", json=job_body())
    conflict = client.post("/jobs", json=job_body(gpus=2))
    assert created.status_code == 201
    assert duplicate.status_code == 200
    assert duplicate.json()["id"] == created.json()["id"]
    assert conflict.status_code == 409


def test_job_lookup_cancel_and_queue(tmp_path):
    client, _broker = make_client(tmp_path)
    job = client.post("/jobs", json=job_body()).json()

    assert client.get(f"/jobs/{job['id']}").status_code == 200
    assert client.get("/jobs/missing").status_code == 404
    assert [item["id"] for item in client.get("/queue").json()] == [job["id"]]
    assert client.post(f"/jobs/{job['id']}/cancel").json()["status"] == "cancelled"
    assert client.post(f"/jobs/{job['id']}/cancel").status_code == 409


def test_nodes_cost_admin_and_metrics(tmp_path):
    client, broker = make_client(tmp_path)
    job = client.post("/jobs", json=job_body()).json()

    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 200
    assert len(client.get("/nodes").json()) == 1
    assert client.post("/admin/advance", json={"minutes": 8}).status_code == 401
    advanced = client.post(
        "/admin/advance",
        json={"minutes": 8},
        headers={"X-Admin-Token": "test-token"},
    )
    assert advanced.status_code == 200
    assert advanced.json()["sim_time"] == 8 * 60
    assert client.get(f"/jobs/{job['id']}").json()["status"] == "running"

    cost = client.get(
        "/cost",
        params={
            "from": "1970-01-01T00:00:00Z",
            "to": "1970-01-01T01:00:00+00:00",
        },
    )
    assert cost.status_code == 200
    assert cost.json()["teams"]["vision"]["total_cost"] > 0

    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    assert "queue_depth{priority=\"high\"}" in metrics.text
    assert "spend_total_usd " in metrics.text
    assert "oldest_queued_job_age_seconds " in metrics.text
    assert broker.clock.now == 8 * 60


def test_cost_requires_utc_and_admin_requires_one_advance_field(tmp_path):
    client, _broker = make_client(tmp_path)
    invalid_time = client.get(
        "/cost",
        params={"from": "2025-01-01T00:00:00", "to": "2025-01-02T00:00:00Z"},
    )
    assert invalid_time.status_code == 422

    both_fields = client.post(
        "/admin/advance",
        json={"minutes": 1, "to": 60},
        headers={"X-Admin-Token": "test-token"},
    )
    assert both_fields.status_code == 422


def test_event_log_lines_are_structured_json(tmp_path):
    client, _broker = make_client(tmp_path)
    logger = logging.getLogger("job_broker.events")
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    try:
        response = client.post("/jobs", json=job_body())
    finally:
        logger.removeHandler(handler)
    assert response.status_code == 201

    lines = [line for line in stream.getvalue().splitlines() if line]
    events = [json.loads(line) for line in lines]
    assert events
    assert all(
        set(event) == {"time", "job_id", "node_id", "event"} for event in events
    )
    assert any(event["event"] == "job_submitted" for event in events)


def test_metrics_count_never_ready_launches(tmp_path, monkeypatch):
    client, broker = make_client(tmp_path)

    monkeypatch.setattr(
        broker.provider,
        "launch_details",
        lambda node_id, _node_type, now: ProviderLaunch(node_id, None, now + 900),
    )
    client.post("/jobs", json=job_body())
    advanced = client.post(
        "/admin/advance",
        json={"minutes": 15},
        headers={"X-Admin-Token": "test-token"},
    )

    assert advanced.status_code == 200
    assert "launch_failures_total 1\n" in client.get("/metrics").text
