from pathlib import Path

from broker.broker import Broker, BrokerConfig
from broker.store import Store


def test_daily_budget_stops_new_launches(tmp_path):
    config = BrokerConfig(
        db_path=str(tmp_path / "budget.db"),
        max_nodes=10,
        daily_budget_usd=0.0,
        seed=42,
    )
    broker = Broker(store=Store(config.db_path), config=config)
    broker.submit("vision", 8, 30, "normal", "budget-job")

    assert broker._alive_node_count() == 0


def test_delivery_artifacts_exist():
    root = Path(__file__).resolve().parents[1]
    for relative in (
        "Dockerfile",
        "docker-compose.yml",
        "Makefile",
        ".env.example",
        ".github/workflows/ci.yml",
        "docs/DEPLOY.md",
        "docs/RUNBOOK.md",
    ):
        assert (root / relative).is_file()
