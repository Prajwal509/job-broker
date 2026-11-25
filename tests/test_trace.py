from pathlib import Path

import pytest

from broker.trace import clean_trace, replay_trace


def test_clean_trace_normalizes_sorts_and_deduplicates(tmp_path):
    trace = tmp_path / "trace.csv"
    trace.write_text(
        "submitted_at,team,gpus,estimated_minutes,actual_minutes,priority,outcome\n"
        "2025-03-03T06:00:00+01:00,vision,1,10,20,normal,succeeded\n"
        "2025-03-03T05:00:00Z,speech,3,10,20,normal,succeeded\n"
        "2025-03-03T05:00:00Z,speech,3,10,20,normal,succeeded\n"
        "2025-03-03T04:00:00Z,vision,1,10,20,normal,succeeded\n"
    )

    report = clean_trace(trace)

    assert [row["submitted_at"] for row in report.rows] == [
        "2025-03-03T04:00:00Z",
        "2025-03-03T05:00:00Z",
    ]
    assert report.duplicate_rows == 1
    assert len(report.rejected_gpus) == 1


def test_replay_is_deterministic_and_costs_add_up(tmp_path):
    cleaning = clean_trace(Path("data/01a_trace.csv"))

    first = replay_trace(cleaning, "fair", 42, tmp_path / "a.db")
    second = replay_trace(cleaning, "fair", 42, tmp_path / "b.db")

    assert first == second
    assert first["jobs"] == len(cleaning.rows)
    assert 1 <= first["peak_nodes"] <= 10
    cost = first["cost"]
    teams_total = sum(team["total_cost"] for team in cost["teams"].values())
    # Each figure is rounded to cents on its own, so allow one cent per figure.
    assert teams_total + cost["unattributed_cost"] == pytest.approx(
        cost["total_cost"], abs=0.01 * (len(cost["teams"]) + 1)
    )
