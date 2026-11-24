from pathlib import Path

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
    assert report.duplicate_rows == 1


def test_replay_trace_cost_invariant_and_output_shape(tmp_path):
    trace = Path("data/01a_trace.csv")
    db = tmp_path / "fair.db"

    result = replay_trace(trace, "fair", 42, db, idle_timeout_minutes=10)

    assert result["jobs_counted"] > 0
    assert result["peak_nodes"] >= 1
    assert result["cost"]["total_cost"] >= 0
    assert (
        round(sum(team["total_cost"] for team in result["cost"]["teams"].values())
              + result["cost"]["platform_cost"], 2)
        == result["cost"]["total_cost"]
    )
