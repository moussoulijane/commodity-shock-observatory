from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from observatory import validation_v2 as target
from observatory.contracts import make_quote
from observatory.signals import current_snapshot, score_history
from observatory.store import records


@pytest.fixture
def multimode_parity(monkeypatch):
    start = datetime(2024, 1, 1, tzinfo=UTC)
    observed = datetime(2025, 1, 1, tzinfo=UTC)
    quotes = []
    for index in range(25):
        live = make_quote(
            "XAUUSD",
            start + timedelta(seconds=index),
            2400 + index * 0.25,
            "live-capture",
            observed,
            "LIVE",
        ).model_dump(mode="json")
        # Replaying a capture preserves its identity while changing its mode.
        quotes.extend([live, {**live, "mode": "REPLAY"}])
    history = pd.DataFrame(quotes)
    for field in ("event_time", "observed_at", "ingested_at"):
        history[field] = pd.to_datetime(history[field], utc=True, format="ISO8601")
    served = records(score_history(current_snapshot(history)))
    reports = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, *_):
            return self

        def fetchall(self):
            return [{"payload": row} for row in served]

    monkeypatch.setattr(target, "connect", Connection)
    monkeypatch.setattr(target, "observation_frame", lambda _: history.copy())
    monkeypatch.setattr(target, "save_report", lambda _, report: reports.append(report))
    return served, reports


def test_verify_preserves_live_and_replay_with_the_same_event_ids(multimode_parity):
    served, _ = multimode_parity
    assert len({row["event_id"] for row in served}) == 25
    report = target.verify()
    assert report["status"] == "passed"
    assert report["expected_observations"] == report["serving_observations"] == 50
    assert report["missing"] == report["feature_mismatches"] == 0


def test_verify_detects_feature_mismatch_in_only_one_mode(multimode_parity):
    served, reports = multimode_parity
    replay = next(row for row in reversed(served) if row["mode"] == "REPLAY")
    replay["robust_z"] += 1
    with pytest.raises(AssertionError, match="V2 parity failed"):
        target.verify()
    report = reports[-1]
    assert report["missing"] == 0
    assert report["feature_mismatches"] == 1
    assert report["examples"][0]["mode"] == "REPLAY"
    assert report["examples"][0]["field"] == "robust_z"


def test_verify_does_not_substitute_live_for_a_missing_replay_row(multimode_parity):
    served, reports = multimode_parity
    served.pop(next(index for index, row in enumerate(served) if row["mode"] == "REPLAY"))
    with pytest.raises(AssertionError, match="V2 parity failed"):
        target.verify()
    assert reports[-1]["missing"] == 1
    assert reports[-1]["feature_mismatches"] == 0
