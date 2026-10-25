import numpy as np
import pandas as pd

from observatory.signals import current_snapshot, score_history


def history(n=150, frequency="daily"):
    rng = np.random.default_rng(53)
    return pd.DataFrame(
        {
            "mode": "REPLAY",
            "instrument": "WTI",
            "frequency": frequency,
            "event_time": pd.date_range("2020-01-01", periods=n, tz="UTC"),
            "observed_at": pd.Timestamp("2026-01-01", tz="UTC"),
            "revision": "v1",
            "value": 70 + rng.normal(0, 0.3, n).cumsum(),
        }
    )


def test_signal_is_prefix_causal():
    frame = history()
    prefix = score_history(frame.iloc[:100])
    frame.loc[100:, "value"] += 10000
    result = score_history(frame).iloc[:100]
    pd.testing.assert_series_equal(prefix.robust_z, result.robust_z)
    pd.testing.assert_series_equal(prefix.robust_alert, result.robust_alert)


def test_large_up_and_down_moves_flagged():
    frame = history()
    frame.loc[80:, "value"] += 30
    frame.loc[120:, "value"] -= 60
    scored = score_history(frame)
    assert scored.loc[80, "robust_alert"]
    assert scored.loc[120, "robust_alert"]
    assert scored.loc[80, "direction"] == "UP"
    assert scored.loc[120, "direction"] == "DOWN"


def test_zero_crossing_is_finite_and_zero_denominator_is_null():
    frame = history()
    frame.loc[80:83, "value"] = [10, -37.63, 0, 12]
    scored = score_history(frame)
    assert scored.loc[81, "change_pct"] == -476.3
    assert np.isnan(scored.loc[83, "change_pct"])
    assert not np.isinf(scored.robust_z).any()


def test_modes_do_not_contaminate_signals():
    frame = history()
    fake = frame.copy()
    fake["mode"] = "SYNTHETIC"
    fake["value"] *= 9000
    expected = score_history(frame)
    actual = score_history(pd.concat([frame, fake]))
    pd.testing.assert_series_equal(
        expected.robust_z.reset_index(drop=True),
        actual[actual["mode"] == "REPLAY"].robust_z.reset_index(drop=True),
    )


def test_warmup_and_monthly_window():
    daily = score_history(history())
    monthly = score_history(history(frequency="monthly"))
    assert daily.robust_z.first_valid_index() == 21
    assert monthly.robust_z.first_valid_index() == 13


def test_revision_uses_observed_time_not_hash_order():
    frame = history(2)
    older = frame.iloc[[0]].copy()
    older["revision"] = "zzzz"
    older["observed_at"] = pd.Timestamp("2025-01-01", tz="UTC")
    older["value"] = 100000
    snapshot = current_snapshot(pd.concat([older, frame]))
    assert len(snapshot) == 2
    assert snapshot.value.max() < 100
