import httpx
import numpy as np
import pandas as pd
import pytest

from observatory.macro import asof_features, build_samples, fetch_macro, initial_release_response


def panel():
    dates = pd.date_range("2000-01-01", periods=160, freq="MS", tz="UTC")
    cpi = pd.DataFrame(
        {
            "series": "CPI",
            "period": dates,
            "value": 100 * 1.002 ** np.arange(160),
            "available_at": dates + pd.Timedelta(days=45),
        }
    )
    oil = pd.DataFrame(
        {
            "series": "OIL",
            "period": dates + pd.offsets.MonthEnd(0),
            "value": 40 + np.sin(np.arange(160)) * 4,
            "available_at": dates + pd.offsets.MonthEnd(0) + pd.Timedelta(days=5),
        }
    )
    return pd.concat([cpi, oil], ignore_index=True)


def test_future_release_cannot_change_features():
    frame = panel()
    origin = pd.Timestamp("2008-01-31", tz="UTC")
    expected = asof_features(frame, origin)
    frame.loc[frame.available_at > origin, "value"] *= 500
    np.testing.assert_array_equal(expected, asof_features(frame, origin))


def test_target_is_future_and_unavailable_at_origin():
    samples = build_samples(panel(), 3)
    assert len(samples) > 100
    for sample in samples:
        assert sample["target_available_at"] > sample["origin"]
        assert (
            pd.Period(sample["target_month"], freq="M")
            == sample["origin"].tz_localize(None).to_period("M") + 3
        )


def test_strict_requires_key_without_fallback(monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="no silent fallback"):
        fetch_macro(strict=True)


def test_key_is_not_exposed_on_network_failure(monkeypatch):
    original = httpx.Client

    def fail(request):
        raise httpx.ConnectError(str(request.url))

    monkeypatch.setattr(httpx, "Client", lambda **_: original(transport=httpx.MockTransport(fail)))
    with pytest.raises(RuntimeError) as error:
        initial_release_response("CPIAUCSL", "unit-test-placeholder")
    assert "unit-test-placeholder" not in str(error.value)
    assert error.value.__suppress_context__
