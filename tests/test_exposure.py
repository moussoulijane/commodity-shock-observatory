from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from observatory.exposure import PROFILES, ScenarioRequest, calculate_scenario


def reference(value=100, unit="USD/barrel", kind="spot", mode="REPLAY"):
    return {
        "value": value,
        "unit": unit,
        "kind": kind,
        "mode": mode,
        "event_time": "2020-01-01T00:00:00Z",
        "observed_at": "2026-01-01T00:00:00Z",
        "provider": "test reference",
    }


def test_decomposition_reconciles():
    result = calculate_scenario(
        ScenarioRequest(profile_id="energy-importer"), {"BRENT": reference()}
    )
    totals = result["totals"]
    assert totals["delta_cost_mad"] == pytest.approx(
        sum(totals[k] for k in ("commodity_effect_mad", "fx_effect_mad", "interaction_effect_mad"))
    )
    assert totals["stressed_cost_mad"] == pytest.approx(
        totals["baseline_cost_mad"] + totals["delta_cost_mad"]
    )


def test_ten_percent_price_and_three_percent_fx_is_13_3_without_basis(monkeypatch):
    monkeypatch.setitem(PROFILES[0]["lines"][0], "fixed_basis_usd_per_unit", 0)
    result = calculate_scenario(
        ScenarioRequest(profile_id="energy-importer"), {"BRENT": reference()}
    )
    assert result["delta_pct"] == pytest.approx(13.3)


def test_full_hedges_remove_scenario_change(monkeypatch):
    line = PROFILES[0]["lines"][0]
    monkeypatch.setitem(line, "commodity_hedged_fraction", 1)
    monkeypatch.setitem(line, "fx_hedged_fraction", 1)
    result = calculate_scenario(
        ScenarioRequest(profile_id="energy-importer"), {"BRENT": reference()}
    )
    assert result["totals"]["delta_cost_mad"] == 0


def test_baseline_notional_fx_hedge_keeps_incremental_dollar_exposure(monkeypatch):
    line = PROFILES[0]["lines"][0]
    monkeypatch.setitem(line, "quantity", 1)
    monkeypatch.setitem(line, "fixed_basis_usd_per_unit", 0)
    monkeypatch.setitem(line, "commodity_hedged_fraction", 0)
    monkeypatch.setitem(line, "fx_hedged_fraction", 1)
    request = ScenarioRequest(profile_id="energy-importer", commodity_shock_pct=10, fx_shock_pct=10)
    result = calculate_scenario(request, {"BRENT": reference()})
    assert result["totals"]["stressed_cost_mad"] == pytest.approx(1110)
    assert result["totals"]["interaction_effect_mad"] == pytest.approx(10)


def test_future_reference_rejected_without_asof():
    ref = reference()
    ref["event_time"] = "2099-01-01T00:00:00Z"
    ref["observed_at"] = "2099-01-02T00:00:00Z"
    with pytest.raises(ValueError, match="after"):
        calculate_scenario(ScenarioRequest(profile_id="energy-importer"), {"BRENT": ref})


@pytest.mark.parametrize(
    "overrides", [{"value": -1}, {"unit": "EUR/barrel"}, {"kind": "futures"}, {"mode": "SYNTHETIC"}]
)
def test_invalid_benchmarks_rejected(overrides):
    ref = reference()
    ref.update(overrides)
    with pytest.raises(ValueError):
        calculate_scenario(ScenarioRequest(profile_id="energy-importer"), {"BRENT": ref})


def test_future_reference_rejected():
    with pytest.raises(ValueError, match="after"):
        calculate_scenario(
            ScenarioRequest(profile_id="energy-importer", as_of=datetime(2019, 1, 1, tzinfo=UTC)),
            {"BRENT": reference()},
        )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), 0, -1])
def test_invalid_fx_rejected(value):
    with pytest.raises(ValidationError):
        ScenarioRequest(profile_id="energy-importer", fx_reference_mad_per_usd=value)
