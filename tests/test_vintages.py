import json

import httpx
import numpy as np
import pandas as pd
import pytest

import observatory.macro as macro


def trusted_fixture(panel):
    """An explicit test stand-in for the trusted HTTP acquisition path, never a CSV assertion."""
    panel.attrs.update(macro.API_VINTAGE_PROVENANCE)
    panel.attrs["provenance"] = "synthetic test fixture; not external-source evidence"
    return panel


def history_panel(n=160):
    dates = pd.date_range("2000-01-01", periods=n, freq="MS", tz="UTC")
    cpi = pd.DataFrame(
        {
            "series": "CPI",
            "period": dates,
            "value": 100 * 1.002 ** np.arange(n),
            "available_at": dates + pd.Timedelta(days=45),
        }
    )
    oil = pd.DataFrame(
        {
            "series": "OIL",
            "period": dates + pd.offsets.MonthEnd(0),
            "value": 40 + np.sin(np.arange(n)) * 4,
            "available_at": dates + pd.offsets.MonthEnd(0) + pd.Timedelta(days=5),
        }
    )
    return pd.concat([cpi, oil], ignore_index=True)


def test_latest_known_revision_is_selected_before_monthly_aggregation():
    panel = history_panel()
    origin = pd.Timestamp("2008-01-31", tz="UTC")
    revised = pd.DataFrame(
        [
            {
                "series": "OIL",
                "period": pd.Timestamp("2007-12-31", tz="UTC"),
                "available_at": pd.Timestamp("2008-01-10", tz="UTC"),
                "value": 80,
            },
            {
                "series": "OIL",
                "period": pd.Timestamp("2007-12-31", tz="UTC"),
                "available_at": pd.Timestamp("2008-02-10", tz="UTC"),
                "value": 8000,
            },
        ]
    )
    frame = pd.concat([panel, revised], ignore_index=True)
    features = macro.asof_features(frame, origin)
    previous = panel[
        (panel.series == "OIL") & (panel.period == pd.Timestamp("2007-11-30", tz="UTC"))
    ].value.iloc[0]
    assert features[3] == pytest.approx(100 * (80 - previous) / previous)
    snapshot = macro.asof_snapshot(frame, origin)
    assert not snapshot.duplicated(["series", "period"]).any()
    assert snapshot[snapshot.series == "OIL"].iloc[-1].value == 80


def test_first_release_target_uses_denominator_known_at_that_release():
    panel = pd.DataFrame(
        [
            {
                "series": "CPI",
                "period": "2000-01-01",
                "available_at": "2000-02-15T00:00:00Z",
                "value": 100,
            },
            {
                "series": "CPI",
                "period": "2000-01-01",
                "available_at": "2001-02-10T00:00:00Z",
                "value": 105,
            },
            {
                "series": "CPI",
                "period": "2001-01-01",
                "available_at": "2001-02-15T00:00:00Z",
                "value": 121,
            },
            {
                "series": "CPI",
                "period": "2000-01-01",
                "available_at": "2001-04-15T00:00:00Z",
                "value": 108,
            },
            {
                "series": "CPI",
                "period": "2001-01-01",
                "available_at": "2001-04-15T00:00:00Z",
                "value": 130,
            },
        ]
    )
    targets, _ = macro._first_release_targets(macro.normalize_panel(panel))
    target = targets[pd.Period("2001-01", freq="M")]
    assert target["target"] == pytest.approx(100 * (121 / 105 - 1))
    assert target["target_final"] == pytest.approx(100 * (130 / 108 - 1))
    assert target["target_available_at"] == pd.Timestamp("2001-02-15", tz="UTC")


def test_archive_backfill_is_not_claimed_as_original_release():
    panel = pd.DataFrame(
        [
            {
                "series": "CPI",
                "period": "2000-01-01",
                "available_at": "2005-01-01T00:00:00Z",
                "value": 100,
            },
        ]
    )
    normalized = macro.normalize_panel(panel)
    targets, excluded = macro._first_release_targets(normalized)
    assert not targets
    assert excluded["archive_censored"] == 1
    assert macro.coverage_diagnostics(normalized)["CPI"]["first_seen_over_120_days"] == 1


def test_missing_revision_withdraws_a_known_value():
    panel = pd.DataFrame(
        [
            {
                "series": "CPI",
                "period": "2000-01-01",
                "available_at": "2000-02-15T00:00:00Z",
                "value": "100",
            },
            {
                "series": "CPI",
                "period": "2000-01-01",
                "available_at": "2000-03-15T00:00:00Z",
                "value": ".",
            },
        ]
    )
    result = macro.asof_snapshot(panel, pd.Timestamp("2000-04-01", tz="UTC"))
    assert len(result) == 1
    assert pd.isna(result.iloc[0].value)


def test_naive_availability_and_conflicting_same_vintage_are_rejected():
    panel = pd.DataFrame(
        [
            {"series": "CPI", "period": "2000-01-01", "available_at": "2000-02-15", "value": 100},
        ]
    )
    with pytest.raises(ValueError, match="timezone"):
        macro.normalize_panel(panel)
    panel["available_at"] = "2000-02-15T00:00:00Z"
    duplicate = panel.copy()
    duplicate["value"] = 200
    with pytest.raises(ValueError, match="Conflicting"):
        macro.normalize_panel(pd.concat([panel, duplicate], ignore_index=True))


def test_overlapping_dataframe_indexes_do_not_duplicate_revision_events():
    panel = history_panel(20)
    revised = panel.iloc[[0]].copy()
    revised["available_at"] = pd.Timestamp("2002-01-01", tz="UTC")
    revised["value"] *= 1.01
    combined = pd.concat([panel, revised])
    assert not combined.index.is_unique
    result = macro.normalize_panel(combined)
    assert len(result) == len(panel) + 1
    assert not result.duplicated(["series", "period", "available_at"]).any()


def test_strict_vintage_matrix_is_paginated_and_compacted(monkeypatch):
    calls = []
    original_client = httpx.Client
    dates = ["2020-02-15", "2020-03-15", "2020-04-15"]
    values = {
        "2020-01-01": ["100", "101", "101"],
        "2020-02-01": [".", "102", "102"],
        "2020-03-01": [".", ".", "103"],
    }

    def handle(request):
        params = dict(request.url.params)
        calls.append((request.url.path, params))
        offset, limit = int(params["offset"]), int(params["limit"])
        if request.url.path.endswith("vintagedates"):
            return httpx.Response(
                200, json={"count": len(dates), "vintage_dates": dates[offset : offset + limit]}
            )
        assert params["output_type"] == "2"
        selected = params["vintage_dates"].split(",")
        rows = [
            {
                "date": period,
                **{
                    f"CPIAUCSL_{vintage.replace('-', '_')}": levels[dates.index(vintage)]
                    for vintage in selected
                },
            }
            for period, levels in values.items()
        ]
        return httpx.Response(
            200, json={"count": len(rows), "observations": rows[offset : offset + limit]}
        )

    monkeypatch.setattr(
        httpx, "Client", lambda **_: original_client(transport=httpx.MockTransport(handle))
    )
    result = macro.revision_response(
        "CPIAUCSL", "unit-test-placeholder", page_size=2, vintage_batch_size=2
    )
    assert result["output_type"] == 2
    assert result["vintage_dates_count"] == 3
    assert len(result["observations"]) == 4
    january = [r for r in result["observations"] if r["period"].startswith("2020-01")]
    assert [r["value"] for r in january] == [100, 101]
    observation_calls = [params for path, params in calls if path.endswith("observations")]
    assert [p["offset"] for p in observation_calls] == ["0", "2", "0", "2"]
    assert all(len(p["vintage_dates"].split(",")) <= 2 for p in observation_calls)


def test_incomplete_pagination_is_rejected(monkeypatch):
    original_client = httpx.Client

    def handle(request):
        if request.url.params["offset"] == "0":
            return httpx.Response(200, json={"count": 2, "vintage_dates": ["2020-01-01"]})
        return httpx.Response(200, json={"count": 2, "vintage_dates": []})

    monkeypatch.setattr(
        httpx, "Client", lambda **_: original_client(transport=httpx.MockTransport(handle))
    )
    with pytest.raises(RuntimeError, match="empty page"):
        macro.revision_response("CPIAUCSL", "unit-test-placeholder", page_size=1)


def test_explicit_offline_csv_does_not_require_key(monkeypatch, tmp_path):
    path = tmp_path / "vintages.csv"
    history_panel(20).to_csv(path, index=False)
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    monkeypatch.setenv("FRED_VINTAGE_CSV", str(path))
    result = macro.fetch_macro(strict=True)
    assert len(result) == 40
    assert "source audit" in result.attrs["provenance"]
    assert result.attrs["information_set_kind"] == "unverified_csv"
    assert result.attrs["strict_eligible"] is False


def test_unverified_csv_report_describes_supplied_availability_without_invented_lags(
    monkeypatch, tmp_path
):
    path = tmp_path / "custom-availability.csv"
    frame = history_panel(40)
    cpi = frame.series.eq("CPI")
    frame.loc[cpi, "available_at"] = frame.loc[cpi, "period"] + pd.Timedelta(days=17)
    frame.loc[~cpi, "available_at"] = frame.loc[~cpi, "period"] + pd.Timedelta(days=9)
    frame.to_csv(path, index=False)
    loaded = macro.load_vintage_panel(path)
    monkeypatch.setattr(macro, "ARTIFACTS", tmp_path)
    report = macro.evaluate(loaded, strict=False, min_train=120, max_origins=1)
    cpi_lags = (
        loaded.loc[loaded.series.eq("CPI"), "available_at"]
        - loaded.loc[loaded.series.eq("CPI"), "period"]
    ).dt.days
    assert cpi_lags.eq(17).all()
    assert report["evaluation_type"] == "exploratory_unverified_csv"
    assert "user-supplied availability timestamps" in report["information_policy"]
    assert "45" not in report["information_policy"]
    assert "oil=5" not in report["information_policy"]
    assert "not genuine first-release evidence" in report["target_policy"]
    assert report["provenance"]["strict_eligible"] is False
    assert not (tmp_path / "macro-strict.json").exists()


def test_revised_snapshot_policy_labels_remain_unchanged(monkeypatch, tmp_path):
    panel = history_panel(40)
    panel.attrs["information_set_kind"] = "revised_snapshot_with_assumed_lags"
    monkeypatch.setattr(macro, "ARTIFACTS", tmp_path)
    report = macro.evaluate(panel, strict=False, min_train=120, max_origins=1)
    assert report["evaluation_type"] == "exploratory_revised_data"
    assert "CPI=45 calendar days" in report["information_policy"]
    assert "oil=5 days" in report["information_policy"]
    assert "Current revised CPI ratio" in report["target_policy"]


def test_unverified_csv_cannot_create_strict_evidence(monkeypatch, tmp_path):
    path = tmp_path / "revised.csv"
    frame = history_panel(40)
    # CSV columns named like provenance fields must not authenticate their own contents.
    frame["strict_eligible"] = True
    frame["information_set_kind"] = "alfred_revision_events"
    frame.to_csv(path, index=False)
    loaded = macro.load_vintage_panel(path)
    monkeypatch.setattr(macro, "ARTIFACTS", tmp_path)

    def forbidden(*_):
        raise AssertionError("An unverified panel must be rejected before fitting")

    monkeypatch.setattr(macro, "_estimates", forbidden)
    with pytest.raises(ValueError, match="cannot be upgraded by --strict"):
        macro.evaluate(loaded, strict=True, min_train=3, max_origins=1)
    assert not (tmp_path / "macro-strict.json").exists()


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {
            "information_set_kind": "alfred_revision_events",
            "strict_eligible": True,
            "availability_semantics": "assumed_lags",
        },
        {
            "information_set_kind": "revised_snapshot_with_assumed_lags",
            "strict_eligible": True,
            "availability_semantics": macro.VINTAGE_AVAILABILITY,
        },
    ],
)
def test_strict_provenance_requires_kind_flag_and_availability_policy(
    metadata, monkeypatch, tmp_path
):
    panel = history_panel(40)
    panel.attrs.update(metadata)
    monkeypatch.setattr(macro, "ARTIFACTS", tmp_path)
    with pytest.raises(ValueError, match="acquisition provenance"):
        macro.evaluate(panel, strict=True, min_train=3, max_origins=1)
    assert not (tmp_path / "macro-strict.json").exists()


def test_http_vintage_acquisition_marks_the_verified_input_path(monkeypatch, tmp_path):
    original_client = httpx.Client

    def handle(request):
        if request.url.path.endswith("vintagedates"):
            return httpx.Response(200, json={"count": 1, "vintage_dates": ["2020-03-15"]})
        fred_id = request.url.params["series_id"]
        period = "2020-01-01" if fred_id == "CPIAUCSL" else "2020-01-31"
        return httpx.Response(
            200,
            json={
                "count": 1,
                "observations": [
                    {"date": period, f"{fred_id}_2020_03_15": "100"},
                ],
            },
        )

    monkeypatch.setattr(
        httpx, "Client", lambda **_: original_client(transport=httpx.MockTransport(handle))
    )
    monkeypatch.delenv("FRED_VINTAGE_CSV", raising=False)
    monkeypatch.setenv("FRED_API_KEY", "unit-test-placeholder")
    monkeypatch.setattr(macro, "DATA", tmp_path)
    panel = macro.fetch_macro(strict=True)
    assert panel.attrs["information_set_kind"] == "alfred_revision_events"
    assert panel.attrs["strict_eligible"] is True
    assert panel.attrs["availability_semantics"] == macro.VINTAGE_AVAILABILITY
    assert "independent audit" in panel.attrs["source_audit_status"]


def test_block_intervals_preserve_paired_difference_and_reproduce():
    result = macro.block_loss_interval(np.ones(96) * -0.25, horizon=3)
    assert result["lower_95"] == pytest.approx(-0.25)
    assert result["upper_95"] == pytest.approx(-0.25)
    assert result["block_length"] >= 4
    assert result == macro.block_loss_interval(np.ones(96) * -0.25, horizon=3)
    assert macro.block_loss_interval([1, 2], horizon=1)["status"] == "insufficient_origins"


def test_prediction_interval_ignores_unreleased_and_future_residuals():
    origin = pd.Timestamp("2020-01-31", tz="UTC")
    history = [
        {
            "model": "AR ridge",
            "origin": origin - pd.Timedelta(days=100 + i),
            "target_available_at": origin - pd.Timedelta(days=30 + i),
            "forecast": 2.0,
            "actual": 1.0,
        }
        for i in range(30)
    ]
    history += [
        {
            "model": "AR ridge",
            "origin": origin - pd.Timedelta(days=5),
            "target_available_at": origin + pd.Timedelta(days=30),
            "forecast": 10000.0,
            "actual": -10000.0,
        },
        {
            "model": "AR ridge",
            "origin": origin + pd.Timedelta(days=1),
            "target_available_at": origin - pd.Timedelta(days=1),
            "forecast": 10000.0,
            "actual": -10000.0,
        },
    ]
    result = macro.past_residual_interval(history, "AR ridge", origin, 3.0)
    assert result["lower_90"] == 2
    assert result["upper_90"] == 4
    assert result["interval_calibration_n"] == 30


def test_prediction_interval_uses_the_declared_discrete_residual_rank():
    origin = pd.Timestamp("2020-01-31", tz="UTC")
    history = [
        {
            "model": "AR ridge",
            "origin": origin - pd.Timedelta(days=100 + i),
            "target_available_at": origin - pd.Timedelta(days=30 + i),
            "forecast": float(i + 1),
            "actual": 0.0,
        }
        for i in range(30)
    ]
    # The conservative 90% rank among 30 observations is ceil(31 * .9) = 28.
    result = macro.past_residual_interval(history, "AR ridge", origin, 0.0)
    assert result["lower_90"] == -28
    assert result["upper_90"] == 28


def _fast_estimates(x, y, current):
    return {
        "Persistence": float(current[0, 0]),
        **{name: float(y.mean()) for name in macro.MODELS if name != "Persistence"},
    }


def test_final_revisions_never_enter_training_labels(monkeypatch, tmp_path):
    monkeypatch.setattr(macro, "ARTIFACTS", tmp_path)
    monkeypatch.setattr(macro, "_estimates", _fast_estimates)
    panel = trusted_fixture(history_panel())
    baseline = macro.evaluate(panel, strict=True, min_train=24, max_origins=4)
    revised = panel[panel.series == "CPI"].copy()
    revised["value"] *= 1 + 0.1 * np.sin(np.arange(len(revised)))
    revised["available_at"] = pd.Timestamp("2020-01-01", tz="UTC")
    result = macro.evaluate(
        trusted_fixture(pd.concat([panel, revised], ignore_index=True)),
        strict=True,
        min_train=24,
        max_origins=4,
    )
    assert [r["forecast"] for r in result["predictions"]] == [
        r["forecast"] for r in baseline["predictions"]
    ]
    assert [r["actual"] for r in result["predictions"]] == [
        r["actual"] for r in baseline["predictions"]
    ]
    assert (
        result["retrospective_final_vintage_metrics"]
        != baseline["retrospective_final_vintage_metrics"]
    )
    assert json.loads((tmp_path / "macro-strict.json").read_text())["method_version"] == "macro-v2"


def test_future_targets_cannot_change_a_forecast_or_its_interval(monkeypatch, tmp_path):
    monkeypatch.setattr(macro, "ARTIFACTS", tmp_path)
    monkeypatch.setattr(macro, "_estimates", _fast_estimates)
    panel = trusted_fixture(history_panel())
    baseline = macro.evaluate(panel, strict=True, min_train=24, max_origins=4)
    origin = min(row["origin"] for row in baseline["predictions"])
    changed = panel.copy()
    changed.loc[
        (changed.series == "CPI") & (changed.available_at > pd.Timestamp(origin)), "value"
    ] *= 4
    result = macro.evaluate(changed, strict=True, min_train=24, max_origins=4)
    original = [r for r in baseline["predictions"] if r["origin"] == origin]
    actual = [r for r in result["predictions"] if r["origin"] == origin]
    assert [(r["forecast"], r["lower_90"], r["upper_90"]) for r in actual] == [
        (r["forecast"], r["lower_90"], r["upper_90"]) for r in original
    ]
    for row in result["predictions"]:
        assert pd.Timestamp(row["latest_input_available_at"]) <= pd.Timestamp(row["origin"])
        if row["interval_calibration_last_available_at"]:
            assert pd.Timestamp(row["interval_calibration_last_available_at"]) <= pd.Timestamp(
                row["origin"]
            )
