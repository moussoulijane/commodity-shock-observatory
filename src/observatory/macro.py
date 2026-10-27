"""Point-in-time commodity / inflation research with explicit vintage and target policies.

Strict mode reconstructs the latest revision known at each forecast origin. The
target is first-release YoY CPI, using the denominator known at that release.
Exploratory mode retains current-snapshot revision leakage and says so.
"""

import hashlib
import io
import json
import math
import os
import re
from datetime import UTC, datetime
from pathlib import Path

import httpx
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

from observatory.config import ARTIFACTS, DATA
from observatory.sources import download
from observatory.store import save_report

SERIES = {"CPI": "CPIAUCSL", "OIL": "DCOILBRENTEU"}
FRED_BASE = "https://api.stlouisfed.org/fred/"
MAX_FIRST_RELEASE_LAG_DAYS = 120
MODELS = ("Persistence", "AR ridge", "AR + oil ridge", "XGBoost")
VINTAGE_AVAILABILITY = "next_utc_midnight_after_vintage_date"
API_VINTAGE_PROVENANCE = {
    "information_set_kind": "alfred_revision_events",
    "strict_eligible": True,
    "availability_semantics": VINTAGE_AVAILABILITY,
    "source_audit_status": "API vintage acquisition path; archival release coverage requires independent audit",
}
TARGET_POLICY = (
    "First finite CPI level for target month divided by the latest year-earlier "
    "CPI level known at that target release; frozen thereafter for training. "
    "First-seen lags above 120 days are treated as archive-censored, not original releases."
)


def _fred_json(client, endpoint, key, params):
    try:
        response = client.get(
            FRED_BASE + endpoint, params={"api_key": key, "file_type": "json", **params}
        )
        if response.status_code != 200:
            raise RuntimeError(f"FRED API request rejected ({response.status_code}); key omitted")
        body = response.json()
    except (httpx.HTTPError, ValueError):
        raise RuntimeError("FRED network/JSON request failed; credentials omitted") from None
    if not isinstance(body, dict) or "error_code" in body:
        raise RuntimeError("FRED returned an invalid response; credentials omitted")
    return body


def initial_release_response(series, key):
    """Legacy helper retained for compatibility; strict V2 uses all vintages instead."""
    with httpx.Client(timeout=120) as client:
        return _fred_json(
            client,
            "series/observations",
            key,
            {
                "series_id": series,
                "output_type": 4,
                "realtime_start": "1776-07-04",
                "realtime_end": datetime.now(UTC).date().isoformat(),
                "observation_start": "1990-01-01",
                "limit": 100000,
            },
        )


def _paged(client, endpoint, key, params, field, page_size):
    offset, expected_count = 0, None
    while True:
        body = _fred_json(client, endpoint, key, {**params, "offset": offset, "limit": page_size})
        rows = body.get(field)
        count = body.get("count")
        if not isinstance(rows, list) or not isinstance(count, int) or count < 0:
            raise RuntimeError("FRED pagination metadata is missing or invalid")
        if expected_count is not None and count != expected_count:
            raise RuntimeError(
                "FRED result count changed during pagination; retry a fixed snapshot"
            )
        expected_count = count
        if not rows and offset < count:
            raise RuntimeError("FRED returned an empty page before the declared end")
        yield rows
        offset += len(rows)
        if offset >= count:
            break


def _matrix_events(rows, series):
    """Decode both output_type=2 vintage matrices and scalar interval-style JSON."""
    prefix = "_" + series if series[0].isdigit() else series
    pattern = re.compile(rf"^{re.escape(prefix)}[_-](\d{{4}})[_-]?(\d{{2}})[_-]?(\d{{2}})$")
    events = []
    for row in rows:
        if not isinstance(row, dict) or "date" not in row:
            raise RuntimeError("Unrecognised FRED observation row")
        if "value" in row and "realtime_start" in row:
            events.append(
                {
                    "period": row["date"],
                    "value": row["value"],
                    "vintage_date": row["realtime_start"],
                }
            )
            continue
        matched = False
        for name, value in row.items():
            match = pattern.match(name)
            if match:
                matched = True
                vintage = "-".join(match.groups())
                events.append({"period": row["date"], "value": value, "vintage_date": vintage})
        if not matched and len(row) > 1:
            raise RuntimeError("Unrecognised FRED output_type=2 vintage columns")
    return events


def normalize_panel(panel):
    """Validate and compact a revision-event panel; missing revisions are withdrawals."""
    required = {"series", "period", "available_at", "value"}
    if not required.issubset(panel.columns):
        raise ValueError(f"Vintage panel requires columns: {sorted(required)}")
    result = panel[["series", "period", "available_at", "value"]].copy().reset_index(drop=True)
    if not set(result.series).issubset(SERIES):
        raise ValueError("Vintage panel supports CPI and OIL only")
    result["period"] = pd.to_datetime(result.period, utc=True, format="mixed").dt.normalize()
    cpi = result.series.eq("CPI")
    result.loc[cpi, "period"] = (
        result.loc[cpi, "period"]
        .dt.tz_localize(None)
        .dt.to_period("M")
        .dt.to_timestamp()
        .dt.tz_localize("UTC")
    )
    if isinstance(result.available_at.dtype, pd.DatetimeTZDtype):
        result["available_at"] = result.available_at.dt.tz_convert("UTC")
    else:
        dates = [pd.Timestamp(value) for value in result.available_at]
        if any(pd.isna(value) or value.tzinfo is None for value in dates):
            raise ValueError("available_at requires explicit timezone-aware timestamps")
        result["available_at"] = pd.to_datetime(dates, utc=True)
    result["value"] = pd.to_numeric(
        result.value.replace({"": np.nan, ".": np.nan, "NA": np.nan, "nan": np.nan}), errors="raise"
    ).astype(float)
    if result[["period", "available_at"]].isna().any().any() or np.isinf(result.value).any():
        raise ValueError("Invalid dates or infinite values in vintage panel")
    finite = result.value.notna()
    if (finite & (result.available_at < result.period)).any():
        raise ValueError("A finite observation cannot be available before its economic period")
    if (cpi & finite & result.value.le(0)).any():
        raise ValueError("CPI levels must be positive")
    keys = ["series", "period", "available_at"]
    if result.groupby(keys).value.nunique(dropna=False).gt(1).any():
        raise ValueError("Conflicting values at the same series/period/availability timestamp")
    result = result.drop_duplicates(keys).sort_values(
        ["available_at", "series", "period"], kind="stable"
    )
    # Output_type=2 repeats unchanged values at every vintage. Keep genuine changes,
    # including a withdrawal after publication; discard pre-publication missing cells.
    keep, last = [], {}
    for row in result.itertuples():
        key = (row.series, row.period)
        value = None if pd.isna(row.value) else row.value
        if key not in last and value is None:
            continue
        if key not in last or last[key] != value:
            keep.append(row.Index)
            last[key] = value
    result = result.loc[keep].reset_index(drop=True)
    result.attrs.update(panel.attrs)
    return result


def revision_response(series, key, page_size=10000, vintage_batch_size=128):
    """Fetch all advertised vintages, batched below FRED's JSON vintage limit.

    Output_type=2 is a matrix. It can be large for daily histories: batches bound
    response memory, and unchanged snapshots are compressed before returning.
    """
    if not 1 <= vintage_batch_size <= 2000 or not 1 <= page_size <= 100000:
        raise ValueError("Invalid FRED page or vintage batch size")
    end = datetime.now(UTC).date().isoformat()
    all_events = []
    with httpx.Client(timeout=120) as client:
        vintages = []
        for page in _paged(
            client,
            "series/vintagedates",
            key,
            {
                "series_id": series,
                "realtime_start": "1776-07-04",
                "realtime_end": end,
                "sort_order": "asc",
            },
            "vintage_dates",
            min(page_size, 10000),
        ):
            vintages.extend(page)
        if any(
            not isinstance(date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date)
            for date in vintages
        ):
            raise RuntimeError("FRED returned invalid vintage dates")
        # Observations start in 1990; earlier snapshots cannot contain those releases.
        vintages = sorted({date for date in vintages if "1990-01-01" <= date <= end})
        if not vintages:
            raise RuntimeError("FRED returned no vintage coverage for the requested history")
        for start in range(0, len(vintages), vintage_batch_size):
            chosen = vintages[start : start + vintage_batch_size]
            for page in _paged(
                client,
                "series/observations",
                key,
                {
                    "series_id": series,
                    "output_type": 2,
                    "vintage_dates": ",".join(chosen),
                    "observation_start": "1990-01-01",
                    "observation_end": end,
                    "sort_order": "asc",
                },
                "observations",
                page_size,
            ):
                all_events.extend(_matrix_events(page, series))
            # Bound memory growth from repeated unchanged snapshot cells.
            if all_events:
                partial = pd.DataFrame(all_events)
                partial["series"] = next(
                    name for name, fred_id in SERIES.items() if fred_id == series
                )
                partial["available_at"] = pd.to_datetime(
                    partial.vintage_date, utc=True
                ) + pd.Timedelta(days=1)
                compact = normalize_panel(partial)
                all_events = [
                    {
                        "period": row.period.isoformat(),
                        "value": row.value,
                        "vintage_date": (row.available_at - pd.Timedelta(days=1))
                        .date()
                        .isoformat(),
                    }
                    for row in compact.itertuples()
                ]
    return {
        "observations": all_events,
        "vintage_dates_count": len(vintages),
        "output_type": 2,
        "retrieved_at": datetime.now(UTC).isoformat(),
        **API_VINTAGE_PROVENANCE,
    }


def load_vintage_panel(path: str | Path):
    """Offline import: CSV series,period,value,available_at; availability must be explicit."""
    panel = normalize_panel(pd.read_csv(path, keep_default_na=False))
    panel.attrs.update(
        {
            "provenance": "user_supplied_vintage_csv; release timestamps require source audit",
            "information_set_kind": "unverified_csv",
            "strict_eligible": False,
            "availability_semantics": "user_supplied_unverified_timestamps",
        }
    )
    return panel


def fetch_macro(strict=False):
    if strict and os.getenv("FRED_VINTAGE_CSV"):
        return load_vintage_panel(os.environ["FRED_VINTAGE_CSV"])
    frames, fetch_diagnostics = [], {}
    for name, series in SERIES.items():
        if strict:
            key = os.getenv("FRED_API_KEY")
            if not key:
                raise RuntimeError("Strict evaluation requires FRED_API_KEY; no silent fallback")
            body = revision_response(series, key)
            frame = pd.DataFrame(body["observations"])
            if frame.empty:
                raise RuntimeError(f"No vintage observations for {name}")
            frame["available_at"] = pd.to_datetime(frame.vintage_date, utc=True) + pd.Timedelta(
                days=1
            )
            fetch_diagnostics[name] = {k: v for k, v in body.items() if k != "observations"}
        else:
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}&cosd=1990-01-01"
            raw, _ = download(url, DATA / "raw" / f"macro-{series}.csv")
            frame = pd.read_csv(io.BytesIO(raw), na_values=["."])
            frame.columns = ["period", "value"]
            dates = pd.to_datetime(frame.period, utc=True)
            frame["available_at"] = dates + pd.Timedelta(days=45 if name == "CPI" else 5)
        frame["series"] = name
        frames.append(frame)
    panel = normalize_panel(pd.concat(frames, ignore_index=True))
    panel.attrs.update(
        {
            "provenance": "FRED output_type=2 all advertised vintage dates"
            if strict
            else "current revised FRED snapshot with assumed availability",
            "fetch": fetch_diagnostics,
            **(
                API_VINTAGE_PROVENANCE
                if strict
                else {
                    "information_set_kind": "revised_snapshot_with_assumed_lags",
                    "strict_eligible": False,
                    "availability_semantics": "assumed_CPI_45_days_OIL_5_days",
                }
            ),
        }
    )
    DATA.mkdir(parents=True, exist_ok=True)
    panel.to_csv(DATA / ("macro-vintages.csv" if strict else "macro-revised.csv"), index=False)
    return panel


def _known_snapshot(panel, origin):
    return panel[panel.available_at <= origin].drop_duplicates(["series", "period"], keep="last")


def asof_snapshot(panel, origin):
    """Public inspection helper: one latest-known revision per series and period."""
    return _known_snapshot(normalize_panel(panel), pd.Timestamp(origin))


def _feature_snapshot(panel, origin):
    known = _known_snapshot(panel, origin).copy()
    known["month"] = known.period.dt.tz_localize(None).dt.to_period("M")
    cpi = known[known.series == "CPI"].set_index("month").value.sort_index()
    if len(cpi) < 16:
        return None
    cpi = cpi.reindex(pd.period_range(cpi.index.min(), cpi.index.max(), freq="M"))
    yoy = 100 * (cpi / cpi.shift(12) - 1)
    ar = yoy.iloc[-3:].to_numpy()[::-1]
    # Deduplication precedes aggregation: revisions are not additional trading days.
    oil = known[known.series == "OIL"].groupby("month").value.mean().sort_index()
    previous = origin.tz_localize(None).to_period("M") - 1
    oil = oil[oil.index <= previous]
    if len(oil) < 5 or not np.isfinite(ar).all():
        return None
    oil = oil.reindex(pd.period_range(oil.index.min(), oil.index.max(), freq="M"))
    last = oil.iloc[-1]
    changes = [
        100 * (last - oil.iloc[-1 - lag]) / abs(oil.iloc[-1 - lag])
        if abs(oil.iloc[-1 - lag]) > 1e-8
        else np.nan
        for lag in (1, 3)
    ]
    full = np.array([*ar, *changes], dtype=float)
    if not np.isfinite(full).all():
        return None
    return {
        "features": full,
        "latest_cpi_period": str(cpi.index.max()),
        "latest_oil_month": str(oil.index.max()),
        "latest_input_available_at": known.available_at.max(),
    }


def asof_features(panel, origin):
    snapshot = _feature_snapshot(normalize_panel(panel), pd.Timestamp(origin))
    return None if snapshot is None else snapshot["features"]


def _first_release_targets(panel, enforce_archive_lag=True):
    cpi = panel[panel.series == "CPI"].copy()
    finite = cpi[cpi.value.notna()]
    first = finite.drop_duplicates("period", keep="first")
    final = cpi.drop_duplicates("period", keep="last").set_index("period").value
    targets, excluded = {}, {"archive_censored": 0, "missing_denominator_at_release": 0}
    for row in first.itertuples():
        if (
            enforce_archive_lag
            and (row.available_at - row.period).days > MAX_FIRST_RELEASE_LAG_DAYS
        ):
            excluded["archive_censored"] += 1
            continue
        period = row.period.tz_localize(None).to_period("M")
        prior_date = (period - 12).to_timestamp().tz_localize("UTC")
        previous = cpi[(cpi.period == prior_date) & (cpi.available_at <= row.available_at)]
        if previous.empty or pd.isna(previous.iloc[-1].value):
            excluded["missing_denominator_at_release"] += 1
            continue
        value = 100 * (row.value / previous.iloc[-1].value - 1)
        latest_target, latest_prior = final.get(row.period), final.get(prior_date)
        final_yoy = None
        if pd.notna(latest_target) and pd.notna(latest_prior):
            final_yoy = float(100 * (latest_target / latest_prior - 1))
        targets[period] = {
            "target": float(value),
            "target_available_at": row.available_at,
            "target_final": final_yoy,
            "target_denominator_available_at": previous.iloc[-1].available_at,
        }
    return targets, excluded


def _build_samples(panel, horizon):
    targets, excluded = _first_release_targets(panel)
    cpi_periods = panel[panel.series == "CPI"].period
    rows = []
    diagnostics = {
        "candidate_origins": 0,
        "missing_features": 0,
        "missing_target": 0,
        "target_exclusions": excluded,
    }
    if cpi_periods.empty:
        return rows, diagnostics
    first = cpi_periods.min().tz_localize(None).to_period("M")
    last = cpi_periods.max().tz_localize(None).to_period("M")
    for month in pd.period_range(first + 24, last - horizon, freq="M"):
        diagnostics["candidate_origins"] += 1
        origin = month.to_timestamp(how="end").tz_localize("UTC")
        target = targets.get(month + horizon)
        if target is None:
            diagnostics["missing_target"] += 1
            continue
        snapshot = _feature_snapshot(panel, origin)
        if snapshot is None:
            diagnostics["missing_features"] += 1
            continue
        rows.append({"origin": origin, "target_month": str(month + horizon), **target, **snapshot})
    diagnostics["usable_samples"] = len(rows)
    return rows, diagnostics


def build_samples(panel, horizon):
    if horizon not in (1, 3):
        raise ValueError("Supported horizons are 1 and 3 calendar months from origin")
    return _build_samples(normalize_panel(panel), horizon)[0]


def coverage_diagnostics(panel):
    result = {}
    for series, group in panel.groupby("series"):
        finite = group[group.value.notna()]
        first = finite.drop_duplicates("period", keep="first")
        lags = (first.available_at - first.period).dt.total_seconds() / 86400
        result[series] = {
            "economic_periods": int(group.period.nunique()),
            "revision_events": len(group),
            "additional_revision_events": int(len(group) - group.period.nunique()),
            "withdrawal_events": int(group.value.isna().sum()),
            "first_period": group.period.min().isoformat(),
            "last_period": group.period.max().isoformat(),
            "first_available_at": group.available_at.min().isoformat(),
            "last_available_at": group.available_at.max().isoformat(),
            "initial_lag_median_days": float(lags.median()) if len(lags) else None,
            "initial_lag_p95_days": float(lags.quantile(0.95)) if len(lags) else None,
            "first_seen_over_120_days": int(lags.gt(MAX_FIRST_RELEASE_LAG_DAYS).sum()),
        }
    return result


def block_loss_interval(differences, horizon, bootstrap_samples=1000, seed=2026):
    """Paired circular moving-block percentile interval for mean loss differences."""
    values = np.asarray(differences, dtype=float)
    values = values[np.isfinite(values)]
    n = len(values)
    if n < 12:
        return {
            "mean_difference_pp": float(values.mean()) if n else None,
            "lower_95": None,
            "upper_95": None,
            "n": n,
            "status": "insufficient_origins",
            "block_length": None,
        }
    length = min(n, max(horizon + 1, math.ceil(n ** (1 / 3))))
    rng = np.random.default_rng(seed + horizon)
    starts = rng.integers(0, n, size=(bootstrap_samples, math.ceil(n / length)))
    indexes = (starts[:, :, None] + np.arange(length)) % n
    means = values[indexes.reshape(bootstrap_samples, -1)[:, :n]].mean(axis=1)
    return {
        "mean_difference_pp": float(values.mean()),
        "lower_95": float(np.quantile(means, 0.025)),
        "upper_95": float(np.quantile(means, 0.975)),
        "n": n,
        "status": "estimated",
        "block_length": length,
        "bootstrap_samples": bootstrap_samples,
    }


def past_residual_interval(history, model, origin, forecast, min_calibration=24, window=60):
    visible = [
        r
        for r in history
        if r["model"] == model and r["origin"] < origin and r["target_available_at"] <= origin
    ]
    visible = visible[-window:]
    n = len(visible)
    if n < min_calibration:
        return {
            "lower_90": None,
            "upper_90": None,
            "interval_calibration_n": n,
            "interval_calibration_last_available_at": None,
        }
    residuals = np.array([abs(r["forecast"] - r["actual"]) for r in visible])
    rank = min(n, math.ceil((n + 1) * 0.9))
    width = float(np.partition(residuals, rank - 1)[rank - 1])
    return {
        "lower_90": forecast - width,
        "upper_90": forecast + width,
        "interval_calibration_n": n,
        "interval_calibration_last_available_at": max(
            r["target_available_at"] for r in visible
        ).isoformat(),
    }


def _regime(origin):
    if origin.year < 2020:
        return "before_2020"
    return "2020_to_2022" if origin.year <= 2022 else "2023_onward"


def _metric(rows, target="actual"):
    valid = [r for r in rows if r.get(target) is not None and np.isfinite(r[target])]
    if not valid:
        return {"n": 0, "mae_pp": None, "rmse_pp": None}
    errors = np.array([r["forecast"] - r[target] for r in valid])
    return {
        "n": len(errors),
        "mae_pp": float(abs(errors).mean()),
        "rmse_pp": float(np.sqrt(np.mean(errors**2))),
    }


def _estimates(x, y, current):
    result = {"Persistence": float(current[0, 0])}
    for name, width, model in [
        ("AR ridge", 3, make_pipeline(StandardScaler(), Ridge(alpha=10))),
        ("AR + oil ridge", 5, make_pipeline(StandardScaler(), Ridge(alpha=10))),
        (
            "XGBoost",
            5,
            XGBRegressor(
                n_estimators=100,
                max_depth=2,
                learning_rate=0.03,
                subsample=1,
                colsample_bytree=1,
                reg_lambda=10,
                random_state=42,
                n_jobs=2,
            ),
        ),
    ]:
        model.fit(x[:, :width], y)
        result[name] = float(model.predict(current[:, :width])[0])
    return result


def _evaluation_labels(strict, provenance):
    if strict:
        return {
            "evaluation_type": "initial_release_asof",
            "target_policy": TARGET_POLICY,
            "information_policy": (
                "Latest known revision per period at each origin; FRED vintage dates available "
                "at next midnight. Frozen first-release targets and historical-origin features."
            ),
        }
    if provenance.get("information_set_kind") == "unverified_csv":
        return {
            "evaluation_type": "exploratory_unverified_csv",
            "target_policy": (
                "First-seen numerator in the supplied panel divided by the year-earlier value "
                "visible at its supplied availability timestamp. These are unverified "
                "first-seen input values, not genuine first-release evidence."
            ),
            "information_policy": (
                "Uses the user-supplied availability timestamps as given; publication lags "
                "and source release/revision history are unverified. This is exploratory "
                "as-of arithmetic, not a verified real-time backtest."
            ),
        }
    return {
        "evaluation_type": "exploratory_revised_data",
        "target_policy": (
            "Current revised CPI ratio for target month and year-earlier period; "
            "availability is ASSUMED at period start +45 days. This single-snapshot "
            "pseudo-release panel does not establish first-release targets."
        ),
        "information_policy": (
            "CURRENT revised snapshot; ASSUMED lags CPI=45 calendar days from month start, "
            "oil=5 days. NOT a real-time backtest."
        ),
    }


def evaluate(panel, strict=False, min_train=120, max_origins=96):
    if min_train < 3 or max_origins < 1:
        raise ValueError("min_train >=3 and max_origins >=1 are required")
    provenance = panel.attrs.copy()
    if strict and not (
        provenance.get("information_set_kind") == "alfred_revision_events"
        and provenance.get("strict_eligible") is True
        and provenance.get("availability_semantics") == VINTAGE_AVAILABILITY
    ):
        raise ValueError(
            "Strict evaluation requires ALFRED revision-event acquisition provenance. "
            "Unverified CSV panels and revised snapshots cannot be upgraded by --strict; "
            "evaluate them without --strict. Offline strict imports require a future "
            "audited provenance-manifest path, which is not implemented."
        )
    panel = normalize_panel(panel)
    results, predictions, regime_metrics, retrospective = [], [], [], []
    interval_metrics, horizon_summaries = [], {}
    for horizon in (1, 3):
        samples, diagnostics = _build_samples(panel, horizon)
        eligible = [
            s
            for s in samples
            if sum(
                t["origin"] < s["origin"] and t["target_available_at"] <= s["origin"]
                for t in samples
            )
            >= min_train
        ]
        selected = eligible[-max_origins:]
        evaluation_origins = {s["origin"] for s in selected}
        history = []
        # Earlier OOS forecasts warm interval calibration. They are not scored in
        # the headline evaluation window; in-sample residuals are never substituted.
        for sample in eligible[-(max_origins + 36) :]:
            train = [
                s
                for s in samples
                if s["origin"] < sample["origin"] and s["target_available_at"] <= sample["origin"]
            ]
            x = np.stack([s["features"] for s in train])
            y = np.array([s["target"] for s in train])
            current = sample["features"].reshape(1, -1)
            estimates = _estimates(x, y, current)
            for name, estimate in estimates.items():
                row = {
                    "horizon": horizon,
                    "model": name,
                    "origin": sample["origin"],
                    "target_month": sample["target_month"],
                    "actual": sample["target"],
                    "actual_final": sample["target_final"],
                    "forecast": estimate,
                    "train_n": len(train),
                    "target_available_at": sample["target_available_at"],
                    "latest_cpi_period": sample["latest_cpi_period"],
                    "latest_oil_month": sample["latest_oil_month"],
                    "latest_input_available_at": sample["latest_input_available_at"].isoformat(),
                    "regime": _regime(sample["origin"]),
                    **past_residual_interval(history, name, sample["origin"], estimate),
                }
                history.append(row)
                if sample["origin"] in evaluation_origins:
                    predictions.append(
                        {
                            **row,
                            "origin": row["origin"].isoformat(),
                            "target_available_at": row["target_available_at"].isoformat(),
                        }
                    )
        horizon_summaries[str(horizon)] = {
            **diagnostics,
            "eligible_origins": len(eligible),
            "evaluated_origins": len(selected),
            "first_origin": selected[0]["origin"].isoformat() if selected else None,
            "last_origin": selected[-1]["origin"].isoformat() if selected else None,
            "evaluation_span_months": len(
                pd.period_range(
                    selected[0]["origin"].tz_localize(None).to_period("M"),
                    selected[-1]["origin"].tz_localize(None).to_period("M"),
                    freq="M",
                )
            )
            if selected
            else 0,
        }
        rows_h = [r for r in predictions if r["horizon"] == horizon]
        baseline = {r["origin"]: r for r in rows_h if r["model"] == "Persistence"}
        for name in MODELS:
            rows = [r for r in rows_h if r["model"] == name]
            if not rows:
                continue
            losses = [
                abs(r["forecast"] - r["actual"])
                - abs(baseline[r["origin"]]["forecast"] - r["actual"])
                for r in rows
            ]
            metrics = _metric(rows)
            baseline_mae = _metric(list(baseline.values()))["mae_pp"]
            results.append(
                {
                    "horizon": horizon,
                    "model": name,
                    **metrics,
                    "mae_skill_vs_persistence": 1 - metrics["mae_pp"] / baseline_mae
                    if baseline_mae and baseline_mae > 0
                    else None,
                    "paired_absolute_loss_difference": block_loss_interval(losses, horizon),
                }
            )
            retrospective.append(
                {"horizon": horizon, "model": name, **_metric(rows, "actual_final")}
            )
            available = [r for r in rows if r["lower_90"] is not None]
            interval_metrics.append(
                {
                    "horizon": horizon,
                    "model": name,
                    "n": len(available),
                    "without_interval": len(rows) - len(available),
                    "coverage_90": float(
                        np.mean([r["lower_90"] <= r["actual"] <= r["upper_90"] for r in available])
                    )
                    if available
                    else None,
                    "mean_width_pp": float(
                        np.mean([r["upper_90"] - r["lower_90"] for r in available])
                    )
                    if available
                    else None,
                }
            )
            for regime in sorted({r["regime"] for r in rows}):
                subset = [r for r in rows if r["regime"] == regime]
                regime_metrics.append(
                    {"horizon": horizon, "model": name, "regime": regime, **_metric(subset)}
                )
    report = {
        "status": "completed" if results else "insufficient_data",
        **_evaluation_labels(strict, provenance),
        "method_version": "macro-v2",
        "panel_sha256": hashlib.sha256(
            panel.to_csv(index=False, lineterminator="\n").encode()
        ).hexdigest(),
        "created_at": datetime.now(UTC).isoformat(),
        "target": "US CPI seasonally adjusted YoY, percentage points",
        "coverage": coverage_diagnostics(panel),
        "provenance": provenance,
        "horizon_summaries": horizon_summaries,
        "metrics": results,
        "predictions": predictions,
        "regime_metrics": regime_metrics,
        "interval_metrics": interval_metrics,
        "retrospective_final_vintage_metrics": retrospective,
        "uncertainty_policy": {
            "loss_difference": "Paired circular moving-block percentile 95% interval, 1000 replicates; negative means lower MAE than persistence.",
            "prediction_intervals": "90% empirical absolute-error bands from up to 60 past OOS residuals whose targets were already released; minimum 24, up to 36 warm-up origins.",
            "caution": "Time dependence and changing regimes invalidate a distribution-free coverage guarantee. Block results are exploratory, block-length dependent and unadjusted for model multiplicity.",
        },
        "limitations": [
            "No causal interpretation",
            "Fixed hyperparameters; no tuning on evaluation origins",
            "Overlapping horizons, nonstationarity and missing origin months remain; bootstrap blocks use available forecast order",
            "Coverage is first-seen archive coverage, not independently verified release history",
            "Daily vintage dates do not establish intraday release times",
            "Retrospective final-vintage outcomes are diagnostic only; never training labels",
            "Vintage HTTP mapping is mock-tested; independent source coverage and release-history audit remain necessary"
            if strict
            else "User-supplied availability and revision history are unverified; supplied timestamps do not authenticate initial releases"
            if provenance.get("information_set_kind") == "unverified_csv"
            else "Revision leakage remains despite assumed release lags",
        ],
    }
    save_report(ARTIFACTS / ("macro-strict.json" if strict else "macro-exploratory.json"), report)
    print(json.dumps({k: v for k, v in report.items() if k != "predictions"}, indent=2))
    return report
