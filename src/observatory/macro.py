"""Expanding-window forecasts with explicit observation-availability assumptions.

Exploratory mode uses today's revised levels and assumed release lags. Strict mode
requires an ALFRED initial-release panel. It forecasts initial-release YoY inflation;
neither mode estimates a causal commodity pass-through coefficient.
"""

import io
import json
import os
from datetime import UTC, datetime

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


def initial_release_response(series, key):
    try:
        with httpx.Client(timeout=120) as client:
            response = client.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id": series,
                    "api_key": key,
                    "file_type": "json",
                    "output_type": 4,
                    "realtime_start": "1776-07-04",
                    "realtime_end": "9999-12-31",
                    "observation_start": "1990-01-01",
                    "limit": 100000,
                },
            )
    except httpx.HTTPError:
        raise RuntimeError("FRED network request failed; credentials omitted") from None
    if response.status_code != 200:
        raise RuntimeError(f"FRED API request rejected ({response.status_code}); key omitted")
    return response.json()


def fetch_macro(strict=False):
    frames = []
    for name, series in SERIES.items():
        if strict:
            key = os.getenv("FRED_API_KEY")
            if not key:
                raise RuntimeError("Strict evaluation requires FRED_API_KEY; no silent fallback")
            # output_type=4 returns initial releases. realtime_start is the vintage date.
            body = initial_release_response(series, key)
            if body["count"] > len(body["observations"]):
                raise RuntimeError("Pagination required; refusing incomplete vintage history")
            observations = body["observations"]
            frame = pd.DataFrame(
                {
                    "period": [r["date"] for r in observations],
                    "value": [r["value"] for r in observations],
                    "available_at": [r["realtime_start"] for r in observations],
                }
            )
            frame["available_at"] = pd.to_datetime(frame.available_at, utc=True) + pd.Timedelta(
                days=1
            )
            # Daily vintage dates have no time; next midnight is conservative.
        else:
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}&cosd=1990-01-01"
            raw, _ = download(url, DATA / "raw" / f"macro-{series}.csv")
            frame = pd.read_csv(io.BytesIO(raw), na_values=["."])
            frame.columns = ["period", "value"]
            dates = pd.to_datetime(frame.period, utc=True)
            frame["available_at"] = dates + pd.Timedelta(days=45 if name == "CPI" else 5)
        frame["period"] = pd.to_datetime(frame.period, utc=True)
        frame["value"] = pd.to_numeric(frame.value, errors="coerce")
        frame["series"] = name
        frames.append(frame.dropna())
    panel = pd.concat(frames, ignore_index=True)
    DATA.mkdir(parents=True, exist_ok=True)
    panel.to_csv(
        DATA / ("macro-initial-releases.csv" if strict else "macro-revised.csv"), index=False
    )
    return panel


def asof_features(panel, origin):
    known = panel[panel.available_at <= origin].copy()
    known["month"] = known.period.dt.tz_localize(None).dt.to_period("M")
    cpi = (
        known[known.series == "CPI"]
        .sort_values("available_at")
        .drop_duplicates("month", keep="first")
    )
    cpi = cpi.set_index("month").value.sort_index()
    if len(cpi) < 16:
        return None
    cpi = cpi.reindex(pd.period_range(cpi.index.min(), cpi.index.max(), freq="M"))
    yoy = 100 * (cpi / cpi.shift(12) - 1)
    ar = yoy.iloc[-3:].to_numpy()[::-1]
    oil = known[known.series == "OIL"].groupby("month").value.mean().sort_index()
    # Exclude the current partial month; monthly aggregates must be complete.
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
    return full if np.isfinite(full).all() else None


def build_samples(panel, horizon):
    cpi = panel[panel.series == "CPI"].copy()
    cpi["month"] = cpi.period.dt.tz_localize(None).dt.to_period("M")
    cpi = cpi.sort_values("available_at").drop_duplicates("month", keep="first").set_index("month")
    rows = []
    if cpi.empty:
        return rows
    for month in pd.period_range(cpi.index.min() + 24, cpi.index.max() - horizon, freq="M"):
        origin = (
            month.to_timestamp(how="end").normalize().tz_localize("UTC")
            + pd.Timedelta(days=1)
            - pd.Timedelta(microseconds=1)
        )
        target, prior = month + horizon, month + horizon - 12
        if target not in cpi.index or prior not in cpi.index:
            continue
        features = asof_features(panel, origin)
        if features is None:
            continue
        value = 100 * (cpi.loc[target, "value"] / cpi.loc[prior, "value"] - 1)
        available = max(cpi.loc[target, "available_at"], cpi.loc[prior, "available_at"])
        if not np.isfinite(value):
            continue
        rows.append(
            {
                "origin": origin,
                "target_month": str(target),
                "target_available_at": available,
                "features": features,
                "target": float(value),
            }
        )
    return rows


def evaluate(panel, strict=False, min_train=120, max_origins=96):
    results, predictions = [], []
    for horizon in (1, 3):
        samples = build_samples(panel, horizon)
        eligible = [
            s
            for s in samples
            if sum(t["target_available_at"] <= s["origin"] for t in samples) >= min_train
        ]
        for sample in eligible[-max_origins:]:
            train = [
                s
                for s in samples
                if s["origin"] < sample["origin"] and s["target_available_at"] <= sample["origin"]
            ]
            x = np.stack([s["features"] for s in train])
            y = np.array([s["target"] for s in train])
            current = sample["features"].reshape(1, -1)
            estimates = {"Persistence": float(current[0, 0])}
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
                estimates[name] = float(model.predict(current[:, :width])[0])
            for name, estimate in estimates.items():
                predictions.append(
                    {
                        "horizon": horizon,
                        "model": name,
                        "origin": sample["origin"].isoformat(),
                        "target_month": sample["target_month"],
                        "actual": sample["target"],
                        "forecast": estimate,
                        "train_n": len(train),
                    }
                )
        for name in ("Persistence", "AR ridge", "AR + oil ridge", "XGBoost"):
            rows = [r for r in predictions if r["horizon"] == horizon and r["model"] == name]
            if not rows:
                continue
            errors = np.array([r["forecast"] - r["actual"] for r in rows])
            results.append(
                {
                    "horizon": horizon,
                    "model": name,
                    "n": len(rows),
                    "mae_pp": float(np.mean(abs(errors))),
                    "rmse_pp": float(np.sqrt(np.mean(errors**2))),
                }
            )
    report = {
        "status": "completed" if results else "insufficient_data",
        "evaluation_type": "initial_release_asof" if strict else "exploratory_revised_data",
        "created_at": datetime.now(UTC).isoformat(),
        "target": "US CPI seasonally adjusted YoY, percentage points",
        "information_policy": "Initial releases only, next-day vintage availability"
        if strict
        else "CURRENT revised snapshot; ASSUMED lags CPI=45 calendar days from month start, oil=5 days. NOT a real-time backtest.",
        "limitations": [
            "No causal interpretation",
            "Fixed hyperparameters; no tuning on evaluation origins",
            "Overlapping horizons: errors are dependent; no significance claim",
            "Initial-release targets differ from final revised inflation"
            if strict
            else "Revision leakage remains despite assumed release lags",
        ],
        "metrics": results,
        "predictions": predictions,
    }
    save_report(ARTIFACTS / ("macro-strict.json" if strict else "macro-exploratory.json"), report)
    print(json.dumps({k: v for k, v in report.items() if k != "predictions"}, indent=2))
    return report
