import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import psycopg
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CollectorRegistry, Gauge, generate_latest

from observatory.config import ARTIFACTS, DATA, INSTRUMENTS
from observatory.store import connect, init

Mode = Literal["REPLAY", "LIVE", "SYNTHETIC"]


@asynccontextmanager
async def lifespan(_):
    init()
    yield


app = FastAPI(title="Commodity Shock Observatory", version="0.1.0", lifespan=lifespan)


def read_report(name):
    path = ARTIFACTS / name
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


@app.get("/api/health")
def health():
    try:
        with connect() as conn:
            conn.execute("SELECT 1")
    except psycopg.Error:
        raise HTTPException(503, "Database unavailable") from None
    return {
        "status": "ok",
        "scope": "API and database only; see /api/operations for ingestion freshness",
    }


@app.get("/api/workspace")
def workspace(mode: Mode = "REPLAY"):
    with connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT ON(instrument) payload FROM signals WHERE mode=%s "
            "ORDER BY instrument,event_time DESC",
            (mode,),
        ).fetchall()
        counts = conn.execute("SELECT count(*) AS n FROM signals WHERE mode=%s", (mode,)).fetchone()
        alerts = conn.execute(
            "SELECT count(*) AS n FROM signals WHERE mode=%s AND (payload->>'robust_alert')::bool",
            (mode,),
        ).fetchone()
    source_path = DATA / "source-report.json"
    sources = json.loads(source_path.read_text()) if source_path.exists() else None
    return {
        "mode": mode,
        "instruments": [
            {**r["payload"], "name": INSTRUMENTS[r["payload"]["instrument"]]["name"]} for r in rows
        ],
        "observations": counts["n"],
        "alerts": alerts["n"],
        "sources": sources,
        "server_time": datetime.now(UTC).isoformat(),
        "scope": "Historical revised snapshots"
        if mode == "REPLAY"
        else "Synthetic engineering test data"
        if mode == "SYNTHETIC"
        else "Provider intraday quotes",
    }


@app.get("/api/history/{instrument}")
def history(instrument: str, mode: Mode = "REPLAY", limit: int = Query(6000, ge=1, le=20000)):
    if instrument not in INSTRUMENTS:
        raise HTTPException(404, "Unknown instrument")
    with connect() as conn:
        rows = conn.execute(
            "SELECT payload FROM signals WHERE mode=%s AND instrument=%s "
            "ORDER BY event_time DESC LIMIT %s",
            (mode, instrument, limit + 1),
        ).fetchall()
    return {
        "instrument": instrument,
        "mode": mode,
        "spec": INSTRUMENTS[instrument],
        "points": [r["payload"] for r in reversed(rows[:limit])],
        "truncated": len(rows) > limit,
        "limit": limit,
    }


@app.get("/api/alerts")
def alerts(
    mode: Mode = "REPLAY",
    instrument: str | None = None,
    direction: Literal["UP", "DOWN"] | None = None,
    limit: int = Query(100, ge=1, le=1000),
):
    conditions, args = ["mode=%s", "(payload->>'robust_alert')::bool"], [mode]
    if instrument:
        conditions.append("instrument=%s")
        args.append(instrument)
    if direction:
        conditions.append("payload->>'direction'=%s")
        args.append(direction)
    with connect() as conn:
        rows = conn.execute(
            "SELECT payload FROM signals WHERE "
            + " AND ".join(conditions)
            + " ORDER BY event_time DESC,instrument LIMIT %s",
            (*args, limit),
        ).fetchall()
    return {
        "items": [r["payload"] for r in rows],
        "detector": "delta-robust-v1",
        "threshold": 4,
        "interpretation": "Unusual absolute price change relative to prior changes; not a causal macro forecast",
    }


@app.get("/api/research")
def research():
    return {
        "exploratory": read_report("macro-exploratory.json"),
        "strict": read_report("macro-strict.json"),
        "detectors": read_report("detector-evaluation.json"),
    }


@app.get("/api/operations")
def operations():
    with connect() as conn:
        stats = conn.execute(
            "SELECT count(*) AS deliveries,count(*) FILTER(WHERE NOT valid) AS rejected,"
            "max(processed_at) AS last_processed FROM deliveries"
        ).fetchone()
        counts = conn.execute(
            "SELECT mode,count(*) AS n FROM observations GROUP BY mode"
        ).fetchall()
        batches = conn.execute(
            "SELECT * FROM pipeline_runs ORDER BY "
            "(payload->>'completed_at') DESC NULLS LAST LIMIT 20"
        ).fetchall()
        windows = conn.execute("SELECT count(*) AS n FROM delivery_windows").fetchone()
    return {
        **stats,
        "canonical_counts": counts,
        "batches": batches,
        "window_count": windows["n"],
        "spark_ingest": read_report("spark-ingest.json"),
        "spark_windows": read_report("spark-windows.json"),
        "reliability": read_report("reliability.json"),
        "load": read_report("load-test.json"),
        "deployment": "Local single broker; no HA, TLS, SSO or production SLA",
    }


@app.get("/api/catalog")
def catalog():
    return {
        "instruments": INSTRUMENTS,
        "sources": [
            {
                "name": "EIA via FRED",
                "url": "https://fred.stlouisfed.org/series/DCOILBRENTEU",
                "publication": "daily; exact original publication times absent from downloaded snapshot",
            },
            {
                "name": "World Bank Pink Sheet",
                "url": "https://www.worldbank.org/en/research/commodity-markets",
                "publication": "monthly; averages assigned to month end",
            },
            {
                "name": "ALFRED",
                "url": "https://fred.stlouisfed.org/docs/api/fred/realtime_period.html",
                "publication": "initial release vintage dates require API key",
            },
        ],
    }


@app.get("/metrics")
def metrics():
    registry = CollectorRegistry()
    with connect() as conn:
        row = conn.execute(
            "SELECT count(*) AS n,count(*) FILTER(WHERE NOT valid) AS bad,"
            "extract(epoch FROM max(processed_at)) AS last FROM deliveries"
        ).fetchone()
    for name, value, description in [
        ("cso_deliveries", row["n"], "Durable Kafka delivery coordinates processed"),
        ("cso_quarantined", row["bad"], "Rejected durable delivery coordinates"),
        (
            "cso_last_delivery_timestamp_seconds",
            row["last"] or 0,
            "Last serving ingestion UTC epoch",
        ),
    ]:
        Gauge(name, description, registry=registry).set(float(value))
    return Response(generate_latest(registry), media_type="text/plain; version=0.0.4")


dist = Path("frontend/dist")
if dist.exists():
    app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")

    @app.get("/")
    @app.get("/ops")
    def index():
        return FileResponse(dist / "index.html")
