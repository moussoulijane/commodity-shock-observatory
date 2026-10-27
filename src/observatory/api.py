import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import psycopg
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CollectorRegistry, Gauge, generate_latest

from observatory.config import ARTIFACTS, DATA, INSTRUMENTS
from observatory.exposure import PROFILE_VERSION, PROFILES, ScenarioRequest, calculate_scenario
from observatory.incremental_store import init
from observatory.store import connect

Mode = Literal["REPLAY", "LIVE", "SYNTHETIC"]


@asynccontextmanager
async def lifespan(_):
    init()
    yield


app = FastAPI(title="Commodity Shock Observatory", version="0.2.0", lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def invalid_request(_, error: RequestValidationError):
    # Do not echo request bodies. In particular non-finite JSON inputs must yield
    # a 422 response, rather than breaking JSON serialization of validation errors.
    return JSONResponse(
        status_code=422,
        content={
            "detail": [
                {key: issue[key] for key in ("loc", "type", "msg") if key in issue}
                for issue in error.errors()
            ]
        },
    )


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
            "SELECT DISTINCT ON(instrument) payload FROM signals_v2 WHERE mode=%s "
            "ORDER BY instrument,event_time DESC",
            (mode,),
        ).fetchall()
        counts = conn.execute(
            "SELECT count(*) AS n FROM signals_v2 WHERE mode=%s", (mode,)
        ).fetchone()
        alerts = conn.execute(
            "SELECT count(*) AS n FROM signals_v2 WHERE mode=%s AND (payload->>'robust_alert')::bool",
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
            "SELECT payload FROM signals_v2 WHERE mode=%s AND instrument=%s "
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
            "SELECT payload FROM signals_v2 WHERE "
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


@app.get("/api/exposures/profiles")
def exposure_profiles():
    return {
        "profiles": PROFILES,
        "profile_version": PROFILE_VERSION,
        "data_mode": "REPLAY",
        "fx_reference_default_mad_per_usd": 10.0,
        "fx_reference_type": "USER ASSUMPTION — NOT A MARKET USD/MAD QUOTE",
        "reference_selection_policy": "Latest economic observation on or before the cutoff; snapshot revisions may postdate it",
    }


@app.post("/api/exposures/scenario")
def exposure_scenario(request: ScenarioRequest):
    profile = next((item for item in PROFILES if item["id"] == request.profile_id), None)
    if profile is None:
        raise HTTPException(404, "Unknown fictional exposure profile")
    if request.as_of is not None and request.as_of.tzinfo is None:
        raise HTTPException(422, "as_of must include an explicit timezone")
    instruments = [line["instrument"] for line in profile["lines"]]
    now = datetime.now(UTC)
    cutoff = min(request.as_of, now) if request.as_of is not None else now
    conditions = "mode='REPLAY' AND instrument=ANY(%s) AND event_time<=%s"
    args = [instruments, cutoff]
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT ON(instrument) instrument,payload FROM signals_v2 WHERE "
                + conditions
                + " ORDER BY instrument,event_time DESC",
                args,
            ).fetchall()
    except psycopg.Error:
        raise HTTPException(503, "Historical benchmark store unavailable") from None
    references = {row["instrument"]: row["payload"] for row in rows}
    try:
        result = calculate_scenario(request, references)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from None
    return {
        **result,
        "reference_selection_policy": "Economic-date cutoff only: references are revised snapshots, not historical publication vintages",
        "persistence": "Calculation only; scenarios and user inputs are not stored",
    }


@app.get("/api/bars/{instrument}")
def market_bars(instrument: str, mode: Mode = "REPLAY", limit: int = Query(500, ge=1, le=5000)):
    if instrument not in INSTRUMENTS:
        raise HTTPException(404, "Unknown instrument")
    with connect() as conn:
        rows = conn.execute(
            "SELECT bar_start,frequency,payload FROM market_bars_v2 WHERE mode=%s AND instrument=%s "
            "ORDER BY bar_start DESC LIMIT %s",
            (mode, instrument, limit + 1),
        ).fetchall()
    return {
        "instrument": instrument,
        "mode": mode,
        "truncated": len(rows) > limit,
        "items": [
            {"bar_start": row["bar_start"], "frequency": row["frequency"], **row["payload"]}
            for row in reversed(rows[:limit])
        ],
        "scope": "Canonical V2 observations; frequency remains explicit",
    }


@app.get("/api/research")
def research():
    try:
        cached_strict = read_report("macro-strict.json")
    except (json.JSONDecodeError, UnicodeError, OSError):
        cached_strict = {}
    provenance = cached_strict.get("provenance") if isinstance(cached_strict, dict) else None
    verified_metadata = (
        isinstance(cached_strict, dict)
        and cached_strict.get("method_version") == "macro-v2"
        and cached_strict.get("evaluation_type") == "initial_release_asof"
        and cached_strict.get("status") == "completed"
        and isinstance(provenance, dict)
        and provenance.get("information_set_kind") == "alfred_revision_events"
        and provenance.get("strict_eligible") is True
        and provenance.get("availability_semantics") == "next_utc_midnight_after_vintage_date"
    )
    return {
        "exploratory": read_report("macro-exploratory.json"),
        "strict": cached_strict if verified_metadata else None,
        "strict_cache_status": "verified_acquisition_metadata"
        if verified_metadata
        else "absent"
        if cached_strict is None
        else "unverified",
        "strict_cache_notice": "Acquisition metadata consistency check only; no independent authentication of cached reports",
        "detectors": read_report("detector-evaluation.json"),
        "detectors_v2": read_report("detector-evaluation-v2.json"),
    }


@app.get("/api/operations")
def operations():
    with connect() as conn:
        stats = conn.execute(
            "SELECT count(*) AS deliveries,count(*) FILTER(WHERE NOT valid) AS rejected,"
            "max(processed_at) AS last_processed FROM deliveries"
        ).fetchone()
        counts = conn.execute("SELECT mode,count(*) AS n FROM signals_v2 GROUP BY mode").fetchall()
        batches = conn.execute(
            "SELECT * FROM pipeline_runs ORDER BY "
            "(payload->>'completed_at') DESC NULLS LAST LIMIT 20"
        ).fetchall()
        windows = conn.execute("SELECT count(*) AS n FROM delivery_windows_v2").fetchone()
    return {
        **stats,
        "canonical_counts": counts,
        "canonical_count_definition": "Latest economic observations in signals_v2; raw revision identities remain in the audit store",
        "batches": batches,
        "window_count": windows["n"],
        "spark_ingest": read_report("spark-ingest-v2.json"),
        "spark_windows": {
            mode: read_report(f"spark-windows-{mode}.json")
            for mode in ("REPLAY", "LIVE", "SYNTHETIC")
        },
        "reliability": read_report("reliability-v2.json"),
        "load": read_report("load-test-v2.json"),
        "commit_recovery": read_report("commit-recovery-v2.json"),
        "sustained_load": read_report("sustained-load-v2.json"),
        "parity": read_report("parity-v2.json"),
        "spark_checks": read_report("spark-checks-v2.json"),
        "detectors_v2": read_report("detector-evaluation-v2.json"),
        "v1_reference": {
            "reliability": read_report("reliability.json"),
            "load": read_report("load-test.json"),
        },
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
                "publication": "ALFRED output_type=2 revision matrix; latest revision available at each origin for features, first-release targets; API key required",
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
