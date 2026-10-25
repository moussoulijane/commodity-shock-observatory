import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from observatory.config import DATABASE
from observatory.contracts import Quote
from observatory.signals import current_snapshot, score_history

DDL = """
CREATE TABLE IF NOT EXISTS deliveries (
 topic text NOT NULL, partition_id int NOT NULL, offset_id bigint NOT NULL,
 payload text NOT NULL, valid bool NOT NULL, reason text,
 processed_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(topic,partition_id,offset_id));
CREATE TABLE IF NOT EXISTS observations (
 mode text NOT NULL, event_id text NOT NULL, instrument text NOT NULL,
 event_time timestamptz NOT NULL, observed_at timestamptz NOT NULL,
 revision text NOT NULL, value double precision NOT NULL, payload jsonb NOT NULL,
 PRIMARY KEY(mode,event_id));
CREATE INDEX IF NOT EXISTS observations_history ON observations(mode,instrument,event_time);
CREATE TABLE IF NOT EXISTS signals (
 mode text NOT NULL, instrument text NOT NULL, event_id text NOT NULL,
 event_time timestamptz NOT NULL, payload jsonb NOT NULL,
 PRIMARY KEY(mode,event_id));
CREATE TABLE IF NOT EXISTS delivery_windows (
 mode text NOT NULL, instrument text NOT NULL, window_start timestamptz NOT NULL,
 window_end timestamptz NOT NULL, delivery_count bigint NOT NULL,
 mean_value double precision NOT NULL, min_value double precision NOT NULL,
 max_value double precision NOT NULL, PRIMARY KEY(mode,instrument,window_start));
CREATE TABLE IF NOT EXISTS pipeline_runs (
 query_name text NOT NULL, batch_id bigint NOT NULL, payload jsonb NOT NULL,
 PRIMARY KEY(query_name,batch_id));
"""


def connect():
    return psycopg.connect(DATABASE, row_factory=dict_row, connect_timeout=10)


def init():
    with connect() as conn:
        conn.execute(DDL)


def serializable(value):
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if hasattr(value, "item"):
        value = value.item()
    return value


def records(frame):
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def ingest_rows(rows, batch_id):
    """Atomic serving transaction. Re-running an offset never creates another event."""
    counts = {"input": 0, "invalid": 0, "new_events": 0, "duplicate_deliveries": 0}
    with connect() as conn:
        for row in rows:
            counts["input"] += 1
            raw = row["payload"]
            try:
                quote = Quote.model_validate_json(raw)
                expected = f"cso.quotes.{quote.mode.lower()}.v1"
                if row["topic"] != expected:
                    raise ValueError("Topic and payload mode disagree")
                error = None
            except (ValueError, TypeError) as exc:
                # Avoid echoing arbitrary payload content into logs.
                quote, error = None, type(exc).__name__ + ": contract rejected"
            cursor = conn.execute(
                "INSERT INTO deliveries(topic,partition_id,offset_id,payload,valid,reason) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    row["topic"],
                    row["partition_id"],
                    row["offset_id"],
                    raw,
                    quote is not None,
                    error,
                ),
            )
            if cursor.rowcount == 0:
                counts["duplicate_deliveries"] += 1
                continue
            if quote is None:
                counts["invalid"] += 1
                continue
            payload = quote.model_dump(mode="json")
            existing = conn.execute(
                "SELECT payload FROM observations WHERE mode=%s AND event_id=%s",
                (quote.mode, quote.event_id),
            ).fetchone()
            # Reusing an identity with a different price is a corrupt revision contract.
            if existing and existing["payload"]["value"] != quote.value:
                conn.execute(
                    "UPDATE deliveries SET valid=false,reason='identity value conflict' "
                    "WHERE topic=%s AND partition_id=%s AND offset_id=%s",
                    (row["topic"], row["partition_id"], row["offset_id"]),
                )
                counts["invalid"] += 1
                continue
            result = conn.execute(
                "INSERT INTO observations VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    quote.mode,
                    quote.event_id,
                    quote.instrument,
                    quote.event_time,
                    quote.observed_at,
                    quote.revision,
                    quote.value,
                    Jsonb(payload),
                ),
            )
            counts["new_events"] += result.rowcount
        refresh_signals(conn)
        conn.execute(
            "INSERT INTO pipeline_runs VALUES ('ingest',%s,%s) "
            "ON CONFLICT(query_name,batch_id) DO UPDATE SET payload=excluded.payload",
            (batch_id, Jsonb({**counts, "completed_at": datetime.now(UTC).isoformat()})),
        )
    return counts


def observation_frame(conn, mode=None):
    rows = conn.execute(
        "SELECT payload FROM observations" + (" WHERE mode=%s" if mode else ""),
        (mode,) if mode else (),
    ).fetchall()
    frame = pd.DataFrame([r["payload"] for r in rows])
    if not frame.empty:
        for col in ("event_time", "observed_at", "ingested_at"):
            frame[col] = pd.to_datetime(frame[col], utc=True, format="ISO8601")
    return frame


def refresh_signals(conn):
    scored = score_history(current_snapshot(observation_frame(conn)))
    conn.execute("DELETE FROM signals")
    if not scored.empty:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO signals VALUES (%s,%s,%s,%s,%s)",
                [
                    (r["mode"], r["instrument"], r["event_id"], r["event_time"], Jsonb(r))
                    for r in records(scored)
                ],
            )


def save_report(path: Path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(report, indent=2, default=serializable, allow_nan=False), encoding="utf-8"
    )
    temporary.replace(path)
