"""V2 bulk ingestion and bounded context reads; V1 serving tables remain untouched."""

import json
from contextlib import contextmanager
from datetime import UTC, datetime

import pandas as pd
from psycopg.types.json import Jsonb

from observatory.config import TOPICS
from observatory.contracts import Quote
from observatory.store import connect
from observatory.store import init as init_v1

MATERIALIZATION_LOCK = 2026102601
NEIGHBORS = 62
MAX_FEATURE_CONTEXT_ROWS = 250000
MAX_BAR_CONTEXT_ROWS = 50000

DDL = """
-- A Kafka tombstone has no value bytes. Preserve SQL NULL in the audit trail,
-- distinct from the literal JSON text 'null'; it never becomes an observation.
ALTER TABLE deliveries ALTER COLUMN payload DROP NOT NULL;
CREATE TABLE IF NOT EXISTS signals_v2 (
 mode text NOT NULL,instrument text NOT NULL,event_id text NOT NULL,
 event_time timestamptz NOT NULL,payload jsonb NOT NULL,
 PRIMARY KEY(mode,instrument,event_time));
ALTER TABLE signals_v2 ADD COLUMN IF NOT EXISTS generation bigint NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS signals_v2_identity ON signals_v2(mode,event_id);
CREATE TABLE IF NOT EXISTS delivery_windows_v2 (
 mode text NOT NULL,instrument text NOT NULL,window_start timestamptz NOT NULL,
 window_end timestamptz NOT NULL,delivery_count bigint NOT NULL,
 mean_value double precision NOT NULL,min_value double precision NOT NULL,
 max_value double precision NOT NULL,PRIMARY KEY(mode,instrument,window_start));
CREATE TABLE IF NOT EXISTS market_bars_v2 (
 mode text NOT NULL,instrument text NOT NULL,bar_start timestamptz NOT NULL,
 frequency text NOT NULL,payload jsonb NOT NULL,PRIMARY KEY(mode,instrument,bar_start));
ALTER TABLE market_bars_v2 ADD COLUMN IF NOT EXISTS generation bigint NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS affected_ranges_v2 (
 query_name text NOT NULL,batch_id bigint NOT NULL,mode text NOT NULL,instrument text NOT NULL,
 start_time timestamptz NOT NULL,end_time timestamptz NOT NULL,
 PRIMARY KEY(query_name,batch_id,mode,instrument));
CREATE TABLE IF NOT EXISTS affected_events_v2 (
 query_name text NOT NULL,batch_id bigint NOT NULL,mode text NOT NULL,instrument text NOT NULL,
 event_time timestamptz NOT NULL,
 PRIMARY KEY(query_name,batch_id,mode,instrument,event_time));
CREATE TABLE IF NOT EXISTS completed_batches_v2 (
 query_name text NOT NULL,batch_id bigint NOT NULL,completed_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(query_name,batch_id));
CREATE SEQUENCE IF NOT EXISTS materialization_generation_v2;
CREATE TABLE IF NOT EXISTS materialization_epoch_v2 (
 singleton bool PRIMARY KEY DEFAULT true CHECK(singleton),generation bigint NOT NULL);
INSERT INTO materialization_epoch_v2(singleton,generation) VALUES (true,0) ON CONFLICT DO NOTHING;
CREATE INDEX IF NOT EXISTS observations_v2_revision_lookup
 ON observations(mode,instrument,event_time DESC,observed_at DESC,revision DESC);
CREATE INDEX IF NOT EXISTS observations_v2_revision_lookup_asc
 ON observations(mode,instrument,event_time ASC,observed_at DESC,revision DESC);
"""


def init():
    init_v1()
    with connect() as conn:
        conn.execute(DDL)


@contextmanager
def materialization_lease():
    """Serialize canonical reads/materialization and fence old executor attempts.

    The driver must hold this session lock for the complete raw sink, including
    executor sink writes and complete_batch(). Session death releases the lock;
    the next owner advances the durable generation before any newer writes.
    This key differs from ingest_batch's transaction-scoped lock.
    """
    with connect() as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (MATERIALIZATION_LOCK,))
        try:
            generation = conn.execute(
                "SELECT nextval('materialization_generation_v2') AS generation"
            ).fetchone()["generation"]
            conn.execute(
                "UPDATE materialization_epoch_v2 SET generation=%s WHERE singleton", (generation,)
            )
            conn.commit()
            yield int(generation)
        finally:
            conn.rollback()
            conn.execute("SELECT pg_advisory_unlock(%s)", (MATERIALIZATION_LOCK,))
            conn.commit()


def ingest_batch(rows, batch_id):
    """COPY once, reject identity conflicts set-wise, retain durable retry ranges."""
    parsed = []
    for row in rows:
        raw = row["payload"]
        try:
            quote = Quote.model_validate_json(raw)
            if TOPICS[quote.mode] != row["topic"]:
                raise ValueError("Topic mode mismatch")
            payload = quote.model_dump(mode="json")
            data = (
                quote.mode,
                quote.event_id,
                quote.instrument,
                quote.event_time,
                quote.observed_at,
                quote.revision,
                quote.value,
                json.dumps(payload),
            )
            valid, reason = True, None
        except (ValueError, TypeError):
            data, valid = (None,) * 8, False
            reason = "Kafka tombstone" if raw is None else "contract rejected"
        parsed.append(
            (
                row["topic"],
                row["partition_id"],
                row["offset_id"],
                raw,
                valid,
                reason,
                *data,
            )
        )
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20261025)")
        conn.execute(
            "CREATE TEMP TABLE incoming (topic text,part int,off bigint,raw text,valid bool,"
            "reason text,mode text,event_id text,instrument text,event_time timestamptz,"
            "observed_at timestamptz,revision text,value double precision,body text) ON COMMIT DROP"
        )
        with conn.cursor().copy("COPY incoming FROM STDIN") as copy:
            for row in parsed:
                copy.write_row(row)
        conn.execute(
            "CREATE TEMP TABLE winners ON COMMIT DROP AS SELECT DISTINCT ON(mode,event_id) "
            "mode,event_id,value FROM incoming WHERE valid ORDER BY mode,event_id,topic,part,off"
        )
        conn.execute(
            "UPDATE incoming i SET valid=false,reason='identity value conflict' WHERE i.valid AND "
            "(EXISTS(SELECT 1 FROM observations o WHERE o.mode=i.mode AND o.event_id=i.event_id "
            "AND o.value<>i.value) OR (NOT EXISTS(SELECT 1 FROM observations o "
            "WHERE o.mode=i.mode AND o.event_id=i.event_id) AND "
            "EXISTS(SELECT 1 FROM winners w WHERE w.mode=i.mode "
            "AND w.event_id=i.event_id AND w.value<>i.value)))"
        )
        conn.execute(
            "INSERT INTO deliveries(topic,partition_id,offset_id,payload,valid,reason) "
            "SELECT topic,part,off,raw,valid,reason FROM incoming ON CONFLICT DO NOTHING"
        )
        inserted = conn.execute(
            "INSERT INTO observations(mode,event_id,instrument,event_time,observed_at,revision,value,payload) "
            "SELECT DISTINCT ON(mode,event_id) mode,event_id,instrument,event_time,observed_at,revision,value,body::jsonb "
            "FROM incoming WHERE valid ORDER BY mode,event_id,topic,part,off ON CONFLICT DO NOTHING"
        ).rowcount
        conn.execute(
            "INSERT INTO affected_ranges_v2 SELECT 'ingest-v2',%s,mode,instrument,min(event_time),max(event_time) "
            "FROM incoming WHERE valid GROUP BY mode,instrument ON CONFLICT DO NOTHING",
            (batch_id,),
        )
        conn.execute(
            "INSERT INTO affected_events_v2 SELECT DISTINCT 'ingest-v2',%s,mode,instrument,event_time "
            "FROM incoming WHERE valid ON CONFLICT DO NOTHING",
            (batch_id,),
        )
        accepted = conn.execute(
            "SELECT DISTINCT ON(i.mode,i.event_id) o.payload AS body FROM incoming i "
            "JOIN observations o ON o.mode=i.mode AND o.event_id=i.event_id WHERE i.valid "
            "ORDER BY i.mode,i.event_id,i.part,i.off"
        ).fetchall()
        rejects = conn.execute(
            "SELECT topic,part,off,reason FROM incoming WHERE NOT valid"
        ).fetchall()
    return (
        [r["body"] for r in accepted],
        rejects,
        {
            "input": len(parsed),
            "invalid": len(rejects),
            "new_events": inserted,
            "storage_version": 2,
        },
    )


def canonical_rows(
    conn, mode, instrument, start, end, predecessors=0, successors=0, max_middle_rows=25000
):
    """Bound results in SQL before fetching; never materialize an unbounded middle."""
    if not isinstance(max_middle_rows, int) or max_middle_rows < 1:
        raise ValueError("A positive canonical read bound is required")
    args = (mode, instrument)
    projection = "SELECT DISTINCT ON(event_time) event_time,payload FROM observations WHERE mode=%s AND instrument=%s "
    ordering = " ORDER BY event_time {direction},observed_at DESC,revision DESC"
    middle = conn.execute(
        projection
        + "AND event_time BETWEEN %s AND %s"
        + ordering.format(direction="ASC")
        + " LIMIT %s",
        (*args, start, end, max_middle_rows + 1),
    ).fetchall()
    if len(middle) > max_middle_rows:
        raise RuntimeError("Canonical read exceeds bounded context; split the requested backfill")
    before = (
        conn.execute(
            projection + "AND event_time<%s" + ordering.format(direction="DESC") + " LIMIT %s",
            (*args, start, predecessors),
        ).fetchall()
        if predecessors
        else []
    )
    after = (
        conn.execute(
            projection + "AND event_time>%s" + ordering.format(direction="ASC") + " LIMIT %s",
            (*args, end, successors),
        ).fetchall()
        if successors
        else []
    )
    return (
        [r["payload"] for r in reversed(before)]
        + [r["payload"] for r in middle]
        + [r["payload"] for r in after]
    )


def _neighborhoods(conn, mode, instrument, anchors):
    """Indexed lateral probes return at most 125 rows per changed observation."""
    query = """
    SELECT a.anchor,p.event_time,p.payload
    FROM unnest(%s::timestamptz[]) AS a(anchor)
    CROSS JOIN LATERAL (
      (SELECT DISTINCT ON(o.event_time) o.event_time,o.payload FROM observations o
       WHERE o.mode=%s AND o.instrument=%s AND o.event_time<a.anchor
       ORDER BY o.event_time DESC,o.observed_at DESC,o.revision DESC LIMIT %s)
      UNION ALL
      (SELECT DISTINCT ON(o.event_time) o.event_time,o.payload FROM observations o
       WHERE o.mode=%s AND o.instrument=%s AND o.event_time=a.anchor
       ORDER BY o.event_time,o.observed_at DESC,o.revision DESC LIMIT 1)
      UNION ALL
      (SELECT DISTINCT ON(o.event_time) o.event_time,o.payload FROM observations o
       WHERE o.mode=%s AND o.instrument=%s AND o.event_time>a.anchor
       ORDER BY o.event_time,o.observed_at DESC,o.revision DESC LIMIT %s)
    ) AS p
    """
    for offset in range(0, len(anchors), 256):
        chunk = anchors[offset : offset + 256]
        grouped = {anchor: [] for anchor in chunk}
        result = conn.execute(
            query,
            (chunk, mode, instrument, NEIGHBORS, mode, instrument, mode, instrument, NEIGHBORS),
        ).fetchall()
        for row in result:
            grouped[row["anchor"]].append(row)
        for anchor in chunk:
            yield anchor, sorted(grouped[anchor], key=lambda row: row["event_time"])


def feature_context(batch_id):
    all_rows, ranges = {}, []
    with connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        changed_events = conn.execute(
            "SELECT mode,instrument,event_time FROM affected_events_v2 "
            "WHERE query_name='ingest-v2' AND batch_id=%s ORDER BY mode,instrument,event_time",
            (batch_id,),
        ).fetchall()
        groups = {}
        for changed in changed_events:
            groups.setdefault((changed["mode"], changed["instrument"]), []).append(
                changed["event_time"]
            )
        for (mode, instrument), anchors in groups.items():
            # A normal chronological micro-batch is dense. Probe times only,
            # bounded by n+1, so a sparse old/new pair cannot scan the middle.
            probe = conn.execute(
                "SELECT DISTINCT ON(event_time) event_time FROM observations "
                "WHERE mode=%s AND instrument=%s AND event_time BETWEEN %s AND %s "
                "ORDER BY event_time,observed_at DESC,revision DESC LIMIT %s",
                (mode, instrument, anchors[0], anchors[-1], len(anchors) + 1),
            ).fetchall()
            dense = [row["event_time"] for row in probe] == anchors
            if dense:
                history = canonical_rows(
                    conn,
                    mode,
                    instrument,
                    anchors[0],
                    anchors[-1],
                    predecessors=NEIGHBORS,
                    successors=NEIGHBORS,
                    max_middle_rows=len(anchors),
                )
                merged = [
                    {
                        "anchors": anchors,
                        "rows": {
                            pd.Timestamp(item["event_time"]).to_pydatetime(): item
                            for item in history
                        },
                    }
                ]
                neighborhoods = ()
            else:
                merged = []
                neighborhoods = _neighborhoods(conn, mode, instrument, anchors)
            for anchor, neighborhood in neighborhoods:
                if not neighborhood:
                    raise RuntimeError("Changed observation is absent from the canonical store")
                first, last = neighborhood[0]["event_time"], neighborhood[-1]["event_time"]
                if not merged or first > merged[-1]["last"]:
                    merged.append(
                        {
                            "anchors": [anchor],
                            "last": last,
                            "rows": {row["event_time"]: row["payload"] for row in neighborhood},
                        }
                    )
                else:
                    merged[-1]["anchors"].append(anchor)
                    merged[-1]["last"] = max(merged[-1]["last"], last)
                    merged[-1]["rows"].update(
                        {row["event_time"]: row["payload"] for row in neighborhood}
                    )
            for group in merged:
                first_changed, last_changed = min(group["anchors"]), max(group["anchors"])
                ranges.append(
                    {
                        "query_name": "ingest-v2",
                        "batch_id": batch_id,
                        "mode": mode,
                        "instrument": instrument,
                        "start_time": first_changed,
                        "end_time": last_changed,
                        "changed_times": group["anchors"],
                    }
                )
                for when, payload in group["rows"].items():
                    key = (mode, instrument, when)
                    record = dict(payload)
                    record["emit"] = when >= first_changed
                    if key in all_rows:
                        record["emit"] |= all_rows[key]["emit"]
                    all_rows[key] = record
                    if len(all_rows) > MAX_FEATURE_CONTEXT_ROWS:
                        raise RuntimeError(
                            "Affected neighborhoods exceed local row bound; split backfill batches"
                        )
    return [record for _, record in sorted(all_rows.items())], ranges


def bars_context(ranges):
    all_rows, grouped = {}, {}
    for changed in ranges:
        grouped.setdefault((changed["mode"], changed["instrument"]), []).append(changed)
    with connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        for (mode, instrument), changed_ranges in grouped.items():
            sample = conn.execute(
                "SELECT payload FROM observations WHERE mode=%s AND instrument=%s LIMIT 1",
                (mode, instrument),
            ).fetchone()["payload"]
            freq = sample["frequency"]
            intervals = []
            for changed in changed_ranges:
                anchors = changed.get("changed_times")
                if anchors is not None:
                    intervals.extend(_bucket_bounds(freq, when) for when in anchors)
                else:
                    first = _bucket_bounds(freq, changed["start_time"])[0]
                    last = _bucket_bounds(freq, changed["end_time"])[1]
                    intervals.append((first, last))
            merged = []
            for start, end in sorted(set(intervals)):
                if merged and start <= merged[-1][1] + pd.Timedelta(microseconds=1):
                    merged[-1] = (merged[-1][0], max(merged[-1][1], end))
                else:
                    merged.append((start, end))
            for start, end in merged:
                remaining = MAX_BAR_CONTEXT_ROWS - len(all_rows)
                if remaining < 1:
                    raise RuntimeError(
                        "Affected bars exceed local row bound; split backfill batches"
                    )
                history = canonical_rows(
                    conn, mode, instrument, start, end, max_middle_rows=remaining
                )
                for payload in history:
                    key = (mode, instrument, pd.Timestamp(payload["event_time"]))
                    all_rows[key] = payload
    return [record for _, record in sorted(all_rows.items())]


def _bucket_bounds(frequency, when):
    when = pd.Timestamp(when)
    if frequency == "monthly":
        start = when.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start + pd.offsets.MonthBegin(1)
    else:
        start = when.floor("min" if frequency == "tick" else "D")
        end = start + pd.Timedelta(minutes=1 if frequency == "tick" else 1440)
    return start.to_pydatetime(), (end - pd.Timedelta(microseconds=1)).to_pydatetime()


def write_signals(rows, generation=0):
    batch = []
    for row in rows:
        payload = json.loads(row["payload"])
        batch.append(
            (
                payload["mode"],
                payload["instrument"],
                payload["event_id"],
                payload["event_time"],
                Jsonb(payload),
            )
        )
        if len(batch) >= 1000:
            _upsert("signals_v2", batch, generation)
            batch = []
    if batch:
        _upsert("signals_v2", batch, generation)


def write_bars(rows, generation=0):
    batch = []
    for row in rows:
        payload = json.loads(row["payload"])
        batch.append(
            (
                payload["mode"],
                payload["instrument"],
                payload["bar_start"],
                payload["frequency"],
                Jsonb(payload),
            )
        )
        if len(batch) >= 1000:
            _upsert("market_bars_v2", batch, generation)
            batch = []
    if batch:
        _upsert("market_bars_v2", batch, generation)


def _upsert(table, rows, generation=0):
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 0:
        raise ValueError("Materialization generation must be a non-negative integer")
    rows = [(*row, generation) for row in rows]
    with connect() as conn:
        with conn.cursor() as cursor:
            if table == "signals_v2":
                cursor.executemany(
                    "INSERT INTO signals_v2(mode,instrument,event_id,event_time,payload,generation) "
                    "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(mode,instrument,event_time) "
                    "DO UPDATE SET event_id=excluded.event_id,payload=excluded.payload,generation=excluded.generation "
                    "WHERE excluded.generation>=signals_v2.generation",
                    rows,
                )
            else:
                cursor.executemany(
                    "INSERT INTO market_bars_v2(mode,instrument,bar_start,frequency,payload,generation) "
                    "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(mode,instrument,bar_start) "
                    "DO UPDATE SET frequency=excluded.frequency,payload=excluded.payload,generation=excluded.generation "
                    "WHERE excluded.generation>=market_bars_v2.generation",
                    rows,
                )


def complete_batch(batch_id, stats):
    generation = stats.get("generation")
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 0:
        raise ValueError("Completed batch requires a non-negative generation in stats")
    with connect() as conn:
        current = conn.execute(
            "SELECT generation FROM materialization_epoch_v2 WHERE singleton FOR UPDATE"
        ).fetchone()["generation"]
        if generation != current:
            raise RuntimeError("Obsolete materialization generation cannot complete a batch")
        conn.execute(
            "INSERT INTO completed_batches_v2(query_name,batch_id) VALUES ('ingest-v2',%s) ON CONFLICT DO NOTHING",
            (batch_id,),
        )
        conn.execute(
            "INSERT INTO pipeline_runs VALUES ('ingest-v2',%s,%s) ON CONFLICT(query_name,batch_id) "
            "DO UPDATE SET payload=excluded.payload",
            (batch_id, Jsonb({**stats, "completed_at": datetime.now(UTC).isoformat()})),
        )
