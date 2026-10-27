"""Opt-in PostgreSQL integration tests, isolated from public application tables.

Set CSO_DB_TESTS=1 with this project's PostgreSQL service available. Each test
creates and drops only its own randomly named schema; all connections use that
schema exclusively, including materialization sequence/epoch metadata.
"""

import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from observatory import incremental_store as target
from observatory import store
from observatory.contracts import make_quote

START = datetime(2020, 1, 1, tzinfo=UTC)
OBSERVED = datetime(2025, 1, 1, tzinfo=UTC)


@pytest.fixture
def integration_db(monkeypatch):
    if os.getenv("CSO_DB_TESTS") != "1":
        pytest.skip("Set CSO_DB_TESTS=1 for isolated PostgreSQL integration tests")
    schema = "cso_v2_test_" + uuid.uuid4().hex
    base_connect = store.connect
    with base_connect() as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    def connect():
        conn = base_connect()
        conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        conn.commit()
        return conn

    monkeypatch.setattr(target, "connect", connect)
    monkeypatch.setattr(target, "init_v1", lambda: None)
    try:
        with connect() as conn:
            conn.execute(store.DDL)
            # Exercise migration of real pre-generation V2 tables, not just CREATE.
            conn.execute(
                "CREATE TABLE signals_v2(mode text NOT NULL,instrument text NOT NULL,"
                "event_id text NOT NULL,event_time timestamptz NOT NULL,payload jsonb NOT NULL,"
                "PRIMARY KEY(mode,instrument,event_time))"
            )
            conn.execute(
                "CREATE TABLE market_bars_v2(mode text NOT NULL,instrument text NOT NULL,"
                "bar_start timestamptz NOT NULL,frequency text NOT NULL,payload jsonb NOT NULL,"
                "PRIMARY KEY(mode,instrument,bar_start))"
            )
        target.init()
        yield connect
    finally:
        assert schema.startswith("cso_v2_test_") and len(schema) == len("cso_v2_test_") + 32
        with base_connect() as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def quote(index=0, value=100, revision="base", observed=OBSERVED):
    return make_quote(
        "XAUUSD", START + timedelta(seconds=index), value, revision, observed, "SYNTHETIC"
    ).model_dump(mode="json")


def delivery(payload, offset):
    return {
        "topic": "cso.quotes.synthetic.v1",
        "partition_id": 0,
        "offset_id": offset,
        "payload": json.dumps(payload),
    }


def seed_history(connect, count):
    with connect() as conn:
        with conn.cursor().copy(
            "COPY observations(mode,event_id,instrument,event_time,"
            "observed_at,revision,value,payload) FROM STDIN"
        ) as copy:
            for index in range(count):
                item = quote(index, 100 + index * 0.001)
                copy.write_row(
                    (
                        item["mode"],
                        item["event_id"],
                        item["instrument"],
                        item["event_time"],
                        item["observed_at"],
                        item["revision"],
                        item["value"],
                        json.dumps(item),
                    )
                )


def test_canonical_value_beats_conflicting_first_incoming_winner(integration_db):
    canonical = quote()
    target.ingest_batch([delivery(canonical, 0)], 1)
    conflicting = {**canonical, "value": 999}
    accepted, rejected, stats = target.ingest_batch(
        [delivery(conflicting, 1), delivery(canonical, 2)], 2
    )
    assert len(accepted) == 1 and accepted[0]["value"] == 100
    assert len(rejected) == 1 and rejected[0]["off"] == 1
    assert stats["new_events"] == 0
    with integration_db() as conn:
        rows = conn.execute("SELECT offset_id,valid FROM deliveries ORDER BY offset_id").fetchall()
    assert [r["valid"] for r in rows] == [True, False, True]


def test_without_canonical_first_kafka_offset_wins_and_retry_is_idempotent(integration_db):
    canonical = quote()
    conflicting = {**canonical, "value": 999}
    batch = [delivery(canonical, 2), delivery(conflicting, 1)]
    accepted, rejected, stats = target.ingest_batch(batch, 1)
    assert accepted[0]["value"] == 999
    assert rejected[0]["off"] == 2
    assert stats["new_events"] == 1
    retried, _, repeated = target.ingest_batch(batch, 1)
    assert retried[0]["value"] == 999
    assert repeated["new_events"] == 0
    with integration_db() as conn:
        assert conn.execute("SELECT count(*) AS n FROM observations").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM affected_events_v2").fetchone()["n"] == 1


def test_sparse_ancient_and_current_updates_do_not_read_middle_history(integration_db):
    seed_history(integration_db, 30050)
    corrections = [
        quote(0, 101, "old-correction", OBSERVED + timedelta(seconds=1)),
        quote(30049, 140, "new-correction", OBSERVED + timedelta(seconds=1)),
    ]
    target.ingest_batch([delivery(item, i) for i, item in enumerate(corrections)], 5)
    records, ranges = target.feature_context(5)
    assert len(records) == 126  # 63 at either dataset edge, not 30,050 middle rows.
    assert len(ranges) == 2
    assert len({(r["mode"], r["instrument"], r["event_time"]) for r in records}) == 126
    assert {
        r["value"] for r in records if r["event_id"] in {q["event_id"] for q in corrections}
    } == {101, 140}
    bars = target.bars_context(ranges)
    assert len(bars) == 110  # First complete minute and final partial minute only.
    assert len({r["event_time"] for r in bars}) == 110


def test_overlapping_neighborhoods_and_same_bar_are_deduplicated(integration_db):
    seed_history(integration_db, 400)
    corrections = [
        quote(i, 150 + i, "correction", OBSERVED + timedelta(seconds=1)) for i in (100, 110)
    ]
    target.ingest_batch([delivery(item, i) for i, item in enumerate(corrections)], 8)
    records, ranges = target.feature_context(8)
    assert len(ranges) == 1
    assert len(records) == 135  # Canonical positions 38..172 inclusive.
    assert ranges[0]["start_time"] == START + timedelta(seconds=100)
    assert ranges[0]["end_time"] == START + timedelta(seconds=110)
    assert len(ranges[0]["changed_times"]) == 2
    assert sum(r["emit"] for r in records) == 73
    assert len(target.bars_context(ranges)) == 60


def test_dense_two_thousand_anchor_batch_fetches_one_context(integration_db, monkeypatch):
    seed_history(integration_db, 3000)
    corrections = [
        quote(i, 150 + i * 0.001, "dense-correction", OBSERVED + timedelta(seconds=1))
        for i in range(100, 2100)
    ]
    target.ingest_batch([delivery(item, i) for i, item in enumerate(corrections)], 9)

    def sparse_fallback(*_):
        raise AssertionError(
            "A dense chronological batch must not expand 2,000 lateral neighborhoods"
        )

    monkeypatch.setattr(target, "_neighborhoods", sparse_fallback)
    records, ranges = target.feature_context(9)
    assert len(records) == 2124
    assert len(ranges) == 1 and len(ranges[0]["changed_times"]) == 2000
    assert sum(record["emit"] for record in records) == 2062
    assert len({r["event_time"] for r in records}) == 2124
    bars = target.bars_context(ranges)
    assert len({r["event_time"] for r in bars}) == len(bars)


def test_canonical_read_limit_is_applied_before_fetching_whole_interval(integration_db):
    seed_history(integration_db, 100)
    with integration_db() as conn, pytest.raises(RuntimeError, match="bounded context"):
        target.canonical_rows(
            conn, "SYNTHETIC", "XAUUSD", START, START + timedelta(seconds=100), max_middle_rows=10
        )


def signal(value):
    return {
        "payload": json.dumps(
            {
                "mode": "SYNTHETIC",
                "instrument": "XAUUSD",
                "event_time": START.isoformat(),
                "event_id": "a" * 32,
                "value": value,
            }
        )
    }


def bar(value):
    return {
        "payload": json.dumps(
            {
                "mode": "SYNTHETIC",
                "instrument": "XAUUSD",
                "bar_start": START.isoformat(),
                "frequency": "tick",
                "close": value,
            }
        )
    }


def test_new_generation_fences_stale_executor_writes_and_completion(integration_db):
    with target.materialization_lease() as first:
        target.write_signals([signal(100)], generation=first)
        target.write_bars([bar(100)], generation=first)
        target.complete_batch(1, {"generation": first})
    with target.materialization_lease() as second:
        assert second > first
        target.write_signals([signal(200)], generation=second)
        target.write_bars([bar(200)], generation=second)
        target.complete_batch(2, {"generation": second})
    target.write_signals([signal(100)], generation=first)
    target.write_bars([bar(100)], generation=first)
    with integration_db() as conn:
        saved_signal = conn.execute("SELECT payload,generation FROM signals_v2").fetchone()
        saved_bar = conn.execute("SELECT payload,generation FROM market_bars_v2").fetchone()
    assert saved_signal["payload"]["value"] == 200 and saved_signal["generation"] == second
    assert saved_bar["payload"]["close"] == 200 and saved_bar["generation"] == second
    with pytest.raises(RuntimeError, match="Obsolete"):
        target.complete_batch(3, {"generation": first})
    with integration_db() as conn:
        assert not conn.execute("SELECT 1 FROM completed_batches_v2 WHERE batch_id=3").fetchone()


def test_session_lease_serializes_whole_materialization(integration_db):
    entered_first, release_first, entered_second = (
        threading.Event(),
        threading.Event(),
        threading.Event(),
    )

    def first_holder():
        with target.materialization_lease() as generation:
            entered_first.set()
            assert release_first.wait(10)
            return generation

    def second_holder():
        with target.materialization_lease() as generation:
            entered_second.set()
            return generation

    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(first_holder)
        assert entered_first.wait(30)
        second = workers.submit(second_holder)
        try:
            assert not entered_second.wait(0.2)
        finally:
            release_first.set()
        assert second.result(timeout=10) > first.result(timeout=10)


def test_legacy_generation_migration_keeps_default_zero(integration_db):
    with integration_db() as conn:
        conn.execute(
            "INSERT INTO signals_v2(mode,instrument,event_id,event_time,payload) VALUES "
            "('SYNTHETIC','XAUUSD','legacy',%s,%s)",
            (START, Jsonb({"legacy": True})),
        )
    target.init()
    with integration_db() as conn:
        row = conn.execute("SELECT generation,payload FROM signals_v2").fetchone()
    assert row["generation"] == 0 and row["payload"] == {"legacy": True}
