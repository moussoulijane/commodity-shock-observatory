"""Executable Spark oracle and watermark-isolation checks; no market API credentials."""

import json
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from observatory.config import ARTIFACTS
from observatory.contracts import make_quote
from observatory.signals import score_history
from observatory.store import save_report


def spark_oracle(spark):
    from observatory.spark_features import bars, frame, score

    rng = np.random.default_rng(2042)
    records = []
    for instrument, frequency, n in [
        ("WTI", "daily", 240),
        ("GOLD", "monthly", 160),
        ("XAUUSD", "tick", 250),
    ]:
        values = 100 + rng.normal(0, 0.4, n).cumsum()
        if instrument == "WTI":
            values[80:84] = [10, -37.63, 0, 12]
        for i, value in enumerate(values):
            start = datetime(2000, 1, 1, tzinfo=UTC)
            timestamp = (
                start + timedelta(days=i * 31)
                if frequency == "monthly"
                else start + timedelta(days=i)
                if frequency == "daily"
                else start + timedelta(microseconds=i * 137)
            )
            records.append(
                make_quote(
                    instrument, timestamp, float(value), "oracle-v2", datetime.now(UTC), "SYNTHETIC"
                ).model_dump(mode="json")
            )
    expected = pd.DataFrame(records)
    for key in ("event_time", "observed_at"):
        expected[key] = pd.to_datetime(expected[key], format="ISO8601", utc=True)
    expected = score_history(expected).set_index("event_id")
    actual = [json.loads(r.payload) for r in score(frame(spark, records)).toLocalIterator()]
    errors = []
    for row in actual:
        reference = expected.loc[row["event_id"]]
        for field in ("delta", "change_pct", "robust_z"):
            wanted = reference[field]
            if pd.isna(wanted):
                if row[field] is not None:
                    errors.append((row["event_id"], field))
            elif row[field] is None or not np.isclose(row[field], wanted, rtol=1e-9, atol=1e-9):
                errors.append((row["event_id"], field))
        for field in ("baseline_alert", "robust_alert"):
            if row[field] != bool(reference[field]):
                errors.append((row["event_id"], field))
        if pd.Timestamp(row["event_time"]) != reference.event_time:
            errors.append((row["event_id"], "microsecond_timestamp"))
    if errors:
        raise AssertionError(f"Spark oracle mismatches: {errors[:10]}")
    # Partial context after a correction must equal the complete-history batch oracle.
    subset = [
        dict(row, emit=(index >= 150))
        for index, row in enumerate(records[:240])
        if index >= 88 and index <= 212
    ]
    partial = [json.loads(r.payload) for r in score(frame(spark, subset)).toLocalIterator()]
    for row in partial:
        assert np.isclose(
            row["robust_z"], expected.loc[row["event_id"], "robust_z"], rtol=1e-9, atol=1e-9
        )
    price_bars = [json.loads(r.payload) for r in bars(frame(spark, records)).toLocalIterator()]
    tick_bar = next(r for r in price_bars if r["instrument"] == "XAUUSD")
    ticks = [r for r in records if r["instrument"] == "XAUUSD"]
    assert tick_bar["observation_count"] == len(ticks)
    assert tick_bar["open"] == ticks[0]["value"]
    assert tick_bar["close"] == ticks[-1]["value"]
    assert tick_bar["high"] == max(r["value"] for r in ticks)
    assert tick_bar["low"] == min(r["value"] for r in ticks)
    return {
        "status": "passed",
        "oracle_observations": len(actual),
        "bounded_context_outputs": len(partial),
        "comparisons": [
            "exact median/MAD parity",
            "negative and zero prices",
            "monthly window",
            "microsecond precision",
            "canonical OHLC",
        ],
    }


def watermark_isolation(spark):
    from pyspark.sql import functions as F

    namespace = "isolation_" + uuid.uuid4().hex[:8]
    queries = []
    with tempfile.TemporaryDirectory() as root:
        paths = {mode: Path(root) / mode for mode in ("REPLAY", "SYNTHETIC")}
        for mode, path in paths.items():
            path.mkdir()
            df = (
                spark.readStream.schema("event_time timestamp, value double")
                .json(str(path))
                .withWatermark("event_time", "7 days")
                .groupBy(F.window("event_time", "1 day"))
                .agg(F.count("*").alias("n"))
            )
            queries.append(
                df.writeStream.format("memory")
                .queryName(namespace + mode)
                .outputMode("update")
                .start()
            )

        def send(mode, filename, event_time):
            (paths[mode] / filename).write_text(
                json.dumps({"event_time": event_time, "value": 1}), encoding="utf-8"
            )

        try:
            send("REPLAY", "a.json", "2010-01-01T00:00:00Z")
            send("SYNTHETIC", "a.json", "2025-01-01T00:00:00Z")
            for q in queries:
                q.processAllAvailable()
            send("SYNTHETIC", "b.json", "2026-01-01T00:00:00Z")
            queries[1].processAllAvailable()
            send("REPLAY", "b.json", "2010-01-02T00:00:00Z")
            queries[0].processAllAvailable()
            rows = spark.sql(
                f"SELECT window.start AS day,max(n) AS n FROM {namespace}REPLAY GROUP BY window.start"
            ).collect()
            assert any(r.day.year == 2010 and r.day.day == 2 and r.n == 1 for r in rows)
            watermark = queries[0].lastProgress.get("eventTime", {}).get("watermark")
            assert watermark and watermark.startswith("2009-")
            return {
                "status": "passed",
                "replay_watermark": watermark,
                "scope": "Actual independent Spark file-stream queries, 16-year clock separation; no fake data enters market topics",
            }
        finally:
            for q in queries:
                q.stop()


def main():
    from observatory.streaming import session

    spark = session()
    try:
        report = {
            "created_at": datetime.now(UTC).isoformat(),
            "spark_oracle": spark_oracle(spark),
            "watermark_isolation": watermark_isolation(spark),
        }
        save_report(ARTIFACTS / "spark-checks-v2.json", report)
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
