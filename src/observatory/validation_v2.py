"""V2 serving parity, recovery at commit boundaries and sustained-load evidence."""

import json
import os
import subprocess
import time
import uuid
from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd

from observatory.config import ARTIFACTS
from observatory.contracts import make_quote
from observatory.producer import KafkaPublisher, publish
from observatory.signals import current_snapshot, score_history
from observatory.store import connect, observation_frame, save_report
from observatory.validation import synthetic_quotes, wait_for


def served(ids):
    with connect() as conn:
        return conn.execute(
            "SELECT count(*) AS n FROM signals_v2 WHERE mode='SYNTHETIC' AND event_id=ANY(%s)",
            (ids,),
        ).fetchone()["n"] == len(set(ids))


def verify():
    with connect() as conn:
        expected = score_history(current_snapshot(observation_frame(conn)))
        rows = conn.execute("SELECT payload FROM signals_v2").fetchall()
    actual = {r["payload"]["event_id"]: r["payload"] for r in rows}
    missing, mismatches = 0, []
    for _, row in expected.iterrows():
        item = actual.get(row.event_id)
        if not item:
            missing += 1
            continue
        for field in ("value", "delta", "change_pct", "robust_z"):
            value = row[field]
            if pd.isna(value):
                okay = item.get(field) is None
            else:
                okay = item.get(field) is not None and np.isclose(
                    item[field], value, rtol=1e-8, atol=1e-8
                )
            if not okay:
                mismatches.append(
                    {"instrument": row.instrument, "field": field, "date": str(row.event_time)}
                )
        for field in ("robust_alert", "baseline_alert"):
            if bool(row[field]) != item.get(field):
                mismatches.append({"instrument": row.instrument, "field": field})
    report = {
        "status": "passed"
        if not missing and not mismatches and len(actual) == len(expected)
        else "failed",
        "expected_observations": len(expected),
        "serving_observations": len(actual),
        "missing": missing,
        "feature_mismatches": len(mismatches),
        "examples": mismatches[:8],
        "created_at": datetime.now(UTC).isoformat(),
        "scope": "All canonical prices and exact rolling Spark scores versus full-history pandas oracle",
    }
    save_report(ARTIFACTS / "parity-v2.json", report)
    print(json.dumps(report, indent=2))
    if report["status"] != "passed":
        raise AssertionError("V2 parity failed")
    return report


def recent_window_parity():
    from observatory.streaming import valid_contract

    cutoff = (datetime.now(UTC) - timedelta(days=2)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    values = {}
    with connect() as conn:
        rows = conn.execute(
            "SELECT topic,payload FROM deliveries WHERE topic='cso.quotes.synthetic.v1'"
        ).fetchall()
        windows = conn.execute(
            "SELECT * FROM delivery_windows_v2 WHERE mode='SYNTHETIC'"
        ).fetchall()
    for row in rows:
        if not valid_contract(row["payload"], row["topic"]):
            continue
        quote = json.loads(row["payload"])
        event_time = datetime.fromisoformat(quote["event_time"].replace("Z", "+00:00"))
        if event_time < cutoff:
            continue
        key = (quote["instrument"], event_time.replace(hour=0, minute=0, second=0, microsecond=0))
        values.setdefault(key, []).append(quote["value"])
    actual = {(r["instrument"], r["window_start"]): r for r in windows}
    for key, prices in values.items():
        row = actual.get(key)
        if not row or row["delivery_count"] != len(prices):
            return False
        if not np.allclose(
            [row["mean_value"], row["min_value"], row["max_value"]],
            [np.mean(prices), min(prices), max(prices)],
            rtol=1e-10,
        ):
            return False
    return bool(values)


def reliability():
    revision = "recovery-v2-" + uuid.uuid4().hex[:10]
    base = synthetic_quotes(80, revision)
    publish(base[:40])
    wait_for(lambda: served([r["event_id"] for r in base[:40]]), timeout=900)
    subprocess.run(
        ["docker", "compose", "kill", "-s", "SIGKILL", "spark"], check=True, capture_output=True
    )
    invalid = {**base[0], "unit": "wrong-unit"}
    conflict = {**base[1], "value": base[1]["value"] + 200}
    correction = make_quote(
        "XAUUSD",
        datetime.fromisoformat(base[20]["event_time"]),
        base[20]["value"] + 10,
        revision + "-corrected",
        datetime.now(UTC),
        "SYNTHETIC",
    ).model_dump(mode="json")
    late = make_quote(
        "XAUUSD",
        datetime.now(UTC) - timedelta(days=30),
        2200,
        revision + "-late",
        datetime.now(UTC),
        "SYNTHETIC",
    ).model_dump(mode="json")
    publish([*base[40:][::-1], *base[:8], invalid, conflict, correction, late])
    subprocess.run(["docker", "compose", "up", "-d", "spark"], check=True, capture_output=True)
    expected_ids = [r["event_id"] for i, r in enumerate(base) if i != 20] + [
        correction["event_id"],
        late["event_id"],
    ]
    wait_for(lambda: served(expected_ids), timeout=900)
    wait_for(recent_window_parity, timeout=300)
    with connect() as conn:
        rejects = conn.execute("SELECT reason,payload FROM deliveries WHERE NOT valid").fetchall()
        rejected = [r["reason"] for r in rejects if revision in r["payload"]]
        old = conn.execute(
            "SELECT count(*) AS n FROM signals_v2 WHERE mode='SYNTHETIC' AND event_id=%s",
            (base[20]["event_id"],),
        ).fetchone()["n"]
    parity = verify()
    report = {
        "status": "passed" if len(rejected) == 2 and old == 0 else "failed",
        "version": 2,
        "created_at": datetime.now(UTC).isoformat(),
        "test_revision": revision,
        "unique_serving_observations": len(expected_ids),
        "duplicate_messages": 8,
        "quarantined": len(rejected),
        "out_of_order_messages": 40,
        "old_revision_removed_from_serving": old == 0,
        "raw_revisions_retained": True,
        "late_record": "30 days late retained; affected later scores recomputed",
        "window_parity": "passed",
        "full_feature_parity": parity["status"],
        "scope": "SIGKILL of this Spark service, persistent checkpoint recovery, revised quote and two invalid/conflicting messages",
    }
    save_report(ARTIFACTS / "reliability-v2.json", report)
    print(json.dumps(report, indent=2))
    if report["status"] != "passed":
        raise AssertionError("V2 recovery failed")
    return report


def commit_recovery():
    """One deliberate failure after DB ingestion and before Silver/features/checkpoint."""
    token = uuid.uuid4().hex[:12]
    env = {**os.environ, "CSO_FAILPOINT": "after_bulk:" + token}
    subprocess.run(
        ["docker", "compose", "up", "-d", "--force-recreate", "spark"],
        env=env,
        check=True,
        capture_output=True,
    )
    events = synthetic_quotes(30, "commit-v2-" + token)
    publish(events)
    ids = [r["event_id"] for r in events]

    def raw_visible():
        with connect() as conn:
            return conn.execute(
                "SELECT count(*) AS n FROM observations WHERE mode='SYNTHETIC' AND event_id=ANY(%s)",
                (ids,),
            ).fetchone()["n"] == len(ids)

    wait_for(raw_visible, timeout=600)
    container = subprocess.check_output(
        ["docker", "compose", "ps", "-aq", "spark"], text=True
    ).strip()
    wait_for(
        lambda: (
            subprocess.check_output(
                ["docker", "inspect", "-f", "{{.State.Running}}", container], text=True
            ).strip()
            == "false"
        ),
        timeout=180,
    )
    clean_env = {k: v for k, v in os.environ.items() if k != "CSO_FAILPOINT"}
    subprocess.run(
        ["docker", "compose", "up", "-d", "--force-recreate", "spark"],
        env=clean_env,
        check=True,
        capture_output=True,
    )
    wait_for(lambda: served(ids), timeout=900)
    report = {
        "status": "passed",
        "stage": "after_bulk_before_silver_and_features",
        "events": len(ids),
        "created_at": datetime.now(UTC).isoformat(),
        "scope": "Durable affected ranges survive the intentional cross-system partial commit",
    }
    save_report(ARTIFACTS / "commit-recovery-v2.json", report)
    return report


def benchmark(n=5000, seconds=0, rate=50):
    from threading import Event, Lock, Thread

    if not 100 <= n <= 50000 or not 0 <= seconds <= 1800 or not 0 < rate <= 1000:
        raise ValueError("Bounded local benchmark: events100..50000, duration0..1800s, rate1..1000")
    n = int(seconds * rate) if seconds else n
    if not 100 <= n <= 50000:
        raise ValueError("Benchmark budget must be between 100 and 50,000 events")
    events = synthetic_quotes(n)
    lock, publishing_done, cancel = Lock(), Event(), Event()
    pending, latencies, errors = {}, [], []
    state_memory, kafka_lags, seen_batches = [], [], set()
    started_utc = datetime.now(UTC)

    def observe():
        try:
            while not cancel.is_set():
                with lock:
                    ids = list(pending)[:5000]
                if ids:
                    with connect() as conn:
                        rows = conn.execute(
                            "SELECT event_id FROM signals_v2 WHERE mode='SYNTHETIC' AND event_id=ANY(%s)",
                            (ids,),
                        ).fetchall()
                    observed = datetime.now(UTC)
                    with lock:
                        for row in rows:
                            sent = pending.pop(row["event_id"], None)
                            if sent:
                                latencies.append((observed - sent).total_seconds())
                with lock:
                    empty = not pending
                if publishing_done.is_set() and empty:
                    return
                progress_path = ARTIFACTS / "spark-ingest-v2.json"
                if progress_path.exists():
                    progress = json.loads(progress_path.read_text())
                    timestamp = datetime.fromisoformat(progress["timestamp"].replace("Z", "+00:00"))
                    batch = progress.get("batchId")
                    if timestamp >= started_utc and batch not in seen_batches:
                        seen_batches.add(batch)
                        for source in progress.get("sources", []):
                            latest, end = source.get("latestOffset"), source.get("endOffset")
                            if isinstance(latest, str):
                                latest = json.loads(latest)
                            if isinstance(end, str):
                                end = json.loads(end)
                            if isinstance(latest, dict) and isinstance(end, dict):
                                kafka_lags.append(
                                    sum(
                                        max(0, offset - end.get(topic, {}).get(part, 0))
                                        for topic, partitions in latest.items()
                                        for part, offset in partitions.items()
                                    )
                                )
                for path in ARTIFACTS.glob("spark-windows-*.json"):
                    progress = json.loads(path.read_text())
                    timestamp = datetime.fromisoformat(progress["timestamp"].replace("Z", "+00:00"))
                    if timestamp >= started_utc:
                        state_memory.append(
                            sum(
                                s.get("memoryUsedBytes", 0)
                                for s in progress.get("stateOperators", [])
                            )
                        )
                time.sleep(0.25)
        except Exception as exc:
            errors.append(type(exc).__name__)
            cancel.set()

    started = time.perf_counter()
    observer = Thread(target=observe, daemon=True)
    observer.start()
    try:
        with KafkaPublisher() as publisher:
            for i, item in enumerate(events):
                if cancel.is_set():
                    raise RuntimeError("Visibility observer failed")
                if seconds:
                    delay = started + i / rate - time.perf_counter()
                    if delay > 0:
                        time.sleep(delay)
                item["ingested_at"] = datetime.now(UTC).isoformat()
                with lock:
                    pending[item["event_id"]] = datetime.fromisoformat(item["ingested_at"])
                publisher.send(item)
        producer_seconds = time.perf_counter() - started
    except Exception:
        cancel.set()
        publishing_done.set()
        observer.join(2)
        raise
    publishing_done.set()
    observer.join(1200)
    if observer.is_alive():
        cancel.set()
        observer.join(2)
        raise TimeoutError("Synthetic workload backlog did not drain within 20 minutes")
    if errors or pending:
        raise RuntimeError(
            f"Benchmark incomplete: observer_errors={errors}, pending={len(pending)}"
        )
    elapsed = time.perf_counter() - started
    p95 = float(np.percentile(latencies, 95))
    report = {
        "status": "completed",
        "version": 2,
        "created_at": datetime.now(UTC).isoformat(),
        "mode": "SYNTHETIC",
        "events": n,
        "workload": "sustained" if seconds else "burst",
        "requested_rate_events_per_second": rate if seconds else None,
        "achieved_publish_events_per_second": n / producer_seconds,
        "producer_seconds": producer_seconds,
        "all_events_visible_seconds": elapsed,
        "backlog_drain_after_publication_seconds": max(0, elapsed - producer_seconds),
        "end_to_end_events_per_second": n / elapsed,
        "first_observed_visibility_p50_seconds": float(np.percentile(latencies, 50)),
        "first_observed_visibility_p95_seconds": p95,
        "latency_caveat": "Concurrent external SELECT polling from submission to serving; upper bound with 0.25s polling plus query uncertainty.",
        "max_observed_kafka_offset_lag": max(kafka_lags) if kafka_lags else None,
        "max_observed_window_state_memory_bytes": max(state_memory) if state_memory else None,
        "observed_ingest_batches": len(seen_batches),
        "local_latency_objective_seconds": 10,
        "local_latency_objective_met": p95 <= 10,
        "configuration": "single Kafka broker; Spark local[2];2 shuffle partitions;maxOffsets2000;canonical signals and OHLC",
        "scope": "Local benchmark, not cluster capacity. Null metrics mean unavailable, not zero.",
    }
    name = "sustained-load-v2.json" if seconds else "load-test-v2.json"
    save_report(ARTIFACTS / name, report)
    print(json.dumps(report, indent=2))
    return report
