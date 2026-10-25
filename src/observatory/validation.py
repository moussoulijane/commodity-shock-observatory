import json
import subprocess
import time
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd

from observatory.config import ARTIFACTS, DATA
from observatory.contracts import Quote, make_quote
from observatory.producer import publish
from observatory.signals import current_snapshot, score_history
from observatory.store import connect, observation_frame, records, save_report


def wait_for(predicate, timeout=480):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(2)
    raise TimeoutError(
        "Pipeline did not reach the required state; inspect docker compose logs spark"
    )


def contains_all(ids):
    with connect() as conn:
        result = conn.execute(
            "SELECT count(*) AS n FROM observations WHERE mode='SYNTHETIC' AND event_id=ANY(%s)",
            (ids,),
        ).fetchone()
    return result["n"] == len(set(ids))


def recent_window_parity():
    """Independent Python aggregate for recent synthetic contract-valid deliveries."""
    from observatory.streaming import valid_contract

    expected = {}
    cutoff = (datetime.now(UTC) - timedelta(days=2)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    with connect() as conn:
        deliveries = conn.execute(
            "SELECT topic,payload FROM deliveries WHERE topic='cso.quotes.synthetic.v1'"
        ).fetchall()
        windows = conn.execute("SELECT * FROM delivery_windows WHERE mode='SYNTHETIC'").fetchall()
    for row in deliveries:
        if not valid_contract(row["payload"], row["topic"]):
            continue
        event = Quote.model_validate_json(row["payload"])
        if event.event_time < cutoff:
            continue
        day = event.event_time.replace(hour=0, minute=0, second=0, microsecond=0)
        expected.setdefault((event.instrument, day), []).append(event.value)
    actual = {(r["instrument"], r["window_start"]): r for r in windows}
    if not expected:
        return False
    for key, values in expected.items():
        if key not in actual:
            return False
        row = actual[key]
        if row["delivery_count"] != len(values):
            return False
        if not np.allclose(
            [row["mean_value"], row["min_value"], row["max_value"]],
            [np.mean(values), min(values), max(values)],
            rtol=1e-10,
        ):
            return False
    return True


def synthetic_quotes(n, revision=None):
    revision = revision or "load-" + uuid.uuid4().hex[:12]
    now = datetime.now(UTC)
    start = now - timedelta(seconds=n + 300)
    rng = np.random.default_rng(17)
    levels = 2400 + np.cumsum(rng.normal(0, 0.4, n))
    return [
        make_quote(
            "XAUUSD", start + timedelta(seconds=i), float(value), revision, now, "SYNTHETIC"
        ).model_dump(mode="json")
        for i, value in enumerate(levels)
    ]


def verify():
    """Independent file-to-serving price comparison, after Kafka/Spark processing."""
    expected = [
        Quote.model_validate_json(line).model_dump(mode="json")
        for line in (DATA / "quotes.jsonl").read_text().splitlines()
        if line.strip()
    ]
    frame = pd.DataFrame(expected)
    frame = frame.sort_values(["observed_at", "revision"]).drop_duplicates(
        ["instrument", "event_time"], keep="last"
    )
    with connect() as conn:
        rows = conn.execute(
            "SELECT instrument,event_time,payload FROM signals WHERE mode='REPLAY'"
        ).fetchall()
        delivery = conn.execute(
            "SELECT count(*) AS n,count(*) FILTER(WHERE NOT valid) AS invalid FROM deliveries "
            "WHERE topic='cso.quotes.replay.v1'"
        ).fetchone()
    actual = {(r["instrument"], r["event_time"].isoformat()): r["payload"]["value"] for r in rows}
    missing, mismatched = [], []
    for row in frame.to_dict("records"):
        key = (row["instrument"], datetime.fromisoformat(row["event_time"]).isoformat())
        if key not in actual:
            missing.append(key)
        elif not np.isclose(actual[key], row["value"], rtol=0, atol=1e-10):
            mismatched.append(key)
    report = {
        "status": "passed" if not missing and not mismatched else "failed",
        "expected_observations": len(frame),
        "serving_observations": len(actual),
        "missing": len(missing),
        "price_mismatches": len(mismatched),
        "deliveries": delivery,
        "scope": "Downloaded current snapshot versus canonical serving prices; not macro accuracy",
        "created_at": datetime.now(UTC).isoformat(),
    }
    save_report(ARTIFACTS / "batch-stream-parity.json", report)
    print(json.dumps(report, indent=2))
    if report["status"] != "passed":
        raise AssertionError("Snapshot parity failed")
    return report


def reliability():
    revision = "reliability-" + uuid.uuid4().hex[:10]
    base = synthetic_quotes(80, revision)
    ids = [r["event_id"] for r in base]
    publish(base[:40])
    wait_for(lambda: contains_all(ids[:40]))
    # Kill only THIS compose service. Durable checkpoints and volumes are preserved.
    subprocess.run(
        ["docker", "compose", "kill", "-s", "SIGKILL", "spark"], check=True, capture_output=True
    )
    invalid = {**base[0], "unit": "wrong-unit"}
    conflict = {**base[1], "value": base[1]["value"] + 200}
    late = make_quote(
        "XAUUSD",
        datetime.now(UTC) - timedelta(days=30),
        2200,
        revision + "-late",
        datetime.now(UTC),
        "SYNTHETIC",
    ).model_dump(mode="json")
    # Deliver an older event after newer ones, and send duplicate identities.
    publish([*base[40:][::-1], *base[:8], invalid, conflict, late])
    subprocess.run(["docker", "compose", "up", "-d", "spark"], check=True, capture_output=True)
    wait_for(lambda: contains_all([*ids, late["event_id"]]))
    wait_for(recent_window_parity)
    with connect() as conn:
        quarantined = conn.execute(
            "SELECT reason,payload FROM deliveries WHERE NOT valid"
        ).fetchall()
        reasons = Counter()
        for row in quarantined:
            try:
                payload = json.loads(row["payload"])
            except (ValueError, TypeError):
                continue
            if isinstance(payload, dict) and payload.get("revision") == revision:
                reasons[row["reason"]] += 1
        rejects = [{"reason": reason, "n": n} for reason, n in sorted(reasons.items())]
        duplicate_count = conn.execute(
            "SELECT count(*) AS n FROM observations WHERE mode='SYNTHETIC' AND event_id=ANY(%s)",
            (ids,),
        ).fetchone()["n"]
        # Independent delivery aggregate for the controlled, non-late portion.
        grouped = conn.execute(
            "SELECT date_trunc('day',event_time) AS day,count(*) AS n "
            "FROM observations WHERE mode='SYNTHETIC' AND event_id=ANY(%s) GROUP BY 1",
            (ids,),
        ).fetchall()
    passed = duplicate_count == 80 and sum(r["n"] for r in rejects) == 2
    report = {
        "status": "passed" if passed else "failed",
        "test_revision": revision,
        "kill": "SIGKILL spark; restart with persistent checkpoints",
        "canonical_expected": 81,
        "canonical_found": duplicate_count + 1,
        "duplicate_messages_sent": 8,
        "invalid_or_conflicting_messages": rejects,
        "out_of_order": "40 valid observations in reverse order",
        "stateful_window_parity": "passed: counts / mean / min / max versus independent delivery aggregate",
        "late_record": "30 days late retained in canonical store; telemetry may drop it",
        "observation_day_counts": [{"day": r["day"].isoformat(), "n": r["n"]} for r in grouped],
        "created_at": datetime.now(UTC).isoformat(),
        "scope": "Single local broker/driver process recovery; not broker HA or distributed exactly-once",
    }
    save_report(ARTIFACTS / "reliability.json", report)
    print(json.dumps(report, indent=2))
    if not passed:
        raise AssertionError("Reliability exercise failed")
    return report


def load_test(n=5000):
    if n < 100 or n > 50000:
        raise ValueError("Local bounded test supports 100..50000 events")
    events = synthetic_quotes(n)
    started = time.perf_counter()
    publish(events)
    published_seconds = time.perf_counter() - started
    pending = {r["event_id"]: datetime.fromisoformat(r["ingested_at"]) for r in events}
    latencies = []
    deadline = time.monotonic() + 900
    while pending and time.monotonic() < deadline:
        with connect() as conn:
            visible = conn.execute(
                "SELECT event_id FROM observations WHERE mode='SYNTHETIC' AND event_id=ANY(%s)",
                (list(pending),),
            ).fetchall()
        observed_at = datetime.now(UTC)
        for row in visible:
            latencies.append((observed_at - pending.pop(row["event_id"])).total_seconds())
        if pending:
            time.sleep(0.5)
    if pending:
        raise TimeoutError(f"{len(pending)} synthetic events not visible before deadline")
    seconds = time.perf_counter() - started
    report = {
        "status": "completed",
        "mode": "SYNTHETIC",
        "events": n,
        "producer_seconds": published_seconds,
        "all_events_visible_seconds": seconds,
        "end_to_end_events_per_second": n / seconds,
        "first_observed_visibility_p50_seconds": float(np.percentile(latencies, 50)),
        "first_observed_visibility_p95_seconds": float(np.percentile(latencies, 95)),
        "latency_caveat": "From event construction to first external SELECT visibility; includes publication and 0.5s polling plus query time. Not intrinsic Spark processing latency.",
        "configuration": "Docker Desktop, Spark local[2], 2 shuffle partitions, maxOffsetsPerTrigger=2000, single Kafka broker",
        "created_at": datetime.now(UTC).isoformat(),
    }
    save_report(ARTIFACTS / "load-test.json", report)
    print(json.dumps(report, indent=2))
    return report


def evaluate_detectors():
    with connect() as conn:
        frame = current_snapshot(observation_frame(conn, "REPLAY"))
    scored = score_history(frame)
    real = []
    if not scored.empty:
        for name, group in scored.groupby("instrument"):
            eligible = group[group.robust_z.notna()]
            real.append(
                {
                    "instrument": name,
                    "frequency": group.frequency.iloc[0],
                    "eligible": len(eligible),
                    "robust_alerts": int(eligible.robust_alert.sum()),
                    "baseline_5pct_alerts": int(eligible.baseline_alert.sum()),
                    "top_events": records(
                        eligible.assign(magnitude=eligible.robust_z.abs()).nlargest(5, "magnitude")[
                            ["event_time", "value", "robust_z"]
                        ]
                    ),
                }
            )
    rng = np.random.default_rng(2026)
    n = 2400
    increments = rng.normal(0, 0.4, n)
    labels = np.zeros(n, dtype=bool)
    for i, sign in zip(range(200, n, 200), [1, -1] * 6, strict=False):
        increments[i] += sign * 12
        labels[i] = True
    sample = pd.DataFrame(
        {
            "mode": "SYNTHETIC",
            "instrument": "XAUUSD",
            "frequency": "tick",
            "event_time": pd.date_range("2020-01-01", periods=n, freq="s", tz="UTC"),
            "observed_at": pd.Timestamp("2026-01-01", tz="UTC"),
            "revision": "synthetic-detector-v1",
            "value": 2000 + increments.cumsum(),
        }
    )
    test = score_history(sample)
    outcomes = []
    valid = test.robust_z.notna().to_numpy()
    for detector in ("baseline_alert", "robust_alert"):
        predicted = test[detector].to_numpy()
        tp = int((predicted & labels & valid).sum())
        fp = int((predicted & ~labels & valid).sum())
        fn = int((~predicted & labels & valid).sum())
        outcomes.append(
            {
                "detector": detector,
                "true_positives": tp,
                "false_positives": fp,
                "false_negatives": fn,
                "precision": tp / (tp + fp) if tp + fp else None,
                "recall": tp / (tp + fn) if tp + fn else None,
            }
        )
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "real_data": real,
        "synthetic_controlled": outcomes,
        "synthetic_seed": 2026,
        "interpretation": "Synthetic jump recovery is a controlled sensitivity check. Historical alert counts are not precision/recall; no independent real shock labels.",
        "detector_parameters": "robust |z|>=4; prior 60 changes / 36 monthly changes; warmup 20 / 12; baseline |pct|>=5",
    }
    save_report(ARTIFACTS / "detector-evaluation.json", report)
    print(json.dumps(report, indent=2))
    return report
