from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from observatory.api import app

client = TestClient(app)


def test_unknown_instrument_rejected_before_database():
    assert client.get("/api/history/UNKNOWN").status_code == 404


def test_invalid_mode_rejected():
    assert client.get("/api/history/BRENT?mode=pretend-live").status_code == 422


def test_unbounded_request_rejected():
    assert client.get("/api/history/BRENT?limit=1000000").status_code == 422


def test_catalog_contract():
    result = client.get("/api/catalog")
    assert result.status_code == 200
    assert result.json()["instruments"]["WTI"]["kind"] == "spot"


def test_metrics_does_not_expose_keys_in_catalog():
    body = client.get("/api/catalog").text.lower()
    assert "apikey=" not in body


class ReferenceConnection:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def execute(self, query, args=()):
        self.queries.append((query, args))
        return self

    def fetchall(self):
        return self.rows


def reference():
    return {
        "instrument": "BRENT",
        "mode": "REPLAY",
        "kind": "spot",
        "unit": "USD/barrel",
        "value": 100,
        "frequency": "daily",
        "provider": "EIA via FRED",
        "event_time": "2024-01-02T00:00:00+00:00",
        "observed_at": "2026-01-01T00:00:00+00:00",
        "revision": "test-snapshot",
    }


def test_exposure_profiles_are_fictional_and_fx_is_not_a_quote():
    result = client.get("/api/exposures/profiles")
    assert result.status_code == 200
    body = result.json()
    assert len(body["profiles"]) == 3
    assert body["fx_reference_default_mad_per_usd"] == 10
    assert "USER ASSUMPTION" in body["fx_reference_type"]
    assert all("Fictional" in profile["description"] for profile in body["profiles"])


def test_unknown_exposure_profile_rejected_before_database():
    assert client.post("/api/exposures/scenario", json={"profile_id": "unknown"}).status_code == 404


@pytest.mark.parametrize(
    "extra",
    [
        {"fx_reference_mad_per_usd": 0},
        {"commodity_shock_pct": -100},
        {"fx_shock_pct": float("inf")},
        {"mode": "SYNTHETIC"},
    ],
)
def test_invalid_scenario_rejected(extra):
    if "fx_shock_pct" in extra:
        # JSON cannot carry infinity: send raw bytes to exercise server validation.
        result = client.post(
            "/api/exposures/scenario",
            content='{"profile_id":"energy-importer","fx_shock_pct":1e999}',
            headers={"Content-Type": "application/json"},
        )
    else:
        result = client.post(
            "/api/exposures/scenario", json={"profile_id": "energy-importer", **extra}
        )
    assert result.status_code == 422


def test_naive_scenario_cutoff_is_rejected_before_database():
    result = client.post(
        "/api/exposures/scenario",
        json={"profile_id": "energy-importer", "as_of": "2024-01-31T23:59:59"},
    )
    assert result.status_code == 422


def test_scenario_reads_v2_replay_references_and_does_not_persist(monkeypatch):
    conn = ReferenceConnection([{"instrument": "BRENT", "payload": reference()}])
    monkeypatch.setattr("observatory.api.connect", lambda: conn)
    result = client.post(
        "/api/exposures/scenario",
        json={"profile_id": "energy-importer", "as_of": "2024-01-31T23:59:59+00:00"},
    )
    assert result.status_code == 200
    body = result.json()
    assert body["totals"]["baseline_cost_mad"] == pytest.approx(12540000)
    assert body["totals"]["stressed_cost_mad"] == pytest.approx(14152200)
    assert body["data_mode"] == "REPLAY"
    assert "not stored" in body["persistence"]
    assert "revised snapshots" in body["reference_selection_policy"]
    assert len(conn.queries) == 1
    query, args = conn.queries[0]
    assert query.startswith("SELECT") and "signals_v2" in query
    assert "mode='REPLAY'" in query and "event_time<=%s" in query
    assert args[0] == ["BRENT"]
    assert args[1] == datetime(2024, 1, 31, 23, 59, 59, tzinfo=UTC)


@pytest.mark.parametrize("as_of", [None, "2999-01-01T00:00:00+00:00"])
def test_scenario_always_bounds_reference_date_by_now(monkeypatch, as_of):
    conn = ReferenceConnection([{"instrument": "BRENT", "payload": reference()}])
    monkeypatch.setattr("observatory.api.connect", lambda: conn)
    before = datetime.now(UTC)
    body = {"profile_id": "energy-importer"}
    if as_of:
        body["as_of"] = as_of
    result = client.post("/api/exposures/scenario", json=body)
    after = datetime.now(UTC)
    assert result.status_code == 200
    query, args = conn.queries[0]
    assert "event_time<=%s" in query
    assert before <= args[1] <= after


def valid_strict_report():
    return {
        "method_version": "macro-v2",
        "evaluation_type": "initial_release_asof",
        "status": "completed",
        "provenance": {
            "information_set_kind": "alfred_revision_events",
            "strict_eligible": True,
            "availability_semantics": "next_utc_midnight_after_vintage_date",
        },
    }


@pytest.mark.parametrize(
    "mutation",
    [
        {"method_version": "macro-v1"},
        {"evaluation_type": "exploratory_revised_data"},
        {"status": "insufficient_data"},
        {"provenance": None},
        {
            "provenance": {
                "information_set_kind": "initial_releases_only",
                "strict_eligible": True,
                "availability_semantics": "next_utc_midnight_after_vintage_date",
            }
        },
        {
            "provenance": {
                "information_set_kind": "alfred_revision_events",
                "strict_eligible": "true",
                "availability_semantics": "next_utc_midnight_after_vintage_date",
            }
        },
        {
            "provenance": {
                "information_set_kind": "alfred_revision_events",
                "strict_eligible": True,
                "availability_semantics": "same_day",
            }
        },
    ],
)
def test_research_rejects_legacy_or_unverified_strict_cache(monkeypatch, mutation):
    report = {**valid_strict_report(), **mutation}
    monkeypatch.setattr(
        "observatory.api.read_report", lambda name: report if name == "macro-strict.json" else None
    )
    result = client.get("/api/research").json()
    assert result["strict"] is None
    assert result["strict_cache_status"] == "unverified"


def test_research_accepts_current_acquisition_metadata_without_authentication_claim(monkeypatch):
    report = valid_strict_report()
    monkeypatch.setattr(
        "observatory.api.read_report", lambda name: report if name == "macro-strict.json" else None
    )
    result = client.get("/api/research").json()
    assert result["strict"] == report
    assert result["strict_cache_status"] == "verified_acquisition_metadata"
    assert "no independent authentication" in result["strict_cache_notice"]


def test_research_marks_absent_strict_cache(monkeypatch):
    monkeypatch.setattr("observatory.api.read_report", lambda _: None)
    result = client.get("/api/research").json()
    assert result["strict"] is None
    assert result["strict_cache_status"] == "absent"


def test_missing_benchmark_returns_actionable_conflict(monkeypatch):
    monkeypatch.setattr("observatory.api.connect", lambda: ReferenceConnection([]))
    result = client.post("/api/exposures/scenario", json={"profile_id": "energy-importer"})
    assert result.status_code == 409
    assert "BRENT" in result.json()["detail"]


def test_non_replay_reference_cannot_enter_scenario(monkeypatch):
    wrong = {**reference(), "mode": "SYNTHETIC"}
    monkeypatch.setattr(
        "observatory.api.connect",
        lambda: ReferenceConnection([{"instrument": "BRENT", "payload": wrong}]),
    )
    assert (
        client.post("/api/exposures/scenario", json={"profile_id": "energy-importer"}).status_code
        == 409
    )


def test_market_bars_are_bounded_and_read_v2(monkeypatch):
    conn = ReferenceConnection(
        [
            {
                "bar_start": datetime(2024, 1, 2, tzinfo=UTC),
                "frequency": "daily",
                "payload": {"open": 100, "close": 101},
            },
            {
                "bar_start": datetime(2024, 1, 1, tzinfo=UTC),
                "frequency": "daily",
                "payload": {"open": 98, "close": 99},
            },
        ]
    )
    monkeypatch.setattr("observatory.api.connect", lambda: conn)
    result = client.get("/api/bars/BRENT?limit=1")
    assert result.status_code == 200
    assert result.json()["truncated"]
    assert result.json()["items"][0]["close"] == 101
    assert "market_bars_v2" in conn.queries[0][0]
    assert client.get("/api/bars/UNKNOWN").status_code == 404
    assert client.get("/api/bars/BRENT?limit=5001").status_code == 422


def test_operations_counts_served_observations_not_revision_identities(monkeypatch):
    # A correction is another durable raw identity, but replaces one economic
    # observation in serving. The two counts must deliberately differ.
    raw_revisions = [
        ("REPLAY", "BRENT", "2024-01-02", "old"),
        ("REPLAY", "BRENT", "2024-01-02", "corrected"),
        ("REPLAY", "BRENT", "2024-01-03", "initial"),
    ]
    canonical_keys = {row[:3] for row in raw_revisions}

    class OperationsConnection(ReferenceConnection):
        def execute(self, query, args=()):
            self.current_query = query
            return super().execute(query, args)

        def fetchall(self):
            if "FROM signals_v2" in self.current_query:
                return [{"mode": "REPLAY", "n": len(canonical_keys)}]
            if "FROM observations" in self.current_query:
                return [{"mode": "REPLAY", "n": len(raw_revisions)}]
            if "FROM pipeline_runs" in self.current_query:
                return []
            raise AssertionError("Unexpected operations list query")

        def fetchone(self):
            if "FROM deliveries" in self.current_query:
                return {"deliveries": 3, "rejected": 0, "last_processed": None}
            if "FROM delivery_windows_v2" in self.current_query:
                return {"n": 2}
            raise AssertionError("Unexpected operations scalar query")

    conn = OperationsConnection([])
    reports = {
        "commit-recovery-v2.json": {"status": "passed", "fixture": "commit-recovery"},
        "sustained-load-v2.json": {"status": "completed", "objective_met": False},
        "parity-v2.json": {"status": "passed", "canonical_observations": 2},
        "spark-checks-v2.json": {"status": "passed", "fixture": "spark-checks"},
    }
    monkeypatch.setattr("observatory.api.connect", lambda: conn)
    monkeypatch.setattr("observatory.api.read_report", lambda name: reports.get(name))
    result = client.get("/api/operations")
    assert result.status_code == 200
    body = result.json()
    assert body["canonical_counts"] == [{"mode": "REPLAY", "n": 2}]
    assert body["canonical_counts"][0]["n"] != len(raw_revisions)
    assert body["commit_recovery"] == reports["commit-recovery-v2.json"]
    assert body["sustained_load"] == reports["sustained-load-v2.json"]
    assert body["sustained_load"]["objective_met"] is False
    assert body["parity"] == reports["parity-v2.json"]
    assert body["spark_checks"] == reports["spark-checks-v2.json"]
