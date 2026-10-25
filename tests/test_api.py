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
