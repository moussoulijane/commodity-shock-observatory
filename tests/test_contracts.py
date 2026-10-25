from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from observatory.contracts import Quote, make_quote
from observatory.streaming import valid_contract


@pytest.fixture
def quote():
    now = datetime.now(UTC)
    return make_quote("WTI", now - timedelta(days=1), -37.63, "fixture-v1", now)


def test_negative_wti_and_stable_identity(quote):
    another = Quote.model_validate_json(quote.model_dump_json())
    assert another.value == -37.63
    assert another.event_id == quote.event_id


@pytest.mark.parametrize(
    "field,value",
    [
        ("value", float("nan")),
        ("value", float("inf")),
        ("unit", "EUR/barrel"),
        ("provider", "unknown"),
        ("frequency", "monthly"),
        ("schema_version", 2),
        ("event_id", "0" * 32),
        ("unexpected", 1),
        ("kind", "futures"),
    ],
)
def test_reject_invalid_contract(quote, field, value):
    with pytest.raises(ValidationError):
        Quote.model_validate({**quote.model_dump(), field: value})


def test_naive_timestamp_rejected(quote):
    with pytest.raises(ValidationError):
        Quote.model_validate({**quote.model_dump(), "event_time": datetime(2020, 1, 1)})


def test_negative_gold_rejected():
    now = datetime.now(UTC)
    with pytest.raises(ValidationError):
        make_quote("GOLD", now - timedelta(days=1), -1, "v1", now)


def test_future_observation_rejected():
    now = datetime.now(UTC)
    with pytest.raises(ValidationError):
        make_quote("BRENT", now + timedelta(days=1), 50, "v1", now)


def test_live_daily_snapshot_forbidden(quote):
    with pytest.raises(ValidationError):
        Quote.model_validate({**quote.model_dump(), "mode": "LIVE"})


def test_topic_mode_contract(quote):
    assert valid_contract(quote.model_dump_json(), "cso.quotes.replay.v1")
    assert not valid_contract(quote.model_dump_json(), "cso.quotes.live.v1")
    assert not valid_contract("invalid-json", "cso.quotes.replay.v1")
