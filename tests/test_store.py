from datetime import UTC, datetime

from observatory.contracts import make_quote
from observatory.store import observation_frame


def test_daily_and_subsecond_events_coexist():
    now = datetime(2025, 10, 25, 15, 30, 12, 123456, tzinfo=UTC)
    daily = make_quote("WTI", datetime(2020, 1, 1, tzinfo=UTC), 60, "v1", now)
    tick = make_quote("XAUUSD", now, 2400, "test-v1", now, "SYNTHETIC")

    class Connection:
        def execute(self, *_):
            return self

        def fetchall(self):
            return [
                {"payload": daily.model_dump(mode="json")},
                {"payload": tick.model_dump(mode="json")},
            ]

    frame = observation_frame(Connection())
    assert len(frame) == 2
    assert frame.iloc[1].event_time.microsecond == 123456
    assert frame.iloc[0].event_time.year == 2020
