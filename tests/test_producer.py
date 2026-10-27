import asyncio
import json
import traceback
from datetime import UTC, datetime

import pytest
import websockets

from observatory import producer
from observatory.contracts import make_quote


class FakeProducer:
    instances = []

    def __init__(self, config):
        self.config = config
        self.queue = []
        self.records = []
        self.polls = []
        self.flushes = 0
        self.peak = 0
        self.buffer_once = False
        self.delivery_error = None
        self.drain = True
        type(self).instances.append(self)

    def produce(self, topic, *, key, value, on_delivery):
        if self.buffer_once:
            self.buffer_once = False
            raise BufferError("queue full")
        self.records.append((topic, key, json.loads(value)))
        self.queue.append(on_delivery)
        self.peak = max(self.peak, len(self.queue))

    def poll(self, timeout):
        self.polls.append(timeout)
        if self.drain and timeout > 0 and self.queue:
            self.queue.pop(0)(self.delivery_error, None)

    def flush(self, timeout):
        self.flushes += 1
        while self.queue:
            self.queue.pop(0)(self.delivery_error, None)
        return 0


@pytest.fixture(autouse=True)
def fresh_fakes(monkeypatch):
    FakeProducer.instances = []
    monkeypatch.setattr(producer, "Producer", FakeProducer)


def payload(i=0):
    return {"mode": "SYNTHETIC", "instrument": "XAUUSD", "value": 2400 + i}


def test_publish_creates_one_producer_and_flushes_once():
    assert producer.publish(payload(i) for i in range(20)) == 20
    assert len(FakeProducer.instances) == 1
    fake = FakeProducer.instances[0]
    assert fake.flushes == 1
    assert len(fake.records) == 20
    assert fake.config["enable.idempotence"] is True
    assert fake.config["acks"] == "all"
    assert fake.config["queue.buffering.max.messages"] == 10000
    assert fake.config["queue.buffering.max.kbytes"] == 65536


def test_bounded_backpressure_polls_and_does_not_drop():
    publisher = producer.KafkaPublisher(max_pending=2)
    fake = FakeProducer.instances[0]
    fake.buffer_once = True
    for i in range(7):
        publisher.send(payload(i))
    publisher.close()
    assert fake.peak <= 2
    assert len(fake.records) == 7
    assert any(t > 0 for t in fake.polls)
    assert publisher.acknowledged == 7
    assert publisher.pending == 0


def test_backpressure_has_deadline(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(producer.time, "monotonic", lambda: clock[0])
    publisher = producer.KafkaPublisher(max_pending=1, backpressure_timeout=0.2)
    fake = FakeProducer.instances[0]
    fake.drain = False

    def poll(timeout):
        clock[0] += max(timeout, 0.001)

    fake.poll = poll
    publisher.send(payload())
    with pytest.raises(TimeoutError, match="no event was silently discarded"):
        publisher.send(payload(1))
    assert len(fake.records) == 1
    publisher.close()


@pytest.mark.parametrize(
    "options",
    [
        {"backpressure_timeout": float("nan")},
        {"backpressure_timeout": float("inf")},
        {"max_pending": 1.5},
        {"max_pending": True},
    ],
)
def test_queue_limits_are_finite_and_integral(options):
    with pytest.raises(ValueError):
        producer.KafkaPublisher(**options)
    assert not FakeProducer.instances


def test_live_backpressure_timeout_is_fatal_not_a_socket_reconnect():
    class FullQueue:
        def send(self, _):
            raise TimeoutError("unit-test-placeholder")

    with pytest.raises(producer.KafkaPublishError) as error:
        asyncio.run(producer._send_live(FullQueue(), payload()))
    assert "unit-test-placeholder" not in str(error.value)
    assert not isinstance(error.value, OSError)


def test_flush_requires_successful_acknowledgements():
    publisher = producer.KafkaPublisher()
    fake = FakeProducer.instances[0]
    publisher.send(payload())
    fake.flush = lambda _: 1
    with pytest.raises(producer.KafkaPublishError, match="pending=1"):
        publisher.close()


def test_delivery_failure_is_acknowledged_without_error_payload():
    publisher = producer.KafkaPublisher()
    fake = FakeProducer.instances[0]
    fake.delivery_error = "apikey=unit-test-placeholder"
    publisher.send(payload())
    with pytest.raises(RuntimeError) as error:
        publisher.close()
    assert "unit-test-placeholder" not in str(error.value)
    assert publisher.delivery_failures == 1


def test_enqueue_failure_redacts_original_exception():
    publisher = producer.KafkaPublisher()
    fake = FakeProducer.instances[0]

    def fail(*_, **__):
        raise RuntimeError("connection URL contains unit-test-placeholder")

    fake.produce = fail
    with pytest.raises(RuntimeError) as error:
        publisher.send(payload())
    assert "unit-test-placeholder" not in str(error.value)
    assert error.value.__suppress_context__
    publisher.close()


@pytest.mark.parametrize("rate", [-1, float("nan"), float("inf")])
def test_invalid_rate_rejected_before_producer(rate):
    with pytest.raises(ValueError):
        producer.publish([], rate=rate)
    assert not FakeProducer.instances


def test_replay_preserves_identity_and_moves_live_to_replay(tmp_path):
    now = datetime.now(UTC)
    quote = make_quote("XAUUSD", datetime(2020, 1, 1, tzinfo=UTC), 1500, "v1", now, "LIVE")
    path = tmp_path / "capture.jsonl"
    path.write_text(quote.model_dump_json() + "\n", encoding="utf-8")
    assert producer.replay(path) == 1
    item = FakeProducer.instances[0].records[0]
    assert item[0] == "cso.quotes.replay.v1"
    assert item[2]["mode"] == "REPLAY"
    assert item[2]["event_id"] == quote.event_id


def test_synthetic_capture_cannot_be_relabelled_as_replay(tmp_path):
    now = datetime.now(UTC)
    quote = make_quote("XAUUSD", datetime(2020, 1, 1, tzinfo=UTC), 1500, "v1", now, "SYNTHETIC")
    path = tmp_path / "capture.jsonl"
    path.write_text(quote.model_dump_json(), encoding="utf-8")
    with pytest.raises(ValueError, match="must remain"):
        producer.replay(path)


class FakeWebSocket:
    def __init__(self):
        self.index = 0

    async def send(self, _):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index < 3:
            self.index += 1
            return json.dumps(
                {
                    "event": "price",
                    "symbol": "XAU/USD",
                    "timestamp": 1600000000 + self.index,
                    "price": 2400 + self.index,
                }
            )
        await asyncio.Event().wait()


class FakeConnection:
    async def __aenter__(self):
        return FakeWebSocket()

    async def __aexit__(self, *_):
        pass


def authorize_live(monkeypatch):
    monkeypatch.setenv("TWELVE_DATA_API_KEY", "unit-test-placeholder")
    monkeypatch.setenv("MARKET_DATA_LICENSE_ACK", "true")


def test_live_reuses_producer_across_ticks_and_only_flushes_at_shutdown(monkeypatch):
    authorize_live(monkeypatch)
    options = []

    def connect(_, **kwargs):
        options.append(kwargs)
        return FakeConnection()

    monkeypatch.setattr(producer.websockets, "connect", connect)

    async def exercise():
        task = asyncio.create_task(producer.live(["XAU/USD"]))
        try:
            for _ in range(100):
                await asyncio.sleep(0.01)
                if FakeProducer.instances and len(FakeProducer.instances[0].records) == 3:
                    break
            assert len(FakeProducer.instances) == 1
            assert len(FakeProducer.instances[0].records) == 3
            assert FakeProducer.instances[0].flushes == 0
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert FakeProducer.instances[0].flushes == 1

    asyncio.run(exercise())
    assert options[0]["max_queue"] == 64
    assert options[0]["max_size"] == 1_000_000


def test_live_handshake_failure_does_not_expose_key(monkeypatch):
    authorize_live(monkeypatch)

    class RejectedConnection:
        async def __aenter__(self):
            raise websockets.InvalidURI(
                "https://example.test/?apikey=unit-test-placeholder", "bad URI"
            )

        async def __aexit__(self, *_):
            pass

    monkeypatch.setattr(producer.websockets, "connect", lambda *_, **__: RejectedConnection())
    with pytest.raises(ExceptionGroup) as error:
        asyncio.run(producer.live(["XAU/USD"]))
    rendered = "".join(traceback.format_exception(error.value))
    # Suppressed exception contexts must not disclose the original failing URI.
    assert "https://example.test/?apikey=" not in rendered
    assert "Provider connection failed" in rendered
