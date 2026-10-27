import asyncio
import json
import math
import os
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote as escape_query

import websockets
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from observatory.config import BROKERS, TOPICS
from observatory.contracts import Quote, make_quote


class KafkaPublishError(RuntimeError):
    """Fatal publishing failure with credential-free diagnostics."""


class LiveFeedError(RuntimeError):
    """Fatal provider failure with credential-free diagnostics."""


def create_topics():
    admin = AdminClient({"bootstrap.servers": BROKERS, "broker.address.family": "v4"})
    names = [*TOPICS.values(), "cso.dead-letter.v1"]
    for _, future in admin.create_topics([NewTopic(n, 3, 1) for n in names]).items():
        try:
            future.result()
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


class KafkaPublisher:
    """One idempotent producer with a bounded in-flight queue and explicit acknowledgements.

    A full queue blocks the sender until acknowledgements free capacity. It never
    silently drops a quote. The caller may use poll() from another thread; librdkafka
    callbacks and local counters are protected independently of its network threads.
    """

    def __init__(self, max_pending=10000, backpressure_timeout=30, producer_factory=None):
        if (
            not isinstance(max_pending, int)
            or isinstance(max_pending, bool)
            or max_pending < 1
            or not math.isfinite(backpressure_timeout)
            or backpressure_timeout <= 0
        ):
            raise ValueError("Queue capacity and backpressure timeout must be positive")
        self.max_pending = max_pending
        self.backpressure_timeout = backpressure_timeout
        self.pending = 0
        self.acknowledged = 0
        self.enqueued = 0
        self.delivery_failures = 0
        self._closed = False
        self._closing = False
        self._state_lock = threading.Lock()
        self._poll_lock = threading.Lock()
        factory = producer_factory or Producer
        try:
            self._producer = factory(
                {
                    "broker.address.family": "v4",
                    "bootstrap.servers": BROKERS,
                    "enable.idempotence": True,
                    "acks": "all",
                    "delivery.timeout.ms": 120000,
                    "queue.buffering.max.messages": max_pending,
                    "queue.buffering.max.kbytes": 65536,
                    "linger.ms": 20,
                    # Diagnostics use safe counters; broker exceptions can contain credentials.
                    "log_level": 0,
                }
            )
        except Exception:
            raise KafkaPublishError(
                "Kafka producer initialization failed; details omitted"
            ) from None

    def _delivered(self, error, _):
        with self._state_lock:
            self.pending -= 1
            if error:
                self.delivery_failures += 1
            else:
                self.acknowledged += 1

    def _check(self):
        with self._state_lock:
            if self.delivery_failures:
                raise KafkaPublishError(
                    f"Kafka delivery failed for {self.delivery_failures} event(s); details omitted"
                )
            if self._closed or self._closing:
                raise KafkaPublishError("Kafka publisher is closed")

    def poll(self, timeout=0):
        self._check()
        try:
            with self._poll_lock:
                self._producer.poll(timeout)
        except Exception:
            raise KafkaPublishError("Kafka polling failed; details omitted") from None
        self._check()

    def send(self, payload):
        mode = payload.get("mode", "SYNTHETIC")
        if mode not in TOPICS:
            raise ValueError("Unknown publishing mode")
        try:
            value = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode()
        except (TypeError, ValueError):
            raise ValueError("Payload is not finite JSON; content omitted") from None
        if len(value) > 1_000_000:
            raise ValueError("Payload exceeds the one-megabyte publishing limit")
        deadline = time.monotonic() + self.backpressure_timeout
        while True:
            self.poll(0)
            try:
                with self._state_lock:
                    if self._closed or self._closing:
                        raise KafkaPublishError("Kafka publisher is closed")
                    if self.pending < self.max_pending:
                        self._producer.produce(
                            TOPICS[mode],
                            key=payload.get("instrument", "INVALID"),
                            value=value,
                            on_delivery=self._delivered,
                        )
                        # Reserve while holding the lock: a concurrent poll cannot
                        # acknowledge before this counter has been incremented.
                        self.pending += 1
                        self.enqueued += 1
                        return
            except BufferError:
                pass
            except Exception:
                raise KafkaPublishError("Kafka enqueue failed; details omitted") from None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Kafka queue remained full; no event was silently discarded")
            self.poll(min(0.1, remaining))

    def close(self, timeout=120):
        with self._state_lock:
            if self._closed:
                return
            self._closing = True
        try:
            with self._poll_lock:
                pending = self._producer.flush(timeout)
        except Exception:
            raise KafkaPublishError("Kafka flush failed; details omitted") from None
        with self._state_lock:
            self._closed = True
            failed, remaining = self.delivery_failures, self.pending
        if pending or remaining or failed:
            raise KafkaPublishError(
                f"Kafka delivery incomplete: pending={max(pending, remaining)}, failed={failed}"
            )

    def __enter__(self):
        return self

    def __exit__(self, exception_type, *_):
        try:
            self.close()
        except Exception:
            if exception_type is None:
                raise


def publish(payloads, rate=0):
    if not math.isfinite(rate) or rate < 0:
        raise ValueError("Publishing rate must be finite and non-negative")
    count = 0
    with KafkaPublisher() as publisher:
        for payload in payloads:
            publisher.send(payload)
            count += 1
            if rate:
                time.sleep(1 / rate)
    return count


def replay(path: Path, rate=0, start=None, end=None):
    def events():
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                quote = Quote.model_validate_json(line)
                if quote.mode == "SYNTHETIC":
                    raise ValueError("Synthetic captures must remain in the synthetic topic")
                if start and quote.event_time.date().isoformat() < start:
                    continue
                if end and quote.event_time.date().isoformat() > end:
                    continue
                # Replaying a live capture must not republish it as LIVE.
                quote.mode = "REPLAY"
                quote.ingested_at = datetime.now(UTC)
                yield quote.model_dump(mode="json")

    return publish(events(), rate)


async def live(symbols):
    key = os.getenv("TWELVE_DATA_API_KEY")
    if not key or os.getenv("MARKET_DATA_LICENSE_ACK", "false").lower() != "true":
        raise RuntimeError("LIVE requires TWELVE_DATA_API_KEY and MARKET_DATA_LICENSE_ACK=true")
    mapping = {"XAU/USD": "XAUUSD", "WTI/USD": "WTIUSD"}
    if not symbols or not set(symbols).issubset(mapping):
        raise ValueError("Supported symbols: XAU/USD, WTI/USD; provider entitlements still apply")
    # The same producer survives provider reconnects; neither a reconnect nor a tick
    # creates another Kafka connection. Bounded websocket/Kafka buffers propagate pressure.
    publisher = KafkaPublisher()
    try:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(_poll_live_deliveries(publisher))
            await _consume_live(symbols, mapping, key, publisher)
    finally:
        await asyncio.to_thread(publisher.close)


async def _poll_live_deliveries(publisher):
    while True:
        await asyncio.to_thread(publisher.poll, 0.1)
        await asyncio.sleep(0.02)


async def _send_live(publisher, payload):
    # Cancellation must finish an already-started bounded send before closing its
    # shared producer; otherwise a worker thread could enqueue after final flush.
    sending = asyncio.create_task(asyncio.to_thread(publisher.send, payload))
    try:
        await asyncio.shield(sending)
    except asyncio.CancelledError:
        try:
            await sending
        finally:
            raise
    except TimeoutError:
        # TimeoutError is an OSError: do not mistake Kafka backpressure for a
        # reconnectable provider socket failure and silently skip this quote.
        raise KafkaPublishError(
            "Kafka backpressure deadline exceeded; live consumer stopped"
        ) from None


async def _consume_live(symbols, mapping, key, publisher):
    delay = 1
    while True:
        try:
            async with websockets.connect(
                "wss://ws.twelvedata.com/v1/quotes/price?apikey=" + escape_query(key, safe=""),
                max_queue=64,
                max_size=1_000_000,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                await ws.send(
                    json.dumps({"action": "subscribe", "params": {"symbols": ",".join(symbols)}})
                )
                delay = 1
                async for raw in ws:
                    try:
                        message = json.loads(raw)
                        if message.get("event") == "price":
                            now = datetime.now(UTC)
                            quote = make_quote(
                                mapping[message["symbol"]],
                                datetime.fromtimestamp(message["timestamp"], UTC),
                                float(message["price"]),
                                str(message["timestamp"]) + "-" + str(message["price"]),
                                now,
                                "LIVE",
                            )
                            await _send_live(publisher, quote.model_dump(mode="json"))
                        elif message.get("event") == "error" or (
                            message.get("event") == "subscribe-status" and message.get("fails")
                        ):
                            raise ValueError("Provider rejected subscription")
                    except (ValueError, TypeError, KeyError, OverflowError):
                        raise LiveFeedError(
                            "Provider sent an invalid quote or rejected subscription; payload omitted"
                        ) from None
        except (KafkaPublishError, LiveFeedError):
            raise
        except (OSError, websockets.ConnectionClosed):
            # No exception or URL is logged; failures may contain an API-key URL.
            pass
        except Exception:
            raise LiveFeedError(
                "Provider connection failed; endpoint and credentials omitted"
            ) from None
        await asyncio.sleep(delay)
        delay = min(60, delay * 2)
