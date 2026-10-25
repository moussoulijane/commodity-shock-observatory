import asyncio
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import websockets
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from observatory.config import BROKERS, TOPICS
from observatory.contracts import Quote, make_quote


def create_topics():
    admin = AdminClient({"bootstrap.servers": BROKERS, "broker.address.family": "v4"})
    names = [*TOPICS.values(), "cso.dead-letter.v1"]
    for _, future in admin.create_topics([NewTopic(n, 3, 1) for n in names]).items():
        try:
            future.result()
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def publish(payloads, rate=0):
    producer = Producer(
        {
            "broker.address.family": "v4",
            "bootstrap.servers": BROKERS,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 120000,
        }
    )
    errors = []
    count = 0

    def delivered(error, _):
        if error:
            errors.append(str(error))

    for payload in payloads:
        mode = payload.get("mode", "SYNTHETIC")
        key = payload.get("instrument", "INVALID")
        while True:
            try:
                producer.produce(
                    TOPICS[mode], key=key, value=json.dumps(payload), on_delivery=delivered
                )
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        count += 1
        if rate:
            time.sleep(1 / rate)
    pending = producer.flush(120)
    if pending or errors:
        raise RuntimeError(f"Delivery failure: pending={pending}, errors={errors[:2]}")
    return count


def replay(path: Path, rate=0, start=None, end=None):
    def events():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            quote = Quote.model_validate_json(line)
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
    if not set(symbols).issubset(mapping):
        raise ValueError("Supported symbols: XAU/USD, WTI/USD; provider entitlements still apply")
    # Key is never printed. Reconnect has exponential backoff; gaps remain visible.
    delay = 1
    while True:
        try:
            async with websockets.connect(
                f"wss://ws.twelvedata.com/v1/quotes/price?apikey={key}"
            ) as ws:
                await ws.send(
                    json.dumps({"action": "subscribe", "params": {"symbols": ",".join(symbols)}})
                )
                delay = 1
                async for raw in ws:
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
                        await asyncio.to_thread(publish, [quote.model_dump(mode="json")])
                    elif message.get("event") == "subscribe-status":
                        if message.get("fails"):
                            raise ValueError("Provider rejected requested subscription")
        except (OSError, websockets.ConnectionClosed):
            await asyncio.sleep(delay)
            delay = min(60, delay * 2)
