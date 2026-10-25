import os
from pathlib import Path

DATA = Path(os.getenv("DATA_DIR", "data"))
ARTIFACTS = Path(os.getenv("ARTIFACT_DIR", "artifacts"))
DATABASE = os.getenv(
    "DATABASE_URL",
    "postgresql://observatory:local_observatory_only@localhost:5434/observatory",
)
BROKERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPICS = {mode: f"cso.quotes.{mode.lower()}.v1" for mode in ("REPLAY", "LIVE", "SYNTHETIC")}

# Instruments are economic contracts, not interchangeable ticker aliases.
INSTRUMENTS = {
    "BRENT": {
        "name": "Brent",
        "unit": "USD/barrel",
        "frequency": "daily",
        "kind": "spot",
        "source": "EIA via FRED",
        "series": "DCOILBRENTEU",
        "negative": False,
    },
    "WTI": {
        "name": "WTI",
        "unit": "USD/barrel",
        "frequency": "daily",
        "kind": "spot",
        "source": "EIA via FRED",
        "series": "DCOILWTICO",
        "negative": True,
    },
    "GOLD": {
        "name": "Gold",
        "unit": "USD/troy_oz",
        "frequency": "monthly",
        "kind": "benchmark",
        "source": "World Bank",
        "series": "Gold",
        "negative": False,
    },
    "COPPER": {
        "name": "Copper",
        "unit": "USD/metric_ton",
        "frequency": "monthly",
        "kind": "benchmark",
        "source": "World Bank",
        "series": "Copper",
        "negative": False,
    },
    "WHEAT": {
        "name": "Wheat, US HRW",
        "unit": "USD/metric_ton",
        "frequency": "monthly",
        "kind": "benchmark",
        "source": "World Bank",
        "series": "Wheat, US HRW",
        "negative": False,
    },
    "XAUUSD": {
        "name": "Gold / USD",
        "unit": "USD/troy_oz",
        "frequency": "tick",
        "kind": "provider_quote",
        "source": "Twelve Data",
        "series": "XAU/USD",
        "negative": False,
    },
    "WTIUSD": {
        "name": "WTI / USD",
        "unit": "USD/barrel",
        "frequency": "tick",
        "kind": "provider_quote",
        "source": "Twelve Data",
        "series": "WTI/USD",
        "negative": True,
    },
}
