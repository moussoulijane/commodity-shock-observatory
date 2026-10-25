import hashlib
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from observatory.config import INSTRUMENTS


class Quote(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    schema_version: Literal[1] = 1
    mode: Literal["REPLAY", "LIVE", "SYNTHETIC"]
    instrument: str
    provider: str
    kind: str
    frequency: Literal["daily", "monthly", "tick"]
    unit: str
    event_time: datetime
    # For downloaded histories, exact original publication time is UNKNOWN.
    # observed_at is when THIS payload was obtained, not its historical release.
    observed_at: datetime
    ingested_at: datetime
    revision: str = Field(min_length=1, max_length=64)
    value: float
    event_id: str = Field(pattern=r"^[a-f0-9]{32}$")

    @field_validator("event_time", "observed_at", "ingested_at")
    @classmethod
    def utc(cls, value):
        if value.tzinfo is None:
            raise ValueError("Timezone required")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def consistent(self):
        spec = INSTRUMENTS.get(self.instrument)
        if not spec:
            raise ValueError("Unknown instrument")
        for key in ("unit", "frequency", "kind"):
            if getattr(self, key) != spec[key]:
                raise ValueError(f"Incorrect {key} for instrument")
        expected_provider = "Synthetic generator" if self.mode == "SYNTHETIC" else spec["source"]
        if self.provider != expected_provider:
            raise ValueError("Incorrect provider")
        if self.value < 0 and not spec["negative"]:
            raise ValueError("Negative value not permitted for instrument")
        if self.event_time > self.observed_at or self.observed_at > self.ingested_at:
            raise ValueError("Expected event_time <= observed_at <= ingested_at")
        if self.mode == "LIVE" and self.frequency != "tick":
            raise ValueError("LIVE is reserved for the intraday adapter")
        if self.event_id != identity(
            self.instrument, self.event_time, self.revision, self.provider
        ):
            raise ValueError("Incorrect event_id")
        return self


def identity(instrument, event_time, revision, provider):
    key = f"{provider}|{instrument}|{event_time.astimezone(UTC).isoformat()}|{revision}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def make_quote(instrument, event_time, value, revision, observed_at, mode="REPLAY"):
    spec = INSTRUMENTS[instrument]
    provider = "Synthetic generator" if mode == "SYNTHETIC" else spec["source"]
    return Quote(
        mode=mode,
        instrument=instrument,
        provider=provider,
        kind=spec["kind"],
        frequency=spec["frequency"],
        unit=spec["unit"],
        event_time=event_time,
        observed_at=observed_at,
        ingested_at=datetime.now(UTC),
        revision=revision,
        value=value,
        event_id=identity(instrument, event_time, revision, provider),
    )
