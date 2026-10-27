"""Mechanical purchasing-cost scenarios for fictional Moroccan importers.

Amounts are benchmark equivalents, not forecasts, invoices, VaR, profits or credit losses.
No FX quote is downloaded or inferred. The caller supplies an explicitly assumed MAD/USD
reference; quantities, fixed hedge fractions and basis costs are fictional demonstration inputs.
"""

from datetime import UTC, datetime
from math import isfinite

from pydantic import BaseModel, ConfigDict, Field

PROFILE_VERSION = "moroccan-importers-demo-v2"
PROFILES = [
    {
        "id": "energy-importer",
        "name": "Energy importer",
        "sector": "Energy",
        "description": "Fictional unhedged monthly oil purchase programme in Morocco.",
        "lines": [
            {
                "instrument": "BRENT",
                "quantity": 12000.0,
                "unit": "barrel",
                "commodity_hedged_fraction": 0.0,
                "fx_hedged_fraction": 0.0,
                "fixed_basis_usd_per_unit": 4.5,
            }
        ],
    },
    {
        "id": "copper-manufacturer",
        "name": "Copper manufacturer",
        "sector": "Industry",
        "description": "Fictional Moroccan manufacturer with partial commodity and FX hedges.",
        "lines": [
            {
                "instrument": "COPPER",
                "quantity": 85.0,
                "unit": "metric_ton",
                "commodity_hedged_fraction": 0.4,
                "fx_hedged_fraction": 0.25,
                "fixed_basis_usd_per_unit": 180.0,
            }
        ],
    },
    {
        "id": "food-processor",
        "name": "Food processor",
        "sector": "Agriculture",
        "description": "Fictional wheat purchasing programme; monthly benchmark is not an invoice.",
        "lines": [
            {
                "instrument": "WHEAT",
                "quantity": 1800.0,
                "unit": "metric_ton",
                "commodity_hedged_fraction": 0.2,
                "fx_hedged_fraction": 0.15,
                "fixed_basis_usd_per_unit": 22.0,
            }
        ],
    },
]


class ScenarioRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    profile_id: str
    fx_reference_mad_per_usd: float = Field(default=10.0, gt=0, le=100)
    commodity_shock_pct: float = Field(default=10, ge=-90, le=200)
    fx_shock_pct: float = Field(default=3, ge=-90, le=100)
    as_of: datetime | None = None


def calculate_scenario(request: ScenarioRequest, references: dict):
    profile = next((p for p in PROFILES if p["id"] == request.profile_id), None)
    if profile is None:
        raise ValueError("Unknown fictional exposure profile")
    if request.as_of is not None and request.as_of.tzinfo is None:
        raise ValueError("as_of requires an explicit timezone")
    rows = []
    for line in profile["lines"]:
        instrument = line["instrument"]
        reference = references.get(instrument)
        if reference is None:
            raise ValueError(f"No historical reference available for {instrument}")
        price = float(reference["value"])
        if not isfinite(price) or price <= 0:
            raise ValueError("Purchasing-cost scenarios require strictly positive benchmarks")
        event_time = datetime.fromisoformat(str(reference["event_time"]).replace("Z", "+00:00"))
        if event_time.tzinfo is None:
            raise ValueError("Reference observation requires a timezone")
        now = datetime.now(UTC)
        cutoff = min(request.as_of, now) if request.as_of else now
        if event_time > cutoff:
            raise ValueError("Reference observation is after the requested as-of date")
        observed_at = datetime.fromisoformat(str(reference["observed_at"]).replace("Z", "+00:00"))
        if observed_at.tzinfo is None or observed_at < event_time or observed_at > now:
            raise ValueError("Reference acquisition chronology or timezone is invalid")
        if reference.get("mode") != "REPLAY":
            raise ValueError("Exposure demo uses historical REPLAY reference benchmarks only")
        unit = "USD/barrel" if line["unit"] == "barrel" else "USD/metric_ton"
        if reference.get("unit") != unit:
            raise ValueError("Reference benchmark unit is incompatible with purchase quantity")
        if reference.get("kind") not in ("spot", "benchmark"):
            raise ValueError("Reference benchmark kind is incompatible with purchase profile")
        quantity, basis = line["quantity"], line["fixed_basis_usd_per_unit"]
        fx = request.fx_reference_mad_per_usd
        commodity_move = request.commodity_shock_pct / 100
        fx_move = request.fx_shock_pct / 100
        effective_price_move = price * commodity_move * (1 - line["commodity_hedged_fraction"])
        effective_fx_move = fx * fx_move * (1 - line["fx_hedged_fraction"])
        base = quantity * (price + basis) * fx
        commodity_effect = quantity * effective_price_move * fx
        fx_effect = quantity * (price + basis) * effective_fx_move
        # FX forwards hedge BASELINE dollar notional. Incremental commodity dollars
        # remain exposed to the full FX move, even with a full baseline FX hedge.
        interaction = quantity * effective_price_move * fx * fx_move
        stressed = base + commodity_effect + fx_effect + interaction
        unhedged = quantity * (price * (1 + commodity_move) + basis) * fx * (1 + fx_move)
        rows.append(
            {
                **line,
                "reference": reference,
                "baseline_cost_mad": base,
                "stressed_cost_mad": stressed,
                "commodity_effect_mad": commodity_effect,
                "fx_effect_mad": fx_effect,
                "interaction_effect_mad": interaction,
                "delta_cost_mad": stressed - base,
                "delta_pct": 100 * (stressed / base - 1),
                "unhedged_stressed_cost_mad": unhedged,
                "hedge_effect_mad": unhedged - stressed,
                "observation_age_days": max(0, (datetime.now(UTC) - event_time).days),
                "snapshot_acquired_at": observed_at.isoformat(),
            }
        )
    sums = {
        key: sum(r[key] for r in rows)
        for key in (
            "baseline_cost_mad",
            "stressed_cost_mad",
            "commodity_effect_mad",
            "fx_effect_mad",
            "interaction_effect_mad",
            "delta_cost_mad",
            "unhedged_stressed_cost_mad",
            "hedge_effect_mad",
        )
    }
    return {
        "profile": profile,
        "profile_version": PROFILE_VERSION,
        "inputs": request.model_dump(mode="json"),
        "horizon": "one assumed purchasing month",
        "reference_fx_type": "USER ASSUMPTION — NOT A MARKET USD/MAD QUOTE",
        "currency": "MAD",
        "data_mode": "REPLAY",
        "lines": rows,
        "totals": sums,
        "delta_pct": 100 * sums["delta_cost_mad"] / sums["baseline_cost_mad"],
        "limitations": [
            "Fictional quantities, hedges and fixed basis costs; no real company data",
            "Different source frequencies and reference dates are not synchronized",
            "Benchmark equivalents exclude taxes, freight changes and financing",
            "Commodity hedges fix benchmark quantities; FX forwards fix baseline USD notional at the reference FX",
            "Incremental commodity dollars remain exposed to FX; hedges can increase net cost when markets fall",
            "No hedge premiums, maturity mismatch, counterparty risk or cash settlement modelling",
            "Historical snapshots may be revised; this is not a point-in-time historical P&L",
            "Scenario arithmetic is not a forecast of inflation, profit or credit losses",
        ],
    }
