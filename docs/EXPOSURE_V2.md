# Purchasing exposure workspace

The default analyst view, **Exposure & scenarios**, connects a historical commodity benchmark to a fictional Moroccan purchasing programme. It is a mechanical cost calculation with explicit assumptions. It does not estimate a company's real financial exposure, inflation, profits, credit losses or market risk probabilities.

## Analyst workflow

1. Select the fictional energy importer, copper manufacturer or food processor. Read the quantity, commodity hedge, FX hedge and fixed USD basis assumptions.
2. Enter a commodity price shock and a currency shock. A positive currency shock means more MAD per USD.
3. Review the FX reference. The initial **10.00 MAD/USD is a user assumption**, not an observed exchange rate. No market FX series is downloaded by this feature.
4. Optionally select an economic-date cutoff. The API selects the latest stored REPLAY benchmark on or before the end of that date. Historical revisions acquired later are allowed: this feature is not a historical information-set backtest.
5. Calculate the scenario. Inspect reference and stressed costs in MAD, price / FX / interaction contributions, and the separate idealized hedge comparison. Changing an input clears the previous result until recalculation.
6. Inspect each benchmark's provider, unit, observation frequency, economic observation date, snapshot acquisition date and age. Download the calculation as JSON if needed.

## Calculation

For quantity `Q`, benchmark `P`, fixed USD basis `B`, reference FX `F`, commodity shock `sP`, FX shock `sF`, and hedge fractions `hP` / `hF`:

```
dP = P × sP × (1 − hP)
dF = F × sF
reference cost  = Q × (P + B) × F
price effect    = Q × dP × F
FX effect       = Q × (P + B) × dF × (1 − hF)
interaction     = Q × dP × dF
stressed cost   = reference cost + price effect + FX effect + interaction
```

The effects already incorporate the stated hedge fractions. FX forwards cover fixed baseline USD notional; incremental commodity dollars remain exposed to the full FX change in the interaction term. The separate hedge comparison is `fully unhedged stressed cost − profile stressed cost`; it must not be added again to the attribution. Fixed hedge fractions represent idealized fixed-price / fixed-FX coverage anchored at the references. Premiums, maturity mismatch, settlement mechanics, counterparty risk, changing freight, taxes and financing are excluded.

## API

- `GET /api/exposures/profiles`: public fictional specifications and an explicitly assumed FX default.
- `POST /api/exposures/scenario`: validates bounded, finite inputs; reads latest historical references from `signals_v2`; performs arithmetic; returns results without persisting the scenario or user input. Missing/unusable references yield 409; an unknown profile yields 404. An explicit `as_of` timestamp must have a timezone.
- `GET /api/bars/{instrument}`: bounded, mode-specific V2 canonical market-bar read endpoint; every item retains its actual frequency.

The exposures feature always uses REPLAY references. Neither synthetic engineering events nor intraday provider quotes are silently substituted. Different benchmark frequencies and dates are explicitly visible and are not synchronized by this feature.
