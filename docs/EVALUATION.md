# Evaluation protocol v1

Archived V1 protocol. The active implementation uses the [V2 macro protocol](MACRO_V2.md), [V2 detector protocol](DETECTORS_V2.md) and [V2 local engineering validation](LOCAL_VALIDATION_V2.md). In particular, V2 acquires revision events using `output_type=2` and uses a different latency protocol; the descriptions below are historical.

## Detectors

Fixed rules: |percentage change| >= 5 baseline; |robust z| >= 4 challenger. Robust z uses the median and 1.4826 × MAD of previous absolute price changes, with a small price-scale floor. Daily/tick: 60 observations, 20 previous changes minimum. Monthly: 36 observations, 12 changes minimum. No log returns are used. Prefix-causality tests perturb future values and require earlier scores to remain unchanged.

The controlled seed-2026 experiment inserts alternating ±12-unit changes into a 0.4-unit normal random walk. Precision/recall here apply ONLY to synthetic known jumps. It is deliberately a simple sensitivity check; it is not evidence of superiority on real economic crises. The historical report gives eligible observations, alert counts and top events per instrument. It does not invent real-world labels or interpret every alert as a macro shock.

## Macroeconomics

Target: seasonally adjusted US CPI YoY in percentage points, for month of origin +1 and +3. Origin: month end. Expanding training includes only samples whose target was available at that origin. Minimum 120 known training targets; last 96 eligible origins by default. Hyperparameters fixed: ridge alpha=10; XGBoost 100 trees, depth=2, learning_rate=.03, lambda=10, seed=42. No random split, no full-sample scaling, no retrospective hyperparameter selection. The same origins compare persistence, AR ridge, AR + oil ridge, XGBoost.

AR features: latest three known YoY observations. Oil features: one- and three-month changes of monthly Brent means from the available data, excluding the current partial month. Missing calendar months are not filled across unknown periods. Training sample features are computed at their original forecast origins.

The default experiment uses CURRENT revised snapshots. Assumed availability is CPI period start +45 days and oil observation +5 days. These are research assumptions, not verified publication calendars. Revision leakage remains. The strict experiment uses ALFRED `output_type=4` initial releases, and considers a vintage available at the following midnight because the API vintage date has no intraday release time. Initial-release CPI ratios are the target, not final revised inflation. Strict mode refuses to run without a key. Vintage coverage and delayed first availability must be inspected before interpreting its results.

Metrics: MAE/RMSE, number of origins, origin-level forecasts. Overlapping horizons cause dependence; no statistical significance, confidence interval or causal pass-through claim is made. A stronger oil model is not assumed in advance. A negative result is retained.

## Engineering

- File-to-serving parity checks unique canonical instrument/date values against downloaded source snapshots.
- SIGKILL exercise: 40 committed synthetic events, kill only this project's Spark service, send 40 reversed events + eight duplicate messages + malformed unit + identity conflict + a 30-day-late valid record. Restart persistent state and require 81 unique observations plus two rejections.
- Stateful windows count contract-valid deliveries, including re-deliveries and identity conflicts; they are not canonical market bars. The recovery exercise independently compares recent synthetic counts / mean / min / max with a Python aggregate of delivered payloads. Seven-day watermark drops are expected for sufficiently late data, but the raw path must retain that data.
- Load exercise measures producer publication time and time until all synthetic identities are queryable in PostgreSQL. External SELECT polling measures first-observed visibility latency from event construction, with 0.5-second polling plus query-time uncertainty; it is not intrinsic Spark processing latency. Results are machine- and background-load-dependent.

No result demonstrates broker high availability, multi-node scaling, production market feed performance or a business SLA.
