# V2 local validation — 26–27 October 2026

The aggregate reference is [local-v2-reference.json](evidence/local-v2-reference.json). The [V1 reference](evidence/local-reference.json) is preserved separately. Detailed forecasts, raw observations, runtime tables, lake files and checkpoints remain local and ignored by Git. These are measurements on Docker Desktop, with one Kafka broker, Spark `local[2]`, two shuffle partitions and at most 2,000 Kafka offsets per trigger.

## Correctness and recovery

| Check | Observed result | Interpretation |
| --- | --- | --- |
| Public reference history | 13,796 REPLAY observations: Brent 6,491; WTI 6,342; gold, copper and wheat 321 each | Daily/monthly revised snapshots, not intraday coverage |
| Final serving parity | 30,069 canonical observations; zero missing and zero feature mismatches against a full-history pandas oracle | Includes 16,273 synthetic engineering observations; the total is not additional real-market coverage |
| Actual Spark SQL oracle | 650 observations; 63 bounded-context outputs; exact median/MAD, negative/zero prices, monthly windows, microsecond timestamps and canonical OHLC pass | Exercises Spark computations, rather than only an equivalent Python implementation |
| Independent clocks | Historical REPLAY watermark remains in 2009 while a separate SYNTHETIC query receives 2026 data | Actual independent file-stream queries; artificial clock fixtures do not enter market topics |
| PostgreSQL integration | 12 tests pass against disposable private schemas | Conflict precedence, migrations, fencing, bounded context, touched-bar reads and Kafka tombstone progression; public serving data untouched |
| Cross-mode parity regressions | Three tests preserve shared LIVE/REPLAY event IDs and detect a missing or corrupted row in one mode | Comparison identities include both mode and event ID |
| Null Kafka values | Legacy audit constraint migration, mixed/standalone tombstones, distinct SQL NULL versus JSON text `null`, retries and later valid offsets pass | PostgreSQL integration proof; tombstones are quarantined and do not delete economic observations |
| Sparse correction context | Two distant corrections in a 30,050-row fixture read 126 feature-context rows and 110 bar observations | Indexed neighborhoods avoid reading the intervening history |
| Dense correction context | 2,000 adjacent anchors read 2,124 context rows | One merged neighborhood, without per-anchor duplicate histories |
| SIGKILL recovery | 81 unique observations; eight duplicates; two quarantined messages; 40 out-of-order deliveries | Revised quote replaces its prior serving identity; raw revisions remain; 30-day-late correction and affected later scores reconcile |
| Partial-commit recovery | 30 observations recovered after an intentional failure following PostgreSQL ingest, before Silver/features | Checks raw visibility and recovered signal identities; Silver contents, OHLC and completion-ledger convergence are not separately asserted by this exercise |

The final unit suite passed **122 tests**, with **12 opt-in PostgreSQL tests skipped**. Those 12 integration tests also passed in a separate database run. Ruff, frontend production build and Prettier checks passed. A dependency deprecation warning from Starlette's TestClient remains. These counts include the pre-publication regressions; future additions can change them.

## Measured workload and latency

Serving was polled concurrently with publication every 0.25 seconds. Latency is from submission to the first observed canonical row in `signals_v2`; it includes polling and query uncertainty. All-visible and drain metrics do not necessarily wait for OHLC writes or full callback completion. Broker offset lag alone cannot measure in-flight Spark work. These measurements use synthetic traffic only.

| Metric | Burst | Sustained |
| --- | ---: | ---: |
| Events | 5,000 | 6,000 |
| Requested publication workload | Immediate burst | 50 events/s for 120 s |
| Achieved publication rate | 7,486.10 events/s | 50.01 events/s |
| Publication duration | 0.668 s | 119.984 s |
| All events visible, from start | 26.149 s | 126.162 s |
| Drain after publication ends | 25.481 s | 6.178 s |
| First-observed visibility p50 | 17.779 s | 7.924 s |
| First-observed visibility p95 | 26.074 s | 11.157 s |
| End-to-end events/s | 191.21 | 47.56 |
| Maximum sampled Kafka offset lag | 3,000 | 0 |
| Observed ingestion batches | 2 | 21 |

**The local p95 objective of 10 seconds is not met in either workload.** Completing a workload does not mean passing the latency objective. The sustained sampled Kafka lag of zero does not imply zero processing backlog. Maximum sampled window-state memory was 1,608 bytes for a single query; it is neither total system memory nor a sum across all queries.

V1 used a different latency protocol. Its 42.47-second burst result cannot establish a causal speedup factor. These local measurements do not establish multi-node capacity, broker high availability or a production SLA.

## Economic results

The revised-data macro experiment produced 768 forecasts: four models, two horizons and 96 evaluated origins per horizon. The selected origins span 99 calendar months; gaps matter when interpreting serial dependence and block-bootstrap uncertainty.

| Model | MAE at 1 month / pp | MAE at 3 months / pp |
| --- | ---: | ---: |
| Persistence | 0.5505 | 0.8591 |
| AR + oil ridge | 0.5557 | 0.9248 |
| XGBoost | 0.8706 | 1.1985 |

Oil-ridge's paired mean absolute-error difference versus persistence has exploratory 95% moving-block intervals of **[-0.0935, +0.1171] pp** at one month and **[-0.1766, +0.3269] pp** at three months. Both include zero. This specification does not establish an improvement over persistence. It uses current revised snapshots with assumed availability lags, so it does not establish real-time forecast performance or a causal oil effect.

Nominal 90% past-residual bands under-cover: oil-ridge achieves **85.4% / 81.2%**, and XGBoost **81.2% / 77.1%**, at one / three months. These empirical rates are diagnostics, not guaranteed calibration.

The detector study fixes a 1% clean-history alert budget in calibration, then evaluates disjoint held-out seeds across Gaussian, heavy-tailed, volatility-shift and drift regimes. Across 96 inserted test events, the robust detector recalls 68.75%; standard and EWMA detectors recall 65.625%; the return-budget detector recalls 27.08%. Rare-event point precision is only about 11–12% for the first three methods. Historical crisis examples lack independent labels, so historical precision and recall remain unknown. The study does not replace the deployed `|z| >= 4` screening rule.

## Analyst interface

Local browser checks cover the default energy scenario, switching to a partially hedged copper profile, a negative commodity shock, clearing stale results after input changes, source dates and frequencies, and expanded interpretation limits. The energy case reconciles 14,366,400 MAD reference cost to 16,221,511 MAD under +10% commodity / +3% FX assumptions. The copper case shows that a hedge can forego part of a favourable price move.

Desktop and 390-pixel mobile breakpoints were inspected; no horizontal document overflow was observed. Captures are included in [desktop](figures/v2-exposure-desktop.png) and [mobile](figures/v2-exposure-mobile.png). This is local functional and layout verification, not an accessibility certification or a cross-browser test matrix.

## Still conditional

- No personal FRED API key was supplied. The strict ALFRED path is tested with mocks and fabricated revision panels, but has not been exercised against a real account or audited for archival completeness. Generic CSV timestamps remain unverified and cannot become strict evidence through a flag.
- The entitled live-feed adapter has not been exercised with a licensed account. A persistent bounded publisher alone does not validate an external market feed.
- This local report predates the first GitHub Actions run. Consult the repository's Actions page for current remote validation status.
- Scenario quantities, bases, hedge fractions and MAD/USD are explicit demonstration assumptions. The output is purchasing-cost arithmetic, not company P&L, inflation, PD or credit-loss forecasting.

Use the [French V2 guide](V2_GUIDE_FR.md) to reproduce the checks. Recovery exercises deliberately stop this project's Spark process; run them sequentially, and retain each completion ledger with its checkpoint.
