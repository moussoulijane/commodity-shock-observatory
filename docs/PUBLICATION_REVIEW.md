# Publication review — 27 October 2026

## Verdict and positioning

Publish as a data-engineering portfolio project with an applied-ML research component. The strongest contribution is handling mutable market data and testing recovery, rather than claiming a new forecasting model. This is an internal technical review of the repository and its local evidence, not external certification.

For a Data Engineer or ML Engineer interview, the useful discussion is concrete: why delivery identity differs from an economic revision, how late corrections determine bounded recomputation, what happens between PostgreSQL and Delta commits, and what a serving-visibility benchmark actually measures. The implementation and regression tests provide material for each question.

For a Data Scientist interview, the research shows temporal information-set handling, sensible baselines, held-out simulation paths and uncertainty diagnostics. It does **not** demonstrate predictive advantage, independent economic-shock accuracy or quant trading performance. The macro task concerns US CPI; the Moroccan importer scenarios are a separate mechanical illustration.

## Findings addressed before publication

| Finding | Correction | Regression evidence |
| --- | --- | --- |
| A Kafka null value could fail the non-null audit insert repeatedly and stall its batch | V2 migrates the audit column to preserve SQL NULL, quarantines the transport record and allows subsequent offsets to proceed; a tombstone does not delete a market observation | [PostgreSQL integration tests](../tests/test_incremental_store.py) |
| The parity verifier conflated valid LIVE and REPLAY observations sharing an event ID | Compare `(mode, event_id)` throughout the independent feature comparison | [Cross-mode parity regressions](../tests/test_validation_v2.py) cover shared IDs, one-mode corruption and a missing mode-specific row |
| “Held-out regimes” overstated the detector split | Describe held-out paths/seeds across the same four regimes | [Detector protocol](DETECTORS_V2.md) also states that event instances are correlated across profiles |
| The introductory presentation buried the most useful evidence | Add an evidence table, architecture overview and an explicit explanation of why Kafka/Spark is used | [README](../README.md) |

## Why this is more than a generic streaming demonstration

- Indexed neighborhoods bound the work caused by sparse historical corrections. Tests distinguish sparse and dense batches rather than checking only a small happy path.
- Spark computes exact rolling order statistics and canonical OHLC; pandas supplies a separate feature oracle. Delivery telemetry is kept distinct from canonical observations.
- Durable dirty events, generation fencing and forced failures address the gap between multiple storage systems. They do not pretend to create a distributed atomic transaction.
- Independent per-mode clocks prevent synthetic traffic from advancing historical replay watermarks.
- Measured results include an unmet latency target and disappointing model comparisons. These are retained with the protocol needed to interpret them.

## Boundaries that must remain visible

The workload is local and uses a single broker and Spark `local[2]`. Licensed live data, strict external vintage acquisition, high availability and multi-node operations are not validated here. Synthetic detector labels are not independently verified economic shocks. Revised macro inputs cannot establish real-time forecast performance. Exact replication of the dated aggregate metrics requires the omitted frozen input snapshot; a panel hash alone cannot restore it.

Recovery exercises verify specific signal-serving outcomes. The partial-commit exercise does not separately assert convergence of every Silver row, OHLC bar and completion-ledger entry. Full parity starts from the durable PostgreSQL observation history; a separate archive-to-serving completeness check would strengthen the ingestion boundary. These are useful next investigations, not claims already established by the current reports.

The [local validation report](LOCAL_VALIDATION_V2.md), [consistency boundaries](ARCHITECTURE_V2.md) and repository Actions page should be read together. A green build verifies the configured checks; it is not a production-readiness certificate.
