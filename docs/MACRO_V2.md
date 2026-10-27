# Macro research V2: revisions, labels and uncertainty

The public function signatures and report filenames remain compatible with V1. Existing report fields and evaluation-type labels are preserved. Additional sections expose coverage, regimes, paired loss intervals, predictive bands and retrospective revision sensitivity. A SHA256 of the normalized model-input panel identifies the exact economic observations used; acquisition timestamps and credentials are not included in that hash.

## Information set

Strict acquisition enumerates and paginates FRED vintage dates, then requests `output_type=2` (all observations by vintage date) in bounded vintage batches. That output can be a matrix with a column per vintage; the parser handles that format and scalar interval-style rows defensively. Observation pages are paginated and incomplete/changing pagination fails explicitly. Unchanged snapshots are compressed to revision events. This format can require substantial network transfer for a daily history; a real API run remains necessary to audit source coverage and operational cost.

At each forecast origin the feature builder selects the latest available revision per series/economic period, before aggregating daily oil into monthly means. A second vintage of the same trading day is never another observation in the monthly average. Missing revisions after publication withdraw a value rather than carrying an obsolete number forward. API vintage dates become available at the next UTC midnight, conservatively, because intraday publication times are unavailable.

Offline CSV inspection/import supports `series,period,value,available_at`; `series` is CPI or OIL, and `available_at` must contain a timezone. Use `macro --panel file.csv` without `--strict`. Every generic CSV import is marked `information_set_kind=unverified_csv` and `strict_eligible=False`, including a file containing columns that claim otherwise. The optional `FRED_VINTAGE_CSV` loading hook also returns an unverified panel; it does not authenticate a source.

`evaluate(strict=True)` requires acquisition metadata from the ALFRED revision-event path: `information_set_kind=alfred_revision_events`, the boolean `strict_eligible=True`, and the implemented next-UTC-midnight vintage-availability policy. It rejects generic CSV and current revised snapshots before fitting or writing `macro-strict.json`. There is currently no audited offline sidecar-manifest import, so an arbitrary local file cannot become strict evidence merely through `--strict`. These acquisition flags identify the trusted code path, not an independent certification that the archive is complete. HTTP mocks and explicitly tagged fabricated panels are used only inside tests; their output stays in temporary test folders.

## Fixed first-release target

For target month T, the numerator is its first finite CPI level. The year-earlier denominator is the latest value known at T's release, including revisions already available then. The ratio and its availability are frozen thereafter as the training/evaluation label. A first-seen observation more than 120 calendar days after its economic period is treated as archive-censored and excluded from this target, rather than being called an original release. This lag screen is an audit heuristic and can exclude legitimate delayed releases; inspect the diagnostics.

Historical-origin feature vectors are frozen at their own origins. Training includes only labels released by the current forecast origin. Final-vintage target ratios are evaluated separately on the same forecasts for revision-sensitivity diagnostics. They never enter training labels or interval calibration. Horizons refer to the calendar month of origin +1 / +3, not the number of observations since the latest published CPI.

## Uncertainty

Each model is compared against persistence through paired absolute-error differences. A circular moving-block bootstrap supplies an exploratory 95% percentile interval for the mean difference, with 1,000 replicates and block length at least horizon+1. Negative differences favour the challenger. Fewer than 12 origins yield no interval. Nonstationarity, block-length choice and multiple comparisons limit interpretation; this is not a causal or definitive significance claim.

Optional 90% forecast bands use at most 60 prior out-of-sample absolute residuals, with at least 24 matured outcomes, plus up to 36 earlier forecast origins for warm-up. The half-width is the ordered residual at rank `min(n, ceil(.9*(n+1)))`. An outcome is eligible only after its target release. In-sample fitted residuals, unreleased labels and final-vintage targets are excluded. Time dependence and changing regimes preclude a distribution-free coverage guarantee; empirical coverage and width are reported rather than promised.

Regimes are fixed calendar bins (before 2020, 2020–2022, 2023 onward), used descriptively. Their small samples should not be interpreted as independently labelled economic regimes.

## Validation boundary

HTTP mocks cover wide vintage matrices, observation/vintage pagination, incomplete pages and credential-safe errors. Fabricated revision panels test latest-known values, denominator policy, archive censoring, withdrawals and future-data exclusion. No real FRED key was supplied; the strict external source call remains unvalidated.

Official format references: [FRED observations API](https://fred.stlouisfed.org/docs/api/fred/series_observations.html), [ALFRED download formats](https://alfred.stlouisfed.org/help/downloaddata).
