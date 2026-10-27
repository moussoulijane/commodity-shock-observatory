# Detector evaluation V2: held-out seeds and an equal alert budget

V2 is an offline research comparison. It does not modify production V1 scores or
deploy new alert thresholds. The earlier 11-jump example remains a simple V1
sensitivity check, not an accuracy estimate.

## Fixed reference protocol

Four nuisance regimes: Gaussian innovations, unit-variance Student-t(4) tails,
piecewise volatility, and smooth drift. Each path starts at 100 with base
innovation standard deviation 0.4 price units. Calibration seeds are 1101–1104;
disjoint held-out seeds are 2201–2208. Every regime has equal representation.
Each path has 1,600 observations.

Each held-out seed produces paired clean and jump paths with identical noise.
Three signed jumps, of 2, 4 and 8 times the local innovation standard deviation,
are inserted at reproducible positions. These inserted jumps are the only
ground truth. Large natural heavy-tailed innovations are nuisance events under
this definition; they are not verified false economic shocks.

## Methods and calibration

Compare rolling median/MAD (the V1 robust score once its full window is ready),
rolling mean/standard deviation, EWMA mean/innovation variance (decay 0.94), and
absolute percentage returns. Difference scores share the V1 scale floor: 0.1%
of the median absolute prior price, with minimum 1e-8. Percentage changes divide
by the absolute previous price and exclude zero denominators. No log returns
are used for zero or negative prices.

All methods use the same finite observations after 61 warm-up observations.
Scores use previous information only; EWMA updates after scoring the current
change. For each method, calibrate its upper 99th absolute-score percentile on
pooled CLEAN calibration histories only. Higher quantile interpolation plus a
strict greater-than rule makes 1% a conservative maximum training alert rate.
Freeze thresholds before evaluating held-out paths. No threshold is fitted to
test labels. This equal calibration budget does not guarantee equal held-out
false-alert rates after a distribution shift.

The fixed 5% return rule remains a diagnostic, explicitly outside the
budget-matched comparison. It cannot establish detector superiority.

## Metrics and limits

Report point precision/recall and false alerts per 1,000 non-event observation
steps, globally and by regime. Paired held-out clean controls test whether the
calibration alert budget transfers. Recall by jump magnitude exposes difficult
2σ cases instead of hiding them in an overall average.

Alerts separated by at most two unflagged observations form one episode. A new
episode can match one event at/after onset within three observations. Report
episode precision/recall, misses and detection delays in observation steps.
An alarm can match by chance within this tolerance. Delays condition on detected
events and must be read alongside recall; no wall-clock frequency is inferred.
Observations within a path and paired paths are dependent. No significance or
independent economic accuracy claim is made; this is not independent
preregistration.

With a local source snapshot, the report includes method-score coverage and
post-hoc high V1 score examples from selected years (2008, 2020, 2022).
Historical precision/recall remain unavailable without independent labels.
Synthetic thresholds are not validated for transfer to real instruments or
frequencies. Production analyst alerts retain V1.

## Run

```powershell
.venv\Scripts\python.exe -m observatory.detector_evaluation
```

Output: `artifacts/detector-evaluation-v2.json`. The report preserves seeds,
parameters, calibration thresholds, aggregate/stratified metrics and caveats.
Kafka, Spark, a database and API keys are not required; historical coverage is
optional when the local source snapshot is absent.
