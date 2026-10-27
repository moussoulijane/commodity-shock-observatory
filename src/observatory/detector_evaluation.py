"""Held-out detector comparison at a shared, clean-calibration alert budget.

This is a controlled sensitivity experiment, not an estimate of economic-shock
accuracy. Synthetic innovations are nuisance variation; only inserted jumps are
labelled events. Thresholds are frozen before held-out paths are scored. The
production V1 detector and its threshold are deliberately not changed here.
"""

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

from observatory.config import ARTIFACTS, DATA
from observatory.contracts import Quote
from observatory.signals import current_snapshot, score_history

PROFILES = ("gaussian", "heavy_tail_t4", "volatility_regime", "smooth_drift")
METHODS = ("rolling_robust", "rolling_standard", "ewma", "return_budget")
DIAGNOSTIC = "fixed_5pct_diagnostic"


@dataclass(frozen=True)
class EvaluationProtocol:
    calibration_seeds: tuple[int, ...] = (1101, 1102, 1103, 1104)
    test_seeds: tuple[int, ...] = (2201, 2202, 2203, 2204, 2205, 2206, 2207, 2208)
    observations_per_path: int = 1600
    window: int = 60
    alert_budget: float = 0.01
    ewma_decay: float = 0.94
    max_detection_delay: int = 3
    episode_merge_gap: int = 2

    def __post_init__(self):
        if not self.calibration_seeds or not self.test_seeds:
            raise ValueError("Both calibration and held-out seeds are required")
        if len(set(self.calibration_seeds)) != len(self.calibration_seeds) or len(
            set(self.test_seeds)
        ) != len(self.test_seeds):
            raise ValueError("Duplicate seeds are not independent paths")
        if set(self.calibration_seeds) & set(self.test_seeds):
            raise ValueError("Calibration and test seeds must be disjoint")
        if self.observations_per_path < 400 or not 2 <= self.window <= 80:
            raise ValueError("Use at least 400 observations and a window of 2..80")
        if not 0 < self.alert_budget < 0.1 or not 0 < self.ewma_decay < 1:
            raise ValueError("Invalid budget or EWMA decay")
        if self.max_detection_delay < 0 or self.episode_merge_gap < 0:
            raise ValueError("Episode settings cannot be negative")


REFERENCE_PROTOCOL = EvaluationProtocol()


@dataclass
class SyntheticPath:
    profile: str
    seed: int
    values: np.ndarray
    labels: np.ndarray
    events: list[dict]


def generate_path(profile: str, seed: int, n: int = 1600, *, jumps: bool = False):
    """Paired clean/jump paths share innovations; event schedules use a separate RNG."""
    if profile not in PROFILES or n < 400:
        raise ValueError("Unknown profile or insufficient path length")
    rng = np.random.default_rng(seed)
    sigma = np.full(n, 0.4)
    drift = np.zeros(n)
    if profile == "heavy_tail_t4":
        noise = rng.standard_t(4, n) / np.sqrt(2)
    else:
        noise = rng.normal(size=n)
    if profile == "volatility_regime":
        sigma[n // 3 : 2 * n // 3] = 1.2
        sigma[2 * n // 3 :] = 0.25
    if profile == "smooth_drift":
        drift = 0.1 * np.sin(2 * np.pi * np.arange(n) / n)
        drift[n // 2 :] += 0.04
    increments = noise * sigma + drift
    labels = np.zeros(n, dtype=bool)
    events = []
    if jumps:
        event_rng = np.random.default_rng(seed + 900_000)
        magnitudes = event_rng.permutation([2.0, 4.0, 8.0])
        for j, multiple in enumerate(magnitudes):
            index = (j + 1) * n // 4 + int(event_rng.integers(-10, 11))
            sign = 1 if (seed + j) % 2 else -1
            injected_delta = sign * float(multiple) * sigma[index]
            increments[index] += injected_delta
            labels[index] = True
            events.append(
                {
                    "index": index,
                    "sigma_multiple": float(multiple),
                    "direction": "UP" if sign > 0 else "DOWN",
                    "injected_delta": float(injected_delta),
                }
            )
    return SyntheticPath(profile, seed, 100 + np.cumsum(increments), labels, events)


def score_methods(values, *, window=60, ewma_decay=0.94):
    """Signed scores use only observations strictly earlier than the scored change.

    The robust score matches V1 after the complete window is available. All four
    compared methods use the SAME full-window evaluation mask, avoiding different
    warm-up periods. EWMA is initialized from that previous window and updated
    only after each current observation has been scored.
    """
    values = np.asarray(values, dtype=float)
    if values.ndim != 1 or len(values) <= window + 1 or not np.isfinite(values).all():
        raise ValueError("A finite one-dimensional history longer than window+1 is required")
    result = {method: np.full(len(values), np.nan) for method in METHODS}
    delta = np.diff(values)
    prior_changes = sliding_window_view(delta, window)[:-1]
    current = delta[window:]
    previous_prices = sliding_window_view(abs(values), window)[1:-1]
    floor = np.maximum(np.median(previous_prices, axis=1) * 0.001, 1e-8)
    centers = np.median(prior_changes, axis=1)
    mad = np.median(abs(prior_changes - centers[:, None]), axis=1)
    result["rolling_robust"][window + 1 :] = (current - centers) / np.maximum(1.4826 * mad, floor)
    result["rolling_standard"][window + 1 :] = (
        current - np.mean(prior_changes, axis=1)
    ) / np.maximum(np.std(prior_changes, axis=1, ddof=1), floor)
    mean, variance = float(np.mean(delta[:window])), float(np.var(delta[:window], ddof=1))
    for j, change in enumerate(current):
        result["ewma"][window + 1 + j] = (change - mean) / max(np.sqrt(variance), floor[j])
        innovation = change - mean
        mean = ewma_decay * mean + (1 - ewma_decay) * change
        variance = ewma_decay * variance + (1 - ewma_decay) * innovation**2
    previous = abs(values[window:-1])
    with np.errstate(divide="ignore", invalid="ignore"):
        returns = 100 * current / previous
    result["return_budget"][window + 1 :] = np.where(previous > 1e-9, returns, np.nan)
    return result


def common_eligible(scores):
    return np.logical_and.reduce([np.isfinite(scores[method]) for method in METHODS])


def calibrate_thresholds(paths, protocol=REFERENCE_PROTOCOL):
    """Unsupervised upper quantiles of CLEAN calibration histories, never test labels."""
    values = {method: [] for method in METHODS}
    for path in paths:
        if path.labels.any() or path.events:
            raise ValueError("Threshold calibration requires clean paths without inserted jumps")
        scores = score_methods(path.values, window=protocol.window, ewma_decay=protocol.ewma_decay)
        eligible = common_eligible(scores)
        for method in METHODS:
            values[method].extend(abs(scores[method][eligible]).tolist())
    if not all(values.values()):
        raise ValueError("No eligible clean calibration observations")
    thresholds = {
        method: float(np.quantile(v, 1 - protocol.alert_budget, method="higher"))
        for method, v in values.items()
    }
    # Strict > handles quantile ties conservatively; budget is an upper bound.
    rates = {
        method: float(np.mean(np.asarray(values[method]) > thresholds[method]))
        for method in METHODS
    }
    return {
        "thresholds": thresholds,
        "clean_observations": len(values[METHODS[0]]),
        "observed_clean_training_alert_rates": rates,
        "budget_per_eligible_observation": protocol.alert_budget,
        "rule": "absolute score > upper calibration quantile; higher interpolation; no test refit",
    }


def alert_episode_starts(predicted, merge_gap=2):
    indices = np.flatnonzero(predicted)
    if not len(indices):
        return np.array([], dtype=int)
    return indices[np.r_[True, np.diff(indices) > merge_gap + 1]]


def match_episodes(starts, event_indices, max_delay=3):
    """One new alert episode can match one event, at/after onset within fixed tolerance."""
    unused = set(int(i) for i in starts)
    matches = {}
    for event in sorted(event_indices):
        candidates = [start for start in unused if event <= start <= event + max_delay]
        if candidates:
            first = min(candidates)
            matches[int(event)] = first - int(event)
            unused.remove(first)
    return matches


@dataclass
class MetricAccumulator:
    observations: int = 0
    non_event_observations: int = 0
    true_positives: int = 0
    false_positives: int = 0
    false_negatives: int = 0
    events: int = 0
    excluded_events: int = 0
    alert_episodes: int = 0
    matched_episodes: int = 0
    delays: list[int] = field(default_factory=list)

    def add(self, predicted, path, eligible, protocol):
        predicted = np.asarray(predicted, dtype=bool) & eligible
        labels = path.labels & eligible
        self.observations += int(eligible.sum())
        self.non_event_observations += int((eligible & ~path.labels).sum())
        self.true_positives += int((predicted & labels).sum())
        self.false_positives += int((predicted & ~labels).sum())
        self.false_negatives += int((~predicted & labels).sum())
        event_indices = [event["index"] for event in path.events if eligible[event["index"]]]
        self.events += len(event_indices)
        self.excluded_events += len(path.events) - len(event_indices)
        starts = alert_episode_starts(predicted, protocol.episode_merge_gap)
        matches = match_episodes(starts, event_indices, protocol.max_detection_delay)
        self.alert_episodes += len(starts)
        self.matched_episodes += len(matches)
        self.delays.extend(matches.values())

    def summary(self):
        def ratio(n, d):
            return n / d if d else None

        return {
            "eligible_observations": self.observations,
            "non_event_observations": self.non_event_observations,
            "true_positives": self.true_positives,
            "false_positives": self.false_positives,
            "false_negatives": self.false_negatives,
            "point_precision": ratio(
                self.true_positives, self.true_positives + self.false_positives
            ),
            "point_recall": ratio(self.true_positives, self.true_positives + self.false_negatives),
            "false_alerts_per_1000_non_event_observations": ratio(
                1000 * self.false_positives, self.non_event_observations
            ),
            "alert_rate": ratio(self.true_positives + self.false_positives, self.observations),
            "labelled_events": self.events,
            "events_excluded_by_coverage": self.excluded_events,
            "alert_episodes": self.alert_episodes,
            "matched_episodes": self.matched_episodes,
            "episode_precision": ratio(self.matched_episodes, self.alert_episodes),
            "episode_recall": ratio(self.matched_episodes, self.events),
            "undetected_events": self.events - self.matched_episodes,
            "mean_detection_delay_observations": float(np.mean(self.delays))
            if self.delays
            else None,
            "median_detection_delay_observations": float(np.median(self.delays))
            if self.delays
            else None,
            "p90_detection_delay_observations": float(np.percentile(self.delays, 90))
            if self.delays
            else None,
        }


def historical_review(path=None):
    """Unlabelled coverage and post-hoc examples; never historical precision/recall."""
    path = Path(path) if path is not None else DATA / "quotes.jsonl"
    if not path.exists():
        return {
            "status": "not_available",
            "labels_available": False,
            "reason": "No local source snapshot; run cso fetch for historical case studies",
        }
    observations = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        quote = Quote.model_validate_json(line)
        if quote.mode == "REPLAY":
            observations.append(quote.model_dump(mode="json"))
    frame = pd.DataFrame(observations)
    if frame.empty:
        return {"status": "empty", "labels_available": False}
    for column in ("event_time", "observed_at", "ingested_at"):
        frame[column] = pd.to_datetime(frame[column], utc=True, format="ISO8601")
    frame = current_snapshot(frame)
    scored = score_history(frame)
    coverage, cases = [], []
    for instrument, group in scored.groupby("instrument"):
        group = group.sort_values("event_time")
        method_scores = score_methods(group.value.to_numpy()) if len(group) > 61 else None
        coverage.append(
            {
                "instrument": instrument,
                "frequency": group.frequency.iloc[0],
                "observations": len(group),
                "v1_eligible": int(group.robust_z.notna().sum()),
                "v1_fixed_threshold_alerts": int(group.robust_alert.sum()),
                "v2_full_60_observation_score_coverage": {
                    method: int(np.isfinite(method_scores[method]).sum()) if method_scores else 0
                    for method in METHODS
                },
            }
        )
        for year in (2008, 2020, 2022):
            selected = group[group.event_time.dt.year == year].copy()
            selected["magnitude"] = selected.robust_z.abs()
            for _, row in selected.nlargest(2, "magnitude").iterrows():
                if pd.notna(row.robust_z):
                    cases.append(
                        {
                            "instrument": instrument,
                            "period": str(year),
                            "observation_date": row.event_time.isoformat(),
                            "v1_robust_z": float(row.robust_z),
                            "direction": row.direction,
                        }
                    )
    return {
        "status": "completed",
        "labels_available": False,
        "coverage": coverage,
        "case_studies": cases,
        "historical_precision": None,
        "historical_recall": None,
        "interpretation": "Post-hoc high-score examples in selected years, not independently labelled shocks. V2 coverage uses 60 observations; synthetic thresholds are not validated for transfer to these instruments/frequencies.",
    }


def run_evaluation(*, protocol=REFERENCE_PROTOCOL, artifact_dir=None, include_history=True):
    calibration_paths = [
        generate_path(profile, seed, protocol.observations_per_path)
        for profile in PROFILES
        for seed in protocol.calibration_seeds
    ]
    calibration = calibrate_thresholds(calibration_paths, protocol)
    names = (*METHODS, DIAGNOSTIC)
    global_metrics = {name: MetricAccumulator() for name in names}
    clean_metrics = {name: MetricAccumulator() for name in names}
    profile_metrics = {
        profile: {name: MetricAccumulator() for name in names} for profile in PROFILES
    }
    magnitude_counts = {name: {multiple: [0, 0] for multiple in (2.0, 4.0, 8.0)} for name in names}
    for profile in PROFILES:
        for seed in protocol.test_seeds:
            for jumps in (False, True):
                path = generate_path(profile, seed, protocol.observations_per_path, jumps=jumps)
                scores = score_methods(
                    path.values, window=protocol.window, ewma_decay=protocol.ewma_decay
                )
                eligible = common_eligible(scores)
                for name in names:
                    score = scores["return_budget" if name == DIAGNOSTIC else name]
                    predicted = (
                        abs(score) >= 5
                        if name == DIAGNOSTIC
                        else abs(score) > calibration["thresholds"][name]
                    )
                    if jumps:
                        global_metrics[name].add(predicted, path, eligible, protocol)
                        profile_metrics[profile][name].add(predicted, path, eligible, protocol)
                        for event in path.events:
                            if eligible[event["index"]]:
                                count = magnitude_counts[name][event["sigma_multiple"]]
                                count[0] += int(predicted[event["index"]])
                                count[1] += 1
                    else:
                        clean_metrics[name].add(predicted, path, eligible, protocol)
    report = {
        "schema_version": 2,
        "status": "completed",
        "created_at": datetime.now(UTC).isoformat(),
        "evaluation_type": "controlled_synthetic_held_out_seeds",
        "reference_protocol": protocol == REFERENCE_PROTOCOL,
        "protocol": {
            **asdict(protocol),
            "profiles": PROFILES,
            "jump_magnitudes_sigma": [2, 4, 8],
            "jumps_per_path": 3,
            "price_start": 100,
            "base_innovation_sigma": 0.4,
            "observation_time_unit": "one synthetic observation-step; no wall-clock frequency inference",
            "episode_rule": "consecutive alerts separated by at most merge_gap unflagged observations form one episode; one new episode can match one event at/after onset within max_detection_delay",
            "parameter_selection": "Fixed reference design; no threshold or model tuning on held-out paths; not independent preregistration",
        },
        "calibration": calibration,
        "metrics": [
            {
                "method": name,
                "role": "diagnostic_not_budget_matched" if name == DIAGNOSTIC else "budget_matched",
                **global_metrics[name].summary(),
            }
            for name in names
        ],
        "held_out_clean": [{"method": name, **clean_metrics[name].summary()} for name in names],
        "by_profile": [
            {"profile": profile, "method": name, **profile_metrics[profile][name].summary()}
            for profile in PROFILES
            for name in names
        ],
        "point_recall_by_jump_magnitude": [
            {
                "method": name,
                "sigma_multiple": multiple,
                "detected": count[0],
                "events": count[1],
                "point_recall": count[0] / count[1] if count[1] else None,
            }
            for name in names
            for multiple, count in magnitude_counts[name].items()
        ],
        "historical_review": historical_review()
        if include_history
        else {"status": "not_requested", "labels_available": False},
        "production_detector_is_unchanged": True,
        "limitations": [
            "Known inserted synthetic jumps are the only ground truth; heavy-tailed natural innovations are nuisance events under this definition, not verified false economic shocks",
            "Same pooled clean calibration alert budget does not guarantee equal false-alert rates under held-out regimes or real markets",
            "Episode matching permits chance alarms within the fixed delay tolerance; delay statistics condition on detected events and exclude misses",
            "Observations within paths and paired clean/jump paths are dependent; no significance claim or independent economic accuracy estimate",
            "Fixed 5% return threshold is diagnostic only and must not be presented as a fair budget-matched competitor",
            "Synthetic thresholds are research outputs and are not deployed into the V1 analyst alerts",
        ],
    }
    target = Path(artifact_dir) if artifact_dir is not None else ARTIFACTS
    target.mkdir(parents=True, exist_ok=True)
    output = target / "detector-evaluation-v2.json"
    temporary = output.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(output)
    return report


if __name__ == "__main__":
    result = run_evaluation()
    print(
        json.dumps(
            {
                "status": result["status"],
                "calibration": result["calibration"],
                "metrics": result["metrics"],
            },
            indent=2,
        )
    )
