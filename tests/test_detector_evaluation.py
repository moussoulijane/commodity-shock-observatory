import json
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from observatory.detector_evaluation import (
    METHODS,
    PROFILES,
    REFERENCE_PROTOCOL,
    EvaluationProtocol,
    MetricAccumulator,
    SyntheticPath,
    alert_episode_starts,
    calibrate_thresholds,
    common_eligible,
    generate_path,
    historical_review,
    match_episodes,
    run_evaluation,
    score_methods,
)
from observatory.signals import score_history


@pytest.mark.parametrize("profile", PROFILES)
def test_paired_generator_shares_noise_and_covers_jump_magnitudes(profile):
    clean = generate_path(profile, 2201, 400)
    jumps = generate_path(profile, 2201, 400, jumps=True)
    assert not clean.labels.any()
    assert jumps.labels.sum() == 3
    assert sorted(e["sigma_multiple"] for e in jumps.events) == [2, 4, 8]
    expected = np.zeros(400)
    for event in jumps.events:
        expected[event["index"] :] += event["injected_delta"]
    np.testing.assert_allclose(jumps.values - clean.values, expected, atol=1e-12)
    assert all(e["index"] > REFERENCE_PROTOCOL.window + 1 for e in jumps.events)


def test_every_method_is_prefix_causal():
    path = generate_path("volatility_regime", 2205, 800, jumps=True)
    before = score_methods(path.values[:400])
    changed = path.values.copy()
    changed[400:] = changed[400:] * 100 + 500
    after = score_methods(changed)
    for method in METHODS:
        np.testing.assert_allclose(before[method], after[method][:400], equal_nan=True)


def test_robust_score_matches_production_after_full_warmup():
    path = generate_path("heavy_tail_t4", 1104, 400)
    frame = pd.DataFrame(
        {
            "mode": "REPLAY",
            "instrument": "WTI",
            "frequency": "daily",
            "event_time": pd.date_range("2020-01-01", periods=400, tz="UTC"),
            "observed_at": pd.Timestamp("2025-01-01", tz="UTC"),
            "revision": "test",
            "value": path.values,
        }
    )
    production = score_history(frame)
    compared = score_methods(path.values)
    np.testing.assert_allclose(compared["rolling_robust"][61:], production.robust_z[61:])


def test_calibration_rejects_labelled_paths_and_overlapping_seeds():
    with pytest.raises(ValueError, match="disjoint"):
        EvaluationProtocol(calibration_seeds=(1,), test_seeds=(1,))
    with pytest.raises(ValueError, match="clean paths"):
        calibrate_thresholds([generate_path("gaussian", 1101, 400, jumps=True)])


def test_shared_budget_bounds_all_clean_training_alert_rates():
    protocol = replace(REFERENCE_PROTOCOL, observations_per_path=400)
    paths = [
        generate_path(profile, seed, 400)
        for profile in PROFILES
        for seed in protocol.calibration_seeds
    ]
    calibration = calibrate_thresholds(paths, protocol)
    assert calibration["clean_observations"] == 16 * (400 - 61)
    assert set(calibration["thresholds"]) == set(METHODS)
    for rate in calibration["observed_clean_training_alert_rates"].values():
        assert 0 <= rate <= protocol.alert_budget


def test_zero_and_negative_prices_never_produce_infinite_scores():
    values = np.linspace(-20, 20, 401)
    values[200] = 0
    scores = score_methods(values)
    for score in scores.values():
        assert not np.isinf(score).any()
    assert np.isnan(scores["return_budget"][201])
    assert np.isfinite(scores["rolling_robust"][201])
    assert not common_eligible(scores)[201]


def test_episode_matching_is_one_to_one_and_delays_exclude_misses():
    predicted = np.zeros(40, dtype=bool)
    predicted[[2, 3, 5, 11, 12, 24, 37]] = True
    starts = alert_episode_starts(predicted, merge_gap=2)
    assert starts.tolist() == [2, 11, 24, 37]
    assert match_episodes(starts, [10, 22, 30], max_delay=3) == {10: 1, 22: 2}
    path = SyntheticPath(
        "manual",
        1,
        np.zeros(40),
        np.zeros(40, dtype=bool),
        [{"index": 10}, {"index": 22}, {"index": 30}],
    )
    path.labels[[10, 22, 30]] = True
    accumulator = MetricAccumulator()
    accumulator.add(predicted, path, np.ones(40, dtype=bool), REFERENCE_PROTOCOL)
    result = accumulator.summary()
    assert result["episode_recall"] == 2 / 3
    assert result["episode_precision"] == 2 / 4
    assert result["undetected_events"] == 1
    assert result["mean_detection_delay_observations"] == 1.5


def test_report_is_reproducible_and_does_not_claim_real_accuracy(tmp_path):
    protocol = EvaluationProtocol(
        calibration_seeds=(1101,), test_seeds=(2201,), observations_per_path=400
    )
    report = run_evaluation(protocol=protocol, artifact_dir=tmp_path, include_history=False)
    repeated = run_evaluation(protocol=protocol, artifact_dir=tmp_path, include_history=False)
    assert report["calibration"] == repeated["calibration"]
    assert report["metrics"] == repeated["metrics"]
    assert report["reference_protocol"] is False
    assert report["production_detector_is_unchanged"] is True
    assert report["historical_review"]["labels_available"] is False
    assert len(report["metrics"]) == 5
    assert all(row["labelled_events"] == 12 for row in report["metrics"])
    assert report["metrics"][-1]["role"] == "diagnostic_not_budget_matched"
    saved = json.loads((tmp_path / "detector-evaluation-v2.json").read_text())
    assert saved["schema_version"] == 2
    assert saved["metrics"] == report["metrics"]


def test_missing_history_cannot_invent_real_world_labels(tmp_path):
    result = historical_review(tmp_path / "missing.jsonl")
    assert result["status"] == "not_available"
    assert result["labels_available"] is False
