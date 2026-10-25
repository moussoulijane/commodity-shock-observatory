"""Causal, frequency-aware detectors. No log returns: WTI can cross zero."""

import numpy as np
import pandas as pd

DETECTOR_VERSION = "delta-robust-v1"


def score_history(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    out = []
    for (_, _), group in frame.groupby(["mode", "instrument"], sort=True):
        group = group.sort_values(["event_time", "observed_at", "revision"])
        group = group.drop_duplicates("event_time", keep="last").copy()
        values = group.value.astype(float)
        delta = values.diff()
        prior = values.shift(1)
        # Absolute denominator is explicit; crossing zero is not a normal return.
        group["change_pct"] = (100 * delta / prior.abs()).where(prior.abs() > 1e-9)
        group["delta"] = delta
        min_n, lookback = (12, 36) if group.frequency.iloc[0] == "monthly" else (20, 60)
        history = delta.shift(1).rolling(lookback, min_periods=min_n)
        median = history.median()
        mad = history.apply(lambda x: np.nanmedian(np.abs(x - np.nanmedian(x))), raw=True)
        # Floor avoids division by zero on constant histories. Scale is price-unit based.
        floor = prior.abs().rolling(lookback, min_periods=min_n).median() * 0.001
        scale = (1.4826 * mad).clip(lower=floor.clip(lower=1e-8))
        group["robust_z"] = (delta - median) / scale
        group["baseline_alert"] = group.change_pct.abs().ge(5)
        group["robust_alert"] = group.robust_z.abs().ge(4)
        group["direction"] = np.where(delta >= 0, "UP", "DOWN")
        group["detector_version"] = DETECTOR_VERSION
        out.append(group)
    return pd.concat(out, ignore_index=True).replace([np.inf, -np.inf], np.nan)


def current_snapshot(frame):
    """Choose revisions by observation time, never by lexicographic content hash."""
    if frame.empty:
        return frame
    return frame.sort_values(["observed_at", "revision"]).drop_duplicates(
        ["mode", "instrument", "event_time"], keep="last"
    )
