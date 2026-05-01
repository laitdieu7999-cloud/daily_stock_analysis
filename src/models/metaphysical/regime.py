"""Regime-aware weighting helpers for author-derived metaphysical features."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


AUTHOR_REGIME_CUTOFF = pd.Timestamp("2021-03-10")

AUTHOR_FEATURE_REGIME_WEIGHTS = {
    "uranus_retrograde_boundary_distance": {"early": 0.70, "recent": 1.00},
    "uranus_cycle_84_distance": {"early": 0.70, "recent": 0.90},
    "saturn_jupiter_cycle_distance": {"early": 0.40, "recent": 0.60},
    "volatility_plus_score": {"early": 0.60, "recent": 0.80},
}


def classify_author_regime(dt) -> str:
    ts = pd.Timestamp(dt)
    return "early" if ts < AUTHOR_REGIME_CUTOFF else "recent"


def author_feature_weight(feature: str, dt) -> float:
    regime = classify_author_regime(dt)
    weights = AUTHOR_FEATURE_REGIME_WEIGHTS.get(feature)
    if weights is None:
        return 1.0
    return float(weights[regime])


def batch_author_regimes(dates: Iterable) -> list[dict]:
    rows = []
    for dt in pd.DatetimeIndex(dates):
        regime = classify_author_regime(dt)
        rows.append(
            {
                "author_regime": regime,
                "is_author_recent_regime": int(regime == "recent"),
            }
        )
    return rows


def infer_dynamic_author_regimes(frame: pd.DataFrame) -> list[dict]:
    """Infer author regimes from market structure instead of a fixed date cut."""
    result = frame.copy()

    close = pd.to_numeric(result.get("close"), errors="coerce")
    atr_20 = pd.to_numeric(result.get("atr_20"), errors="coerce")
    bollinger_width = pd.to_numeric(result.get("bollinger_width"), errors="coerce")
    quant_return_5d = pd.to_numeric(result.get("quant_return_5d"), errors="coerce")
    quant_volume_ratio_5d = pd.to_numeric(result.get("quant_volume_ratio_5d"), errors="coerce")
    bb_width_roc_3d = pd.to_numeric(result.get("bb_width_roc_3d"), errors="coerce")
    volatility_plus = pd.to_numeric(result.get("volatility_plus_score"), errors="coerce")

    atr_ratio = (atr_20 / close.replace(0, pd.NA)).astype(float)
    atr_ratio_median = atr_ratio.rolling(126, min_periods=20).median().bfill()
    bw_median = bollinger_width.rolling(126, min_periods=20).median().bfill()
    abs_ret_median = quant_return_5d.abs().rolling(126, min_periods=20).median().bfill()

    high_vol = (atr_ratio > atr_ratio_median * 1.05) | (bollinger_width > bw_median * 1.05)
    active_move = (
        (quant_return_5d.abs() > abs_ret_median * 1.10)
        | (volatility_plus >= 0.50)
        | (bb_width_roc_3d.abs() >= 0.12)
        | (quant_volume_ratio_5d >= 1.10)
    )
    recent_mask = ((high_vol & active_move) | (volatility_plus >= 0.62)).fillna(False)

    rows = []
    for is_recent in recent_mask.tolist():
        regime = "recent" if is_recent else "early"
        rows.append(
            {
                "author_regime": regime,
                "is_author_recent_regime": int(is_recent),
            }
        )
    return rows


def apply_author_regime_weights(frame: pd.DataFrame, *, date_index=None, mode: str = "date") -> pd.DataFrame:
    """Attach regime-aware weighted columns for author-selected factors."""
    result = frame.copy()
    dates = pd.DatetimeIndex(date_index if date_index is not None else result.index)

    if mode == "dynamic":
        regime_rows = infer_dynamic_author_regimes(result)
    else:
        regime_rows = batch_author_regimes(dates)
    result["author_regime"] = [row["author_regime"] for row in regime_rows]
    result["is_author_recent_regime"] = [row["is_author_recent_regime"] for row in regime_rows]

    for feature, weights in AUTHOR_FEATURE_REGIME_WEIGHTS.items():
        if feature not in result.columns:
            continue
        weighted_values = []
        regime_weights = []
        for dt, value in zip(dates, result[feature]):
            regime = classify_author_regime(dt)
            weight = float(weights[regime])
            regime_weights.append(weight)
            weighted_values.append(float(value) * weight if pd.notna(value) else value)
        result[f"{feature}_regime_weight"] = regime_weights
        result[f"weighted_{feature}"] = weighted_values

    return result
