"""Backtest-oriented resonance helpers built on top of shared metaphysical features."""

from __future__ import annotations

import pandas as pd

from .adapter import attach_next_production_metaphysical_features, finalize_next_production_candidate_frame
from .service import build_metaphysical_features_if_enabled


def build_resonance_backtest_features(
    dates,
    *,
    enabled=True,
    cache_dir=None,
    allow_missing_dependencies=True,
):
    """Build legacy-style backtest columns from shared metaphysical triggers.

    Output columns:
    - bazi_risk / bazi_event
    - astro_risk / astro_event
    - resonance
    """
    index = pd.DatetimeIndex(dates)
    empty = pd.DataFrame(
        {
            "bazi_risk": 0,
            "bazi_event": "",
            "astro_risk": 0,
            "astro_event": "",
            "resonance": 0,
        },
        index=index,
    )

    features = build_metaphysical_features_if_enabled(
        index,
        enabled=enabled,
        cache_dir=cache_dir,
        allow_missing_dependencies=allow_missing_dependencies,
    )
    if features is None or features.empty:
        return empty

    result = pd.DataFrame(index=index)
    result["bazi_risk"] = features["csi500_liquidity_crisis"].astype(int)
    result["bazi_event"] = result["bazi_risk"].map(
        lambda value: "中证500流动性危机触发" if int(value) == 1 else ""
    )

    astro_mask = (
        features["csi500_flash_crash"].astype(int)
        | features["csi500_capital_drain"].astype(int)
    ).astype(int)
    result["astro_risk"] = astro_mask

    astro_events = []
    for _, row in features.iterrows():
        labels = []
        if int(row.get("csi500_flash_crash", 0) or 0) == 1:
            labels.append("中证500闪崩相位触发")
        if int(row.get("csi500_capital_drain", 0) or 0) == 1:
            labels.append("中证500资金抽离触发")
        astro_events.append("|".join(labels))
    result["astro_event"] = astro_events

    result["resonance"] = (
        (result["bazi_risk"] == 1) & (result["astro_risk"] == 1)
    ).astype(int)
    return result


def build_next_production_backtest_features(
    market_df: pd.DataFrame,
    *,
    date_col: str = "date",
    enabled: bool = True,
    cache_dir=None,
    allow_missing_dependencies: bool = True,
    regime_mode: str = "dynamic",
    candidate_only: bool = True,
) -> pd.DataFrame:
    """Build the current next-production metaphysical candidate frame.

    This is the backtest-facing standard path when the caller already has a
    market dataframe with quant / resonance columns and wants the current
    dynamic regime-aware candidate slice.
    """
    if enabled and date_col in market_df.columns:
        return attach_next_production_metaphysical_features(
            market_df,
            date_col=date_col,
            enabled=enabled,
            cache_dir=cache_dir,
            allow_missing_dependencies=allow_missing_dependencies,
            copy=True,
            regime_mode=regime_mode,
            candidate_only=candidate_only,
        )

    return finalize_next_production_candidate_frame(
        market_df,
        regime_mode=regime_mode,
        candidate_only=candidate_only,
        copy=True,
    )
