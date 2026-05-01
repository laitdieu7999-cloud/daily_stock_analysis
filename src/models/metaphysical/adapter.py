"""Helpers to merge metaphysical research features into market data frames."""

from __future__ import annotations

import pandas as pd

from .feature_sets import NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES
from .regime import apply_author_regime_weights
from .service import build_metaphysical_features_if_enabled


def attach_metaphysical_features(
    df: pd.DataFrame,
    *,
    date_col: str = "date",
    enabled: bool = False,
    cache_dir=None,
    allow_missing_dependencies: bool = True,
    copy: bool = True,
):
    """Attach metaphysical research features to a market dataframe by date.

    Returns the original dataframe unchanged when disabled, when the date column
    is missing, or when optional dependencies are unavailable and
    ``allow_missing_dependencies`` is True.
    """
    if not enabled:
        return df.copy() if copy else df

    if date_col not in df.columns:
        return df.copy() if copy else df

    base_df = df.copy() if copy else df
    normalized_dates = pd.to_datetime(base_df[date_col], errors="coerce")
    valid_dates = pd.DatetimeIndex(normalized_dates.dropna().unique())
    if len(valid_dates) == 0:
        return base_df

    features = build_metaphysical_features_if_enabled(
        valid_dates,
        enabled=True,
        cache_dir=cache_dir,
        allow_missing_dependencies=allow_missing_dependencies,
    )
    if features is None or features.empty:
        return base_df

    feature_df = features.copy()
    feature_df.index = pd.to_datetime(feature_df.index)
    feature_df = feature_df.reset_index().rename(columns={"index": "__meta_date__"})

    merge_key = normalized_dates.dt.normalize()
    base_df["__meta_date__"] = merge_key
    merged = base_df.merge(feature_df, on="__meta_date__", how="left")
    return merged.drop(columns=["__meta_date__"])


def attach_next_production_metaphysical_features(
    df: pd.DataFrame,
    *,
    date_col: str = "date",
    enabled: bool = False,
    cache_dir=None,
    allow_missing_dependencies: bool = True,
    copy: bool = True,
    regime_mode: str = "dynamic",
    candidate_only: bool = False,
):
    """Attach the current best production-candidate metaphysical feature slice.

    This helper standardizes the current recommended path:
    1. attach shared date-based metaphysical features
    2. derive regime-aware weighted author factors from market context
    3. expose only the next-production candidate feature columns
    """
    enriched = attach_metaphysical_features(
        df,
        date_col=date_col,
        enabled=enabled,
        cache_dir=cache_dir,
        allow_missing_dependencies=allow_missing_dependencies,
        copy=copy,
    )
    if not enabled:
        return enriched

    weighted = apply_author_regime_weights(enriched, mode=regime_mode)
    if not candidate_only:
        return weighted

    candidate_columns = [
        column for column in NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES if column in weighted.columns
    ]
    base_columns = [date_col] if date_col in weighted.columns else []
    return weighted[base_columns + candidate_columns].copy()


def finalize_next_production_candidate_frame(
    df: pd.DataFrame,
    *,
    regime_mode: str = "dynamic",
    candidate_only: bool = False,
    copy: bool = True,
) -> pd.DataFrame:
    """Finalize the next-production candidate feature slice from an existing frame.

    Use this after upstream code has already computed market/quant/resonance
    columns and merged raw metaphysical date features.
    """
    base_df = df.copy() if copy else df
    weighted = apply_author_regime_weights(base_df, mode=regime_mode)
    if not candidate_only:
        return weighted

    candidate_columns = [
        column for column in NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES if column in weighted.columns
    ]
    return weighted[candidate_columns].copy()
