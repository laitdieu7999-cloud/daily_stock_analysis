"""High-level service helpers for metaphysical research features.

AI boundary note:
- Treat this module as the single high-level entrypoint for metaphysical
  feature generation.
- Production paths should normally call the guarded
  ``build_metaphysical_features_if_enabled()`` helper instead of depending on
  raw optional dependencies directly.
"""

from __future__ import annotations

import logging

import pandas as pd

from .astro import batch_planet_lons
from .calendar import batch_ganzhi
from .deps import dependencies_available
from .signals import compute_triggers
from .time_law import (
    anniversary_cycle_distance,
    batch_long_cycle_features,
    nearest_lunar_phase_distance,
    solar_term_distance,
)

logger = logging.getLogger(__name__)


def build_metaphysical_features(dates, cache_dir=None):
    dates = pd.DatetimeIndex(dates)
    gz_df = batch_ganzhi(dates)
    planet_lons = batch_planet_lons(dates, cache_dir=cache_dir)
    trigger_df = compute_triggers(gz_df, planet_lons)

    calendar_rows = []
    anchor_dates = [
        pd.Timestamp("2008-09-18"),
        pd.Timestamp("2015-06-15"),
        pd.Timestamp("2024-02-05"),
    ]
    for dt in dates:
        lunar = nearest_lunar_phase_distance(dt)
        solar = solar_term_distance(dt)
        anniversary = anniversary_cycle_distance(dt, anchor_dates)
        calendar_rows.append({**lunar, **solar, **anniversary})

    calendar_df = pd.DataFrame(calendar_rows, index=dates)
    long_cycle_df = pd.DataFrame(batch_long_cycle_features(dates), index=dates)
    return pd.concat([trigger_df, calendar_df, long_cycle_df], axis=1)


def build_metaphysical_features_if_enabled(
    dates,
    *,
    enabled=False,
    cache_dir=None,
    allow_missing_dependencies=True,
):
    """Build research features only when enabled.

    Returns ``None`` when disabled or when optional dependencies are missing and
    ``allow_missing_dependencies`` is True.
    """
    if not enabled:
        return None

    if not dependencies_available():
        if allow_missing_dependencies:
            logger.info(
                "metaphysical features enabled but optional dependencies are unavailable; skipping feature build"
            )
            return None
        raise RuntimeError(
            "metaphysical features enabled but optional dependencies are unavailable"
        )

    return build_metaphysical_features(dates, cache_dir=cache_dir)
