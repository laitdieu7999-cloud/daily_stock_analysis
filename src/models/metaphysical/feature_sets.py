"""Shared feature-set definitions for research experiments.

This module separates:
- current high-value candidate features for volatility models
- metaphysical variables reserved for future macro-event research
"""

from __future__ import annotations


QUANT_FEATURES = [
    "quant_return_1d",
    "quant_return_5d",
    "quant_volume_change",
    "quant_volume_ratio_5d",
    "atr_20",
    "bollinger_width",
]

TRIGGER_FEATURES = [
    "csi500_liquidity_crisis",
    "csi500_flash_crash",
    "csi500_capital_drain",
    "gold_panic_rush",
    "gold_macro_shock",
    "gold_currency_crisis",
]

SELECTED_TRIGGER_FEATURES = [
    "csi500_liquidity_crisis",
    "csi500_flash_crash",
    "gold_panic_rush",
    "gold_currency_crisis",
]

RESONANCE_FEATURES = [
    "bb_width_ratio",
    "bb_position_pct",
    "bb_width_roc_3d",
    "is_bb_squeeze",
    "is_bb_breakout",
    "is_triple_resonance",
]

RESONANCE_PLUS_FEATURES = RESONANCE_FEATURES + [
    "bb_breakout_strength",
    "triple_resonance_score",
    "bb_position_volume_interaction",
    "bb_breakout_volume_interaction",
    "month_turning_point_score",
    "is_month_turning_point_window",
    "volatility_plus_score",
    "is_volatility_plus_signal",
]

CALENDAR_CYCLE_FEATURES = [
    "nearest_lunar_phase_distance",
    "new_moon_distance",
    "full_moon_distance",
    "is_lunar_window",
    "nearest_solar_term_distance",
    "current_solar_term_distance",
    "is_critical_term_window",
    "anniversary_cycle_distance",
    "is_anniversary_window",
]

LONG_CYCLE_FEATURES = [
    "saturn_jupiter_cycle_distance",
    "is_saturn_jupiter_cycle_window",
    "uranus_cycle_84_distance",
    "is_uranus_cycle_window",
    "uranus_retrograde_active",
    "uranus_retrograde_boundary_distance",
]

ASTRO_PHASE_FEATURES = [
    "mars_pluto_square",
    "mars_jupiter_opposition",
    "saturn_pluto_conjunction",
]

ASTRO_LONGITUDE_FEATURES = [
    "mars_lon",
    "jupiter_lon",
    "saturn_lon",
    "venus_lon",
    "mercury_lon",
    "pluto_lon",
]

CURRENT_PRODUCTION_CANDIDATE_FEATURES = QUANT_FEATURES + [
    "bb_width_roc_3d",
    "bb_position_pct",
    "bb_width_ratio",
    "bb_breakout_strength",
    "triple_resonance_score",
    "bb_position_volume_interaction",
    "bb_breakout_volume_interaction",
    "is_bb_breakout",
] + SELECTED_TRIGGER_FEATURES

AUTHOR_TIMING_FEATURES = [
    "month_turning_point_score",
    "is_month_turning_point_window",
    "volatility_plus_score",
    "is_volatility_plus_signal",
]

EXPERIMENTAL_AUTHOR_CANDIDATE_FEATURES = (
    CURRENT_PRODUCTION_CANDIDATE_FEATURES
    + AUTHOR_TIMING_FEATURES
    + LONG_CYCLE_FEATURES
)

SLIM_AUTHOR_CANDIDATE_FEATURES = CURRENT_PRODUCTION_CANDIDATE_FEATURES + [
    "uranus_retrograde_boundary_distance",
    "uranus_cycle_84_distance",
    "saturn_jupiter_cycle_distance",
    "volatility_plus_score",
]

NEXT_PRODUCTION_CANDIDATE_FEATURES = SLIM_AUTHOR_CANDIDATE_FEATURES

REGIME_WEIGHTED_AUTHOR_FEATURES = [
    "weighted_uranus_retrograde_boundary_distance",
    "weighted_uranus_cycle_84_distance",
    "weighted_saturn_jupiter_cycle_distance",
    "weighted_volatility_plus_score",
]

NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES = (
    CURRENT_PRODUCTION_CANDIDATE_FEATURES + REGIME_WEIGHTED_AUTHOR_FEATURES
)

# Reserved for future macro-event research on gold/silver/CSI500.
MACRO_EVENT_METAPHYSICAL_FEATURES = (
    CALENDAR_CYCLE_FEATURES
    + LONG_CYCLE_FEATURES
    + ASTRO_PHASE_FEATURES
    + ASTRO_LONGITUDE_FEATURES
)


def legacy_xuanxue_columns(columns) -> list[str]:
    return [col for col in columns if col.startswith("tiangan_") or col.startswith("dizhi_")]


def legacy_astro_columns(columns) -> list[str]:
    return [col for col in columns if col in ASTRO_PHASE_FEATURES + ASTRO_LONGITUDE_FEATURES]
