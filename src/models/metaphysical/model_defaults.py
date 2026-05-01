"""Shared default configuration for the next-production metaphysical model path."""

from __future__ import annotations


NEXT_PRODUCTION_MODEL_DEFAULTS = {
    "caution_threshold": 0.40,
    "risk_off_threshold": 0.60,
    "min_train_days": 756,
    "retrain_every": 42,
}


def get_next_production_model_defaults() -> dict[str, float | int]:
    """Return a copy of the current best default model/backtest parameters."""
    return dict(NEXT_PRODUCTION_MODEL_DEFAULTS)
