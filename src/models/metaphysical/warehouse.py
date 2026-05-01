"""Local warehouse helpers for durable metaphysical-learning artifacts."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

WAREHOUSE_ROOT = Path(__file__).resolve().parents[3] / "data" / "metaphysical_learning"
PRICE_HISTORY_DIR = WAREHOUSE_ROOT / "price_history"
DATASET_SNAPSHOTS_DIR = WAREHOUSE_ROOT / "dataset_snapshots"
LEDGER_DIR = WAREHOUSE_ROOT / "ledgers"


def ensure_metaphysical_warehouse() -> dict[str, Path]:
    """Create the durable local warehouse directories and return their paths."""
    for path in (WAREHOUSE_ROOT, PRICE_HISTORY_DIR, DATASET_SNAPSHOTS_DIR, LEDGER_DIR):
        path.mkdir(parents=True, exist_ok=True)
    return {
        "warehouse_root": WAREHOUSE_ROOT,
        "price_history_dir": PRICE_HISTORY_DIR,
        "dataset_snapshots_dir": DATASET_SNAPSHOTS_DIR,
        "ledger_dir": LEDGER_DIR,
    }


def archive_price_history(
    prices: pd.DataFrame,
    *,
    symbol: str,
    suffix: str = "latest",
) -> Path:
    """Persist a durable local copy of price history as CSV."""
    ensure_metaphysical_warehouse()
    safe_symbol = symbol.replace("/", "_")
    target = PRICE_HISTORY_DIR / f"{safe_symbol}_{suffix}.csv"
    frame = prices.copy()
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame.to_csv(target, index=False, encoding="utf-8")
    return target


def archive_candidate_dataset(
    frame: pd.DataFrame,
    *,
    symbol: str,
    start: str,
    end: str,
    suffix: str = "latest",
) -> Path:
    """Persist a durable local copy of a training dataset snapshot as CSV."""
    ensure_metaphysical_warehouse()
    safe_symbol = symbol.replace("/", "_")
    target = DATASET_SNAPSHOTS_DIR / f"{safe_symbol}_{start}_{end}_{suffix}.csv"
    snapshot = frame.loc[:, ~frame.columns.duplicated()].copy()
    if "date" in snapshot.columns:
        snapshot["date"] = pd.to_datetime(snapshot["date"]).dt.strftime("%Y-%m-%d")
    snapshot.to_csv(target, index=False, encoding="utf-8")
    return target
