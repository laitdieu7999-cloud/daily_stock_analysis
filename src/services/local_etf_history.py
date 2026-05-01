# -*- coding: utf-8 -*-
"""Local cached ETF daily history helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOCAL_ETF_HISTORY_DIR = PROJECT_ROOT / ".cache" / "local_market_history" / "etf_daily"


def normalize_cn_etf_symbol(symbol: str) -> str:
    cleaned = (symbol or "").strip().lower()
    if not cleaned:
        return cleaned
    if cleaned.startswith(("sh", "sz")) and len(cleaned) == 8:
        return cleaned
    digits = "".join(char for char in cleaned if char.isdigit())
    if len(digits) != 6:
        return cleaned
    if digits.startswith(("51", "52", "56", "58")):
        return f"sh{digits}"
    return f"sz{digits}"


def build_etf_cache_path(symbol: str, base_dir: Path | str = DEFAULT_LOCAL_ETF_HISTORY_DIR) -> Path:
    normalized = normalize_cn_etf_symbol(symbol)
    return Path(base_dir).expanduser().resolve() / f"{normalized}.csv"


def save_cached_etf_daily_ohlcv(
    symbol: str,
    frame: pd.DataFrame,
    base_dir: Path | str = DEFAULT_LOCAL_ETF_HISTORY_DIR,
) -> Path:
    path = build_etf_cache_path(symbol, base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    output = frame.copy()
    output["date"] = pd.to_datetime(output["date"]).dt.strftime("%Y-%m-%d")
    output.to_csv(path, index=False, encoding="utf-8")
    return path


def load_cached_etf_daily_ohlcv(
    symbol: str,
    base_dir: Path | str = DEFAULT_LOCAL_ETF_HISTORY_DIR,
) -> Optional[pd.DataFrame]:
    path = build_etf_cache_path(symbol, base_dir)
    if not path.exists():
        return None
    frame = pd.read_csv(path)
    required = {"date", "open", "close", "high", "low", "volume"}
    if not required.issubset(frame.columns):
        return None
    frame["date"] = pd.to_datetime(frame["date"])
    return frame[["date", "open", "close", "high", "low", "volume"]].sort_values("date").reset_index(drop=True)
