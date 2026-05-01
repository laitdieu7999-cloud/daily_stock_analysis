# -*- coding: utf-8 -*-
"""Lightweight local reader for bootstrapped Qlib daily history."""

from __future__ import annotations

import struct
from functools import lru_cache
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_GITHUB_HISTORY_ROOT = PROJECT_ROOT / ".cache" / "github_history_data" / "investment_data"


def normalize_cn_symbol(symbol: str) -> str:
    cleaned = (symbol or "").strip().lower()
    if not cleaned:
        return cleaned
    if cleaned.startswith(("sh", "sz", "bj")) and len(cleaned) >= 8:
        return cleaned
    digits = "".join(char for char in cleaned if char.isdigit())
    if len(digits) != 6:
        return cleaned
    if digits.startswith(("5", "6", "9")):
        return f"sh{digits}"
    return f"sz{digits}"


def find_latest_bootstrapped_qlib_root(base_dir: Path | str = DEFAULT_GITHUB_HISTORY_ROOT) -> Optional[Path]:
    root = Path(base_dir).expanduser().resolve()
    if not root.exists():
        return None
    candidates = sorted((path for path in root.iterdir() if path.is_dir()), reverse=True)
    for candidate in candidates:
        qlib_root = candidate / "extracted" / "qlib_bin"
        if (qlib_root / "features").exists() and (qlib_root / "instruments").exists():
            return qlib_root
    return None


@lru_cache(maxsize=8)
def _load_day_calendar(qlib_root_str: str) -> pd.DatetimeIndex:
    qlib_root = Path(qlib_root_str)
    calendar_path = qlib_root / "calendars" / "day.txt"
    lines = [line.strip() for line in calendar_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return pd.to_datetime(lines)


def _read_day_feature(symbol: str, feature: str, qlib_root: Path) -> Optional[pd.Series]:
    feature_path = qlib_root / "features" / normalize_cn_symbol(symbol) / f"{feature}.day.bin"
    if not feature_path.exists():
        return None

    with feature_path.open("rb") as handle:
        raw = handle.read()
    if len(raw) <= 4:
        return None

    start_index = int(round(struct.unpack("<f", raw[:4])[0]))
    values = np.frombuffer(raw[4:], dtype="<f4").astype(float)
    calendar = _load_day_calendar(str(qlib_root))
    end_index = min(start_index + len(values), len(calendar))
    if start_index >= end_index:
        return None

    dates = calendar[start_index:end_index]
    clipped_values = values[: len(dates)]
    return pd.Series(clipped_values, index=dates, name=feature)


def load_qlib_daily_ohlcv(symbol: str, qlib_root: Path | str) -> Optional[pd.DataFrame]:
    root = Path(qlib_root).expanduser().resolve()
    fields = ["open", "high", "low", "close", "volume"]
    series_map: dict[str, pd.Series] = {}
    for field in fields:
        series = _read_day_feature(symbol, field, root)
        if series is None:
            return None
        series_map[field] = series

    frame = pd.concat(series_map.values(), axis=1)
    frame.columns = fields
    frame = frame.dropna().reset_index().rename(columns={"index": "date"})
    frame["date"] = pd.to_datetime(frame["date"])
    return frame[["date", "open", "close", "high", "low", "volume"]].sort_values("date").reset_index(drop=True)
