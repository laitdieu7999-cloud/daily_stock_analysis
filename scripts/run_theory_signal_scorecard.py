#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a pure theory scorecard for trading signals.

This script deliberately ignores notification delivery, desktop packaging, and
scheduler health. It only answers one question: did the signal itself have
historical edge under the correct offensive/defensive labels?
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from run_ic_term_structure_validation import _build_term_structure_frame  # noqa: E402
from run_indicator_accuracy_comparison import (  # noqa: E402
    INDICATOR_RULES,
    _build_indicator_frame,
    _finite,
    _load_stock_daily,
    _num,
    _resolve_db_path,
)
from src.config import get_config  # noqa: E402
from src.services.local_etf_history import DEFAULT_LOCAL_ETF_HISTORY_DIR, normalize_cn_etf_symbol  # noqa: E402
from src.services.qlib_local_history import find_latest_bootstrapped_qlib_root, load_qlib_daily_ohlcv  # noqa: E402


Signal = Optional[str]  # "bullish", "bearish", or None
SignalFn = Callable[[pd.Series], Signal]
DYNAMIC_LABEL_FLOOR_PCT = 12.0
DYNAMIC_LABEL_CAP_PCT = 80.0
DYNAMIC_LABEL_ATR_MULTIPLIER = 6.0


@dataclass(frozen=True)
class TheoryRule:
    module: str
    direction_type: str  # offensive / defensive
    name: str
    description: str
    signal_fn: SignalFn
    parameter_family: Optional[str] = None
    parameter_label: Optional[str] = None


def _fmt_pct(value: object) -> str:
    if not _finite(value):
        return "--"
    return f"{float(value):.2f}%"


def _fmt_num(value: object, digits: int = 2) -> str:
    if not _finite(value):
        return "--"
    return f"{float(value):.{digits}f}"


def _safe_mean(values: Iterable[float]) -> float:
    series = pd.Series(list(values), dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    return float(series.mean()) if not series.empty else float("nan")


def _payoff_ratio(returns: pd.Series) -> float:
    values = pd.to_numeric(returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if values.empty:
        return float("nan")
    wins = values[values > 0]
    losses = values[values < 0]
    if wins.empty:
        return 0.0
    if losses.empty:
        return float("inf")
    return float(wins.mean() / abs(losses.mean()))


def _max_drawdown_from_returns(returns: pd.Series) -> float:
    values = pd.to_numeric(returns, errors="coerce").fillna(0.0) / 100.0
    if values.empty:
        return 0.0
    equity = (1.0 + values).cumprod()
    peak = equity.cummax()
    drawdown = equity / peak - 1.0
    return float(drawdown.min() * 100.0)


def _quantile_return(returns: pd.Series, q: float) -> float:
    values = pd.to_numeric(returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    return float(values.quantile(q)) if not values.empty else float("nan")


def _dynamic_label_limit_pct(row: pd.Series, window: int) -> float:
    """Volatility-scaled limit for removing obvious bad labels without killing real fat tails."""

    atr_pct = _num(row, "ATR60_PCT")
    tail_pct = _num(row, "ABS_RET_P99_120")
    vol_base = max(v for v in (atr_pct, tail_pct, 2.0) if _finite(v))
    scaled = vol_base * max(1.0, float(window) ** 0.5) * DYNAMIC_LABEL_ATR_MULTIPLIER
    return float(min(DYNAMIC_LABEL_CAP_PCT, max(DYNAMIC_LABEL_FLOOR_PCT, scaled)))


def _valid_forward_label(row: pd.Series, window: int, *values: object) -> bool:
    limit = _dynamic_label_limit_pct(row, window)
    cleaned = [float(value) for value in values if _finite(value)]
    if not cleaned:
        return False
    return all(abs(value) <= limit for value in cleaned)


def _normalize_focus_code(code: str) -> str:
    cleaned = (code or "").strip().upper()
    if not cleaned:
        return cleaned
    if cleaned == "NDX100":
        return "NDX"
    if cleaned.startswith("HK"):
        return cleaned
    digits = "".join(ch for ch in cleaned if ch.isdigit())
    if len(digits) == 5:
        return f"HK{digits}"
    if len(digits) == 6:
        return digits
    return cleaned


def _normalize_qlib_symbol(code: str) -> str:
    cleaned = _normalize_focus_code(code).lower()
    digits = "".join(ch for ch in cleaned if ch.isdigit())
    if len(digits) != 6:
        return cleaned
    if digits.startswith(("5", "6", "9")):
        return f"sh{digits}"
    if digits.startswith("000"):
        return f"sh{digits}"
    return f"sz{digits}"


def _default_focus_codes() -> list[str]:
    config = get_config()
    codes = list(getattr(config, "stock_list", []) or [])
    codes.extend(list(getattr(config, "watchlist_stock_list", []) or []))
    normalized: list[str] = []
    seen: set[str] = set()
    for code in codes:
        item = _normalize_focus_code(str(code))
        if item and item not in seen:
            normalized.append(item)
            seen.add(item)
    return normalized


def _load_cached_etf_history(code: str, cache_dir: Path) -> Optional[pd.DataFrame]:
    digits = "".join(ch for ch in str(code) if ch.isdigit())
    if len(digits) != 6 or not digits.startswith(("15", "16", "51")):
        return None
    normalized = normalize_cn_etf_symbol(digits)
    path = cache_dir / f"{normalized}.csv"
    if not path.exists():
        return None
    frame = pd.read_csv(path, parse_dates=["date"])
    if frame.empty:
        return None
    frame = frame.rename(columns={c: c.lower() for c in frame.columns})
    frame["code"] = digits
    if "amount" not in frame.columns:
        frame["amount"] = np.nan
    return frame[["code", "date", "open", "high", "low", "close", "volume", "amount"]]


def _load_qlib_focus_history(code: str, qlib_root: Optional[Path]) -> Optional[pd.DataFrame]:
    if qlib_root is None:
        return None
    digits = "".join(ch for ch in str(code) if ch.isdigit())
    if len(digits) != 6:
        return None
    frame = load_qlib_daily_ohlcv(_normalize_qlib_symbol(digits), qlib_root)
    if frame is None or frame.empty:
        return None
    frame = frame.copy()
    frame["code"] = digits
    if "amount" not in frame.columns:
        frame["amount"] = np.nan
    return frame[["code", "date", "open", "high", "low", "close", "volume", "amount"]]


def load_theory_daily_frame(
    *,
    db_path: Path,
    focus_codes: list[str],
    use_qlib: bool = True,
    use_etf_cache: bool = True,
    etf_cache_dir: Path = DEFAULT_LOCAL_ETF_HISTORY_DIR,
    max_rows_per_code: Optional[int] = 1600,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Load the widest available local daily history for focus symbols."""

    base = _load_stock_daily(db_path)
    base["code"] = base["code"].astype(str).map(_normalize_focus_code)
    focus_set = {item for item in focus_codes if item}
    if focus_set:
        base = base[base["code"].isin(focus_set)].copy()

    frames = [base]
    source_rows: list[dict[str, object]] = [
        {"source": "sqlite_stock_daily", "rows": int(len(base)), "codes": int(base["code"].nunique())}
    ]

    qlib_root = find_latest_bootstrapped_qlib_root() if use_qlib else None
    if use_qlib and qlib_root is not None:
        qlib_frames = []
        for code in focus_codes:
            loaded = _load_qlib_focus_history(code, qlib_root)
            if loaded is not None:
                qlib_frames.append(loaded)
        if qlib_frames:
            qlib_df = pd.concat(qlib_frames, ignore_index=True)
            frames.append(qlib_df)
            source_rows.append(
                {
                    "source": "github_qlib_daily",
                    "rows": int(len(qlib_df)),
                    "codes": int(qlib_df["code"].nunique()),
                    "root": str(qlib_root),
                }
            )

    if use_etf_cache:
        etf_frames = []
        for code in focus_codes:
            loaded = _load_cached_etf_history(code, etf_cache_dir)
            if loaded is not None:
                etf_frames.append(loaded)
        if etf_frames:
            etf_df = pd.concat(etf_frames, ignore_index=True)
            frames.append(etf_df)
            source_rows.append(
                {
                    "source": "local_etf_cache",
                    "rows": int(len(etf_df)),
                    "codes": int(etf_df["code"].nunique()),
                    "root": str(etf_cache_dir),
                }
            )

    daily = pd.concat(frames, ignore_index=True)
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        daily[col] = pd.to_numeric(daily[col], errors="coerce")
    daily = daily.dropna(subset=["code", "date", "high", "low", "close"])
    daily = daily.sort_values(["code", "date"])
    # Later sources are wider historical caches; prefer them over short DB slices.
    daily = daily.drop_duplicates(["code", "date"], keep="last").reset_index(drop=True)
    if max_rows_per_code and max_rows_per_code > 0:
        daily = daily.groupby("code", group_keys=False).tail(int(max_rows_per_code)).reset_index(drop=True)
    meta = {
        "focus_codes": focus_codes,
        "source_rows": source_rows,
        "row_count": int(len(daily)),
        "code_count": int(daily["code"].nunique()),
        "date_min": str(daily["date"].min().date()) if not daily.empty else None,
        "date_max": str(daily["date"].max().date()) if not daily.empty else None,
    }
    return daily, meta


def _make_signal_pullback_ma(band_pct: float) -> SignalFn:
    band = band_pct / 100.0

    def _signal(row: pd.Series) -> Signal:
        close = _num(row, "close")
        ma5 = _num(row, "MA5")
        ma10 = _num(row, "MA10")
        ma20 = _num(row, "MA20")
        if not all(_finite(v) and v > 0 for v in (close, ma5, ma10, ma20)):
            return None
        if ma5 >= ma10 >= ma20 and ma10 * (1.0 - band) <= close <= ma5 * (1.0 + band):
            return "bullish"
        return None

    return _signal


def _signal_pullback_ma(row: pd.Series) -> Signal:
    return _make_signal_pullback_ma(1.5)(row)


def _make_signal_ma20_low_risk(max_distance_pct: float) -> SignalFn:
    max_distance = max_distance_pct / 100.0

    def _signal(row: pd.Series) -> Signal:
        close = _num(row, "close")
        ma5 = _num(row, "MA5")
        ma10 = _num(row, "MA10")
        ma20 = _num(row, "MA20")
        ret1 = _num(row, "RET_1")
        if not all(_finite(v) and v > 0 for v in (close, ma5, ma10, ma20)):
            return None
        if ma5 >= ma10 >= ma20 and abs(close / ma20 - 1.0) <= max_distance and -2.5 <= ret1 <= 2.5:
            return "bullish"
        return None

    return _signal


def _signal_ma20_low_risk(row: pd.Series) -> Signal:
    return _make_signal_ma20_low_risk(2.0)(row)


def _make_signal_trend_break_weak_day(weak_day_pct: float) -> SignalFn:
    threshold = -abs(weak_day_pct)

    def _signal(row: pd.Series) -> Signal:
        close = _num(row, "close")
        ma20 = _num(row, "MA20")
        ret1 = _num(row, "RET_1")
        if ma20 > 0 and close < ma20 and ret1 <= threshold:
            return "bearish"
        return None

    return _signal


def _signal_trend_break_weak_day(row: pd.Series) -> Signal:
    return _make_signal_trend_break_weak_day(1.0)(row)


def _make_signal_volume_ma20_break(volume_multiple: float) -> SignalFn:
    def _signal(row: pd.Series) -> Signal:
        base = _signal_ma20_break(row)
        if base != "bearish":
            return None
        volume = _num(row, "volume")
        volume_ma20 = _num(row, "VOLUME_MA20")
        if volume_ma20 > 0 and volume >= volume_ma20 * volume_multiple:
            return "bearish"
        return None

    return _signal


def _legacy_signal_pullback_ma(row: pd.Series) -> Signal:
    close = _num(row, "close")
    ma5 = _num(row, "MA5")
    ma10 = _num(row, "MA10")
    ma20 = _num(row, "MA20")
    if not all(_finite(v) and v > 0 for v in (close, ma5, ma10, ma20)):
        return None
    if ma5 >= ma10 >= ma20 and ma10 * 0.985 <= close <= ma5 * 1.015:
        return "bullish"
    return None


def _legacy_signal_ma20_low_risk(row: pd.Series) -> Signal:
    close = _num(row, "close")
    ma5 = _num(row, "MA5")
    ma10 = _num(row, "MA10")
    ma20 = _num(row, "MA20")
    ret1 = _num(row, "RET_1")
    if not all(_finite(v) and v > 0 for v in (close, ma5, ma10, ma20)):
        return None
    if ma5 >= ma10 >= ma20 and abs(close / ma20 - 1.0) <= 0.02 and -2.5 <= ret1 <= 2.5:
        return "bullish"
    return None


def _legacy_signal_trend_break_weak_day(row: pd.Series) -> Signal:
    close = _num(row, "close")
    ma20 = _num(row, "MA20")
    ret1 = _num(row, "RET_1")
    if ma20 > 0 and close < ma20 and ret1 <= -1.0:
        return "bearish"
    return None


def _signal_ma_bear_stack(row: pd.Series) -> Signal:
    close = _num(row, "close")
    ma5 = _num(row, "MA5")
    ma10 = _num(row, "MA10")
    ma20 = _num(row, "MA20")
    if all(_finite(v) and v > 0 for v in (close, ma5, ma10, ma20)) and close < ma5 < ma10 < ma20:
        return "bearish"
    return None


def _signal_ma20_break(row: pd.Series) -> Signal:
    close = _num(row, "close")
    ma20 = _num(row, "MA20")
    prev_close = _num(row, "PREV_CLOSE")
    prev_ma20 = _num(row, "PREV_MA20")
    if ma20 > 0 and prev_ma20 > 0 and close < ma20 and prev_close >= prev_ma20:
        return "bearish"
    return None


def _signal_volume_ma20_break(row: pd.Series) -> Signal:
    return _make_signal_volume_ma20_break(1.2)(row)


CUSTOM_RULES = [
    TheoryRule("自选股买入", "offensive", "趋势回踩MA5/MA10", "多头排列下回踩MA5/MA10附近", _signal_pullback_ma),
    TheoryRule("自选股买入", "offensive", "MA20低风险买点", "趋势未破且距离MA20不超过2%", _signal_ma20_low_risk),
    TheoryRule("持仓风控", "defensive", "趋势破坏+单日弱势", "跌破MA20且当日跌幅不小于1%", _signal_trend_break_weak_day),
    TheoryRule("持仓风控", "defensive", "MA空头排列", "收盘价低于MA5/10/20且均线空头排列", _signal_ma_bear_stack),
    TheoryRule("持仓风控", "defensive", "跌破MA20", "从MA20上方跌到MA20下方", _signal_ma20_break),
    TheoryRule("持仓风控", "defensive", "放量跌破MA20", "跌破MA20且成交量高于20日均量20%", _signal_volume_ma20_break),
]


CORE_INDICATOR_NAMES = {
    "MA多空排列",
    "MACD趋势确认",
    "RSI反转",
    "BOLL均值回归",
    "RSRS择时",
    "VWAP成本线",
    "相对强弱20日",
}

REGIME_ORDER = ["急跌", "阴跌", "急涨", "上行", "震荡", "其他", "未知"]


def _build_rules(rule_set: str = "core") -> list[TheoryRule]:
    rules = list(CUSTOM_RULES)
    for rule in INDICATOR_RULES:
        if rule_set == "core" and rule.name not in CORE_INDICATOR_NAMES:
            continue
        rules.append(
            TheoryRule(
                "日线技术信号",
                "offensive",
                f"{rule.name}(看多)",
                rule.description,
                lambda row, fn=rule.signal_fn: "bullish" if fn(row) == "bullish" else None,
            )
        )
        rules.append(
            TheoryRule(
                "日线技术信号",
                "defensive",
                f"{rule.name}(看空)",
                rule.description,
                lambda row, fn=rule.signal_fn: "bearish" if fn(row) == "bearish" else None,
            )
        )
    return rules


def _build_parameter_plateau_rules() -> list[TheoryRule]:
    """Small neighborhoods around the hand-written rules for robustness checks."""

    rules: list[TheoryRule] = []
    for band_pct in (1.0, 1.5, 2.0):
        label = f"带宽{band_pct:.1f}%"
        rules.append(
            TheoryRule(
                "参数高原-自选股买入",
                "offensive",
                f"趋势回踩MA5/MA10[{label}]",
                f"多头排列下回踩MA5/MA10附近，允许带宽{band_pct:.1f}%",
                _make_signal_pullback_ma(band_pct),
                parameter_family="趋势回踩MA5/MA10",
                parameter_label=label,
            )
        )
    for distance_pct in (1.0, 2.0, 3.0):
        label = f"MA20距离{distance_pct:.1f}%"
        rules.append(
            TheoryRule(
                "参数高原-自选股买入",
                "offensive",
                f"MA20低风险买点[{label}]",
                f"趋势未破且距离MA20不超过{distance_pct:.1f}%",
                _make_signal_ma20_low_risk(distance_pct),
                parameter_family="MA20低风险买点",
                parameter_label=label,
            )
        )
    for weak_day_pct in (0.5, 1.0, 1.5):
        label = f"单日跌幅{weak_day_pct:.1f}%"
        rules.append(
            TheoryRule(
                "参数高原-持仓风控",
                "defensive",
                f"趋势破坏+单日弱势[{label}]",
                f"跌破MA20且当日跌幅不小于{weak_day_pct:.1f}%",
                _make_signal_trend_break_weak_day(weak_day_pct),
                parameter_family="趋势破坏+单日弱势",
                parameter_label=label,
            )
        )
    for volume_multiple in (1.0, 1.2, 1.5):
        label = f"量能{volume_multiple:.1f}倍"
        rules.append(
            TheoryRule(
                "参数高原-持仓风控",
                "defensive",
                f"放量跌破MA20[{label}]",
                f"跌破MA20且成交量高于20日均量{volume_multiple:.1f}倍",
                _make_signal_volume_ma20_break(volume_multiple),
                parameter_family="放量跌破MA20",
                parameter_label=label,
            )
        )
    return rules


def _classify_market_regime(row: pd.Series) -> str:
    ret1 = _num(row, "RET_1")
    ret5 = _num(row, "RET_5")
    ret20 = _num(row, "RETURN_20")
    close = _num(row, "close")
    ma20 = _num(row, "MA20")
    if ret1 <= -2.5 or ret5 <= -5.0:
        return "急跌"
    if ret20 <= -8.0:
        return "阴跌"
    if ret1 >= 2.5 or ret5 >= 5.0:
        return "急涨"
    if close > 0 and ma20 > 0 and close >= ma20 and ret20 >= 3.0:
        return "上行"
    if abs(ret20) <= 3.0:
        return "震荡"
    return "其他"


def _attach_market_regime(frame: pd.DataFrame, benchmark_code: str) -> pd.DataFrame:
    result = frame.copy()
    benchmark = _normalize_focus_code(benchmark_code)
    bench = result[result["code"].astype(str).map(_normalize_focus_code) == benchmark].copy()
    if not bench.empty:
        bench = bench.sort_values("date").copy()
        bench["market_regime"] = bench.apply(_classify_market_regime, axis=1)
        regime_map = bench[["date", "market_regime"]].drop_duplicates("date")
    else:
        proxy = (
            result.groupby("date", as_index=False)[["RET_1", "RET_5", "RETURN_20"]]
            .mean(numeric_only=True)
            .assign(close=np.nan, MA20=np.nan)
        )
        proxy["market_regime"] = proxy.apply(_classify_market_regime, axis=1)
        regime_map = proxy[["date", "market_regime"]]
    result = result.merge(regime_map, on="date", how="left")
    result["market_regime"] = result["market_regime"].fillna("未知")
    return result


def _prepare_indicator_frame(daily: pd.DataFrame, benchmark_code: str) -> pd.DataFrame:
    frame = _build_indicator_frame(daily, benchmark_code)
    parts = []
    for _, group in frame.groupby("code"):
        group = group.sort_values("date").copy()
        prev_close = group["close"].shift(1)
        true_range = pd.concat(
            [
                group["high"] - group["low"],
                (group["high"] - prev_close).abs(),
                (group["low"] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        group["TR_PCT"] = true_range / prev_close.replace(0, np.nan) * 100.0
        group["ATR60_PCT"] = group["TR_PCT"].rolling(60, min_periods=20).mean()
        group["RET_1"] = group["close"].pct_change() * 100.0
        group["RET_5"] = group["close"].pct_change(5) * 100.0
        group["ABS_RET_P99_120"] = group["RET_1"].abs().rolling(120, min_periods=30).quantile(0.99)
        group["PREV_CLOSE"] = group["close"].shift(1)
        group["PREV_MA20"] = group["MA20"].shift(1)
        group["VOLUME_MA20"] = group["volume"].rolling(20).mean()
        parts.append(group)
    return _attach_market_regime(pd.concat(parts, ignore_index=True), benchmark_code)


def _iter_records_for_rules(indicator_df: pd.DataFrame, windows: list[int], rules: list[TheoryRule]) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    max_window = max(windows)
    for code, group in indicator_df.groupby("code"):
        group = group.sort_values("date").reset_index(drop=True)
        if len(group) <= max_window:
            continue
        for idx in range(0, len(group) - max_window):
            row = group.iloc[idx]
            entry = _num(row, "close")
            if entry <= 0:
                continue
            for rule in rules:
                signal = rule.signal_fn(row)
                if signal is None:
                    continue
                for window in windows:
                    future = group.iloc[idx + 1 : idx + window + 1]
                    if len(future) < window:
                        continue
                    end_close = _num(future.iloc[-1], "close")
                    if end_close <= 0:
                        continue
                    future_return = (end_close - entry) / entry * 100.0
                    max_high = float(pd.to_numeric(future["high"], errors="coerce").max())
                    min_low = float(pd.to_numeric(future["low"], errors="coerce").min())
                    mfe = (max_high - entry) / entry * 100.0 if max_high > 0 else np.nan
                    mae = (min_low - entry) / entry * 100.0 if min_low > 0 else np.nan
                    if not _valid_forward_label(row, window, future_return, mfe, mae):
                        continue
                    records.append(
                        {
                            "module": rule.module,
                            "direction_type": rule.direction_type,
                            "rule": rule.name,
                            "description": rule.description,
                            "parameter_family": rule.parameter_family,
                            "parameter_label": rule.parameter_label,
                            "code": code,
                            "date": row["date"].date().isoformat(),
                            "market_regime": str(row.get("market_regime") or "未知"),
                            "window": window,
                            "entry_price": entry,
                            "label_limit_pct": _dynamic_label_limit_pct(row, window),
                            "future_return_pct": future_return,
                            "mfe_pct": mfe,
                            "mae_pct": mae,
                            "direction_correct": future_return > 0 if signal == "bullish" else future_return < 0,
                            "avoided_drawdown_pct": max(0.0, -mae) if signal == "bearish" and np.isfinite(mae) else np.nan,
                            "false_rebound": bool(mfe >= 3.0) if signal == "bearish" and np.isfinite(mfe) else None,
                        }
                    )
    return records


def _iter_theory_records(indicator_df: pd.DataFrame, windows: list[int], *, rule_set: str = "core") -> list[dict[str, object]]:
    return _iter_records_for_rules(indicator_df, windows, _build_rules(rule_set))


def _grade_offensive(row: pd.Series, min_samples: int) -> str:
    if int(row["sample_count"]) < min_samples:
        return "样本不足"
    if row["avg_return_pct"] <= 0 or row["payoff_ratio"] < 1.0:
        return "不建议升级"
    if row["payoff_ratio"] >= 1.2 and row["avg_mfe_pct"] > abs(row["avg_mae_pct"]):
        return "可进Shadow"
    return "继续观察"


def _grade_defensive(row: pd.Series, min_samples: int) -> str:
    if int(row["sample_count"]) < min_samples:
        return "样本不足"
    if row["avg_avoided_drawdown_pct"] < 2.0:
        return "防守价值不足"
    if (
        row["avg_return_pct"] <= 0
        and row["payoff_ratio"] >= 1.0
        and row["false_rebound_rate_pct"] <= 45
        and row["down_hit_rate_pct"] >= 50
    ):
        return "可进Shadow"
    return "继续观察"


def _summarize_equity_records(
    records: list[dict[str, object]],
    min_samples: int,
    *,
    by_regime: bool = False,
) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    group_cols = ["module", "direction_type", "rule", "description", "window"]
    if by_regime:
        group_cols.append("market_regime")
    grouped = df.groupby(group_cols, as_index=False)
    rows = []
    for key, sample in grouped:
        if not isinstance(key, tuple):
            key = (key,)
        key_map = dict(zip(group_cols, key))
        module = key_map["module"]
        direction_type = key_map["direction_type"]
        rule = key_map["rule"]
        description = key_map["description"]
        window = key_map["window"]
        returns = pd.to_numeric(sample["future_return_pct"], errors="coerce")
        if direction_type == "offensive":
            row = {
                "module": module,
                "direction_type": direction_type,
                "rule": rule,
                "description": description,
                "window": int(window),
                "sample_count": int(len(sample)),
                "direction_accuracy_pct": float(sample["direction_correct"].mean() * 100.0),
                "avg_return_pct": float(returns.mean()),
                "payoff_ratio": _payoff_ratio(returns),
                "avg_mfe_pct": _safe_mean(sample["mfe_pct"]),
                "avg_mae_pct": _safe_mean(sample["mae_pct"]),
                "tail_loss_p5_pct": _quantile_return(returns, 0.05),
                "avg_avoided_drawdown_pct": np.nan,
                "false_rebound_rate_pct": np.nan,
                "down_hit_rate_pct": np.nan,
            }
            row["grade"] = _grade_offensive(pd.Series(row), min_samples)
        else:
            row = {
                "module": module,
                "direction_type": direction_type,
                "rule": rule,
                "description": description,
                "window": int(window),
                "sample_count": int(len(sample)),
                "direction_accuracy_pct": float(sample["direction_correct"].mean() * 100.0),
                "avg_return_pct": float(returns.mean()),
                "payoff_ratio": _payoff_ratio(-returns),
                "avg_mfe_pct": _safe_mean(sample["mfe_pct"]),
                "avg_mae_pct": _safe_mean(sample["mae_pct"]),
                "tail_loss_p5_pct": _quantile_return(returns, 0.05),
                "avg_avoided_drawdown_pct": _safe_mean(sample["avoided_drawdown_pct"]),
                "false_rebound_rate_pct": float(pd.Series(sample["false_rebound"]).dropna().mean() * 100.0),
                "down_hit_rate_pct": float((returns < 0).mean() * 100.0),
            }
            row["grade"] = _grade_defensive(pd.Series(row), min_samples)
        if by_regime:
            row["market_regime"] = key_map.get("market_regime", "未知")
        rows.append(row)
    summary = pd.DataFrame(rows)
    sort_cols = ["window", "direction_type", "grade", "sample_count", "direction_accuracy_pct", "avg_return_pct"]
    return summary.sort_values(sort_cols, ascending=[True, True, True, False, False, False]).reset_index(drop=True)


def _oos_pass(row: pd.Series, min_test_samples: int) -> bool:
    if int(row.get("test_sample_count") or 0) < min_test_samples:
        return False
    direction_type = str(row.get("direction_type") or "")
    if direction_type == "offensive":
        return float(row.get("test_avg_return_pct") or 0.0) > 0 and float(row.get("test_payoff_ratio") or 0.0) >= 1.0
    return (
        float(row.get("test_avg_return_pct") or 0.0) <= 0
        and float(row.get("test_payoff_ratio") or 0.0) >= 1.0
        and float(row.get("test_avg_avoided_drawdown_pct") or 0.0) >= 2.0
    )


def _evaluate_walk_forward(
    records: list[dict[str, object]],
    *,
    primary_window: int,
    min_train_samples: int,
    train_years: int,
) -> dict[str, object]:
    """Train on prior years, then evaluate the promoted signal in the next year."""

    if not records:
        return {"status": "no_records", "rows": [], "summary": {}}

    df = pd.DataFrame(records)
    if df.empty or "date" not in df.columns:
        return {"status": "no_records", "rows": [], "summary": {}}
    df = df[df["window"] == primary_window].copy()
    if df.empty:
        return {"status": "no_primary_window", "rows": [], "summary": {}}

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["year"] = df["date"].dt.year
    years = sorted(int(year) for year in df["year"].dropna().unique())
    if len(years) <= train_years:
        return {"status": "insufficient_years", "rows": [], "summary": {"years": years}}

    key_cols = ["module", "direction_type", "rule", "description", "window"]
    min_test_samples = max(10, int(min_train_samples * 0.2))
    rows: list[dict[str, object]] = []

    for test_year in years:
        train_candidates = [year for year in years if test_year - train_years <= year < test_year]
        if len(train_candidates) < train_years:
            continue
        train_records = df[df["year"].isin(train_candidates)].to_dict(orient="records")
        test_records = df[df["year"] == test_year].to_dict(orient="records")
        if not train_records or not test_records:
            continue

        train_summary = _summarize_equity_records(train_records, min_train_samples)
        test_summary = _summarize_equity_records(test_records, min_test_samples)
        if train_summary.empty or test_summary.empty:
            continue
        promoted = train_summary[train_summary["grade"] == "可进Shadow"].copy()
        if promoted.empty:
            continue

        merged = promoted.merge(
            test_summary,
            on=key_cols,
            how="left",
            suffixes=("_train", "_test"),
        )
        for _, item in merged.iterrows():
            row = {
                "test_year": int(test_year),
                "train_years": f"{min(train_candidates)}-{max(train_candidates)}",
                "module": item["module"],
                "direction_type": item["direction_type"],
                "rule": item["rule"],
                "window": int(item["window"]),
                "train_sample_count": int(item.get("sample_count_train") or 0),
                "train_accuracy_pct": item.get("direction_accuracy_pct_train"),
                "train_avg_return_pct": item.get("avg_return_pct_train"),
                "train_payoff_ratio": item.get("payoff_ratio_train"),
                "test_sample_count": int(item.get("sample_count_test") or 0) if _finite(item.get("sample_count_test")) else 0,
                "test_accuracy_pct": item.get("direction_accuracy_pct_test"),
                "test_avg_return_pct": item.get("avg_return_pct_test"),
                "test_payoff_ratio": item.get("payoff_ratio_test"),
                "test_avg_avoided_drawdown_pct": item.get("avg_avoided_drawdown_pct_test"),
            }
            row["oos_pass"] = _oos_pass(pd.Series(row), min_test_samples)
            row["oos_status"] = "通过" if row["oos_pass"] else (
                "样本不足" if row["test_sample_count"] < min_test_samples else "失效"
            )
            rows.append(row)

    if not rows:
        return {
            "status": "no_promoted_candidates",
            "rows": [],
            "summary": {"primary_window": primary_window, "train_years": train_years, "min_test_samples": min_test_samples},
        }

    result = pd.DataFrame(rows)
    valid = result[result["oos_status"] != "样本不足"].copy()
    pass_count = int(valid["oos_pass"].sum()) if not valid.empty else 0
    total_count = int(len(valid))
    by_rule = []
    if not valid.empty:
        grouped = valid.groupby(["module", "direction_type", "rule"], as_index=False).agg(
            oos_tests=("oos_pass", "size"),
            oos_passes=("oos_pass", "sum"),
            avg_test_return_pct=("test_avg_return_pct", "mean"),
            avg_test_payoff_ratio=("test_payoff_ratio", "mean"),
        )
        grouped["oos_pass_rate_pct"] = grouped["oos_passes"] / grouped["oos_tests"] * 100.0
        by_rule = _to_records(
            grouped.sort_values(["oos_pass_rate_pct", "oos_tests", "avg_test_payoff_ratio"], ascending=[False, False, False])
        )

    return {
        "status": "ok",
        "primary_window": primary_window,
        "train_years": train_years,
        "min_train_samples": min_train_samples,
        "min_test_samples": min_test_samples,
        "summary": {
            "candidate_tests": int(len(rows)),
            "valid_tests": total_count,
            "pass_count": pass_count,
            "pass_rate_pct": float(pass_count / total_count * 100.0) if total_count else None,
        },
        "rows": _to_records(result),
        "by_rule": by_rule,
    }


def _plateau_status(qualified_count: int, variant_count: int) -> str:
    if variant_count <= 0:
        return "无样本"
    if qualified_count >= max(2, (variant_count + 1) // 2):
        return "稳定高原"
    if qualified_count == 1:
        return "单点敏感"
    return "不稳定"


def _evaluate_parameter_plateau(
    indicator_df: pd.DataFrame,
    *,
    primary_window: int,
    min_samples: int,
) -> dict[str, object]:
    rules = _build_parameter_plateau_rules()
    records = _iter_records_for_rules(indicator_df, [primary_window], rules)
    if not records:
        return {"status": "no_records", "primary_window": primary_window, "families": [], "variants": []}

    summary = _summarize_equity_records(records, min_samples)
    if summary.empty:
        return {"status": "no_summary", "primary_window": primary_window, "families": [], "variants": []}

    rule_meta = {
        rule.name: {
            "parameter_family": rule.parameter_family or rule.name,
            "parameter_label": rule.parameter_label or "-",
        }
        for rule in rules
    }
    summary["parameter_family"] = summary["rule"].map(lambda item: rule_meta.get(item, {}).get("parameter_family", item))
    summary["parameter_label"] = summary["rule"].map(lambda item: rule_meta.get(item, {}).get("parameter_label", "-"))

    families: list[dict[str, object]] = []
    for (family, direction_type), group in summary.groupby(["parameter_family", "direction_type"]):
        variants = group.copy()
        qualified = variants[variants["grade"] == "可进Shadow"]
        best_pool = qualified if not qualified.empty else variants
        if direction_type == "offensive":
            sort_cols = ["avg_return_pct", "payoff_ratio", "sample_count"]
            ascending = [False, False, False]
            best_value = "avg_return_pct"
        else:
            sort_cols = ["avg_avoided_drawdown_pct", "payoff_ratio", "sample_count"]
            ascending = [False, False, False]
            best_value = "avg_avoided_drawdown_pct"
        best = best_pool.sort_values(sort_cols, ascending=ascending).iloc[0]
        variant_count = int(len(variants))
        qualified_count = int(len(qualified))
        families.append(
            {
                "parameter_family": str(family),
                "direction_type": str(direction_type),
                "variant_count": variant_count,
                "qualified_count": qualified_count,
                "qualified_rate_pct": float(qualified_count / variant_count * 100.0) if variant_count else None,
                "plateau_status": _plateau_status(qualified_count, variant_count),
                "best_parameter": best.get("parameter_label"),
                "best_rule": best.get("rule"),
                "best_grade": best.get("grade"),
                "best_sample_count": int(best.get("sample_count") or 0),
                "best_avg_return_pct": best.get("avg_return_pct"),
                "best_avg_avoided_drawdown_pct": best.get("avg_avoided_drawdown_pct"),
                "best_payoff_ratio": best.get("payoff_ratio"),
                "best_value_pct": best.get(best_value),
            }
        )

    family_df = pd.DataFrame(families)
    if not family_df.empty:
        family_df["status_order"] = family_df["plateau_status"].map({"稳定高原": 0, "单点敏感": 1, "不稳定": 2, "无样本": 3}).fillna(9)
        family_df = family_df.sort_values(
            ["status_order", "qualified_rate_pct", "best_payoff_ratio"],
            ascending=[True, False, False],
        ).drop(columns=["status_order"])

    variant_df = summary.sort_values(
        ["parameter_family", "direction_type", "grade", "sample_count", "payoff_ratio"],
        ascending=[True, True, True, False, False],
    )
    return {
        "status": "ok",
        "primary_window": primary_window,
        "min_samples": min_samples,
        "families": _to_records(family_df),
        "variants": _to_records(variant_df),
    }


def _build_random_baseline_frame(indicator_df: pd.DataFrame, window: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for code, group in indicator_df.groupby("code"):
        group = group.sort_values("date").reset_index(drop=True)
        if len(group) <= window:
            continue
        for idx in range(0, len(group) - window):
            entry = _num(group.iloc[idx], "close")
            if entry <= 0:
                continue
            future = group.iloc[idx + 1 : idx + window + 1]
            if len(future) < window:
                continue
            end_close = _num(future.iloc[-1], "close")
            if end_close <= 0:
                continue
            future_return = (end_close - entry) / entry * 100.0
            max_high = float(pd.to_numeric(future["high"], errors="coerce").max())
            min_low = float(pd.to_numeric(future["low"], errors="coerce").min())
            mfe = (max_high - entry) / entry * 100.0 if max_high > 0 else np.nan
            mae = (min_low - entry) / entry * 100.0 if min_low > 0 else np.nan
            if not _valid_forward_label(group.iloc[idx], window, future_return, mfe, mae):
                continue
            rows.append(
                {
                    "code": code,
                    "date": group.iloc[idx]["date"].date().isoformat(),
                    "market_regime": str(group.iloc[idx].get("market_regime") or "未知"),
                    "label_limit_pct": _dynamic_label_limit_pct(group.iloc[idx], window),
                    "future_return_pct": future_return,
                    "avoided_drawdown_pct": max(0.0, -mae) if np.isfinite(mae) else np.nan,
                }
            )
    return pd.DataFrame(rows)


def _permutation_status(p_value: object, sample_count: int, min_samples: int) -> str:
    if sample_count < min_samples:
        return "样本不足"
    if not _finite(p_value):
        return "不可评估"
    if float(p_value) <= 0.05:
        return "显著优于随机"
    if float(p_value) <= 0.20:
        return "略优于随机"
    return "不显著"


def _evaluate_permutation_baseline(
    *,
    indicator_df: pd.DataFrame,
    records: list[dict[str, object]],
    summary: pd.DataFrame,
    primary_window: int,
    min_samples: int,
    iterations: int,
    seed: int = 42,
) -> dict[str, object]:
    if summary.empty:
        return {"status": "no_summary", "rows": [], "primary_window": primary_window}
    primary = summary[summary["window"] == primary_window].copy()
    if primary.empty:
        return {"status": "no_primary_window", "rows": [], "primary_window": primary_window}
    baseline = _build_random_baseline_frame(indicator_df, primary_window)
    if baseline.empty:
        return {"status": "no_baseline", "rows": [], "primary_window": primary_window}
    signal_df = pd.DataFrame(records)
    if signal_df.empty:
        return {"status": "no_signal_records", "rows": [], "primary_window": primary_window}
    signal_df = signal_df[signal_df["window"] == primary_window].copy()
    if signal_df.empty:
        return {"status": "no_primary_signal_records", "rows": [], "primary_window": primary_window}

    rng = np.random.default_rng(seed)
    max_draw_size = 5000
    rows: list[dict[str, object]] = []
    baseline = baseline.copy()
    baseline["future_return_pct"] = pd.to_numeric(baseline["future_return_pct"], errors="coerce")
    baseline["avoided_drawdown_pct"] = pd.to_numeric(baseline["avoided_drawdown_pct"], errors="coerce")

    def _pool_map(metric_col: str) -> tuple[dict[tuple[str, str], np.ndarray], dict[str, np.ndarray], dict[str, np.ndarray], np.ndarray]:
        clean = baseline.dropna(subset=[metric_col]).copy()
        by_code_regime = {
            (str(code), str(regime)): group[metric_col].to_numpy(dtype="float64")
            for (code, regime), group in clean.groupby(["code", "market_regime"])
        }
        by_code = {str(code): group[metric_col].to_numpy(dtype="float64") for code, group in clean.groupby("code")}
        by_regime = {str(regime): group[metric_col].to_numpy(dtype="float64") for regime, group in clean.groupby("market_regime")}
        return by_code_regime, by_code, by_regime, clean[metric_col].to_numpy(dtype="float64")

    return_pools = _pool_map("future_return_pct")
    drawdown_pools = _pool_map("avoided_drawdown_pct")

    def _draw_distribution_metric(signal_rows: pd.DataFrame, pools: tuple[dict, dict, dict, np.ndarray]) -> tuple[pd.Series, int]:
        if len(signal_rows) > max_draw_size:
            effective = signal_rows.sample(n=max_draw_size, random_state=seed)
        else:
            effective = signal_rows.copy()
        group_counts = effective.groupby(["code", "market_regime"]).size().reset_index(name="count")
        by_code_regime, by_code, by_regime, global_pool = pools
        random_metrics = []
        for _ in range(max(1, int(iterations))):
            total = 0.0
            total_count = 0
            for _, group_row in group_counts.iterrows():
                code = str(group_row["code"])
                regime = str(group_row["market_regime"])
                count = int(group_row["count"])
                pool = by_code_regime.get((code, regime))
                if pool is None or len(pool) < max(5, min(count, 20)):
                    pool = by_code.get(code)
                if pool is None or len(pool) == 0:
                    pool = by_regime.get(regime)
                if pool is None or len(pool) == 0:
                    pool = global_pool
                if pool is None or len(pool) == 0:
                    continue
                sample = rng.choice(pool, size=count, replace=len(pool) < count)
                total += float(np.sum(sample))
                total_count += count
            if total_count:
                random_metrics.append(total / total_count)
        return pd.Series(random_metrics, dtype="float64"), int(len(effective))

    for _, item in primary.iterrows():
        sample_count = int(item.get("sample_count") or 0)
        if sample_count <= 0:
            continue
        direction_type = str(item.get("direction_type") or "")
        signal_rows = signal_df[
            (signal_df["module"] == item.get("module"))
            & (signal_df["direction_type"] == direction_type)
            & (signal_df["rule"] == item.get("rule"))
        ].copy()
        if signal_rows.empty:
            continue
        if direction_type == "offensive":
            actual_metric = _safe_mean(signal_rows["future_return_pct"])
            random_series, draw_size = _draw_distribution_metric(signal_rows, return_pools)
            metric_name = "T+N平均收益"
        else:
            actual_metric = _safe_mean(signal_rows["avoided_drawdown_pct"])
            random_series, draw_size = _draw_distribution_metric(signal_rows, drawdown_pools)
            metric_name = "平均避免回撤"
        if not np.isfinite(actual_metric) or random_series.empty:
            continue
        p_value = float(((random_series >= actual_metric).sum() + 1) / (len(random_series) + 1))
        percentile = float((random_series < actual_metric).mean() * 100.0)
        rows.append(
            {
                "module": item.get("module"),
                "direction_type": direction_type,
                "rule": item.get("rule"),
                "window": primary_window,
                "sample_count": sample_count,
                "effective_random_sample_count": int(draw_size),
                "scope": "同标的+同市场环境",
                "metric_name": metric_name,
                "actual_metric_pct": actual_metric,
                "random_mean_pct": float(random_series.mean()),
                "random_p95_pct": float(random_series.quantile(0.95)),
                "percentile_pct": percentile,
                "p_value": p_value,
                "status": _permutation_status(p_value, sample_count, min_samples),
            }
        )

    if not rows:
        return {"status": "no_rows", "rows": [], "primary_window": primary_window}
    result = pd.DataFrame(rows)
    status_order = {"显著优于随机": 0, "略优于随机": 1, "不显著": 2, "不可评估": 3, "样本不足": 4}
    result["status_order"] = result["status"].map(status_order).fillna(9)
    result = result.sort_values(["status_order", "p_value", "sample_count"], ascending=[True, True, False]).drop(
        columns=["status_order"]
    )
    return {
        "status": "ok",
        "primary_window": primary_window,
        "iterations": int(iterations),
        "min_samples": min_samples,
        "scope": "same_code_and_regime",
        "baseline_count": int(len(baseline)),
        "rows": _to_records(result),
    }


def _cost_grade(net_returns: pd.Series, sample_count: int, min_samples: int) -> str:
    values = pd.to_numeric(net_returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if sample_count < min_samples or values.empty:
        return "样本不足"
    if float(values.mean()) > 0 and _payoff_ratio(values) >= 1.0:
        return "成本后通过"
    return "成本后失效"


def _evaluate_cost_stress(
    records: list[dict[str, object]],
    *,
    primary_window: int,
    min_samples: int,
    cost_bps_values: list[float],
) -> dict[str, object]:
    if not records:
        return {"status": "no_records", "rows": [], "primary_window": primary_window}
    df = pd.DataFrame(records)
    if df.empty:
        return {"status": "no_records", "rows": [], "primary_window": primary_window}
    df = df[df["window"] == primary_window].copy()
    if df.empty:
        return {"status": "no_primary_window", "rows": [], "primary_window": primary_window}

    rows: list[dict[str, object]] = []
    group_cols = ["module", "direction_type", "rule", "description", "window"]
    for key, sample in df.groupby(group_cols):
        key_map = dict(zip(group_cols, key if isinstance(key, tuple) else (key,)))
        base_returns = pd.to_numeric(sample["future_return_pct"], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if base_returns.empty:
            continue
        direction_type = str(key_map["direction_type"])
        for cost_bps in cost_bps_values:
            cost_pct = float(cost_bps) / 100.0
            if direction_type == "offensive":
                net_returns = base_returns - cost_pct
                metric_name = "成本后收益"
            else:
                # Defensive action benefit: exit/protect now versus holding through the forward window.
                net_returns = -base_returns - cost_pct
                metric_name = "成本后防守收益"
            rows.append(
                {
                    "module": key_map["module"],
                    "direction_type": direction_type,
                    "rule": key_map["rule"],
                    "description": key_map["description"],
                    "window": int(key_map["window"]),
                    "cost_bps": float(cost_bps),
                    "cost_pct": cost_pct,
                    "sample_count": int(len(base_returns)),
                    "metric_name": metric_name,
                    "net_avg_return_pct": float(net_returns.mean()),
                    "net_win_rate_pct": float((net_returns > 0).mean() * 100.0),
                    "net_payoff_ratio": _payoff_ratio(net_returns),
                    "net_tail_p5_pct": _quantile_return(net_returns, 0.05),
                    "grade": _cost_grade(net_returns, int(len(base_returns)), min_samples),
                }
            )

    if not rows:
        return {"status": "no_rows", "rows": [], "primary_window": primary_window}
    result = pd.DataFrame(rows)
    result["grade_order"] = result["grade"].map({"成本后通过": 0, "成本后失效": 1, "样本不足": 2}).fillna(9)
    result = result.sort_values(
        ["cost_bps", "grade_order", "net_avg_return_pct", "net_payoff_ratio"],
        ascending=[True, True, False, False],
    ).drop(columns=["grade_order"])
    return {
        "status": "ok",
        "primary_window": primary_window,
        "min_samples": min_samples,
        "cost_bps_values": cost_bps_values,
        "rows": _to_records(result),
    }


def _pass_fail_label(value: Optional[bool]) -> str:
    if value is True:
        return "通过"
    if value is False:
        return "未通过"
    return "未覆盖"


def _build_graduation_scorecard(
    *,
    summary: pd.DataFrame,
    walk_forward: dict[str, object],
    parameter_plateau: dict[str, object],
    permutation_baseline: dict[str, object],
    cost_stress: dict[str, object],
    primary_window: int,
    min_samples: int,
    standard_cost_bps: float,
) -> dict[str, object]:
    if summary.empty:
        return {"status": "no_summary", "rows": []}
    primary = summary[summary["window"] == primary_window].copy()
    if primary.empty:
        return {"status": "no_primary_window", "rows": []}

    wf_map = {
        (row.get("module"), row.get("direction_type"), row.get("rule")): row
        for row in (walk_forward.get("by_rule") or [])
    }
    plateau_map = {
        row.get("parameter_family"): row
        for row in (parameter_plateau.get("families") or [])
    }
    permutation_map = {
        (row.get("module"), row.get("direction_type"), row.get("rule")): row
        for row in (permutation_baseline.get("rows") or [])
    }
    cost_rows = [
        row
        for row in (cost_stress.get("rows") or [])
        if abs(float(row.get("cost_bps") or 0.0) - float(standard_cost_bps)) < 1e-9
    ]
    cost_map = {
        (row.get("module"), row.get("direction_type"), row.get("rule")): row
        for row in cost_rows
    }

    rows: list[dict[str, object]] = []
    for _, item in primary.iterrows():
        module = item.get("module")
        direction_type = item.get("direction_type")
        rule = item.get("rule")
        sample_count = int(item.get("sample_count") or 0)
        sample_gate = sample_count >= min_samples
        raw_gate = item.get("grade") == "可进Shadow"

        wf = wf_map.get((module, direction_type, rule))
        wf_pass: Optional[bool] = None
        wf_rate = None
        if wf:
            wf_rate = wf.get("oos_pass_rate_pct")
            wf_pass = _finite(wf_rate) and float(wf_rate) >= 50.0

        plateau = plateau_map.get(rule)
        plateau_pass: Optional[bool] = None
        plateau_status = "未覆盖"
        if plateau:
            plateau_status = str(plateau.get("plateau_status") or "未覆盖")
            plateau_pass = plateau_status == "稳定高原"
            if plateau_status == "单点敏感":
                plateau_pass = None
            elif plateau_status == "不稳定":
                plateau_pass = False

        perm = permutation_map.get((module, direction_type, rule))
        permutation_pass: Optional[bool] = None
        permutation_status = "未覆盖"
        p_value = None
        if perm:
            permutation_status = str(perm.get("status") or "未覆盖")
            p_value = perm.get("p_value")
            permutation_pass = permutation_status in {"显著优于随机", "略优于随机"}

        cost = cost_map.get((module, direction_type, rule))
        cost_pass: Optional[bool] = None
        cost_grade = "未覆盖"
        cost_net_avg = None
        if cost:
            cost_grade = str(cost.get("grade") or "未覆盖")
            cost_net_avg = cost.get("net_avg_return_pct")
            cost_pass = cost_grade == "成本后通过"

        hard_fail = not sample_gate or not raw_gate or permutation_pass is False or plateau_pass is False or cost_pass is False
        if (
            sample_gate
            and raw_gate
            and permutation_pass is True
            and plateau_pass is not False
            and wf_pass is not False
            and cost_pass is not False
        ):
            final_decision = "可进Shadow"
        elif direction_type == "defensive" and permutation_pass is True and not raw_gate:
            final_decision = "只作风险提示"
        elif hard_fail:
            final_decision = "不升级"
        else:
            final_decision = "继续观察"

        metric_value = item.get("avg_return_pct") if direction_type == "offensive" else item.get("avg_avoided_drawdown_pct")
        rows.append(
            {
                "module": module,
                "direction_type": direction_type,
                "rule": rule,
                "sample_count": sample_count,
                "raw_grade": item.get("grade"),
                "sample_gate": _pass_fail_label(sample_gate),
                "raw_gate": _pass_fail_label(raw_gate),
                "walk_forward_gate": _pass_fail_label(wf_pass),
                "walk_forward_pass_rate_pct": wf_rate,
                "plateau_gate": _pass_fail_label(plateau_pass),
                "plateau_status": plateau_status,
                "permutation_gate": _pass_fail_label(permutation_pass),
                "permutation_status": permutation_status,
                "p_value": p_value,
                "cost_gate": _pass_fail_label(cost_pass),
                "cost_grade": cost_grade,
                "standard_cost_bps": float(standard_cost_bps),
                "cost_net_avg_return_pct": cost_net_avg,
                "metric_value_pct": metric_value,
                "payoff_ratio": item.get("payoff_ratio"),
                "final_decision": final_decision,
            }
        )

    result = pd.DataFrame(rows)
    decision_order = {"可进Shadow": 0, "只作风险提示": 1, "继续观察": 2, "不升级": 3}
    result["decision_order"] = result["final_decision"].map(decision_order).fillna(9)
    result = result.sort_values(
        ["decision_order", "sample_count", "payoff_ratio"],
        ascending=[True, False, False],
    ).drop(columns=["decision_order"])
    return {
        "status": "ok",
        "primary_window": primary_window,
        "min_samples": min_samples,
        "standard_cost_bps": float(standard_cost_bps),
        "rows": _to_records(result),
    }


def _concentration_status(top1_pct: object, top3_pct: object, positive_code_count: int, code_count: int) -> str:
    if code_count <= 0:
        return "无样本"
    if not _finite(top1_pct) or not _finite(top3_pct):
        return "不可评估"
    min_positive_codes = max(3, int(np.ceil(code_count * 0.35)))
    if float(top1_pct) <= 35.0 and float(top3_pct) <= 65.0 and positive_code_count >= min_positive_codes:
        return "分散"
    if float(top1_pct) >= 50.0 or float(top3_pct) >= 80.0:
        return "高度集中"
    return "偏集中"


def _evaluate_symbol_attribution(
    records: list[dict[str, object]],
    *,
    graduation_scorecard: dict[str, object],
    primary_window: int,
    min_symbol_samples: int,
) -> dict[str, object]:
    if not records:
        return {"status": "no_records", "primary_window": primary_window, "summary": [], "details": []}
    candidate_keys = {
        (row.get("module"), row.get("direction_type"), row.get("rule"))
        for row in (graduation_scorecard.get("rows") or [])
        if row.get("final_decision") == "可进Shadow"
    }
    if not candidate_keys:
        return {"status": "no_shadow_candidates", "primary_window": primary_window, "summary": [], "details": []}

    df = pd.DataFrame(records)
    if df.empty:
        return {"status": "no_records", "primary_window": primary_window, "summary": [], "details": []}
    df = df[df["window"] == primary_window].copy()
    if df.empty:
        return {"status": "no_primary_window", "primary_window": primary_window, "summary": [], "details": []}

    summaries: list[dict[str, object]] = []
    details: list[dict[str, object]] = []
    for module, direction_type, rule in sorted(candidate_keys):
        sample = df[
            (df["module"] == module)
            & (df["direction_type"] == direction_type)
            & (df["rule"] == rule)
        ].copy()
        if sample.empty:
            continue
        grouped = []
        for code, code_df in sample.groupby("code"):
            returns = pd.to_numeric(code_df["future_return_pct"], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
            if returns.empty:
                continue
            if direction_type == "offensive":
                metric = float(returns.mean())
                payoff = _payoff_ratio(returns)
                contribution = float(returns.sum())
            else:
                avoided = pd.to_numeric(code_df["avoided_drawdown_pct"], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
                metric = float(avoided.mean()) if not avoided.empty else float("nan")
                payoff = _payoff_ratio(-returns)
                contribution = float(avoided.sum()) if not avoided.empty else 0.0
            grouped.append(
                {
                    "module": module,
                    "direction_type": direction_type,
                    "rule": rule,
                    "code": str(code),
                    "sample_count": int(len(returns)),
                    "avg_metric_pct": metric,
                    "payoff_ratio": payoff,
                    "positive": bool(metric > 0) if _finite(metric) else False,
                    "raw_contribution": contribution,
                }
            )
        if not grouped:
            continue
        code_df = pd.DataFrame(grouped)
        robust = code_df[code_df["sample_count"] >= min_symbol_samples].copy()
        if robust.empty:
            robust = code_df.copy()
        total_abs = float(robust["raw_contribution"].abs().sum())
        if total_abs <= 0:
            robust["contribution_share_pct"] = 0.0
        else:
            robust["contribution_share_pct"] = robust["raw_contribution"].abs() / total_abs * 100.0
        robust = robust.sort_values(["contribution_share_pct", "sample_count"], ascending=[False, False])
        top1 = float(robust["contribution_share_pct"].iloc[0]) if not robust.empty else np.nan
        top3 = float(robust["contribution_share_pct"].head(3).sum()) if not robust.empty else np.nan
        positive_code_count = int(robust["positive"].sum())
        code_count = int(len(robust))
        summaries.append(
            {
                "module": module,
                "direction_type": direction_type,
                "rule": rule,
                "code_count": code_count,
                "positive_code_count": positive_code_count,
                "min_symbol_samples": min_symbol_samples,
                "top1_contribution_pct": top1,
                "top3_contribution_pct": top3,
                "concentration_status": _concentration_status(top1, top3, positive_code_count, code_count),
            }
        )
        details.extend(_to_records(robust.head(8)))

    if not summaries:
        return {"status": "no_rows", "primary_window": primary_window, "summary": [], "details": []}
    summary_df = pd.DataFrame(summaries)
    summary_df["status_order"] = summary_df["concentration_status"].map({"分散": 0, "偏集中": 1, "高度集中": 2, "不可评估": 3}).fillna(9)
    summary_df = summary_df.sort_values(["status_order", "top1_contribution_pct"], ascending=[True, True]).drop(columns=["status_order"])
    return {
        "status": "ok",
        "primary_window": primary_window,
        "min_symbol_samples": min_symbol_samples,
        "summary": _to_records(summary_df),
        "details": details,
    }


def _evaluate_analysis_history(db_path: Path, windows: list[int]) -> list[dict[str, object]]:
    with sqlite3.connect(str(db_path)) as conn:
        rows = pd.read_sql_query(
            """
            SELECT id, code, name, operation_advice, trend_prediction, created_at, ideal_buy, stop_loss, take_profit
            FROM analysis_history
            ORDER BY created_at
            """,
            conn,
            parse_dates=["created_at"],
        )
    if rows.empty:
        return []
    return [
        {
            "window": window,
            "sample_count": 0,
            "status": "pending",
            "note": "analysis_history 当前多为近日报告，未来价格窗口未充分形成；先不纳入晋升评分。",
        }
        for window in windows
    ]


def _ic_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _evaluate_ic_theory(
    *,
    windows: list[int],
    output_window: int,
    data_cache_dir: str,
    refresh_data_cache: bool,
) -> dict[str, object]:
    start, end = _ic_start_end()
    frame = _build_term_structure_frame(start, end, data_cache_dir, refresh_data_cache)
    front_collapse_threshold = float(frame["front_end_gap"].dropna().quantile(0.80))
    q_anchor = frame["q1_q2_annualized"].dropna()
    q_anchor_median = float(q_anchor.median())
    q_anchor_band = float((q_anchor - q_anchor_median).abs().median()) or 0.01
    trend_broken = frame["trend_intact"] != 1
    front_collapse = frame["front_end_gap"] >= front_collapse_threshold
    q_anchor_unstable = (frame["q1_q2_annualized"] - q_anchor_median).abs() > q_anchor_band

    rows = []
    rule_masks = {
        "M1-M2前端塌陷": front_collapse,
        "M1-M2前端塌陷+趋势破坏": front_collapse & trend_broken,
        "M1-M2前端塌陷+远季锚失稳": front_collapse & q_anchor_unstable,
        "趋势破坏确认": trend_broken,
    }
    max_window = max(windows)
    for rule, mask in rule_masks.items():
        sample = frame[mask].copy()
        if sample.empty:
            continue
        for window in windows:
            future = []
            for idx in sample.index:
                pos = frame.index.get_loc(idx)
                if pos + window >= len(frame):
                    continue
                window_ret = frame.iloc[pos + 1 : pos + window + 1]["tradable_return_1d"].fillna(0.0).sum()
                future.append(float(window_ret * 100.0))
            ret = pd.Series(future, dtype="float64")
            rows.append(
                {
                    "module": "IC吃贴水风控",
                    "direction_type": "defensive",
                    "rule": rule,
                    "window": window,
                    "sample_count": int(len(ret)),
                    "avg_future_return_pct": float(ret.mean()) if not ret.empty else np.nan,
                    "down_hit_rate_pct": float((ret < 0).mean() * 100.0) if not ret.empty else np.nan,
                    "protect_value_pct": float(max(0.0, -ret.mean())) if not ret.empty else np.nan,
                    "payoff_ratio": _payoff_ratio(-ret) if not ret.empty else np.nan,
                    "grade": (
                        "可进Shadow"
                        if len(ret) >= 50 and float((ret < 0).mean() * 100.0) >= 50 and float(max(0.0, -ret.mean())) >= 1.0
                        else "继续观察"
                    ),
                }
            )
    return {
        "status": "ok",
        "start": start,
        "end": end,
        "front_collapse_threshold_pct": front_collapse_threshold * 100.0,
        "q_anchor_median_pct": q_anchor_median * 100.0,
        "q_anchor_band_pct": q_anchor_band * 100.0,
        "rows": rows,
        "primary_window": output_window,
        "sample_count": int(len(frame.iloc[:-max_window])),
    }


def _to_records(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    cleaned = df.replace([np.inf, -np.inf], np.nan)
    return json.loads(cleaned.to_json(orient="records", force_ascii=False))


def _write_outputs(
    *,
    output_dir: Path,
    db_path: Path,
    daily_meta: dict[str, object],
    summary: pd.DataFrame,
    regime_summary: pd.DataFrame,
    walk_forward: dict[str, object],
    parameter_plateau: dict[str, object],
    permutation_baseline: dict[str, object],
    cost_stress: dict[str, object],
    graduation_scorecard: dict[str, object],
    symbol_attribution: dict[str, object],
    analysis_history: list[dict[str, object]],
    ic_summary: dict[str, object],
    windows: list[int],
    min_samples: int,
) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().date().isoformat()
    report_path = output_dir / f"{today}_理论信号准确率评分表.md"
    json_path = output_dir / f"{today}_theory_signal_scorecard.json"
    latest_path = output_dir / "latest_theory_signal_scorecard.md"

    primary_window = 5 if 5 in windows else windows[0]
    primary = summary[summary["window"] == primary_window].copy() if not summary.empty else pd.DataFrame()

    offensive = primary[primary["direction_type"] == "offensive"].copy()
    defensive = primary[primary["direction_type"] == "defensive"].copy()
    offensive = offensive.sort_values(["grade", "sample_count", "payoff_ratio", "avg_return_pct"], ascending=[True, False, False, False]).head(16)
    defensive = defensive.sort_values(["grade", "sample_count", "avg_avoided_drawdown_pct", "false_rebound_rate_pct"], ascending=[True, False, False, True]).head(16)
    regime_primary = regime_summary[regime_summary["window"] == primary_window].copy() if not regime_summary.empty else pd.DataFrame()
    if not regime_primary.empty:
        regime_primary["regime_order"] = regime_primary["market_regime"].map(
            {name: idx for idx, name in enumerate(REGIME_ORDER)}
        ).fillna(len(REGIME_ORDER))
        regime_parts = []
        for regime in REGIME_ORDER:
            regime_df = regime_primary[regime_primary["market_regime"] == regime]
            if regime_df.empty:
                continue
            for direction_type in ("defensive", "offensive"):
                direction_df = regime_df[regime_df["direction_type"] == direction_type].sort_values(
                    ["grade", "sample_count", "payoff_ratio"],
                    ascending=[True, False, False],
                )
                if not direction_df.empty:
                    regime_parts.append(direction_df.head(4))
        regime_primary = pd.concat(regime_parts, ignore_index=True) if regime_parts else pd.DataFrame()

    lines = [
        f"# {today} 理论信号准确率评分表",
        "",
        "- 范围: 只评估理论信号本身，不测试飞书、桌面端、定时任务或接口稳定性。",
        "- 评价原则: 进攻信号看收益质量和入场时机；防守信号看是否能避免后续回撤。",
        f"- 数据库: `{db_path}`",
        f"- 评估窗口: {', '.join('T+' + str(w) for w in windows)}",
        f"- 主展示窗口: T+{primary_window}",
        f"- 最小参考样本: {min_samples}",
        f"- 标签清洗: 使用滚动 ATR/尾部分位数动态阈值，基础参数 floor={DYNAMIC_LABEL_FLOOR_PCT:.0f}%, cap={DYNAMIC_LABEL_CAP_PCT:.0f}%, multiplier={DYNAMIC_LABEL_ATR_MULTIPLIER:.1f}。",
        f"- 日线样本: {daily_meta.get('row_count')} 行 / {daily_meta.get('code_count')} 个标的 / {daily_meta.get('date_min')} 至 {daily_meta.get('date_max')}",
        "",
        "## 数据源覆盖",
        "",
        "| 来源 | 行数 | 标的数 | 根目录 |",
        "| --- | ---: | ---: | --- |",
    ]
    for item in daily_meta.get("source_rows", []):
        lines.append(
            f"| {item.get('source')} | {item.get('rows')} | {item.get('codes')} | {item.get('root', '-')} |"
        )

    lines.extend(
        [
            "",
            "## 进攻信号评分（买入/收益）",
            "",
            "| 模块 | 信号 | 样本 | 方向准确率 | T+N收益 | MFE | MAE | 5%尾部亏损 | 盈亏比 | 评级 |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    if offensive.empty:
        lines.append("| - | 无可评估样本 | 0 | - | - | - | - | - | - | 样本不足 |")
    else:
        for _, row in offensive.iterrows():
            lines.append(
                f"| {row['module']} | {row['rule']} | {int(row['sample_count'])} | "
                f"{_fmt_pct(row['direction_accuracy_pct'])} | {_fmt_pct(row['avg_return_pct'])} | "
                f"{_fmt_pct(row['avg_mfe_pct'])} | {_fmt_pct(row['avg_mae_pct'])} | "
                f"{_fmt_pct(row['tail_loss_p5_pct'])} | {_fmt_num(row['payoff_ratio'])} | {row['grade']} |"
            )

    lines.extend(
        [
            "",
            "## 防守信号评分（风控/止损）",
            "",
            "| 模块 | 信号 | 样本 | 下跌命中率 | T+N收益 | 平均避免回撤 | 假阳性反弹率 | 防守盈亏比 | 评级 |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    if defensive.empty:
        lines.append("| - | 无可评估样本 | 0 | - | - | - | - | - | 样本不足 |")
    else:
        for _, row in defensive.iterrows():
            lines.append(
                f"| {row['module']} | {row['rule']} | {int(row['sample_count'])} | "
                f"{_fmt_pct(row['down_hit_rate_pct'])} | {_fmt_pct(row['avg_return_pct'])} | "
                f"{_fmt_pct(row['avg_avoided_drawdown_pct'])} | {_fmt_pct(row['false_rebound_rate_pct'])} | "
                f"{_fmt_num(row['payoff_ratio'])} | {row['grade']} |"
            )

    lines.extend(
        [
            "",
            "## 分市场环境评分",
            "",
            "> 这里用基准指数近1/5/20日走势粗分环境，目的是检查信号是否只在单一行情里有效。",
            "",
            "| 环境 | 类型 | 模块 | 信号 | 样本 | 准确率 | 收益/防守价值 | 盈亏比 | 评级 |",
            "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    if regime_primary.empty:
        lines.append("| 未知 | - | - | 无可评估样本 | 0 | - | - | - | 样本不足 |")
    else:
        for _, row in regime_primary.iterrows():
            value = row["avg_return_pct"] if row["direction_type"] == "offensive" else row["avg_avoided_drawdown_pct"]
            type_label = "进攻" if row["direction_type"] == "offensive" else "防守"
            lines.append(
                f"| {row['market_regime']} | {type_label} | {row['module']} | {row['rule']} | "
                f"{int(row['sample_count'])} | {_fmt_pct(row['direction_accuracy_pct'])} | "
                f"{_fmt_pct(value)} | {_fmt_num(row['payoff_ratio'])} | {row['grade']} |"
            )

    lines.extend(["", "## Walk-forward 样本外验证", ""])
    if walk_forward.get("status") == "ok":
        wf_summary = walk_forward.get("summary", {}) or {}
        lines.extend(
            [
                f"- 训练窗口: 过去 {walk_forward.get('train_years')} 年",
                f"- 测试窗口: 下一自然年，主窗口 T+{walk_forward.get('primary_window')}",
                f"- 候选测试: {wf_summary.get('candidate_tests')} 次",
                f"- 有效测试: {wf_summary.get('valid_tests')} 次",
                f"- 样本外通过率: {_fmt_pct(wf_summary.get('pass_rate_pct'))}",
                "",
                "| 模块 | 类型 | 信号 | 样本外次数 | 通过次数 | 通过率 | 测试收益 | 测试盈亏比 |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in (walk_forward.get("by_rule") or [])[:16]:
            type_label = "进攻" if row.get("direction_type") == "offensive" else "防守"
            lines.append(
                f"| {row.get('module')} | {type_label} | {row.get('rule')} | "
                f"{int(row.get('oos_tests') or 0)} | {int(row.get('oos_passes') or 0)} | "
                f"{_fmt_pct(row.get('oos_pass_rate_pct'))} | {_fmt_pct(row.get('avg_test_return_pct'))} | "
                f"{_fmt_num(row.get('avg_test_payoff_ratio'))} |"
            )
        lines.extend(
            [
                "",
                "### 最近样本外明细",
                "",
                "| 测试年 | 训练期 | 类型 | 信号 | 训练样本 | 测试样本 | 测试收益 | 测试盈亏比 | 状态 |",
                "| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row in sorted(walk_forward.get("rows") or [], key=lambda item: (item.get("test_year", 0), item.get("rule", "")), reverse=True)[:20]:
            type_label = "进攻" if row.get("direction_type") == "offensive" else "防守"
            lines.append(
                f"| {row.get('test_year')} | {row.get('train_years')} | {type_label} | {row.get('rule')} | "
                f"{int(row.get('train_sample_count') or 0)} | {int(row.get('test_sample_count') or 0)} | "
                f"{_fmt_pct(row.get('test_avg_return_pct'))} | {_fmt_num(row.get('test_payoff_ratio'))} | {row.get('oos_status')} |"
            )
    else:
        lines.append(f"- Walk-forward 未生成: {walk_forward.get('status')}")

    lines.extend(["", "## 参数高原测试", ""])
    if parameter_plateau.get("status") == "ok":
        lines.extend(
            [
                f"- 测试窗口: T+{parameter_plateau.get('primary_window')}",
                f"- 最小参考样本: {parameter_plateau.get('min_samples')}",
                "",
                "| 参数族 | 类型 | 变体数 | 可进Shadow变体 | 稳定性 | 最佳参数 | 最佳样本 | 最佳收益/防守价值 | 最佳盈亏比 |",
                "| --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: |",
            ]
        )
        for row in parameter_plateau.get("families") or []:
            type_label = "进攻" if row.get("direction_type") == "offensive" else "防守"
            value = row.get("best_avg_return_pct") if row.get("direction_type") == "offensive" else row.get("best_avg_avoided_drawdown_pct")
            lines.append(
                f"| {row.get('parameter_family')} | {type_label} | {int(row.get('variant_count') or 0)} | "
                f"{int(row.get('qualified_count') or 0)} | {row.get('plateau_status')} | {row.get('best_parameter')} | "
                f"{int(row.get('best_sample_count') or 0)} | {_fmt_pct(value)} | {_fmt_num(row.get('best_payoff_ratio'))} |"
            )
        lines.extend(
            [
                "",
                "### 参数变体明细",
                "",
                "| 参数族 | 参数 | 类型 | 样本 | 收益 | 防守价值 | 盈亏比 | 评级 |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row in parameter_plateau.get("variants") or []:
            type_label = "进攻" if row.get("direction_type") == "offensive" else "防守"
            lines.append(
                f"| {row.get('parameter_family')} | {row.get('parameter_label')} | {type_label} | "
                f"{int(row.get('sample_count') or 0)} | {_fmt_pct(row.get('avg_return_pct'))} | "
                f"{_fmt_pct(row.get('avg_avoided_drawdown_pct'))} | {_fmt_num(row.get('payoff_ratio'))} | {row.get('grade')} |"
            )
    else:
        lines.append(f"- 参数高原测试未生成: {parameter_plateau.get('status')}")

    lines.extend(["", "## 随机置换检验", ""])
    if permutation_baseline.get("status") == "ok":
        lines.extend(
            [
                f"- 测试窗口: T+{permutation_baseline.get('primary_window')}",
                f"- 随机次数: {permutation_baseline.get('iterations')}",
                f"- 抽样范围: 同标的 + 同市场环境，样本不足时逐级回退到同标的/同环境/全局",
                f"- 随机基准样本: {permutation_baseline.get('baseline_count')}",
                "",
                "| 模块 | 类型 | 信号 | 样本 | 指标 | 真实值 | 随机均值 | 随机95分位 | 百分位 | p值 | 结论 |",
                "| --- | --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row in (permutation_baseline.get("rows") or [])[:20]:
            type_label = "进攻" if row.get("direction_type") == "offensive" else "防守"
            lines.append(
                f"| {row.get('module')} | {type_label} | {row.get('rule')} | {int(row.get('sample_count') or 0)} | "
                f"{row.get('metric_name')} | {_fmt_pct(row.get('actual_metric_pct'))} | "
                f"{_fmt_pct(row.get('random_mean_pct'))} | {_fmt_pct(row.get('random_p95_pct'))} | "
                f"{_fmt_pct(row.get('percentile_pct'))} | {_fmt_num(row.get('p_value'), 3)} | {row.get('status')} |"
            )
    else:
        lines.append(f"- 随机置换检验未生成: {permutation_baseline.get('status')}")

    lines.extend(["", "## 交易成本压力测试", ""])
    if cost_stress.get("status") == "ok":
        lines.extend(
            [
                f"- 测试窗口: T+{cost_stress.get('primary_window')}",
                f"- 成本档位: {', '.join(str(item) + 'bp' for item in cost_stress.get('cost_bps_values', []))}",
                "",
                "| 成本 | 模块 | 类型 | 信号 | 样本 | 成本后均值 | 成本后胜率 | 成本后盈亏比 | 5%尾损 | 评级 |",
                "| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row in (cost_stress.get("rows") or [])[:36]:
            type_label = "进攻" if row.get("direction_type") == "offensive" else "防守"
            lines.append(
                f"| {float(row.get('cost_bps') or 0.0):.0f}bp | {row.get('module')} | {type_label} | {row.get('rule')} | "
                f"{int(row.get('sample_count') or 0)} | {_fmt_pct(row.get('net_avg_return_pct'))} | "
                f"{_fmt_pct(row.get('net_win_rate_pct'))} | {_fmt_num(row.get('net_payoff_ratio'))} | "
                f"{_fmt_pct(row.get('net_tail_p5_pct'))} | {row.get('grade')} |"
            )
    else:
        lines.append(f"- 交易成本压力测试未生成: {cost_stress.get('status')}")

    lines.extend(["", "## 综合晋升门槛", ""])
    if graduation_scorecard.get("status") == "ok":
        lines.extend(
            [
                "> 最终是否升级，不再只看单一收益或单一胜率；必须同时参考样本量、原始评级、样本外、参数稳定性和随机基准。",
                "",
                f"- 标准成本门槛: {graduation_scorecard.get('standard_cost_bps')}bp",
                "",
                "| 模块 | 类型 | 信号 | 样本 | 原评级 | 样本 | 样本外 | 参数 | 随机 | 成本 | 指标值 | 成本后均值 | 盈亏比 | 最终结论 |",
                "| --- | --- | --- | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |",
            ]
        )
        for row in (graduation_scorecard.get("rows") or [])[:24]:
            type_label = "进攻" if row.get("direction_type") == "offensive" else "防守"
            lines.append(
                f"| {row.get('module')} | {type_label} | {row.get('rule')} | {int(row.get('sample_count') or 0)} | "
                f"{row.get('raw_grade')} | {row.get('sample_gate')} | {row.get('walk_forward_gate')} | "
                f"{row.get('plateau_gate')} | {row.get('permutation_gate')} | {row.get('cost_gate')} | "
                f"{_fmt_pct(row.get('metric_value_pct'))} | {_fmt_pct(row.get('cost_net_avg_return_pct'))} | "
                f"{_fmt_num(row.get('payoff_ratio'))} | {row.get('final_decision')} |"
            )
    else:
        lines.append(f"- 综合晋升门槛未生成: {graduation_scorecard.get('status')}")

    lines.extend(["", "## 按标的贡献拆分", ""])
    if symbol_attribution.get("status") == "ok":
        lines.extend(
            [
                f"- 测试窗口: T+{symbol_attribution.get('primary_window')}",
                f"- 单标的最小稳健样本: {symbol_attribution.get('min_symbol_samples')}",
                "",
                "| 类型 | 信号 | 覆盖标的 | 正贡献标的 | Top1贡献 | Top3贡献 | 集中度 |",
                "| --- | --- | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row in symbol_attribution.get("summary") or []:
            type_label = "进攻" if row.get("direction_type") == "offensive" else "防守"
            lines.append(
                f"| {type_label} | {row.get('rule')} | {int(row.get('code_count') or 0)} | "
                f"{int(row.get('positive_code_count') or 0)} | {_fmt_pct(row.get('top1_contribution_pct'))} | "
                f"{_fmt_pct(row.get('top3_contribution_pct'))} | {row.get('concentration_status')} |"
            )
        lines.extend(
            [
                "",
                "### 标的贡献明细",
                "",
                "| 信号 | 标的 | 样本 | 平均指标 | 盈亏比 | 贡献占比 |",
                "| --- | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in symbol_attribution.get("details") or []:
            lines.append(
                f"| {row.get('rule')} | {row.get('code')} | {int(row.get('sample_count') or 0)} | "
                f"{_fmt_pct(row.get('avg_metric_pct'))} | {_fmt_num(row.get('payoff_ratio'))} | "
                f"{_fmt_pct(row.get('contribution_share_pct'))} |"
            )
    else:
        lines.append(f"- 按标的贡献拆分未生成: {symbol_attribution.get('status')}")

    lines.extend(["", "## IC 衍生品风控评分", ""])
    if ic_summary.get("status") == "ok":
        lines.extend(
            [
                f"- 验证区间: {ic_summary.get('start')} 至 {ic_summary.get('end')}",
                f"- 前端塌陷阈值: {ic_summary.get('front_collapse_threshold_pct'):.2f}%",
                f"- 远季锚中位数: {ic_summary.get('q_anchor_median_pct'):.2f}%",
                "",
                "| 信号 | 窗口 | 样本 | 未来收益 | 下跌命中率 | 防守价值 | 防守盈亏比 | 评级 |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row in ic_summary.get("rows", []):
            if int(row.get("window", -1)) != primary_window:
                continue
            lines.append(
                f"| {row['rule']} | T+{row['window']} | {row['sample_count']} | "
                f"{_fmt_pct(row['avg_future_return_pct'])} | {_fmt_pct(row['down_hit_rate_pct'])} | "
                f"{_fmt_pct(row['protect_value_pct'])} | {_fmt_num(row['payoff_ratio'])} | {row['grade']} |"
            )
    else:
        lines.append(f"- IC评分未生成: {ic_summary.get('error', 'unknown error')}")

    lines.extend(
        [
            "",
            "## 大模型日线报告结论",
            "",
            "| 窗口 | 样本 | 状态 | 说明 |",
            "| ---: | ---: | --- | --- |",
        ]
    )
    for row in analysis_history:
        lines.append(f"| T+{row['window']} | {row['sample_count']} | {row['status']} | {row['note']} |")

    lines.extend(
        [
            "",
            "## 治理结论",
            "",
            "- 进攻和防守信号已经分开评分，后续不要再用单一胜率评价所有信号。",
            "- `可进Shadow` 只代表理论样本允许继续盲跑，不代表可以直接实盘加仓或减仓。",
            "- 参数高原用于识别阈值是否脆弱；同标的/同环境随机置换用于识别信号是否只是吃到市场 Beta。",
            "- 交易成本压力测试用于过滤利润过薄、实盘滑点一扣就失效的信号。",
            "- 当前报告优先用于淘汰低样本、低盈亏比、假阳性过高的规则。",
        ]
    )

    content = "\n".join(lines) + "\n"
    report_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "db_path": str(db_path),
        "windows": windows,
        "primary_window": primary_window,
        "min_samples": min_samples,
        "daily_meta": daily_meta,
        "equity_summary": _to_records(summary),
        "regime_summary": _to_records(regime_summary),
        "walk_forward": walk_forward,
        "parameter_plateau": parameter_plateau,
        "permutation_baseline": permutation_baseline,
        "cost_stress": cost_stress,
        "graduation_scorecard": graduation_scorecard,
        "symbol_attribution": symbol_attribution,
        "analysis_history": analysis_history,
        "ic_summary": ic_summary,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return report_path, json_path, latest_path


def _parse_codes(raw: str | None) -> list[str]:
    if not raw:
        return _default_focus_codes()
    return [_normalize_focus_code(item) for item in raw.split(",") if _normalize_focus_code(item)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None, help="SQLite database path. Defaults to DATABASE_PATH or ./data/stock_analysis.db")
    parser.add_argument("--codes", default=None, help="Comma-separated focus codes. Defaults to STOCK_LIST + WATCHLIST_STOCK_LIST.")
    parser.add_argument("--benchmark-code", default="510300", help="Benchmark code for relative strength labels.")
    parser.add_argument("--windows", nargs="+", type=int, default=[3, 5, 10], help="Forward windows in trading days.")
    parser.add_argument("--min-samples", type=int, default=50, help="Minimum signal count for upgrade consideration.")
    parser.add_argument("--walk-forward-train-years", type=int, default=3, help="Prior years used to select promoted signals for OOS validation.")
    parser.add_argument("--output-dir", default="reports/backtests", help="Output directory.")
    parser.add_argument("--no-qlib", action="store_true", help="Do not use bootstrapped Qlib daily history.")
    parser.add_argument("--no-etf-cache", action="store_true", help="Do not use local ETF history cache.")
    parser.add_argument("--skip-ic", action="store_true", help="Skip IC term-structure scoring.")
    parser.add_argument("--skip-parameter-plateau", action="store_true", help="Skip parameter plateau robustness checks.")
    parser.add_argument("--skip-permutation", action="store_true", help="Skip random permutation baseline checks.")
    parser.add_argument("--permutation-iterations", type=int, default=300, help="Random draws per signal for permutation checks.")
    parser.add_argument("--skip-cost-stress", action="store_true", help="Skip transaction cost stress checks.")
    parser.add_argument("--cost-bps", nargs="+", type=float, default=[10.0, 20.0, 30.0], help="Round-trip cost assumptions in basis points.")
    parser.add_argument("--graduation-cost-bps", type=float, default=20.0, help="Cost gate used by the final graduation scorecard.")
    parser.add_argument("--min-symbol-samples", type=int, default=20, help="Minimum per-symbol samples for robust attribution tables.")
    parser.add_argument("--rule-set", choices=["core", "full"], default="core", help="Signal rule universe to evaluate.")
    parser.add_argument(
        "--max-rows-per-code",
        type=int,
        default=1600,
        help="Limit rows per symbol after loading local caches. 1600 trading days is roughly 6 years.",
    )
    parser.add_argument(
        "--ic-data-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "ic_basis_history"),
        help="IC data cache directory.",
    )
    parser.add_argument("--refresh-ic-data-cache", action="store_true", help="Refresh IC data cache from public sources.")
    args = parser.parse_args()

    windows = sorted({int(item) for item in args.windows if int(item) > 0})
    if not windows:
        raise ValueError("--windows must include at least one positive integer")

    db_path = _resolve_db_path(args.db)
    focus_codes = _parse_codes(args.codes)
    daily, daily_meta = load_theory_daily_frame(
        db_path=db_path,
        focus_codes=focus_codes,
        use_qlib=not args.no_qlib,
        use_etf_cache=not args.no_etf_cache,
        max_rows_per_code=args.max_rows_per_code,
    )
    if daily.empty:
        raise RuntimeError("No daily history available for theory signal scorecard.")

    indicator_df = _prepare_indicator_frame(daily, args.benchmark_code)
    records = _iter_theory_records(indicator_df, windows, rule_set=args.rule_set)
    summary = _summarize_equity_records(records, args.min_samples)
    regime_summary = _summarize_equity_records(records, args.min_samples, by_regime=True)
    primary_window = 5 if 5 in windows else windows[0]
    walk_forward = _evaluate_walk_forward(
        records,
        primary_window=primary_window,
        min_train_samples=int(args.min_samples),
        train_years=max(1, int(args.walk_forward_train_years)),
    )
    if args.skip_parameter_plateau:
        parameter_plateau: dict[str, object] = {"status": "skipped"}
    else:
        parameter_plateau = _evaluate_parameter_plateau(
            indicator_df,
            primary_window=primary_window,
            min_samples=int(args.min_samples),
        )
    if args.skip_permutation:
        permutation_baseline: dict[str, object] = {"status": "skipped"}
    else:
        permutation_baseline = _evaluate_permutation_baseline(
            indicator_df=indicator_df,
            records=records,
            summary=summary,
            primary_window=primary_window,
            min_samples=int(args.min_samples),
            iterations=max(1, int(args.permutation_iterations)),
        )
    if args.skip_cost_stress:
        cost_stress: dict[str, object] = {"status": "skipped"}
    else:
        cost_stress = _evaluate_cost_stress(
            records,
            primary_window=primary_window,
            min_samples=int(args.min_samples),
            cost_bps_values=sorted({float(item) for item in args.cost_bps if float(item) >= 0}),
        )
    graduation_scorecard = _build_graduation_scorecard(
        summary=summary,
        walk_forward=walk_forward,
        parameter_plateau=parameter_plateau,
        permutation_baseline=permutation_baseline,
        cost_stress=cost_stress,
        primary_window=primary_window,
        min_samples=int(args.min_samples),
        standard_cost_bps=float(args.graduation_cost_bps),
    )
    symbol_attribution = _evaluate_symbol_attribution(
        records,
        graduation_scorecard=graduation_scorecard,
        primary_window=primary_window,
        min_symbol_samples=max(1, int(args.min_symbol_samples)),
    )
    analysis_history = _evaluate_analysis_history(db_path, windows)
    if args.skip_ic:
        ic_summary: dict[str, object] = {"status": "skipped"}
    else:
        try:
            ic_summary = _evaluate_ic_theory(
                windows=windows,
                output_window=5 if 5 in windows else windows[0],
                data_cache_dir=args.ic_data_cache_dir,
                refresh_data_cache=bool(args.refresh_ic_data_cache),
            )
        except Exception as exc:
            ic_summary = {"status": "error", "error": str(exc)}

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir
    report_path, json_path, latest_path = _write_outputs(
        output_dir=output_dir.resolve(),
        db_path=db_path,
        daily_meta=daily_meta,
        summary=summary,
        regime_summary=regime_summary,
        walk_forward=walk_forward,
        parameter_plateau=parameter_plateau,
        permutation_baseline=permutation_baseline,
        cost_stress=cost_stress,
        graduation_scorecard=graduation_scorecard,
        symbol_attribution=symbol_attribution,
        analysis_history=analysis_history,
        ic_summary=ic_summary,
        windows=windows,
        min_samples=int(args.min_samples),
    )

    print(f"generated: {report_path}")
    print(f"json: {json_path}")
    print(f"latest: {latest_path}")
    if not summary.empty:
        primary_window = 5 if 5 in windows else windows[0]
        top = summary[(summary["window"] == primary_window) & (summary["grade"] == "可进Shadow")].head(8)
        if not top.empty:
            print(top[["module", "direction_type", "rule", "sample_count", "direction_accuracy_pct", "grade"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
