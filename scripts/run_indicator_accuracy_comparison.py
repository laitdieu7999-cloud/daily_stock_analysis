#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compare single-indicator directional accuracy on local stock_daily data.

This script is intentionally read-only for the database. It generates markdown
reports under reports/backtests so indicator claims can be checked against the
user's actual local stock pool instead of relying on generic online claims.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stock_analyzer import StockTrendAnalyzer  # noqa: E402


Signal = Optional[str]  # "bullish", "bearish", or None


@dataclass(frozen=True)
class IndicatorRule:
    name: str
    description: str
    signal_fn: Callable[[pd.Series], Signal]


def _finite(value: object) -> bool:
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def _num(row: pd.Series, key: str, default: float = np.nan) -> float:
    value = row.get(key, default)
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if np.isfinite(parsed) else default


def _signal_ma(row: pd.Series) -> Signal:
    close = _num(row, "close")
    ma5 = _num(row, "MA5")
    ma10 = _num(row, "MA10")
    ma20 = _num(row, "MA20")
    if not all(_finite(v) and v > 0 for v in (close, ma5, ma10, ma20)):
        return None
    if close > ma5 > ma10 > ma20:
        return "bullish"
    if close < ma5 < ma10 < ma20:
        return "bearish"
    return None


def _signal_macd(row: pd.Series) -> Signal:
    dif = _num(row, "MACD_DIF")
    dea = _num(row, "MACD_DEA")
    bar = _num(row, "MACD_BAR")
    if dif > dea and bar > 0:
        return "bullish"
    if dif < dea and bar < 0:
        return "bearish"
    return None


def _signal_rsi(row: pd.Series) -> Signal:
    rsi = _num(row, "RSI_12", 50)
    if rsi <= 30:
        return "bullish"
    if rsi >= 70:
        return "bearish"
    return None


def _signal_boll(row: pd.Series) -> Signal:
    close = _num(row, "close")
    upper = _num(row, "BOLL_UPPER")
    lower = _num(row, "BOLL_LOWER")
    if not all(_finite(v) and v > 0 for v in (close, upper, lower)):
        return None
    if close <= lower:
        return "bullish"
    if close >= upper:
        return "bearish"
    return None


def _signal_kdj(row: pd.Series) -> Signal:
    k = _num(row, "KDJ_K", 50)
    d = _num(row, "KDJ_D", 50)
    if k > d and k < 80:
        return "bullish"
    if k < d and k > 20:
        return "bearish"
    return None


def _signal_adx_dmi(row: pd.Series) -> Signal:
    adx = _num(row, "ADX")
    plus_di = _num(row, "PLUS_DI")
    minus_di = _num(row, "MINUS_DI")
    if adx < 25:
        return None
    if plus_di > minus_di:
        return "bullish"
    if minus_di > plus_di:
        return "bearish"
    return None


def _signal_mfi(row: pd.Series) -> Signal:
    mfi = _num(row, "MFI", 50)
    if mfi <= 20:
        return "bullish"
    if mfi >= 80:
        return "bearish"
    return None


def _signal_cci(row: pd.Series) -> Signal:
    cci = _num(row, "CCI")
    if cci <= -100:
        return "bullish"
    if cci >= 100:
        return "bearish"
    return None


def _signal_roc(row: pd.Series) -> Signal:
    roc = _num(row, "ROC")
    if roc >= 3:
        return "bullish"
    if roc <= -3:
        return "bearish"
    return None


def _signal_donchian(row: pd.Series) -> Signal:
    close = _num(row, "close")
    upper_prev = _num(row, "DONCHIAN_UPPER_PREV")
    lower_prev = _num(row, "DONCHIAN_LOWER_PREV")
    if upper_prev > 0 and close >= upper_prev:
        return "bullish"
    if lower_prev > 0 and close <= lower_prev:
        return "bearish"
    return None


def _signal_williams(row: pd.Series) -> Signal:
    wr = _num(row, "WILLIAMS_R", -50)
    if wr <= -80:
        return "bullish"
    if wr >= -20:
        return "bearish"
    return None


def _signal_stoch_rsi(row: pd.Series) -> Signal:
    stoch_rsi = _num(row, "STOCH_RSI", 50)
    if stoch_rsi <= 20:
        return "bullish"
    if stoch_rsi >= 80:
        return "bearish"
    return None


def _signal_cmf(row: pd.Series) -> Signal:
    cmf = _num(row, "CMF")
    if cmf >= 0.05:
        return "bullish"
    if cmf <= -0.05:
        return "bearish"
    return None


def _signal_obv(row: pd.Series) -> Signal:
    obv = _num(row, "OBV")
    obv_ma = _num(row, "OBV_MA")
    obv_change = _num(row, "OBV_CHANGE_5")
    if obv > obv_ma and obv_change > 0:
        return "bullish"
    if obv < obv_ma and obv_change < 0:
        return "bearish"
    return None


def _signal_rsrs(row: pd.Series) -> Signal:
    score = _num(row, "RSRS_R2_WEIGHTED")
    if score >= 0.7:
        return "bullish"
    if score <= -0.7:
        return "bearish"
    return None


def _signal_vwap(row: pd.Series) -> Signal:
    distance = _num(row, "VWAP_DISTANCE_PCT")
    if 0 < distance <= 5:
        return "bullish"
    if distance < 0:
        return "bearish"
    return None


def _signal_relative_strength(row: pd.Series) -> Signal:
    strength = _num(row, "REL_STRENGTH_20")
    if strength >= 2:
        return "bullish"
    if strength <= -2:
        return "bearish"
    return None


INDICATOR_RULES = [
    IndicatorRule("MA多空排列", "收盘价与MA5/10/20顺向排列", _signal_ma),
    IndicatorRule("MACD趋势确认", "DIF/DEA与柱状图同向", _signal_macd),
    IndicatorRule("RSI反转", "RSI<=30看多，RSI>=70看空", _signal_rsi),
    IndicatorRule("BOLL均值回归", "跌破下轨看多，突破上轨看空", _signal_boll),
    IndicatorRule("KDJ方向", "K/D相对位置过滤超买超卖", _signal_kdj),
    IndicatorRule("ADX/DMI趋势强度", "ADX>=25时按+DI/-DI判断方向", _signal_adx_dmi),
    IndicatorRule("MFI资金强弱", "MFI<=20看多，MFI>=80看空", _signal_mfi),
    IndicatorRule("CCI偏离", "CCI<=-100看多，CCI>=100看空", _signal_cci),
    IndicatorRule("ROC动量", "12日ROC超过正负3%触发", _signal_roc),
    IndicatorRule("唐奇安突破", "突破前20日通道上下轨", _signal_donchian),
    IndicatorRule("Williams %R", "%R<=-80看多，%R>=-20看空", _signal_williams),
    IndicatorRule("StochRSI", "StochRSI<=20看多，>=80看空", _signal_stoch_rsi),
    IndicatorRule("CMF资金流", "20日CMF超过正负0.05", _signal_cmf),
    IndicatorRule("OBV量价", "OBV相对均线与5日变化同向", _signal_obv),
    IndicatorRule("RSRS择时", "R²加权分超过正负0.7", _signal_rsrs),
    IndicatorRule("VWAP成本线", "价格相对20日VWAP位置", _signal_vwap),
    IndicatorRule("相对强弱20日", "20日收益相对基准超过正负2%", _signal_relative_strength),
]


def _resolve_db_path(raw: Optional[str]) -> Path:
    value = raw or os.environ.get("DATABASE_PATH") or "./data/stock_analysis.db"
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _load_stock_daily(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        raise FileNotFoundError(f"database not found: {db_path}")
    with sqlite3.connect(str(db_path)) as conn:
        df = pd.read_sql_query(
            """
            SELECT code, date, open, high, low, close, volume, amount
            FROM stock_daily
            WHERE close IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
            ORDER BY code, date
            """,
            conn,
            parse_dates=["date"],
        )
    if df.empty:
        raise ValueError("stock_daily is empty; run data fetch first")
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["code", "date", "high", "low", "close", "volume"])


def _build_indicator_frame(df: pd.DataFrame, benchmark_code: str) -> pd.DataFrame:
    analyzer = StockTrendAnalyzer()
    frames: list[pd.DataFrame] = []
    for code, group in df.groupby("code"):
        if len(group) < 30:
            continue
        enriched = analyzer.prepare_indicator_frame(group.copy())
        enriched["RETURN_20"] = enriched["close"].pct_change(20) * 100
        enriched["OBV_CHANGE_5"] = enriched["OBV"].diff(5)
        frames.append(enriched)

    if not frames:
        raise ValueError("not enough rows to compute indicators")

    all_rows = pd.concat(frames, ignore_index=True)
    benchmark_rows = all_rows[all_rows["code"] == benchmark_code][["date", "RETURN_20"]].rename(
        columns={"RETURN_20": "BENCH_RETURN_20"}
    )
    if benchmark_rows.empty:
        all_rows["REL_STRENGTH_20"] = np.nan
        return all_rows

    all_rows = all_rows.merge(benchmark_rows, on="date", how="left")
    all_rows["REL_STRENGTH_20"] = all_rows["RETURN_20"] - all_rows["BENCH_RETURN_20"]
    return all_rows


def _iter_signal_records(indicator_df: pd.DataFrame, windows: Iterable[int]) -> list[dict]:
    records: list[dict] = []
    max_window = max(windows)
    for code, group in indicator_df.groupby("code"):
        group = group.sort_values("date").reset_index(drop=True)
        if len(group) <= max_window:
            continue
        for idx in range(0, len(group) - max_window):
            row = group.iloc[idx]
            close = _num(row, "close")
            if close <= 0:
                continue
            for rule in INDICATOR_RULES:
                signal = rule.signal_fn(row)
                if signal is None:
                    continue
                for window in windows:
                    future_close = _num(group.iloc[idx + window], "close")
                    if future_close <= 0:
                        continue
                    future_return = (future_close - close) / close * 100
                    correct = future_return > 0 if signal == "bullish" else future_return < 0
                    aligned_return = future_return if signal == "bullish" else -future_return
                    records.append(
                        {
                            "code": code,
                            "date": row["date"].date(),
                            "indicator": rule.name,
                            "description": rule.description,
                            "signal": signal,
                            "window": window,
                            "future_return_pct": future_return,
                            "direction_correct": correct,
                            "signal_aligned_return_pct": aligned_return,
                        }
                    )
    return records


def _summarize(records: list[dict], min_samples: int) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    signals = pd.DataFrame(records)
    grouped = signals.groupby(["window", "indicator", "description"], as_index=False)
    summary = grouped.agg(
        sample_count=("direction_correct", "size"),
        bullish_count=("signal", lambda values: int((values == "bullish").sum())),
        bearish_count=("signal", lambda values: int((values == "bearish").sum())),
        direction_accuracy_pct=("direction_correct", lambda values: float(values.mean() * 100)),
        avg_signal_return_pct=("signal_aligned_return_pct", "mean"),
        bullish_win_rate_pct=(
            "future_return_pct",
            lambda values: float((values > 0).mean() * 100),
        ),
        bullish_avg_return_pct=("future_return_pct", "mean"),
    )
    summary["sample_quality"] = np.where(summary["sample_count"] >= min_samples, "可参考", "样本少")
    return summary.sort_values(
        ["window", "sample_quality", "direction_accuracy_pct", "avg_signal_return_pct"],
        ascending=[True, True, False, False],
    )


def _fmt_pct(value: object) -> str:
    if not _finite(value):
        return "--"
    return f"{float(value):.2f}%"


def _write_report(
    *,
    summary: pd.DataFrame,
    output_dir: Path,
    db_path: Path,
    benchmark_code: str,
    windows: list[int],
    min_samples: int,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    dated_path = output_dir / f"{today}_indicator_accuracy_comparison.md"
    latest_path = output_dir / "latest_indicator_accuracy_comparison.md"

    lines = [
        "# 单指标准确率对比",
        "",
        f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 数据库: `{db_path}`",
        f"- 基准代码: `{benchmark_code}`",
        f"- 评估窗口: {', '.join('T+' + str(w) for w in windows)}",
        f"- 最小参考样本: {min_samples}",
        "",
        "> 说明：这是单指标方向验证，不等同于完整交易系统收益。准确率只表示信号方向与未来窗口涨跌方向是否一致；真正可用性还要结合平均收益、回撤、交易成本和样本量。",
        "",
    ]

    if summary.empty:
        lines.extend(["暂无可用信号。", ""])
    else:
        for window in windows:
            window_df = summary[summary["window"] == window].copy()
            if window_df.empty:
                continue
            lines.extend(
                [
                    f"## T+{window} 对比",
                    "",
                    "| 指标 | 样本 | 看多 | 看空 | 方向准确率 | 信号方向平均收益 | 原始平均收益 | 样本质量 | 规则 |",
                    "|---|---:|---:|---:|---:|---:|---:|---|---|",
                ]
            )
            for _, row in window_df.iterrows():
                lines.append(
                    "| "
                    f"{row['indicator']} | "
                    f"{int(row['sample_count'])} | "
                    f"{int(row['bullish_count'])} | "
                    f"{int(row['bearish_count'])} | "
                    f"{_fmt_pct(row['direction_accuracy_pct'])} | "
                    f"{_fmt_pct(row['avg_signal_return_pct'])} | "
                    f"{_fmt_pct(row['bullish_avg_return_pct'])} | "
                    f"{row['sample_quality']} | "
                    f"{row['description']} |"
                )
            lines.append("")

    content = "\n".join(lines)
    dated_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return dated_path, latest_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None, help="SQLite database path. Defaults to DATABASE_PATH or ./data/stock_analysis.db")
    parser.add_argument("--benchmark-code", default="510300", help="Benchmark code for relative strength")
    parser.add_argument("--windows", nargs="+", type=int, default=[1, 3, 5], help="Forward windows in trading days")
    parser.add_argument("--min-samples", type=int, default=10, help="Minimum sample count for a row to be marked as referenceable")
    parser.add_argument("--output-dir", default="reports/backtests", help="Report output directory")
    args = parser.parse_args()

    windows = sorted({int(w) for w in args.windows if int(w) > 0})
    if not windows:
        raise ValueError("--windows must contain at least one positive integer")

    db_path = _resolve_db_path(args.db)
    daily = _load_stock_daily(db_path)
    indicator_df = _build_indicator_frame(daily, args.benchmark_code)
    records = _iter_signal_records(indicator_df, windows)
    summary = _summarize(records, args.min_samples)
    dated_path, latest_path = _write_report(
        summary=summary,
        output_dir=(PROJECT_ROOT / args.output_dir).resolve(),
        db_path=db_path,
        benchmark_code=args.benchmark_code,
        windows=windows,
        min_samples=args.min_samples,
    )

    print(f"generated: {dated_path}")
    print(f"latest: {latest_path}")
    if not summary.empty:
        printable = summary[summary["window"] == windows[0]].head(8)
        print(printable[["indicator", "sample_count", "direction_accuracy_pct", "avg_signal_return_pct", "sample_quality"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
