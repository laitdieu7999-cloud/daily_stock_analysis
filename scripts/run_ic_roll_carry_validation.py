#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate IC roll/carry labels on the dominant futures series.

This script is more trading-oriented than the CSI500 ETF proxy line.
It uses:
- CSI500 spot history
- CFFEX IC contract panel history
- daily dominant IC contract (highest open interest)

Then it evaluates whether high annualized carry / discount regimes actually
help a "long IC to eat discount" workflow.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from run_ic_basis_overlay_validation import (  # noqa: E402
    _annualized_sharpe,
    _extract_expiry,
    _load_cffex_ic_panel,
    _load_spot_history,
    _max_drawdown,
)


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Validate IC roll/carry labels on dominant futures history.")
    parser.add_argument("--start", default=default_start)
    parser.add_argument("--end", default=default_end)
    parser.add_argument(
        "--data-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "ic_basis_history"),
        help="Directory used to cache CSI500 spot and CFFEX IC panel history.",
    )
    parser.add_argument(
        "--refresh-data-cache",
        action="store_true",
        help="Ignore local IC history cache and rebuild it from data sources.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "reports" / "backtests"),
        help="Directory where the markdown report will be written.",
    )
    return parser


def _build_dominant_ic_frame(start: str, end: str, data_cache_dir: str, refresh_data_cache: bool) -> pd.DataFrame:
    cache_dir = Path(data_cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    spot = _load_spot_history(cache_dir, refresh_cache=refresh_data_cache)
    if spot is None or spot.empty:
        raise RuntimeError("未获取到中证500现货历史")
    spot = spot.copy()
    spot["date"] = pd.to_datetime(spot["date"])
    spot = spot[(spot["date"] >= pd.Timestamp(start)) & (spot["date"] <= pd.Timestamp(end))].copy()

    start_year = int(start[:4])
    end_year = int(end[:4])
    futures_frames = _load_cffex_ic_panel(cache_dir, start_year, end_year, refresh_cache=refresh_data_cache)
    if not futures_frames:
        raise RuntimeError("未获取到 CFFEX IC 合约历史")

    panel = pd.concat(futures_frames, ignore_index=True)
    panel = panel[panel["variety"] == "IC"].copy()
    panel["date"] = pd.to_datetime(panel["date"])
    panel = panel[(panel["date"] >= pd.Timestamp(start)) & (panel["date"] <= pd.Timestamp(end))].copy()
    panel = panel.sort_values(["date", "open_interest", "volume", "symbol"], ascending=[True, False, False, True])
    dominant = panel.groupby("date").head(1).copy()
    dominant["expiry"] = dominant["symbol"].apply(_extract_expiry)
    dominant["days_to_expiry"] = (dominant["expiry"] - dominant["date"]).dt.days.clip(lower=1)

    merged = spot[["date", "close"]].rename(columns={"close": "spot_close"}).merge(
        dominant[["date", "symbol", "close", "open_interest", "volume", "days_to_expiry"]].rename(
            columns={"close": "future_close"}
        ),
        on="date",
        how="inner",
    )
    if merged.empty:
        raise RuntimeError("现货和主力 IC 合约未对齐出有效样本")

    merged = merged.sort_values("date").reset_index(drop=True)
    merged["discount_ratio"] = (merged["spot_close"] - merged["future_close"]) / merged["future_close"]
    merged["annualized_carry"] = merged["discount_ratio"] / (merged["days_to_expiry"] / 365.0)
    merged["discount_change_5d"] = merged["discount_ratio"].diff(5)
    merged["annualized_carry_change_5d"] = merged["annualized_carry"].diff(5)
    merged["future_ret_1d"] = merged["future_close"].pct_change()
    merged["spot_ret_1d"] = merged["spot_close"].pct_change()
    merged["future_ret_5d_fwd"] = merged["future_close"].shift(-5) / merged["future_close"] - 1.0
    merged["spot_ret_5d_fwd"] = merged["spot_close"].shift(-5) / merged["spot_close"] - 1.0
    merged["excess_ret_5d"] = merged["future_ret_5d_fwd"] - merged["spot_ret_5d_fwd"]
    merged["roll_switch"] = (merged["symbol"] != merged["symbol"].shift(1)).astype(int)
    merged["ma_20"] = merged["spot_close"].rolling(20).mean()
    merged["trend_intact"] = (
        (merged["spot_close"] >= merged["ma_20"])
        & (merged["spot_ret_1d"].rolling(5).sum().fillna(0.0) > -0.02)
    ).astype(int)
    merged["near_roll_window"] = (merged["days_to_expiry"] <= 7).astype(int)
    return merged


def _evaluate_window(df: pd.DataFrame, mask: pd.Series) -> dict[str, float | int]:
    sample = df[mask].copy()
    if sample.empty:
        return {
            "count": 0,
            "future_ret_5d": 0.0,
            "spot_ret_5d": 0.0,
            "excess_ret_5d": 0.0,
            "excess_win_rate": 0.0,
        }
    excess = sample["excess_ret_5d"].dropna()
    return {
        "count": int(len(sample)),
        "future_ret_5d": float(sample["future_ret_5d_fwd"].mean()),
        "spot_ret_5d": float(sample["spot_ret_5d_fwd"].mean()),
        "excess_ret_5d": float(sample["excess_ret_5d"].mean()),
        "excess_win_rate": float((excess > 0).mean()) if not excess.empty else 0.0,
    }


def _evaluate_strategy(df: pd.DataFrame, label: str, position: pd.Series) -> dict[str, object]:
    result = df.copy()
    result["signal_position"] = position.reindex(result.index).fillna(0.0)
    # Use next-session execution to avoid same-day lookahead.
    result["position"] = result["signal_position"].shift(1).fillna(0.0)
    result["strategy_return"] = result["position"] * result["future_ret_1d"].fillna(0.0)
    result["strategy_equity"] = (1.0 + result["strategy_return"]).cumprod()
    result["buy_hold_return"] = result["future_ret_1d"].fillna(0.0)
    result["buy_hold_equity"] = (1.0 + result["buy_hold_return"]).cumprod()
    return {
        "label": label,
        "sample_count": int(len(result)),
        "strategy_total_return": float(result["strategy_equity"].iloc[-1] - 1.0),
        "buy_hold_total_return": float(result["buy_hold_equity"].iloc[-1] - 1.0),
        "strategy_max_drawdown": _max_drawdown(result["strategy_equity"]),
        "strategy_sharpe": _annualized_sharpe(result["strategy_return"]),
        "avg_position": float(result["position"].mean()),
    }


def _build_report(
    *,
    frame: pd.DataFrame,
    window_stats: dict[str, dict[str, float | int]],
    strategies: list[dict[str, object]],
    start: str,
    end: str,
    carry_threshold: float,
    carry_change_threshold: float,
) -> str:
    ordered_strategies = sorted(strategies, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC移仓贴水收益验证报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 标的: CFFEX IC 主力合约连续切换序列 + 中证500现货",
        "- 目标: 验证高贴水/高年化贴水是否真的对“做多IC吃贴水”有持续帮助",
        f"- 高贴水阈值: 年化贴水 >= {carry_threshold:.2%}",
        f"- 陷阱阈值: 年化贴水5日变化 >= {carry_change_threshold:.2%} 且趋势破坏",
        "",
        "## 贴水收益窗口统计",
        "",
        "| 窗口 | 样本数 | 期货5日收益 | 现货5日收益 | 超额收益 | 超额胜率 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for label, stats in window_stats.items():
        lines.append(
            f"| {label} | {stats['count']} | {stats['future_ret_5d'] * 100:.2f}% | {stats['spot_ret_5d'] * 100:.2f}% | "
            f"{stats['excess_ret_5d'] * 100:.2f}% | {stats['excess_win_rate'] * 100:.1f}% |"
        )

    lines.extend([
        "",
        "## 连续主力策略比较",
        "",
        "| 方案 | 样本数 | 策略收益 | 持有主力收益 | 最大回撤 | Sharpe | 平均仓位 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for item in ordered_strategies:
        lines.append(
            f"| {item['label']} | {item['sample_count']} | {item['strategy_total_return'] * 100:.2f}% | "
            f"{item['buy_hold_total_return'] * 100:.2f}% | {item['strategy_max_drawdown'] * 100:.2f}% | "
            f"{item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} |"
        )

    best = ordered_strategies[0]
    carry_support = window_stats["高贴水+趋势完整"]
    low_carry = window_stats["低贴水环境"]
    trap = window_stats["贴水陷阱"]
    lines.extend(["", "## 结论", ""])
    if carry_support["excess_ret_5d"] > low_carry["excess_ret_5d"]:
        lines.append("- 高贴水+趋势完整窗口的 5 日超额收益，高于低贴水环境，说明“吃贴水”这条线本身有统计价值。")
    else:
        lines.append("- 高贴水窗口并没有稳定跑赢低贴水环境，说明单靠贴水高低还不够构成交易优势。")
    if trap["excess_ret_5d"] < 0:
        lines.append("- 贴水陷阱窗口的超额收益为负，说明“贴水快速走阔 + 趋势破坏”更像应避开的基差陷阱。")
    else:
        lines.append("- 贴水陷阱窗口没有显著转负，说明当前陷阱定义还不够锋利。")
    lines.append(f"- 当前最优连续主力方案是 `{best['label']}`，Sharpe {best['strategy_sharpe']:.2f}。")
    lines.append("- 这份报告比中证500ETF代理更贴近你的真实 IC 持有/移仓动作，后面更适合在这条线上继续优化。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame = _build_dominant_ic_frame(args.start, args.end, args.data_cache_dir, args.refresh_data_cache)
    carry_threshold = float(frame["annualized_carry"].dropna().quantile(0.80))
    carry_change_threshold = float(frame["annualized_carry_change_5d"].dropna().quantile(0.80))

    high_carry = frame["annualized_carry"] >= carry_threshold
    low_carry = frame["annualized_carry"] <= float(frame["annualized_carry"].dropna().quantile(0.20))
    trend_intact = frame["trend_intact"] == 1
    trap = (frame["annualized_carry_change_5d"] >= carry_change_threshold) & (~trend_intact)

    window_stats = {
        "高贴水+趋势完整": _evaluate_window(frame, high_carry & trend_intact),
        "低贴水环境": _evaluate_window(frame, low_carry),
        "贴水陷阱": _evaluate_window(frame, trap),
        "临近移仓高贴水": _evaluate_window(frame, high_carry & (frame["near_roll_window"] == 1)),
    }

    strategies = [
        _evaluate_strategy(frame, "始终持有主力IC", pd.Series(1.0, index=frame.index)),
        _evaluate_strategy(frame, "仅高贴水+趋势完整持有", (high_carry & trend_intact).astype(float)),
        _evaluate_strategy(frame, "默认持有，陷阱窗口空仓", pd.Series(1.0, index=frame.index).mask(trap, 0.0)),
        _evaluate_strategy(
            frame,
            "高贴水满仓/普通半仓/陷阱空仓",
            pd.Series(0.5, index=frame.index).mask(high_carry & trend_intact, 1.0).mask(trap, 0.0),
        ),
    ]

    report = _build_report(
        frame=frame,
        window_stats=window_stats,
        strategies=strategies,
        start=args.start,
        end=args.end,
        carry_threshold=carry_threshold,
        carry_change_threshold=carry_change_threshold,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC移仓贴水收益验证报告.md"
    latest_path = output_dir / "latest_ic_roll_carry_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC移仓贴水收益验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
