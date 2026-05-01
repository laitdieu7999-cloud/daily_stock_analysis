#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate a seasonally dividend-adjusted carry proxy for the IC carry line."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from run_ic_full_return_label_validation import (  # noqa: E402
    DIVIDEND_MONTHS,
    _build_tradable_frame,
    _evaluate_strategy,
    _evaluate_window,
)


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Validate seasonally dividend-adjusted IC carry signals.")
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


def _build_seasonally_adjusted_frame(start: str, end: str, data_cache_dir: str, refresh_data_cache: bool) -> tuple[pd.DataFrame, pd.Series]:
    frame = _build_tradable_frame(start, end, data_cache_dir, refresh_data_cache)
    month_baseline = frame.groupby(frame["date"].dt.month)["annualized_carry"].median()
    frame = frame.copy()
    frame["carry_month"] = frame["date"].dt.month
    frame["seasonal_carry_baseline"] = frame["carry_month"].map(month_baseline)
    frame["adj_annualized_carry"] = frame["annualized_carry"] - frame["seasonal_carry_baseline"]
    frame["adj_annualized_carry_change_5d"] = frame["adj_annualized_carry"].diff(5)
    return frame, month_baseline


def _build_report(
    *,
    month_baseline: pd.Series,
    raw_window_stats: dict[str, dict[str, float | int]],
    adjusted_window_stats: dict[str, dict[str, float | int]],
    strategies: list[dict[str, object]],
    start: str,
    end: str,
    raw_threshold: float,
    adjusted_threshold: float,
) -> str:
    ordered = sorted(strategies, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC分红季调整验证报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 方法: 用按月份统计的年化贴水中位数，作为分红季/季节性贴水基线，构造季节性调整后的贴水信号",
        "- 说明: 这不是精确分红点数模型，而是当前阶段的季节性分红调整代理",
        f"- 原始高贴水阈值: 年化贴水 >= {raw_threshold:.2%}",
        f"- 调整后高贴水阈值: 调整后年化贴水 >= {adjusted_threshold:.2%}",
        "",
        "## 月份贴水基线",
        "",
        "| 月份 | 年化贴水中位数 |",
        "| --- | ---: |",
    ]
    for month, value in month_baseline.items():
        mark = " (分红季)" if int(month) in DIVIDEND_MONTHS else ""
        lines.append(f"| {int(month)}{mark} | {float(value):.2%} |")

    lines.extend([
        "",
        "## 原始贴水窗口",
        "",
        "| 窗口 | 样本数 | 合约5日收益 | 现货5日收益 | 超额收益 | 超额胜率 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ])
    for label, stats in raw_window_stats.items():
        lines.append(
            f"| {label} | {stats['count']} | {stats['contract_ret_5d'] * 100:.2f}% | {stats['spot_ret_5d'] * 100:.2f}% | "
            f"{stats['excess_ret_5d'] * 100:.2f}% | {stats['excess_win_rate'] * 100:.1f}% |"
        )

    lines.extend([
        "",
        "## 调整后贴水窗口",
        "",
        "| 窗口 | 样本数 | 合约5日收益 | 现货5日收益 | 超额收益 | 超额胜率 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ])
    for label, stats in adjusted_window_stats.items():
        lines.append(
            f"| {label} | {stats['count']} | {stats['contract_ret_5d'] * 100:.2f}% | {stats['spot_ret_5d'] * 100:.2f}% | "
            f"{stats['excess_ret_5d'] * 100:.2f}% | {stats['excess_win_rate'] * 100:.1f}% |"
        )

    lines.extend([
        "",
        "## 策略比较",
        "",
        "| 方案 | 样本数 | 策略收益 | 持有主力收益 | 最大回撤 | Sharpe | 平均仓位 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for item in ordered:
        lines.append(
            f"| {item['label']} | {item['sample_count']} | {item['strategy_total_return'] * 100:.2f}% | "
            f"{item['buy_hold_total_return'] * 100:.2f}% | {item['strategy_max_drawdown'] * 100:.2f}% | "
            f"{item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} |"
        )

    raw_div = raw_window_stats["原始高贴水+趋势完整(分红季)"]["excess_ret_5d"]
    adj_div = adjusted_window_stats["调整后高贴水+趋势完整(分红季)"]["excess_ret_5d"]
    best = ordered[0]
    lines.extend(["", "## 结论", ""])
    if adj_div > raw_div:
        lines.append("- 季节性调整后，分红季高贴水窗口的超额收益更干净，说明“按月份扣减分红季基线”是有帮助的。")
    else:
        lines.append("- 季节性调整后，分红季高贴水窗口并没有明显更优，说明仅靠月份中位数扣减还不够精确。")
    lines.append(f"- 当前最优策略是 `{best['label']}`，Sharpe {best['strategy_sharpe']:.2f}。")
    lines.append("- 这份报告更适合回答“分红季高贴水要不要做显式调整，以及调整后是否更贴近真实可交易信号”。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame, month_baseline = _build_seasonally_adjusted_frame(
        args.start,
        args.end,
        args.data_cache_dir,
        args.refresh_data_cache,
    )

    raw_threshold = float(frame["annualized_carry"].dropna().quantile(0.80))
    adjusted_threshold = float(frame["adj_annualized_carry"].dropna().quantile(0.80))

    raw_high = frame["annualized_carry"] >= raw_threshold
    adjusted_high = frame["adj_annualized_carry"] >= adjusted_threshold
    trend_intact = frame["trend_intact"] == 1
    in_dividend = frame["dividend_season"] == 1
    not_dividend = frame["dividend_season"] == 0

    raw_window_stats = {
        "原始高贴水+趋势完整": _evaluate_window(frame, raw_high & trend_intact),
        "原始高贴水+趋势完整(分红季)": _evaluate_window(frame, raw_high & trend_intact & in_dividend),
        "原始高贴水+趋势完整(非分红季)": _evaluate_window(frame, raw_high & trend_intact & not_dividend),
    }
    adjusted_window_stats = {
        "调整后高贴水+趋势完整": _evaluate_window(frame, adjusted_high & trend_intact),
        "调整后高贴水+趋势完整(分红季)": _evaluate_window(frame, adjusted_high & trend_intact & in_dividend),
        "调整后高贴水+趋势完整(非分红季)": _evaluate_window(frame, adjusted_high & trend_intact & not_dividend),
    }

    raw_signal = pd.Series(0.5, index=frame.index)
    raw_signal.loc[raw_high & trend_intact] = 1.0
    adjusted_signal = pd.Series(0.5, index=frame.index)
    adjusted_signal.loc[adjusted_high & trend_intact] = 1.0
    adjusted_non_div_signal = pd.Series(0.5, index=frame.index)
    adjusted_non_div_signal.loc[adjusted_high & trend_intact & not_dividend] = 1.0

    strategies = [
        _evaluate_strategy(frame, "始终持有主力IC", pd.Series(1.0, index=frame.index)),
        _evaluate_strategy(frame, "原始高贴水满仓/普通半仓", raw_signal),
        _evaluate_strategy(frame, "调整后高贴水满仓/普通半仓", adjusted_signal),
        _evaluate_strategy(frame, "调整后高贴水满仓/普通半仓(非分红季)", adjusted_non_div_signal),
    ]

    report = _build_report(
        month_baseline=month_baseline,
        raw_window_stats=raw_window_stats,
        adjusted_window_stats=adjusted_window_stats,
        strategies=strategies,
        start=args.start,
        end=args.end,
        raw_threshold=raw_threshold,
        adjusted_threshold=adjusted_threshold,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC分红季调整验证报告.md"
    latest_path = output_dir / "latest_ic_dividend_season_adjustment_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC分红季调整验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
