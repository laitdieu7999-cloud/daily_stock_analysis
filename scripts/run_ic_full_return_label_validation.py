#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate IC carry labels against a tradable rolled futures return series."""

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

from run_ic_basis_overlay_validation import _extract_expiry, _load_cffex_ic_panel, _load_spot_history  # noqa: E402
from run_ic_roll_carry_validation import _annualized_sharpe, _max_drawdown  # noqa: E402


DIVIDEND_MONTHS = {5, 6, 7}


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Validate IC full-return labels on tradable rolled returns.")
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


def _build_tradable_frame(start: str, end: str, data_cache_dir: str, refresh_data_cache: bool) -> pd.DataFrame:
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
    panel["expiry"] = pd.to_datetime(panel["symbol"].apply(_extract_expiry))
    panel["days_to_expiry"] = (panel["expiry"] - panel["date"]).dt.days.clip(lower=1)
    panel = panel.sort_values(["date", "open_interest", "volume", "symbol"], ascending=[True, False, False, True])
    dominant = panel.groupby("date").head(1).copy()

    close_pivot = panel.pivot_table(index="date", columns="symbol", values="close", aggfunc="last").sort_index()
    merged = spot[["date", "close"]].rename(columns={"close": "spot_close"}).merge(
        dominant[["date", "symbol", "close", "days_to_expiry"]].rename(columns={"close": "future_close"}),
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
    merged["spot_ret_1d"] = merged["spot_close"].pct_change()
    merged["spot_ret_5d_fwd"] = merged["spot_close"].shift(-5) / merged["spot_close"] - 1.0
    merged["ma_20"] = merged["spot_close"].rolling(20).mean()
    merged["trend_intact"] = (
        (merged["spot_close"] >= merged["ma_20"])
        & (merged["spot_ret_1d"].rolling(5).sum().fillna(0.0) > -0.02)
    ).astype(int)
    merged["dividend_season"] = merged["date"].dt.month.isin(DIVIDEND_MONTHS).astype(int)

    # Tradable rolled return: signal decided on t-1 close, hold that contract through t.
    exec_symbol = merged["symbol"].shift(1)
    exec_prev_close = []
    exec_next_close = []
    for idx, row in merged.iterrows():
        if idx == 0 or pd.isna(exec_symbol.iloc[idx]):
            exec_prev_close.append(pd.NA)
            exec_next_close.append(pd.NA)
            continue
        sym = exec_symbol.iloc[idx]
        prev_date = merged.loc[idx - 1, "date"]
        cur_date = row["date"]
        prev_close = close_pivot.at[prev_date, sym] if sym in close_pivot.columns and prev_date in close_pivot.index else pd.NA
        cur_close = close_pivot.at[cur_date, sym] if sym in close_pivot.columns and cur_date in close_pivot.index else pd.NA
        exec_prev_close.append(prev_close)
        exec_next_close.append(cur_close)
    merged["exec_symbol"] = exec_symbol
    merged["exec_prev_close"] = exec_prev_close
    merged["exec_cur_close"] = exec_next_close
    merged["tradable_return_1d"] = pd.to_numeric(merged["exec_cur_close"], errors="coerce") / pd.to_numeric(
        merged["exec_prev_close"], errors="coerce"
    ) - 1.0

    # Signal-day 5d contract return on the current dominant symbol.
    contract_ret_5d = []
    for idx, row in merged.iterrows():
        sym = row["symbol"]
        cur_date = row["date"]
        if idx + 5 >= len(merged):
            contract_ret_5d.append(pd.NA)
            continue
        future_date = merged.loc[idx + 5, "date"]
        cur_close = close_pivot.at[cur_date, sym] if sym in close_pivot.columns and cur_date in close_pivot.index else pd.NA
        future_close = close_pivot.at[future_date, sym] if sym in close_pivot.columns and future_date in close_pivot.index else pd.NA
        if pd.isna(cur_close) or pd.isna(future_close):
            contract_ret_5d.append(pd.NA)
        else:
            contract_ret_5d.append(float(future_close / cur_close - 1.0))
    merged["contract_ret_5d_fwd"] = contract_ret_5d
    merged["excess_ret_5d"] = merged["contract_ret_5d_fwd"] - merged["spot_ret_5d_fwd"]
    return merged


def _evaluate_window(df: pd.DataFrame, mask: pd.Series) -> dict[str, float | int]:
    sample = df[mask].copy()
    if sample.empty:
        return {
            "count": 0,
            "contract_ret_5d": 0.0,
            "spot_ret_5d": 0.0,
            "excess_ret_5d": 0.0,
            "excess_win_rate": 0.0,
        }
    excess = sample["excess_ret_5d"].dropna()
    return {
        "count": int(len(sample)),
        "contract_ret_5d": float(sample["contract_ret_5d_fwd"].mean()),
        "spot_ret_5d": float(sample["spot_ret_5d_fwd"].mean()),
        "excess_ret_5d": float(sample["excess_ret_5d"].mean()),
        "excess_win_rate": float((excess > 0).mean()) if not excess.empty else 0.0,
    }


def _evaluate_strategy(df: pd.DataFrame, label: str, signal_position: pd.Series) -> dict[str, object]:
    result = df.copy()
    result["signal_position"] = signal_position.reindex(result.index).fillna(0.0)
    result["position"] = result["signal_position"].shift(1).fillna(0.0)
    result["strategy_return"] = result["position"] * result["tradable_return_1d"].fillna(0.0)
    result["equity"] = (1.0 + result["strategy_return"]).cumprod()
    result["buy_hold_return"] = result["tradable_return_1d"].fillna(0.0)
    result["buy_hold_equity"] = (1.0 + result["buy_hold_return"]).cumprod()
    return {
        "label": label,
        "sample_count": int(len(result)),
        "strategy_total_return": float(result["equity"].iloc[-1] - 1.0),
        "buy_hold_total_return": float(result["buy_hold_equity"].iloc[-1] - 1.0),
        "strategy_max_drawdown": _max_drawdown(result["equity"]),
        "strategy_sharpe": _annualized_sharpe(result["strategy_return"]),
        "avg_position": float(result["position"].mean()),
    }


def _build_report(
    *,
    window_stats: dict[str, dict[str, float | int]],
    strategies: list[dict[str, object]],
    start: str,
    end: str,
    carry_threshold: float,
    carry_change_threshold: float,
) -> str:
    ordered = sorted(strategies, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC全收益标签验证报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 标的: CFFEX IC 主力合约连续切换序列 + 中证500现货",
        "- 标签定义: 可交易的主力滚动收益序列，而不是简单连续价差",
        f"- 高贴水阈值: 年化贴水 >= {carry_threshold:.2%}",
        f"- 陷阱阈值: 年化贴水5日变化 >= {carry_change_threshold:.2%} 且趋势破坏",
        "- 分红季定义: 5-7月",
        "",
        "## 贴水窗口统计",
        "",
        "| 窗口 | 样本数 | 合约5日收益 | 现货5日收益 | 超额收益 | 超额胜率 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for label, stats in window_stats.items():
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

    best = ordered[0]
    lines.extend(["", "## 结论", ""])
    if window_stats["高贴水+趋势完整"]["excess_ret_5d"] > window_stats["低贴水环境"]["excess_ret_5d"]:
        lines.append("- 在可交易主力滚动收益口径下，高贴水+趋势完整窗口仍优于低贴水环境，说明“吃贴水”逻辑站得住。")
    else:
        lines.append("- 在可交易主力滚动收益口径下，高贴水窗口并未优于低贴水环境，说明之前的贴水优势可能部分来自连续价差口径。")
    if window_stats["高贴水+趋势完整(非分红季)"]["excess_ret_5d"] > window_stats["高贴水+趋势完整(分红季)"]["excess_ret_5d"]:
        lines.append("- 剔除分红季后，高贴水窗口更干净，说明分红季确实会污染贴水信号。")
    else:
        lines.append("- 分红季和非分红季差异不明显，说明当前贴水信号的污染不只来自分红。")
    lines.append(f"- 当前最优策略是 `{best['label']}`，Sharpe {best['strategy_sharpe']:.2f}。")
    lines.append("- 这份报告更适合回答“IC 吃贴水这条线，标签该怎么定义”这个问题。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame = _build_tradable_frame(args.start, args.end, args.data_cache_dir, args.refresh_data_cache)
    carry_threshold = float(frame["annualized_carry"].dropna().quantile(0.80))
    carry_change_threshold = float(frame["annualized_carry_change_5d"].dropna().quantile(0.80))
    high_carry = frame["annualized_carry"] >= carry_threshold
    low_carry = frame["annualized_carry"] <= float(frame["annualized_carry"].dropna().quantile(0.20))
    trend_intact = frame["trend_intact"] == 1
    trap = (frame["annualized_carry_change_5d"] >= carry_change_threshold) & (~trend_intact)
    not_dividend = frame["dividend_season"] == 0
    in_dividend = frame["dividend_season"] == 1

    window_stats = {
        "高贴水+趋势完整": _evaluate_window(frame, high_carry & trend_intact),
        "高贴水+趋势完整(非分红季)": _evaluate_window(frame, high_carry & trend_intact & not_dividend),
        "高贴水+趋势完整(分红季)": _evaluate_window(frame, high_carry & trend_intact & in_dividend),
        "低贴水环境": _evaluate_window(frame, low_carry),
        "贴水陷阱": _evaluate_window(frame, trap),
    }

    base_signal = pd.Series(0.5, index=frame.index)
    base_signal[high_carry & trend_intact] = 1.0
    ex_div_signal = pd.Series(0.5, index=frame.index)
    ex_div_signal[high_carry & trend_intact & not_dividend] = 1.0
    trap_def_signal = base_signal.mask(trap, 0.0)

    strategies = [
        _evaluate_strategy(frame, "始终持有主力IC", pd.Series(1.0, index=frame.index)),
        _evaluate_strategy(frame, "高贴水满仓/普通半仓", base_signal),
        _evaluate_strategy(frame, "高贴水满仓/普通半仓(剔除分红季)", ex_div_signal),
        _evaluate_strategy(frame, "高贴水满仓/普通半仓/陷阱空仓", trap_def_signal),
    ]

    report = _build_report(
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
    report_path = output_dir / f"{report_date}_IC全收益标签验证报告.md"
    latest_path = output_dir / "latest_ic_full_return_label_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC全收益标签验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
