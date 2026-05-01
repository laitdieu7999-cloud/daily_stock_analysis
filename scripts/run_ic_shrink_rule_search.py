#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Search action-oriented shrink rules for the IC long line."""

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

from run_ic_roll_carry_validation import (  # noqa: E402
    _annualized_sharpe,
    _build_dominant_ic_frame,
    _max_drawdown,
)


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Search better shrink rules for the IC long workflow.")
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


def _evaluate_strategy(df: pd.DataFrame, label: str, signal_position: pd.Series) -> dict[str, object]:
    result = df.copy()
    result["signal_position"] = signal_position.reindex(result.index).fillna(0.0)
    result["position"] = result["signal_position"].shift(1).fillna(0.0)
    result["strategy_return"] = result["position"] * result["future_ret_1d"].fillna(0.0)
    result["equity"] = (1.0 + result["strategy_return"]).cumprod()
    result["buy_hold_return"] = result["future_ret_1d"].fillna(0.0)
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


def _build_candidate_rules(frame: pd.DataFrame) -> dict[str, pd.Series]:
    frame = frame.copy()
    frame["spot_ret_5d"] = frame["spot_close"].pct_change(5)
    frame["spot_vol_5d"] = frame["spot_ret_1d"].rolling(5).std()

    carry_q80 = frame["annualized_carry"].dropna().quantile(0.80)
    carrychg_q80 = frame["annualized_carry_change_5d"].dropna().quantile(0.80)
    dischg_q80 = frame["discount_change_5d"].dropna().quantile(0.80)
    spot5_q20 = frame["spot_ret_5d"].dropna().quantile(0.20)
    spot1_q20 = frame["spot_ret_1d"].dropna().quantile(0.20)
    vol_q80 = frame["spot_vol_5d"].dropna().quantile(0.80)

    trend_broken = frame["trend_intact"] == 0
    high_carry = frame["annualized_carry"] >= carry_q80

    return {
        "趋势破坏": trend_broken,
        "趋势破坏+单日弱势": trend_broken & (frame["spot_ret_1d"] <= spot1_q20),
        "趋势破坏+5日弱动量": trend_broken & (frame["spot_ret_5d"] <= spot5_q20),
        "趋势破坏+高波动": trend_broken & (frame["spot_vol_5d"] >= vol_q80),
        "趋势破坏+贴水走阔": trend_broken & (frame["annualized_carry_change_5d"] >= carrychg_q80),
        "趋势破坏+贴水率走阔": trend_broken & (frame["discount_change_5d"] >= dischg_q80),
        "趋势破坏+高贴水+走阔": trend_broken & high_carry & (frame["annualized_carry_change_5d"] >= carrychg_q80),
        "趋势破坏+走阔+弱动量": trend_broken & (frame["annualized_carry_change_5d"] >= carrychg_q80) & (frame["spot_ret_5d"] <= spot5_q20),
        "趋势破坏+高波动+弱动量": trend_broken & (frame["spot_vol_5d"] >= vol_q80) & (frame["spot_ret_5d"] <= spot5_q20),
        "临近移仓+趋势破坏": (frame["near_roll_window"] == 1) & trend_broken,
    }


def _build_report(
    *,
    baseline: dict[str, object],
    rows: list[dict[str, object]],
    start: str,
    end: str,
) -> str:
    ranked = sorted(rows, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC多头收缩规则搜索报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 基线策略: 高贴水满仓 / 普通半仓",
        "- 执行方式: 当日信号, 次日仓位执行",
        "- 目标: 找出什么情况下更值得把 IC 多头收缩掉",
        "",
        "## 基线",
        "",
        f"- 基线收益: {baseline['strategy_total_return'] * 100:.2f}%",
        f"- 基线Sharpe: {baseline['strategy_sharpe']:.2f}",
        f"- 基线最大回撤: {baseline['strategy_max_drawdown'] * 100:.2f}%",
        "",
        "## 候选规则排名",
        "",
        "| 规则 | 动作 | 触发天数 | 触发后5日均值 | 策略收益 | 最大回撤 | Sharpe | 平均仓位 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in ranked[:12]:
        lines.append(
            f"| {item['rule']} | {item['action']} | {item['count']} | {item['mean_5d_future'] * 100:.2f}% | "
            f"{item['strategy_total_return'] * 100:.2f}% | {item['strategy_max_drawdown'] * 100:.2f}% | "
            f"{item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} |"
        )

    best = ranked[0]
    lines.extend(["", "## 结论", ""])
    if float(best["strategy_sharpe"]) > float(baseline["strategy_sharpe"]):
        lines.append(f"- 当前最优收缩规则是 `{best['rule']} / {best['action']}`，Sharpe {best['strategy_sharpe']:.2f}，优于基线 {baseline['strategy_sharpe']:.2f}。")
    else:
        lines.append("- 当前候选收缩规则都没有明显跑赢基线，说明多头收缩还需要更强的事件或波动标签。")
    lines.append("- 这份结果更适合回答“何时该缩 IC 多头”，而不是“何时该完全转空”。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame = _build_dominant_ic_frame(args.start, args.end, args.data_cache_dir, args.refresh_data_cache)
    frame["spot_ret_5d"] = frame["spot_close"].pct_change(5)
    high_carry = frame["annualized_carry"] >= float(frame["annualized_carry"].dropna().quantile(0.80))
    base_signal = pd.Series(0.5, index=frame.index)
    base_signal[high_carry & (frame["trend_intact"] == 1)] = 1.0

    baseline = _evaluate_strategy(frame, "基线策略", base_signal)
    rows: list[dict[str, object]] = []
    for rule_name, mask in _build_candidate_rules(frame).items():
        mask = mask.fillna(False)
        for action in ("降一档", "清到0"):
            signal = base_signal.copy()
            if action == "降一档":
                signal.loc[mask] = signal.loc[mask].map(lambda x: 0.5 if x >= 1.0 else 0.0)
            else:
                signal.loc[mask] = 0.0
            metrics = _evaluate_strategy(frame, f"{rule_name}-{action}", signal)
            metrics["rule"] = rule_name
            metrics["action"] = action
            metrics["count"] = int(mask.sum())
            metrics["mean_5d_future"] = float(frame.loc[mask, "future_ret_5d_fwd"].mean()) if int(mask.sum()) else 0.0
            rows.append(metrics)

    report = _build_report(baseline=baseline, rows=rows, start=args.start, end=args.end)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC多头收缩规则搜索报告.md"
    latest_path = output_dir / "latest_ic_shrink_rule_search.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC多头收缩规则搜索完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
