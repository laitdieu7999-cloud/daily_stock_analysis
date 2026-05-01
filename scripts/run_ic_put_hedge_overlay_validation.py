#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate synthetic put-hedge overlays for the IC carry workflow."""

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

from run_ic_full_return_label_validation import _build_tradable_frame  # noqa: E402
from run_ic_roll_carry_validation import _annualized_sharpe, _max_drawdown  # noqa: E402


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Validate synthetic put overlays for IC long-shrink decisions.")
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


def _evaluate_base_strategy(df: pd.DataFrame, label: str, signal_position: pd.Series) -> dict[str, object]:
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


def _evaluate_put_overlay(
    df: pd.DataFrame,
    *,
    label: str,
    signal_position: pd.Series,
    hedge_signal: pd.Series,
    hedge_ratio: float,
    premium_pct: float,
    strike_buffer_pct: float,
) -> dict[str, object]:
    result = df.copy()
    result["signal_position"] = signal_position.reindex(result.index).fillna(0.0)
    result["position"] = result["signal_position"].shift(1).fillna(0.0)
    result["hedge_signal"] = hedge_signal.reindex(result.index).fillna(False)
    result["hedge_active"] = result["hedge_signal"].shift(1).fillna(False)

    underlying_ret = result["spot_ret_1d"].fillna(0.0)
    put_payoff_unit = (-underlying_ret - strike_buffer_pct).clip(lower=0.0) - premium_pct
    result["hedge_return"] = (
        result["hedge_active"].astype(float)
        * result["position"]
        * hedge_ratio
        * put_payoff_unit
    )
    result["strategy_return"] = result["position"] * result["tradable_return_1d"].fillna(0.0) + result["hedge_return"]
    result["equity"] = (1.0 + result["strategy_return"]).cumprod()
    result["buy_hold_return"] = result["tradable_return_1d"].fillna(0.0)
    result["buy_hold_equity"] = (1.0 + result["buy_hold_return"]).cumprod()

    hedged_days = int(result["hedge_active"].sum())
    avg_hedge_return = float(result.loc[result["hedge_active"], "hedge_return"].mean()) if hedged_days else 0.0
    return {
        "label": label,
        "sample_count": int(len(result)),
        "strategy_total_return": float(result["equity"].iloc[-1] - 1.0),
        "buy_hold_total_return": float(result["buy_hold_equity"].iloc[-1] - 1.0),
        "strategy_max_drawdown": _max_drawdown(result["equity"]),
        "strategy_sharpe": _annualized_sharpe(result["strategy_return"]),
        "avg_position": float(result["position"].mean()),
        "hedged_days": hedged_days,
        "avg_hedge_return": avg_hedge_return,
        "hedge_ratio": hedge_ratio,
        "premium_pct": premium_pct,
        "strike_buffer_pct": strike_buffer_pct,
    }


def _build_report(
    *,
    baseline: dict[str, object],
    shrink: dict[str, object],
    rows: list[dict[str, object]],
    start: str,
    end: str,
    trigger_count: int,
    trigger_full_count: int,
    trigger_half_count: int,
    weak_threshold: float,
) -> str:
    ranked = sorted(rows, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC认沽保护联动验证报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 目标: 判断 IC 收缩规则下，认沽保护更适合“替代减仓”还是“配合减仓”",
        "- 注意: 当前使用的是合成认沽保护代理，不是真实期权历史成交数据",
        f"- 收缩触发器: 趋势破坏 + 单日弱势 (spot_ret_1d <= {weak_threshold:.2%})",
        f"- 触发天数: {trigger_count}",
        f"- 触发时基础仓位分布: 满仓 {trigger_full_count} 天 / 半仓 {trigger_half_count} 天",
        "",
        "## 基线比较",
        "",
        f"- 原始高贴水满仓/普通半仓: Sharpe {baseline['strategy_sharpe']:.2f} | 收益 {baseline['strategy_total_return'] * 100:.2f}% | 回撤 {baseline['strategy_max_drawdown'] * 100:.2f}%",
        f"- 纯减仓规则(降一档): Sharpe {shrink['strategy_sharpe']:.2f} | 收益 {shrink['strategy_total_return'] * 100:.2f}% | 回撤 {shrink['strategy_max_drawdown'] * 100:.2f}%",
        "",
        "## 认沽联动候选",
        "",
        "| 方案 | 保护比例 | 权利金成本 | 虚值缓冲 | 对冲天数 | 平均对冲收益 | 策略收益 | 最大回撤 | Sharpe |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in ranked[:12]:
        lines.append(
            f"| {item['label']} | {item['hedge_ratio']:.1f} | {item['premium_pct'] * 100:.2f}% | "
            f"{item['strike_buffer_pct'] * 100:.2f}% | {item['hedged_days']} | {item['avg_hedge_return'] * 100:.2f}% | "
            f"{item['strategy_total_return'] * 100:.2f}% | {item['strategy_max_drawdown'] * 100:.2f}% | {item['strategy_sharpe']:.2f} |"
        )

    best = ranked[0]
    lines.extend(["", "## 结论", ""])
    if float(best["strategy_sharpe"]) > float(shrink["strategy_sharpe"]):
        lines.append(
            f"- 当前最优联动方案是 `{best['label']}`，Sharpe {best['strategy_sharpe']:.2f}，优于纯减仓 {shrink['strategy_sharpe']:.2f}。"
        )
    else:
        lines.append(
            f"- 当前最优联动方案 `{best['label']}` 仍未跑赢纯减仓 {shrink['strategy_sharpe']:.2f}，说明认沽保护更适合作为候选防守层，而不是立刻替代减仓。"
        )
    if trigger_half_count and not trigger_full_count:
        lines.append("- 当前触发器出现时，基础仓位全部已经处于半仓，所以“减仓+认沽”天然接近“纯减仓”；这说明认沽更像“替代降到0”的选择，而不是“配合继续减半”。")
    lines.append("- 这份报告更适合回答“遇到 IC 风险信号时，是先减仓，还是先买认沽保护”。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame = _build_tradable_frame(args.start, args.end, args.data_cache_dir, args.refresh_data_cache)
    high_carry = frame["annualized_carry"] >= float(frame["annualized_carry"].dropna().quantile(0.80))
    base_signal = pd.Series(0.5, index=frame.index)
    base_signal.loc[high_carry & (frame["trend_intact"] == 1)] = 1.0

    weak_threshold = float(frame["spot_ret_1d"].dropna().quantile(0.20))
    trigger = (frame["trend_intact"] == 0) & (frame["spot_ret_1d"] <= weak_threshold)
    trigger_full_count = int((base_signal.loc[trigger] >= 1.0).sum())
    trigger_half_count = int((base_signal.loc[trigger] == 0.5).sum())

    shrink_signal = base_signal.copy()
    shrink_signal.loc[trigger] = shrink_signal.loc[trigger].map(lambda value: 0.5 if value >= 1.0 else 0.0)

    baseline = _evaluate_base_strategy(frame, "原始高贴水满仓/普通半仓", base_signal)
    shrink = _evaluate_base_strategy(frame, "纯减仓规则(降一档)", shrink_signal)

    rows: list[dict[str, object]] = []
    for hedge_ratio in (0.5, 1.0):
        for premium_pct in (0.002, 0.004, 0.006):
            for strike_buffer_pct in (0.0, 0.01, 0.02):
                rows.append(
                    _evaluate_put_overlay(
                        frame,
                        label=f"只做认沽保护(r={hedge_ratio:.1f},p={premium_pct:.3f},k={strike_buffer_pct:.2f})",
                        signal_position=base_signal,
                        hedge_signal=trigger,
                        hedge_ratio=hedge_ratio,
                        premium_pct=premium_pct,
                        strike_buffer_pct=strike_buffer_pct,
                    )
                )
                rows.append(
                    _evaluate_put_overlay(
                        frame,
                        label=f"减仓+认沽保护(r={hedge_ratio:.1f},p={premium_pct:.3f},k={strike_buffer_pct:.2f})",
                        signal_position=shrink_signal,
                        hedge_signal=trigger,
                        hedge_ratio=hedge_ratio,
                        premium_pct=premium_pct,
                        strike_buffer_pct=strike_buffer_pct,
                    )
                )

    report = _build_report(
        baseline=baseline,
        shrink=shrink,
        rows=rows,
        start=args.start,
        end=args.end,
        trigger_count=int(trigger.sum()),
        trigger_full_count=trigger_full_count,
        trigger_half_count=trigger_half_count,
        weak_threshold=weak_threshold,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC认沽保护联动验证报告.md"
    latest_path = output_dir / "latest_ic_put_hedge_overlay_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC认沽保护联动验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
