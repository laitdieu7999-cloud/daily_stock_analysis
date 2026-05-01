#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Search cooldown and re-entry variants after the IC second-confirmation trigger."""

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
from run_ic_put_hedge_overlay_validation import _evaluate_base_strategy  # noqa: E402
from run_ic_second_confirmation_execution_validation import _build_second_confirmation_trigger  # noqa: E402


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Search cooldown and re-entry variants after IC second confirmation.")
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


def _apply_reentry_variant(
    base_signal: pd.Series,
    trigger: pd.Series,
    trend_intact: pd.Series,
    high_carry: pd.Series,
    variant: str,
) -> pd.Series:
    signal = base_signal.copy()
    trigger = trigger.fillna(False)
    trend_intact = trend_intact.fillna(False)
    high_carry = high_carry.fillna(False)

    state = "normal"
    remaining = 0
    for idx in signal.index:
        if bool(trigger.loc[idx]):
            if variant == "空仓1日":
                state, remaining = "cooldown", 1
            elif variant == "空仓2日":
                state, remaining = "cooldown", 2
            elif variant == "空仓3日":
                state, remaining = "cooldown", 3
            elif variant == "空仓5日":
                state, remaining = "cooldown", 5
            elif variant == "空仓至趋势修复":
                state, remaining = "wait_trend", 0
            elif variant == "空仓至趋势修复且高贴水":
                state, remaining = "wait_trend_carry", 0
            elif variant == "空仓2日后需趋势修复":
                state, remaining = "cooldown_then_trend", 2
            elif variant == "空仓2日后需趋势修复且高贴水":
                state, remaining = "cooldown_then_trend_carry", 2
            else:
                raise ValueError(f"Unsupported re-entry variant: {variant}")

        if state == "normal":
            continue

        if state == "cooldown":
            signal.loc[idx] = 0.0
            remaining -= 1
            if remaining <= 0:
                state = "normal"
            continue

        if state == "wait_trend":
            signal.loc[idx] = 0.0
            if bool(trend_intact.loc[idx]):
                state = "normal"
            continue

        if state == "wait_trend_carry":
            signal.loc[idx] = 0.0
            if bool(trend_intact.loc[idx]) and bool(high_carry.loc[idx]):
                state = "normal"
            continue

        if state == "cooldown_then_trend":
            signal.loc[idx] = 0.0
            remaining -= 1
            if remaining <= 0:
                state = "wait_trend_after_cooldown"
            continue

        if state == "cooldown_then_trend_carry":
            signal.loc[idx] = 0.0
            remaining -= 1
            if remaining <= 0:
                state = "wait_trend_carry_after_cooldown"
            continue

        if state == "wait_trend_after_cooldown":
            signal.loc[idx] = 0.0
            if bool(trend_intact.loc[idx]):
                state = "normal"
            continue

        if state == "wait_trend_carry_after_cooldown":
            signal.loc[idx] = 0.0
            if bool(trend_intact.loc[idx]) and bool(high_carry.loc[idx]):
                state = "normal"
            continue

    return signal


def _evaluate_variant(
    frame: pd.DataFrame,
    *,
    label: str,
    signal: pd.Series,
    trigger: pd.Series,
) -> dict[str, object]:
    result = _evaluate_base_strategy(frame, label, signal)
    trigger_mask = trigger.fillna(False)
    result["trigger_days"] = int(trigger_mask.sum())
    result["mean_future_ret_5d"] = float(frame.loc[trigger_mask, "contract_ret_5d_fwd"].mean()) if int(trigger_mask.sum()) else 0.0
    result["mean_excess_ret_5d"] = float(frame.loc[trigger_mask, "excess_ret_5d"].mean()) if int(trigger_mask.sum()) else 0.0
    return result


def _build_report(
    *,
    start: str,
    end: str,
    thresholds: dict[str, float],
    base: dict[str, object],
    rows: list[dict[str, object]],
    trigger_half_count: int,
    trigger_full_count: int,
) -> str:
    ranked = sorted(rows, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC第二确认冷静期与回补搜索报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 基线策略: 高贴水满仓 / 普通半仓",
        "- 第二确认触发器: 趋势破坏 + 单日弱势",
        f"- 单日弱势阈值: {thresholds['weak_threshold']:.2%}",
        f"- 低贴水阈值(参考): {thresholds['carry_q20']:.2%}",
        f"- 触发时基础仓位分布: 满仓 {trigger_full_count} 天 / 半仓 {trigger_half_count} 天",
        "- 目标: 比较第二确认触发后，空仓多久、以及何时恢复半仓更值。",
        "",
        "## 基线",
        "",
        f"- 原始高贴水满仓/普通半仓: 收益 {base['strategy_total_return'] * 100:.2f}% | 最大回撤 {base['strategy_max_drawdown'] * 100:.2f}% | Sharpe {base['strategy_sharpe']:.2f}",
        "",
        "## 冷静期与回补方案比较",
        "",
        "| 方案 | 触发天数 | 策略收益 | 最大回撤 | Sharpe | 平均仓位 | 触发后5日合约均值 | 触发后5日超额均值 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in ranked:
        lines.append(
            f"| {item['label']} | {item['trigger_days']} | {item['strategy_total_return'] * 100:.2f}% | "
            f"{item['strategy_max_drawdown'] * 100:.2f}% | {item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} | "
            f"{item['mean_future_ret_5d'] * 100:.2f}% | {item['mean_excess_ret_5d'] * 100:.2f}% |"
        )

    best = ranked[0]
    lines.extend(["", "## 结论", ""])
    lines.append(
        f"- 当前最优冷静期与回补方案是 `{best['label']}`，Sharpe {best['strategy_sharpe']:.2f}，收益 {best['strategy_total_return'] * 100:.2f}%，回撤 {best['strategy_max_drawdown'] * 100:.2f}%。"
    )
    if trigger_full_count == 0:
        lines.append("- 这份结果继续确认：第二确认发生时，研究重点是半仓的退出和回补节奏，不是满仓如何退。")
    if "趋势修复" in str(best["label"]):
        lines.append("- 从结果看，回补时机比空仓天数更关键，等趋势修复再回来更有助于控制回撤。")
    else:
        lines.append("- 从结果看，固定冷静期本身已经能显著改善风险收益比，说明第二确认后的短期噪音期确实值得避开。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame = _build_tradable_frame(args.start, args.end, args.data_cache_dir, args.refresh_data_cache)

    high_carry = frame["annualized_carry"] >= float(frame["annualized_carry"].dropna().quantile(0.80))
    base_signal = pd.Series(0.5, index=frame.index)
    base_signal.loc[high_carry & (frame["trend_intact"] == 1)] = 1.0

    trigger, thresholds = _build_second_confirmation_trigger(frame)
    trigger_full_count = int((base_signal.loc[trigger] >= 1.0).sum())
    trigger_half_count = int((base_signal.loc[trigger] == 0.5).sum())

    base = _evaluate_base_strategy(frame, "原始高贴水满仓/普通半仓", base_signal)
    rows: list[dict[str, object]] = []
    for label in (
        "空仓1日",
        "空仓2日",
        "空仓3日",
        "空仓5日",
        "空仓至趋势修复",
        "空仓至趋势修复且高贴水",
        "空仓2日后需趋势修复",
        "空仓2日后需趋势修复且高贴水",
    ):
        signal = _apply_reentry_variant(base_signal, trigger, frame["trend_intact"] == 1, high_carry, label)
        rows.append(_evaluate_variant(frame, label=label, signal=signal, trigger=trigger))

    report = _build_report(
        start=args.start,
        end=args.end,
        thresholds=thresholds,
        base=base,
        rows=rows,
        trigger_half_count=trigger_half_count,
        trigger_full_count=trigger_full_count,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC第二确认冷静期与回补搜索报告.md"
    latest_path = output_dir / "latest_ic_second_confirmation_reentry_search.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC第二确认冷静期与回补搜索完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
