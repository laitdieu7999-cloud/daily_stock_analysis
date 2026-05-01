#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate execution variants after the IC second-confirmation trigger."""

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


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Validate second-confirmation execution variants for IC carry.")
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


def _apply_execution_variant(base_signal: pd.Series, trigger: pd.Series, variant: str) -> pd.Series:
    signal = base_signal.copy()
    trigger = trigger.fillna(False)

    if variant == "第二确认即清零":
        signal.loc[trigger] = 0.0
        return signal

    if variant == "第二确认保留0.10底仓":
        signal.loc[trigger] = 0.10
        return signal

    if variant == "第二确认保留0.25底仓":
        signal.loc[trigger] = 0.25
        return signal

    if variant == "第二确认空仓两日":
        cooldown = 0
        for idx in signal.index:
            if bool(trigger.loc[idx]):
                cooldown = 2
            if cooldown > 0:
                signal.loc[idx] = 0.0
                cooldown -= 1
        return signal

    if variant == "第二确认首日0.25/连续再清零":
        prev_trigger = False
        for idx in signal.index:
            if not bool(trigger.loc[idx]):
                prev_trigger = False
                continue
            signal.loc[idx] = 0.0 if prev_trigger else 0.25
            prev_trigger = True
        return signal

    raise ValueError(f"Unsupported execution variant: {variant}")


def _evaluate_variant(
    frame: pd.DataFrame,
    *,
    label: str,
    base_signal: pd.Series,
    trigger: pd.Series,
    signal: pd.Series,
) -> dict[str, object]:
    result = _evaluate_base_strategy(frame, label, signal)
    trigger_mask = trigger.fillna(False)
    result["trigger_days"] = int(trigger_mask.sum())
    result["avg_signal_on_trigger"] = float(signal.loc[trigger_mask].mean()) if int(trigger_mask.sum()) else 0.0
    result["mean_future_ret_5d"] = float(frame.loc[trigger_mask, "contract_ret_5d_fwd"].mean()) if int(trigger_mask.sum()) else 0.0
    result["mean_excess_ret_5d"] = float(frame.loc[trigger_mask, "excess_ret_5d"].mean()) if int(trigger_mask.sum()) else 0.0
    result["position_delta_vs_base"] = float(signal.mean() - base_signal.mean())
    return result


def _build_second_confirmation_trigger(frame: pd.DataFrame) -> tuple[pd.Series, dict[str, float]]:
    weak_threshold = float(frame["spot_ret_1d"].dropna().quantile(0.20))
    carry_q20 = float(frame["annualized_carry"].dropna().quantile(0.20))
    trigger = ((frame["trend_intact"] == 0) & (frame["spot_ret_1d"] <= weak_threshold)).fillna(False)
    return trigger, {"weak_threshold": weak_threshold, "carry_q20": carry_q20}


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
        f"# {datetime.now().strftime('%Y-%m-%d')} IC第二确认执行细节验证报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 基线策略: 高贴水满仓 / 普通半仓",
        "- 第二确认触发器: 趋势破坏 + 单日弱势",
        f"- 单日弱势阈值: {thresholds['weak_threshold']:.2%}",
        f"- 低贴水阈值(参考): {thresholds['carry_q20']:.2%}",
        f"- 触发时基础仓位分布: 满仓 {trigger_full_count} 天 / 半仓 {trigger_half_count} 天",
        "- 目标: 比较第二确认触发后，半仓到底是清到0、留底仓，还是分节奏退出更值。",
        "",
        "## 基线",
        "",
        f"- 原始高贴水满仓/普通半仓: 收益 {base['strategy_total_return'] * 100:.2f}% | 最大回撤 {base['strategy_max_drawdown'] * 100:.2f}% | Sharpe {base['strategy_sharpe']:.2f}",
        "",
        "## 执行细节比较",
        "",
        "| 方案 | 触发天数 | 触发日目标仓位 | 策略收益 | 最大回撤 | Sharpe | 平均仓位 | 触发后5日合约均值 | 触发后5日超额均值 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in ranked:
        lines.append(
            f"| {item['label']} | {item['trigger_days']} | {item['avg_signal_on_trigger']:.2f} | "
            f"{item['strategy_total_return'] * 100:.2f}% | {item['strategy_max_drawdown'] * 100:.2f}% | "
            f"{item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} | "
            f"{item['mean_future_ret_5d'] * 100:.2f}% | {item['mean_excess_ret_5d'] * 100:.2f}% |"
        )

    best = ranked[0]
    lines.extend(["", "## 结论", ""])
    lines.append(
        f"- 当前最优第二确认执行方案是 `{best['label']}`，Sharpe {best['strategy_sharpe']:.2f}，收益 {best['strategy_total_return'] * 100:.2f}%，回撤 {best['strategy_max_drawdown'] * 100:.2f}%。"
    )
    if trigger_full_count == 0:
        lines.append("- 这次验证再次确认：第二确认触发时基础仓位实际上已经是半仓，因此这份报告真正回答的是“半仓如何退出”而不是“满仓如何退到半仓”。")
    if "0.10" in str(best["label"]) or "0.25" in str(best["label"]):
        lines.append("- 从结果看，保留小底仓比一刀切清零更有性价比，说明第二确认更适合“压缩敞口”而不是“彻底放弃贴水”。")
    elif "空仓两日" in str(best["label"]) or "清零" in str(best["label"]):
        lines.append("- 从结果看，第二确认后更适合果断清仓或短期退出，说明这类弱势窗口里继续恋战贴水的收益补偿不够。")
    else:
        lines.append("- 从结果看，分节奏退出比单步动作更平衡，说明第二确认后的节奏控制比纯粹的仓位大小更关键。")
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
        "第二确认即清零",
        "第二确认保留0.10底仓",
        "第二确认保留0.25底仓",
        "第二确认空仓两日",
        "第二确认首日0.25/连续再清零",
    ):
        signal = _apply_execution_variant(base_signal, trigger, label)
        rows.append(
            _evaluate_variant(
                frame,
                label=label,
                base_signal=base_signal,
                trigger=trigger,
                signal=signal,
            )
        )

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
    report_path = output_dir / f"{report_date}_IC第二确认执行细节验证报告.md"
    latest_path = output_dir / "latest_ic_second_confirmation_execution_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC第二确认执行细节验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
