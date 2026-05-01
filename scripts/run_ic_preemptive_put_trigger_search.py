#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Search earlier synthetic put triggers for the IC carry workflow."""

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

from run_ic_put_hedge_overlay_validation import _evaluate_base_strategy, _evaluate_put_overlay  # noqa: E402
from run_ic_term_structure_validation import _build_term_structure_frame  # noqa: E402


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Search earlier put triggers for IC protection.")
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


def _build_candidate_triggers(frame: pd.DataFrame) -> tuple[dict[str, pd.Series], dict[str, float]]:
    weak_threshold = float(frame["spot_ret_1d"].dropna().quantile(0.20))
    carry_q20 = float(frame["annualized_carry"].dropna().quantile(0.20))
    flat_threshold = float(frame["far_near_annualized"].dropna().quantile(0.80))
    trend_broken = frame["trend_intact"] == 0
    weak = frame["spot_ret_1d"] <= weak_threshold
    flat = frame["far_near_annualized"] >= flat_threshold
    low_carry = frame["annualized_carry"] <= carry_q20
    triggers = {
        "趋势破坏": trend_broken,
        "趋势破坏+平坦斜率": trend_broken & flat,
        "趋势破坏+低贴水": trend_broken & low_carry,
        "趋势破坏+平坦斜率+低贴水": trend_broken & flat & low_carry,
        "趋势破坏+单日弱势": trend_broken & weak,
    }
    thresholds = {
        "weak_threshold": weak_threshold,
        "carry_q20": carry_q20,
        "flat_threshold": flat_threshold,
    }
    return triggers, thresholds


def _build_report(
    *,
    baseline: dict[str, object],
    shrink: dict[str, object],
    rows: list[dict[str, object]],
    start: str,
    end: str,
    thresholds: dict[str, float],
) -> str:
    ranked = sorted(rows, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC认沽先行触发搜索报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 目标: 搜索哪些更早期的触发器，更适合先上认沽保护，而不是等到单日弱势确认",
        "- 保护假设: 合成认沽代理，保护比例=1.0，权利金=0.20%，虚值缓冲=0%",
        f"- 单日弱势阈值: {thresholds['weak_threshold']:.2%}",
        f"- 低贴水阈值: {thresholds['carry_q20']:.2%}",
        f"- 平坦斜率阈值: {thresholds['flat_threshold']:.2%}",
        "",
        "## 基线比较",
        "",
        f"- 原始高贴水满仓/普通半仓: Sharpe {baseline['strategy_sharpe']:.2f} | 收益 {baseline['strategy_total_return'] * 100:.2f}%",
        f"- 纯减仓规则(趋势破坏+单日弱势): Sharpe {shrink['strategy_sharpe']:.2f} | 收益 {shrink['strategy_total_return'] * 100:.2f}%",
        "",
        "## 候选认沽触发器",
        "",
        "| 触发器 | 触发天数 | 策略收益 | 最大回撤 | Sharpe | 平均仓位 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in ranked:
        lines.append(
            f"| {item['label']} | {item['hedged_days']} | {item['strategy_total_return'] * 100:.2f}% | "
            f"{item['strategy_max_drawdown'] * 100:.2f}% | {item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} |"
        )
    best = ranked[0]
    lines.extend(["", "## 结论", ""])
    if float(best["strategy_sharpe"]) > float(shrink["strategy_sharpe"]):
        lines.append(f"- 当前最优认沽先行触发器是 `{best['label']}`，Sharpe {best['strategy_sharpe']:.2f}，优于纯减仓 {shrink['strategy_sharpe']:.2f}。")
    else:
        lines.append("- 当前认沽先行触发器都未跑赢纯减仓，说明更早保护还需要盘中微观结构信号来支撑。")
    lines.append("- 这份报告更适合回答“认沽保护要不要比减仓更早触发”。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame = _build_term_structure_frame(args.start, args.end, args.data_cache_dir, args.refresh_data_cache)
    high_carry = frame["annualized_carry"] >= float(frame["annualized_carry"].dropna().quantile(0.80))
    base_signal = pd.Series(0.5, index=frame.index)
    base_signal.loc[high_carry & (frame["trend_intact"] == 1)] = 1.0

    triggers, thresholds = _build_candidate_triggers(frame)
    shrink_trigger = triggers["趋势破坏+单日弱势"]
    shrink_signal = base_signal.copy()
    shrink_signal.loc[shrink_trigger] = shrink_signal.loc[shrink_trigger].map(lambda value: 0.5 if value >= 1.0 else 0.0)

    baseline = _evaluate_base_strategy(frame, "原始高贴水满仓/普通半仓", base_signal)
    shrink = _evaluate_base_strategy(frame, "纯减仓规则(趋势破坏+单日弱势)", shrink_signal)

    rows: list[dict[str, object]] = []
    for label, trigger in triggers.items():
        rows.append(
            _evaluate_put_overlay(
                frame,
                label=label,
                signal_position=base_signal,
                hedge_signal=trigger,
                hedge_ratio=1.0,
                premium_pct=0.002,
                strike_buffer_pct=0.0,
            )
        )

    report = _build_report(
        baseline=baseline,
        shrink=shrink,
        rows=rows,
        start=args.start,
        end=args.end,
        thresholds=thresholds,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC认沽先行触发搜索报告.md"
    latest_path = output_dir / "latest_ic_preemptive_put_trigger_search.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC认沽先行触发搜索完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
