#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Search combined IC long-shrink rules using trend, weakness, and term structure."""

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

from run_ic_roll_carry_validation import _annualized_sharpe, _max_drawdown  # noqa: E402
from run_ic_term_structure_validation import _build_term_structure_frame  # noqa: E402


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Search combined shrink rules for the IC carry workflow.")
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


def _build_candidate_rules(frame: pd.DataFrame) -> tuple[dict[str, pd.Series], dict[str, float]]:
    frame = frame.copy()
    spot1_q20 = float(frame["spot_ret_1d"].dropna().quantile(0.20))
    carry_q20 = float(frame["annualized_carry"].dropna().quantile(0.20))
    steep_threshold = float(frame["far_near_annualized"].dropna().quantile(0.20))
    flat_threshold = float(frame["far_near_annualized"].dropna().quantile(0.80))
    front_collapse_threshold = float(frame["front_end_gap"].dropna().quantile(0.80))
    q_anchor = frame["q1_q2_annualized"].dropna()
    q_anchor_median = float(q_anchor.median())
    q_anchor_band = float((q_anchor - q_anchor_median).abs().median())
    if q_anchor_band <= 0:
        q_anchor_band = float(q_anchor.std(ddof=0) * 0.5) if len(q_anchor) else 0.0
    if q_anchor_band <= 0:
        q_anchor_band = 0.01

    trend_broken = frame["trend_intact"] == 0
    weak_1d = frame["spot_ret_1d"] <= spot1_q20
    flat_curve = frame["far_near_annualized"] >= flat_threshold
    steep_curve = frame["far_near_annualized"] <= steep_threshold
    low_carry = frame["annualized_carry"] <= carry_q20
    front_collapse = frame["front_end_gap"] >= front_collapse_threshold
    q_anchor_unstable = (frame["q1_q2_annualized"] - q_anchor_median).abs() > q_anchor_band

    rules = {
        "趋势破坏+单日弱势": trend_broken & weak_1d,
        "趋势破坏+平坦斜率": trend_broken & flat_curve,
        "趋势破坏+陡峭负斜率": trend_broken & steep_curve,
        "趋势破坏+单日弱势+平坦斜率": trend_broken & weak_1d & flat_curve,
        "趋势破坏+单日弱势+陡峭负斜率": trend_broken & weak_1d & steep_curve,
        "趋势破坏+平坦斜率+低贴水": trend_broken & flat_curve & low_carry,
        "趋势破坏+单日弱势+平坦斜率+低贴水": trend_broken & weak_1d & flat_curve & low_carry,
        "趋势破坏+单日弱势+高贴水": trend_broken & weak_1d & ~low_carry,
        "前端塌陷": front_collapse,
        "前端塌陷+趋势破坏": front_collapse & trend_broken,
        "前端塌陷+单日弱势": front_collapse & weak_1d,
        "前端塌陷+趋势破坏+单日弱势": front_collapse & trend_broken & weak_1d,
        "前端塌陷+远季锚失稳": front_collapse & q_anchor_unstable,
        "前端塌陷+趋势破坏+远季锚失稳": front_collapse & trend_broken & q_anchor_unstable,
    }
    thresholds = {
        "spot1_q20": spot1_q20,
        "carry_q20": carry_q20,
        "steep_threshold": steep_threshold,
        "flat_threshold": flat_threshold,
        "front_collapse_threshold": front_collapse_threshold,
        "q_anchor_median": q_anchor_median,
        "q_anchor_band": q_anchor_band,
    }
    return rules, thresholds


def _build_report(
    *,
    baseline: dict[str, object],
    rows: list[dict[str, object]],
    start: str,
    end: str,
    thresholds: dict[str, float],
) -> str:
    ranked = sorted(rows, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC组合收缩规则搜索报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 基线策略: 高贴水满仓 / 普通半仓",
        "- 执行方式: 当日信号, 次日仓位执行",
        "- 目标: 组合检验趋势破坏、单日弱势、期限结构是否能更稳地决定 IC 多头收缩",
        f"- 单日弱势阈值: spot_ret_1d <= {thresholds['spot1_q20']:.2%}",
        f"- 低贴水阈值: annualized_carry <= {thresholds['carry_q20']:.2%}",
        f"- 陡峭负斜率阈值: far-near annualized <= {thresholds['steep_threshold']:.2%}",
        f"- 平坦斜率阈值: far-near annualized >= {thresholds['flat_threshold']:.2%}",
        f"- 前端塌陷阈值: front_end_gap >= {thresholds['front_collapse_threshold']:.2%}",
        f"- 远季锚稳定带宽: 中位数 {thresholds['q_anchor_median']:.2%} ± {thresholds['q_anchor_band']:.2%}",
        "",
        "## 基线",
        "",
        f"- 基线收益: {baseline['strategy_total_return'] * 100:.2f}%",
        f"- 基线Sharpe: {baseline['strategy_sharpe']:.2f}",
        f"- 基线最大回撤: {baseline['strategy_max_drawdown'] * 100:.2f}%",
        "",
        "## 组合规则排名",
        "",
        "| 规则 | 动作 | 触发天数 | 触发后5日超额收益 | 策略收益 | 最大回撤 | Sharpe | 平均仓位 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in ranked[:12]:
        lines.append(
            f"| {item['rule']} | {item['action']} | {item['count']} | {item['mean_5d_excess'] * 100:.2f}% | "
            f"{item['strategy_total_return'] * 100:.2f}% | {item['strategy_max_drawdown'] * 100:.2f}% | "
            f"{item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} |"
        )
    best = ranked[0]
    lines.extend(["", "## 结论", ""])
    if float(best["strategy_sharpe"]) > float(baseline["strategy_sharpe"]):
        lines.append(
            f"- 当前最优组合收缩规则是 `{best['rule']} / {best['action']}`，Sharpe {best['strategy_sharpe']:.2f}，优于基线 {baseline['strategy_sharpe']:.2f}。"
        )
    else:
        lines.append("- 当前组合规则没有明显跑赢基线，说明收缩条件还需要更强的盘中或微观结构信号。")
    lines.append("- 这份结果更适合回答“单一触发器之外，哪些条件组合更适合缩 IC 多头”。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame = _build_term_structure_frame(args.start, args.end, args.data_cache_dir, args.refresh_data_cache)
    high_carry = frame["annualized_carry"] >= float(frame["annualized_carry"].dropna().quantile(0.80))
    base_signal = pd.Series(0.5, index=frame.index)
    base_signal.loc[high_carry & (frame["trend_intact"] == 1)] = 1.0

    baseline = _evaluate_strategy(frame, "基线策略", base_signal)
    rules, thresholds = _build_candidate_rules(frame)
    rows: list[dict[str, object]] = []
    for rule_name, mask in rules.items():
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
            metrics["mean_5d_excess"] = float(frame.loc[mask, "excess_ret_5d"].mean()) if int(mask.sum()) else 0.0
            rows.append(metrics)

    report = _build_report(
        baseline=baseline,
        rows=rows,
        start=args.start,
        end=args.end,
        thresholds=thresholds,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC组合收缩规则搜索报告.md"
    latest_path = output_dir / "latest_ic_combined_shrink_search.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC组合收缩规则搜索完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
