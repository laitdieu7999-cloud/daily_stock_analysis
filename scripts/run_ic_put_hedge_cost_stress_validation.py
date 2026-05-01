#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate preemptive IC put triggers under a more conservative synthetic cost model."""

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

from run_ic_preemptive_put_trigger_search import _build_candidate_triggers  # noqa: E402
from run_ic_put_hedge_overlay_validation import _evaluate_base_strategy  # noqa: E402
from run_ic_term_structure_validation import _build_term_structure_frame  # noqa: E402
from run_ic_roll_carry_validation import _annualized_sharpe, _max_drawdown  # noqa: E402


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Stress-test synthetic IC put overlays with harsher cost assumptions.")
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


def _trigger_config(label: str) -> tuple[float, float, float]:
    """Return base premium, strike buffer, and late-trigger uplift."""
    if label == "趋势破坏":
        return 0.006, 0.015, 0.000
    if label == "趋势破坏+平坦斜率":
        return 0.007, 0.010, 0.001
    if label == "趋势破坏+低贴水":
        return 0.007, 0.010, 0.001
    if label == "趋势破坏+平坦斜率+低贴水":
        return 0.008, 0.005, 0.002
    if label == "趋势破坏+单日弱势":
        return 0.009, 0.000, 0.003
    return 0.007, 0.010, 0.001


def _evaluate_dynamic_put_overlay(
    df: pd.DataFrame,
    *,
    label: str,
    signal_position: pd.Series,
    hedge_signal: pd.Series,
    hedge_ratio: float,
    base_premium: float,
    strike_buffer_pct: float,
    extra_uplift: float,
) -> dict[str, object]:
    result = df.copy()
    result["signal_position"] = signal_position.reindex(result.index).fillna(0.0)
    result["position"] = result["signal_position"].shift(1).fillna(0.0)
    result["hedge_signal"] = hedge_signal.reindex(result.index).fillna(False)
    result["hedge_active"] = result["hedge_signal"].shift(1).fillna(False)
    result["spot_vol_5d"] = result["spot_ret_1d"].rolling(5).std().fillna(result["spot_ret_1d"].abs().median())

    underlying_ret = result["spot_ret_1d"].fillna(0.0)
    dynamic_premium = base_premium + 0.35 * result["spot_vol_5d"].clip(lower=0.0, upper=0.05) + extra_uplift
    put_payoff_unit = (-underlying_ret - strike_buffer_pct).clip(lower=0.0) - dynamic_premium

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
    active_premium = dynamic_premium[result["hedge_active"]].mean() if hedged_days else 0.0
    return {
        "label": label,
        "sample_count": int(len(result)),
        "strategy_total_return": float(result["equity"].iloc[-1] - 1.0),
        "buy_hold_total_return": float(result["buy_hold_equity"].iloc[-1] - 1.0),
        "strategy_max_drawdown": _max_drawdown(result["equity"]),
        "strategy_sharpe": _annualized_sharpe(result["strategy_return"]),
        "avg_position": float(result["position"].mean()),
        "hedged_days": hedged_days,
        "avg_dynamic_premium": float(active_premium) if hedged_days else 0.0,
        "base_premium": base_premium,
        "strike_buffer_pct": strike_buffer_pct,
        "extra_uplift": extra_uplift,
        "hedge_ratio": hedge_ratio,
    }


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
        f"# {datetime.now().strftime('%Y-%m-%d')} IC认沽成本压力验证报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 目标: 在更保守的期权成本/隐波假设下，检验认沽是否仍应早于减仓触发",
        "- 成本模型: 保护越晚、波动越高、越接近平值，认沽成本越高",
        f"- 单日弱势阈值: {thresholds['weak_threshold']:.2%}",
        f"- 低贴水阈值: {thresholds['carry_q20']:.2%}",
        f"- 平坦斜率阈值: {thresholds['flat_threshold']:.2%}",
        "",
        "## 基线比较",
        "",
        f"- 原始高贴水满仓/普通半仓: Sharpe {baseline['strategy_sharpe']:.2f} | 收益 {baseline['strategy_total_return'] * 100:.2f}%",
        f"- 纯减仓规则(趋势破坏+单日弱势): Sharpe {shrink['strategy_sharpe']:.2f} | 收益 {shrink['strategy_total_return'] * 100:.2f}%",
        "",
        "## 压力测试后的认沽先行触发器",
        "",
        "| 触发器 | 对冲天数 | 平均动态成本 | 行权缓冲 | 策略收益 | 最大回撤 | Sharpe |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in ranked:
        lines.append(
            f"| {item['label']} | {item['hedged_days']} | {item['avg_dynamic_premium'] * 100:.2f}% | "
            f"{item['strike_buffer_pct'] * 100:.2f}% | {item['strategy_total_return'] * 100:.2f}% | "
            f"{item['strategy_max_drawdown'] * 100:.2f}% | {item['strategy_sharpe']:.2f} |"
        )
    best = ranked[0]
    lines.extend(["", "## 结论", ""])
    if float(best["strategy_sharpe"]) > float(shrink["strategy_sharpe"]):
        lines.append(
            f"- 在更保守的成本模型下，当前最优先行触发器仍是 `{best['label']}`，Sharpe {best['strategy_sharpe']:.2f}，优于纯减仓 {shrink['strategy_sharpe']:.2f}。"
        )
    else:
        lines.append("- 在更保守的成本模型下，认沽先行方案已不再跑赢纯减仓，说明当前“先上认沽”的结论依赖过于理想化的期权定价。")
    lines.append("- 这份报告更适合回答“认沽先于减仓”这个方向，在更真实成本下还站不站得住。")
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
        base_premium, strike_buffer_pct, extra_uplift = _trigger_config(label)
        rows.append(
            _evaluate_dynamic_put_overlay(
                frame,
                label=label,
                signal_position=base_signal,
                hedge_signal=trigger,
                hedge_ratio=1.0,
                base_premium=base_premium,
                strike_buffer_pct=strike_buffer_pct,
                extra_uplift=extra_uplift,
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
    report_path = output_dir / f"{report_date}_IC认沽成本压力验证报告.md"
    latest_path = output_dir / "latest_ic_put_hedge_cost_stress_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC认沽成本压力验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
