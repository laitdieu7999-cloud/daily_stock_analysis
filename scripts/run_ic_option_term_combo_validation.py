#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate combined QVIX proxy and front-end term-structure warnings for IC carry."""

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

from run_ic_option_proxy_history_validation import _add_forward_returns  # noqa: E402
from run_ic_preemptive_put_trigger_search import _build_candidate_triggers  # noqa: E402
from run_ic_put_hedge_overlay_validation import _evaluate_base_strategy  # noqa: E402
from run_ic_put_hedge_qvix_proxy_validation import _prepare_frame  # noqa: E402


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Validate combined option-proxy and term-structure warnings.")
    parser.add_argument("--start", default=default_start)
    parser.add_argument("--end", default=default_end)
    parser.add_argument(
        "--data-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "ic_basis_history"),
        help="Directory used to cache CSI500 spot, CFFEX IC panel history, and 500ETF QVIX history.",
    )
    parser.add_argument(
        "--refresh-data-cache",
        action="store_true",
        help="Ignore local caches and rebuild them from public data sources.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "reports" / "backtests"),
        help="Directory where the markdown report will be written.",
    )
    return parser


def _compute_combo_thresholds(frame: pd.DataFrame) -> dict[str, float]:
    jump_abs = frame["qvix_jump_pct_hist"].abs().dropna()
    q_anchor = frame["q1_q2_annualized"].dropna()
    q_anchor_median = float(q_anchor.median()) if not q_anchor.empty else 0.0
    q_anchor_band = float((q_anchor - q_anchor_median).abs().median()) if not q_anchor.empty else 0.01
    if q_anchor_band <= 0:
        q_anchor_band = float(q_anchor.std(ddof=0) * 0.5) if len(q_anchor) else 0.01
    if q_anchor_band <= 0:
        q_anchor_band = 0.01
    return {
        "front_gap_threshold": float(frame["front_end_gap"].dropna().quantile(0.80)),
        "qvix_jump_threshold": float(jump_abs.quantile(0.80)) if not jump_abs.empty else 8.0,
        "qvix_z_threshold": 1.0,
        "q_anchor_median": q_anchor_median,
        "q_anchor_band": q_anchor_band,
    }


def _rule_stats(frame: pd.DataFrame, rule: str, mask: pd.Series) -> dict[str, object]:
    sample = frame[mask].copy().dropna(subset=["spot_ret_1d_fwd", "spot_ret_3d_fwd"])
    if sample.empty:
        return {
            "rule": rule,
            "sample_days": 0,
            "avg_spot_ret_1d": 0.0,
            "avg_spot_ret_3d": 0.0,
            "down_rate_1d": 0.0,
            "down_rate_3d": 0.0,
            "avg_qvix_z": 0.0,
            "avg_qvix_jump_pct": 0.0,
            "avg_front_end_gap": 0.0,
            "avg_q_anchor": 0.0,
            "mask": mask,
        }
    return {
        "rule": rule,
        "sample_days": int(len(sample)),
        "avg_spot_ret_1d": float(sample["spot_ret_1d_fwd"].mean()),
        "avg_spot_ret_3d": float(sample["spot_ret_3d_fwd"].mean()),
        "down_rate_1d": float((sample["spot_ret_1d_fwd"] < 0).mean()),
        "down_rate_3d": float((sample["spot_ret_3d_fwd"] < 0).mean()),
        "avg_qvix_z": float(sample["qvix_z"].mean()),
        "avg_qvix_jump_pct": float(sample["qvix_jump_pct_hist"].abs().mean()),
        "avg_front_end_gap": float(sample["front_end_gap"].mean()),
        "avg_q_anchor": float(sample["q1_q2_annualized"].mean()),
        "mask": mask,
    }


def _search_combo_rules(frame: pd.DataFrame, thresholds: dict[str, float]) -> list[dict[str, object]]:
    front_collapse = frame["front_end_gap"] >= thresholds["front_gap_threshold"]
    qvix_z = frame["qvix_z"] >= thresholds["qvix_z_threshold"]
    qvix_jump = frame["qvix_jump_pct_hist"].abs() >= thresholds["qvix_jump_threshold"]
    q_anchor_unstable = (frame["q1_q2_annualized"] - thresholds["q_anchor_median"]).abs() > thresholds["q_anchor_band"]

    candidates: dict[str, pd.Series] = {
        "前端塌陷": front_collapse,
        f"QVIX zscore>={thresholds['qvix_z_threshold']:.1f}": qvix_z,
        f"|QVIX jump|>={thresholds['qvix_jump_threshold']:.2f}%": qvix_jump,
        "前端塌陷 + QVIX zscore": front_collapse & qvix_z,
        "前端塌陷 + QVIX jump": front_collapse & qvix_jump,
        "前端塌陷 + QVIX共振": front_collapse & qvix_z & qvix_jump,
        "前端塌陷 + 远季锚失稳": front_collapse & q_anchor_unstable,
        "前端塌陷 + QVIX共振 + 远季锚失稳": front_collapse & qvix_z & qvix_jump & q_anchor_unstable,
    }

    rows = [_rule_stats(frame, label, mask) for label, mask in candidates.items() if mask.any()]
    return sorted(
        rows,
        key=lambda item: (
            float(item["avg_spot_ret_3d"]),
            float(item["avg_spot_ret_1d"]),
            -int(item["sample_days"]),
        ),
    )


def _evaluate_warning_overlay(frame: pd.DataFrame, label: str, base_signal: pd.Series, trigger: pd.Series) -> dict[str, object]:
    warning_signal = base_signal.copy()
    warning_signal.loc[trigger] = warning_signal.loc[trigger].map(lambda value: 0.5 if value >= 1.0 else 0.0)
    result = _evaluate_base_strategy(frame, label, warning_signal)
    result["trigger_days"] = int(trigger.sum())
    return result


def _build_report(
    *,
    start: str,
    end: str,
    ranked_rules: list[dict[str, object]],
    warning_rows: list[dict[str, object]],
    base: dict[str, object],
    shrink: dict[str, object],
    thresholds: dict[str, float],
) -> str:
    robust_rows = [item for item in ranked_rules if int(item["sample_days"]) >= 5]
    ordered_warning = sorted(warning_rows, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC期权代理与期限结构组合验证报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 目标: 验证 `500ETF 期权代理` 与 `M1-M2 前端塌陷` 共振后，是否比单独候选更接近第一层预警。",
        f"- 前端塌陷阈值: front_end_gap >= {thresholds['front_gap_threshold']:.2%}",
        f"- QVIX zscore阈值: >= {thresholds['qvix_z_threshold']:.1f}",
        f"- QVIX jump阈值: >= {thresholds['qvix_jump_threshold']:.2f}%",
        f"- 远季锚中位数: {thresholds['q_anchor_median']:.2%}",
        f"- 远季锚稳定带: ±{thresholds['q_anchor_band']:.2%}",
        "",
        "## 组合历史预警搜索（越负越像成功预警）",
        "",
        "| 规则 | 样本日数 | 次日现货均值 | 3日现货均值 | 次日下跌率 | 3日下跌率 | 平均QVIX z | 平均QVIX jump | 平均前端塌陷 | 平均远季锚 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    if not ranked_rules:
        lines.append("| 无可用样本 | 0 | - | - | - | - | - | - | - | - |")
    else:
        for item in ranked_rules:
            rule_label = str(item["rule"]).replace("|", "\\|")
            lines.append(
                f"| {rule_label} | {item['sample_days']} | {item['avg_spot_ret_1d'] * 100:.2f}% | "
                f"{item['avg_spot_ret_3d'] * 100:.2f}% | {item['down_rate_1d'] * 100:.1f}% | "
                f"{item['down_rate_3d'] * 100:.1f}% | {item['avg_qvix_z']:.2f} | "
                f"{item['avg_qvix_jump_pct']:.2f}% | {item['avg_front_end_gap']:.2%} | "
                f"{item['avg_q_anchor']:.2%} |"
            )
    lines.extend(
        [
            "",
            "## 组合预警降一档策略比较",
            "",
            f"- 原始高贴水满仓/普通半仓: Sharpe {base['strategy_sharpe']:.2f} | 收益 {base['strategy_total_return'] * 100:.2f}%",
            f"- 第二确认(趋势破坏+单日弱势): Sharpe {shrink['strategy_sharpe']:.2f} | 收益 {shrink['strategy_total_return'] * 100:.2f}%",
            "",
            "| 组合预警规则 | 触发天数 | 策略收益 | 最大回撤 | Sharpe | 平均仓位 |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in ordered_warning:
        rule_label = str(item["label"]).replace("|", "\\|")
        lines.append(
            f"| {rule_label} | {item['trigger_days']} | {item['strategy_total_return'] * 100:.2f}% | "
            f"{item['strategy_max_drawdown'] * 100:.2f}% | {item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} |"
        )

    lines.extend(["", "## 结论", ""])
    if robust_rows:
        best_robust = robust_rows[0]
        lines.append(
            f"- 若要求至少 5 个样本日，当前更稳的组合候选是 `{best_robust['rule']}`，其后 3 日现货均值 {best_robust['avg_spot_ret_3d'] * 100:.2f}%。"
        )
    else:
        lines.append("- 当前组合候选样本仍偏少，更适合先做影子观察。")
    if ordered_warning and float(ordered_warning[0]["strategy_sharpe"]) > float(shrink["strategy_sharpe"]):
        lines.append(
            f"- 在“触发即先降一档”的简单口径下，`{ordered_warning[0]['label']}` 已经跑赢第二确认，说明组合预警开始具备前置价值。"
        )
    else:
        lines.append("- 在当前简单口径下，组合预警仍未跑赢第二确认；它更适合继续作为候选第一预警，而不是直接替代主规则。")
    lines.append("- 这份报告主要回答：把 `M1-M2 前端塌陷` 和 `500ETF 期权代理` 组合起来，是否比单独看其中一条更像第一层预警。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame, _qvix_thresholds = _prepare_frame(args.start, args.end, args.data_cache_dir, args.refresh_data_cache)
    frame = _add_forward_returns(frame)
    thresholds = _compute_combo_thresholds(frame)
    ranked_rules = _search_combo_rules(frame, thresholds)

    high_carry = frame["annualized_carry"] >= float(frame["annualized_carry"].dropna().quantile(0.80))
    base_signal = pd.Series(0.5, index=frame.index)
    base_signal.loc[high_carry & (frame["trend_intact"] == 1)] = 1.0

    shrink_triggers, _ = _build_candidate_triggers(frame)
    shrink_trigger = shrink_triggers["趋势破坏+单日弱势"]
    shrink_signal = base_signal.copy()
    shrink_signal.loc[shrink_trigger] = shrink_signal.loc[shrink_trigger].map(lambda value: 0.5 if value >= 1.0 else 0.0)

    warning_rows = [
        _evaluate_warning_overlay(frame, str(item["rule"]), base_signal, item["mask"])
        for item in ranked_rules
    ]
    base = _evaluate_base_strategy(frame, "原始高贴水满仓/普通半仓", base_signal)
    shrink = _evaluate_base_strategy(frame, "第二确认(趋势破坏+单日弱势)", shrink_signal)

    report = _build_report(
        start=args.start,
        end=args.end,
        ranked_rules=ranked_rules,
        warning_rows=warning_rows,
        base=base,
        shrink=shrink,
        thresholds=thresholds,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC期权代理与期限结构组合验证报告.md"
    latest_path = output_dir / "latest_ic_option_term_combo_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC期权代理与期限结构组合验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
