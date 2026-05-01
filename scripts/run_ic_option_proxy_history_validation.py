#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate historical 500ETF option-proxy warnings for the IC carry workflow."""

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
from run_ic_put_hedge_qvix_proxy_validation import _prepare_frame  # noqa: E402


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Validate historical 500ETF option proxy warnings.")
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


def _add_forward_returns(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    df["spot_ret_1d_fwd"] = df["spot_close"].shift(-1) / df["spot_close"] - 1.0
    df["spot_ret_3d_fwd"] = df["spot_close"].shift(-3) / df["spot_close"] - 1.0
    df["qvix_jump_pct_hist"] = (df["qvix_known"] / df["qvix_known"].shift(1) - 1.0) * 100.0
    return df


def _search_proxy_rules(frame: pd.DataFrame) -> tuple[list[dict[str, object]], dict[str, float]]:
    qvix_z_thresholds = [1.0, 1.5, 2.0, 2.5]
    jump_abs = frame["qvix_jump_pct_hist"].abs().dropna()
    jump_thresholds = [float(jump_abs.quantile(q)) for q in (0.70, 0.80, 0.90)] if not jump_abs.empty else [2.0, 3.0, 4.0]
    front_gap_threshold = float(frame["front_end_gap"].dropna().quantile(0.80))

    rows: list[dict[str, object]] = []
    for z_thr in qvix_z_thresholds:
        z_mask = frame["qvix_z"] >= z_thr
        if z_mask.any():
            rows.append(_rule_stats(frame, f"QVIX zscore>={z_thr:.1f}", z_mask))
        for jump_thr in jump_thresholds:
            jump_mask = frame["qvix_jump_pct_hist"].abs() >= jump_thr
            if jump_mask.any():
                rows.append(_rule_stats(frame, f"|QVIX jump|>={jump_thr:.2f}%", jump_mask))
            combo = z_mask & jump_mask
            if combo.any():
                rows.append(_rule_stats(frame, f"QVIX zscore>={z_thr:.1f} + |jump|>={jump_thr:.2f}%", combo))
            front_combo = z_mask & (frame["front_end_gap"] >= front_gap_threshold)
            if front_combo.any():
                rows.append(_rule_stats(frame, f"QVIX zscore>={z_thr:.1f} + 前端塌陷", front_combo))
            triple = z_mask & jump_mask & (frame["front_end_gap"] >= front_gap_threshold)
            if triple.any():
                rows.append(_rule_stats(frame, f"QVIX zscore>={z_thr:.1f} + |jump|>={jump_thr:.2f}% + 前端塌陷", triple))

    dedup: dict[str, dict[str, object]] = {}
    for item in rows:
        dedup[str(item["rule"])] = item
    ranked = sorted(
        dedup.values(),
        key=lambda item: (
            float(item["avg_spot_ret_3d"]),
            float(item["avg_spot_ret_1d"]),
            -int(item["sample_days"]),
        ),
    )
    return ranked, {"front_gap_threshold": front_gap_threshold}


def _rule_stats(frame: pd.DataFrame, rule: str, mask: pd.Series) -> dict[str, object]:
    sample = frame[mask].copy()
    sample = sample.dropna(subset=["spot_ret_1d_fwd", "spot_ret_3d_fwd"])
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
        "mask": mask,
    }


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
    qvix_thresholds: dict[str, float],
) -> str:
    robust_rows = [item for item in ranked_rules if int(item["sample_days"]) >= 5]
    ordered_warning = sorted(warning_rows, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC期权代理历史预警验证报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 目标: 在不等盘中实盘样本的前提下，先用公开历史 QVIX + 期限结构回放 500ETF 期权代理预警的历史区分度。",
        "- 期权代理历史源: 500ETF QVIX 日度历史（前一交易日已知）",
        f"- QVIX 分位: p50={qvix_thresholds['qvix_p50']:.2f} / p80={qvix_thresholds['qvix_p80']:.2f} / p95={qvix_thresholds['qvix_p95']:.2f}",
        f"- 前端塌陷阈值: front_end_gap >= {thresholds['front_gap_threshold']:.2%}",
        "",
        "## 历史预警规则搜索（越负越像成功预警）",
        "",
        "| 规则 | 样本日数 | 次日现货均值 | 3日现货均值 | 次日下跌率 | 3日下跌率 | 平均QVIX z | 平均QVIX jump | 平均前端塌陷 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    if not ranked_rules:
        lines.append("| 无可用样本 | 0 | - | - | - | - | - | - | - |")
    else:
        for item in ranked_rules[:12]:
            rule_label = str(item["rule"]).replace("|", "\\|")
            lines.append(
                f"| {rule_label} | {item['sample_days']} | {item['avg_spot_ret_1d'] * 100:.2f}% | "
                f"{item['avg_spot_ret_3d'] * 100:.2f}% | {item['down_rate_1d'] * 100:.1f}% | "
                f"{item['down_rate_3d'] * 100:.1f}% | {item['avg_qvix_z']:.2f} | "
                f"{item['avg_qvix_jump_pct']:.2f}% | {item['avg_front_end_gap']:.2%} |"
            )

    lines.extend(
        [
            "",
            "## 预警降一档策略比较",
            "",
            f"- 原始高贴水满仓/普通半仓: Sharpe {base['strategy_sharpe']:.2f} | 收益 {base['strategy_total_return'] * 100:.2f}%",
            f"- 第二确认(趋势破坏+单日弱势): Sharpe {shrink['strategy_sharpe']:.2f} | 收益 {shrink['strategy_total_return'] * 100:.2f}%",
            "",
            "| 预警规则 | 触发天数 | 策略收益 | 最大回撤 | Sharpe | 平均仓位 |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in ordered_warning[:6]:
        rule_label = str(item["label"]).replace("|", "\\|")
        lines.append(
            f"| {rule_label} | {item['trigger_days']} | {item['strategy_total_return'] * 100:.2f}% | "
            f"{item['strategy_max_drawdown'] * 100:.2f}% | {item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} |"
        )

    lines.extend(["", "## 结论", ""])
    if robust_rows:
        best_robust = robust_rows[0]
        lines.append(
            f"- 若要求至少 5 个样本日，当前更稳的期权代理候选是 `{best_robust['rule']}`，其后 3 日现货均值 {best_robust['avg_spot_ret_3d'] * 100:.2f}%。"
        )
    else:
        lines.append("- 当前公开可得的 QVIX 历史代理样本仍偏稀，暂时更适合作为候选观察，不适合直接升为主预警。")
    if ordered_warning and float(ordered_warning[0]["strategy_sharpe"]) > float(shrink["strategy_sharpe"]):
        lines.append(
            f"- 在“触发即先降一档”的简单执行口径下，`{ordered_warning[0]['label']}` 已经跑赢第二确认规则，说明期权代理有潜力前置。"
        )
    else:
        lines.append("- 在当前简单口径下，期权代理预警还没有跑赢第二确认规则，更适合继续做候选第一预警，而不是直接替代主规则。")
    lines.append("- 这份报告主要回答：只靠公开历史 QVIX，500ETF 期权代理有没有可能成为早于 `趋势破坏+单日弱势` 的候选预警。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame, qvix_thresholds = _prepare_frame(args.start, args.end, args.data_cache_dir, args.refresh_data_cache)
    frame = _add_forward_returns(frame)

    high_carry = frame["annualized_carry"] >= float(frame["annualized_carry"].dropna().quantile(0.80))
    base_signal = pd.Series(0.5, index=frame.index)
    base_signal.loc[high_carry & (frame["trend_intact"] == 1)] = 1.0

    shrink_triggers, _ = _build_candidate_triggers(frame)
    shrink_trigger = shrink_triggers["趋势破坏+单日弱势"]
    shrink_signal = base_signal.copy()
    shrink_signal.loc[shrink_trigger] = shrink_signal.loc[shrink_trigger].map(lambda value: 0.5 if value >= 1.0 else 0.0)

    ranked_rules, thresholds = _search_proxy_rules(frame)
    warning_rows: list[dict[str, object]] = []
    for item in ranked_rules[:6]:
        trigger = item["mask"]
        warning_rows.append(_evaluate_warning_overlay(frame, str(item["rule"]), base_signal, trigger))

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
        qvix_thresholds=qvix_thresholds,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC期权代理历史预警验证报告.md"
    latest_path = output_dir / "latest_ic_option_proxy_history_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC期权代理历史预警验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
