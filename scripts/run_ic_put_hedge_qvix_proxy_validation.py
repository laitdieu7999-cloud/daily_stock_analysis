#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate IC put overlays using 500ETF QVIX as a historical volatility proxy."""

from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
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
    parser = argparse.ArgumentParser(description="Validate IC put overlays with historical 500ETF QVIX proxy.")
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
        help="Ignore local caches and rebuild them from data sources.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "reports" / "backtests"),
        help="Directory where the markdown report will be written.",
    )
    return parser


def _load_qvix_history(cache_dir: Path, refresh_cache: bool) -> pd.DataFrame:
    cache_path = cache_dir / "500etf_qvix_daily.pkl"
    if cache_path.exists() and not refresh_cache:
        try:
            return pd.read_pickle(cache_path)
        except Exception:
            pass

    raw = ak.index_option_500etf_qvix()
    df = raw.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"]).sort_values("date")
    df = df[["date", "close"]].rename(columns={"close": "qvix_close"}).reset_index(drop=True)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(cache_path)
    return df


def _prepare_frame(start: str, end: str, data_cache_dir: str, refresh_data_cache: bool) -> tuple[pd.DataFrame, dict[str, float]]:
    cache_dir = Path(data_cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    frame = _build_term_structure_frame(start, end, data_cache_dir, refresh_data_cache)
    qvix = _load_qvix_history(cache_dir, refresh_data_cache)
    qvix = qvix[(qvix["date"] >= pd.Timestamp(start)) & (qvix["date"] <= pd.Timestamp(end))].copy()
    merged = frame.merge(qvix, on="date", how="left").sort_values("date").reset_index(drop=True)
    merged["qvix_close"] = merged["qvix_close"].ffill()
    qvix_median = float(merged["qvix_close"].median()) if merged["qvix_close"].notna().any() else 22.0
    merged["qvix_close"] = merged["qvix_close"].fillna(qvix_median)
    merged["qvix_known"] = merged["qvix_close"].shift(1).fillna(qvix_median)

    rolling_median = merged["qvix_close"].rolling(20, min_periods=5).median().shift(1)
    rolling_std = merged["qvix_close"].rolling(20, min_periods=5).std().shift(1)
    rolling_std = rolling_std.replace(0.0, pd.NA).fillna(float(merged["qvix_close"].std()) or 1.0)
    merged["qvix_z"] = ((merged["qvix_known"] - rolling_median.fillna(qvix_median)) / rolling_std).clip(-3.0, 3.0)

    thresholds = {
        "qvix_p50": float(merged["qvix_close"].quantile(0.50)),
        "qvix_p80": float(merged["qvix_close"].quantile(0.80)),
        "qvix_p95": float(merged["qvix_close"].quantile(0.95)),
    }
    return merged, thresholds


def _trigger_profile(label: str) -> tuple[float, float, float]:
    """Return premium multiplier, default strike buffer, and late-trigger uplift."""
    if label == "趋势破坏":
        return 0.40, 0.015, 0.0000
    if label == "趋势破坏+平坦斜率":
        return 0.43, 0.010, 0.0005
    if label == "趋势破坏+低贴水":
        return 0.43, 0.010, 0.0005
    if label == "趋势破坏+平坦斜率+低贴水":
        return 0.46, 0.005, 0.0010
    if label == "趋势破坏+单日弱势":
        return 0.50, 0.000, 0.0015
    return 0.43, 0.010, 0.0005


def _evaluate_qvix_put_overlay(
    df: pd.DataFrame,
    *,
    label: str,
    signal_position: pd.Series,
    hedge_signal: pd.Series,
    qvix_thresholds: dict[str, float],
) -> dict[str, object]:
    result = df.copy()
    result["signal_position"] = signal_position.reindex(result.index).fillna(0.0)
    result["position"] = result["signal_position"].shift(1).fillna(0.0)
    result["hedge_signal"] = hedge_signal.reindex(result.index).fillna(False)
    result["hedge_active"] = result["hedge_signal"].shift(1).fillna(False)

    premium_mult, base_buffer, late_uplift = _trigger_profile(label)
    daily_sigma = (result["qvix_known"] / 100.0) / math.sqrt(252.0)
    stress_scale = (
        1.0
        + 0.15 * result["qvix_z"].clip(lower=0.0)
        + 0.10 * (result["qvix_known"] >= qvix_thresholds["qvix_p80"]).astype(float)
        + 0.15 * (result["qvix_known"] >= qvix_thresholds["qvix_p95"]).astype(float)
    )
    dynamic_premium = (daily_sigma * premium_mult * stress_scale + late_uplift).clip(lower=0.0005, upper=0.03)

    strike_buffer = pd.Series(base_buffer, index=result.index, dtype=float)
    strike_buffer.loc[result["qvix_known"] >= qvix_thresholds["qvix_p80"]] = max(base_buffer - 0.005, 0.0)
    strike_buffer.loc[result["qvix_known"] < qvix_thresholds["qvix_p50"]] = base_buffer + 0.005

    underlying_ret = result["spot_ret_1d"].fillna(0.0)
    put_payoff_unit = (-underlying_ret - strike_buffer).clip(lower=0.0) - dynamic_premium
    result["hedge_return"] = (
        result["hedge_active"].astype(float)
        * result["position"]
        * put_payoff_unit
    )
    result["strategy_return"] = result["position"] * result["tradable_return_1d"].fillna(0.0) + result["hedge_return"]
    result["equity"] = (1.0 + result["strategy_return"]).cumprod()
    result["buy_hold_return"] = result["tradable_return_1d"].fillna(0.0)
    result["buy_hold_equity"] = (1.0 + result["buy_hold_return"]).cumprod()

    hedged_days = int(result["hedge_active"].sum())
    active_mask = result["hedge_active"].astype(bool)
    active_premium = float(dynamic_premium[active_mask].mean()) if hedged_days else 0.0
    active_qvix = float(result.loc[active_mask, "qvix_known"].mean()) if hedged_days else 0.0
    return {
        "label": label,
        "sample_count": int(len(result)),
        "strategy_total_return": float(result["equity"].iloc[-1] - 1.0),
        "buy_hold_total_return": float(result["buy_hold_equity"].iloc[-1] - 1.0),
        "strategy_max_drawdown": _max_drawdown(result["equity"]),
        "strategy_sharpe": _annualized_sharpe(result["strategy_return"]),
        "avg_position": float(result["position"].mean()),
        "hedged_days": hedged_days,
        "avg_dynamic_premium": active_premium,
        "avg_qvix": active_qvix,
        "avg_strike_buffer": float(strike_buffer[active_mask].mean()) if hedged_days else 0.0,
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
        f"# {datetime.now().strftime('%Y-%m-%d')} IC认沽Qvix代理验证报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 目标: 用 500ETF QVIX 历史替代纯手工成本假设，重新检验“认沽应否早于减仓触发”",
        "- 期权成本代理: 500ETF QVIX 日度收盘值（前一交易日已知），映射为 1 日保护成本",
        f"- QVIX 分位: p50={thresholds['qvix_p50']:.2f} / p80={thresholds['qvix_p80']:.2f} / p95={thresholds['qvix_p95']:.2f}",
        f"- 单日弱势阈值: {thresholds['weak_threshold']:.2%}",
        f"- 低贴水阈值: {thresholds['carry_q20']:.2%}",
        f"- 平坦斜率阈值: {thresholds['flat_threshold']:.2%}",
        "",
        "## 基线比较",
        "",
        f"- 原始高贴水满仓/普通半仓: Sharpe {baseline['strategy_sharpe']:.2f} | 收益 {baseline['strategy_total_return'] * 100:.2f}%",
        f"- 纯减仓规则(趋势破坏+单日弱势): Sharpe {shrink['strategy_sharpe']:.2f} | 收益 {shrink['strategy_total_return'] * 100:.2f}%",
        "",
        "## QVIX 驱动的认沽先行触发器",
        "",
        "| 触发器 | 对冲天数 | 平均QVIX | 平均动态成本 | 平均缓冲 | 策略收益 | 最大回撤 | Sharpe |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in ranked:
        lines.append(
            f"| {item['label']} | {item['hedged_days']} | {item['avg_qvix']:.2f} | "
            f"{item['avg_dynamic_premium'] * 100:.2f}% | {item['avg_strike_buffer'] * 100:.2f}% | "
            f"{item['strategy_total_return'] * 100:.2f}% | {item['strategy_max_drawdown'] * 100:.2f}% | "
            f"{item['strategy_sharpe']:.2f} |"
        )
    best = ranked[0]
    lines.extend(["", "## 结论", ""])
    if float(best["strategy_sharpe"]) > float(shrink["strategy_sharpe"]):
        lines.append(
            f"- 在 QVIX 历史代理下，当前最优先行触发器是 `{best['label']}`，Sharpe {best['strategy_sharpe']:.2f}，优于纯减仓 {shrink['strategy_sharpe']:.2f}。"
        )
    else:
        lines.append(
            "- 在 QVIX 历史代理下，认沽先行方案仍未跑赢纯减仓，说明“认沽应先于减仓”这条线需要更真实的权利金/执行口径才能站稳。"
        )
    lines.append("- 这份报告更适合回答：如果把认沽成本锚定到真实历史波动率水平，保护优先到底还值不值得做。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame, qvix_thresholds = _prepare_frame(args.start, args.end, args.data_cache_dir, args.refresh_data_cache)
    high_carry = frame["annualized_carry"] >= float(frame["annualized_carry"].dropna().quantile(0.80))
    base_signal = pd.Series(0.5, index=frame.index)
    base_signal.loc[high_carry & (frame["trend_intact"] == 1)] = 1.0

    triggers, thresholds = _build_candidate_triggers(frame)
    thresholds.update(qvix_thresholds)
    shrink_trigger = triggers["趋势破坏+单日弱势"]
    shrink_signal = base_signal.copy()
    shrink_signal.loc[shrink_trigger] = shrink_signal.loc[shrink_trigger].map(lambda value: 0.5 if value >= 1.0 else 0.0)

    baseline = _evaluate_base_strategy(frame, "原始高贴水满仓/普通半仓", base_signal)
    shrink = _evaluate_base_strategy(frame, "纯减仓规则(趋势破坏+单日弱势)", shrink_signal)

    rows: list[dict[str, object]] = []
    for label, trigger in triggers.items():
        rows.append(
            _evaluate_qvix_put_overlay(
                frame,
                label=label,
                signal_position=base_signal,
                hedge_signal=trigger,
                qvix_thresholds=qvix_thresholds,
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
    report_path = output_dir / f"{report_date}_IC认沽Qvix代理验证报告.md"
    latest_path = output_dir / "latest_ic_put_hedge_qvix_proxy_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC认沽Qvix代理验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
