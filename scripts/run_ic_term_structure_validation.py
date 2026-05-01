#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate IC term-structure signals for long-shrink decisions.

This version keeps the older near-vs-far curve view for compatibility, while
adding two more execution-oriented proxies inspired by the user's recent review:

- M1-M2 annualized spread: front-end structure / first-warning candidate
- Q1-Q2 annualized spread: far-quarter anchor / dividend-season calm proxy
"""

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

from run_ic_basis_overlay_validation import _extract_expiry, _load_cffex_ic_panel  # noqa: E402
from run_ic_full_return_label_validation import _build_tradable_frame  # noqa: E402
from run_ic_roll_carry_validation import _annualized_sharpe, _max_drawdown  # noqa: E402


QUARTER_MONTHS = {3, 6, 9, 12}


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Validate IC term-structure signals.")
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


def _annualized_calendar_spread(
    near_close: float,
    far_close: float,
    near_days: float,
    far_days: float,
) -> float:
    tenor_days = max(float(far_days) - float(near_days), 1.0)
    return float((float(far_close) - float(near_close)) / float(near_close)) / (tenor_days / 365.0)


def _build_term_structure_frame(start: str, end: str, data_cache_dir: str, refresh_data_cache: bool) -> pd.DataFrame:
    frame = _build_tradable_frame(start, end, data_cache_dir, refresh_data_cache)
    cache_dir = Path(data_cache_dir)
    start_year = int(start[:4])
    end_year = int(end[:4])
    futures_frames = _load_cffex_ic_panel(cache_dir, start_year, end_year, refresh_cache=refresh_data_cache)
    panel = pd.concat(futures_frames, ignore_index=True)
    panel = panel[panel["variety"] == "IC"].copy()
    panel["date"] = pd.to_datetime(panel["date"])
    panel = panel[(panel["date"] >= pd.Timestamp(start)) & (panel["date"] <= pd.Timestamp(end))].copy()
    panel["expiry"] = panel["symbol"].apply(_extract_expiry)
    panel = panel.sort_values(["date", "expiry"])

    rows = []
    for date, grp in panel.groupby("date"):
        grp = grp.sort_values("expiry")
        if len(grp) < 2:
            continue
        near = grp.iloc[0]
        next_month = grp.iloc[1]
        far = grp.iloc[-1]
        quarter_grp = grp[grp["expiry"].dt.month.isin(QUARTER_MONTHS)].sort_values("expiry")
        quarter_1 = quarter_grp.iloc[0] if len(quarter_grp) >= 1 else None
        quarter_2 = quarter_grp.iloc[1] if len(quarter_grp) >= 2 else None
        row = {
            "date": date,
            "near_symbol": near["symbol"],
            "near_close": near["close"],
            "near_days": (near["expiry"] - date).days,
            "next_symbol": next_month["symbol"],
            "next_close": next_month["close"],
            "next_days": (next_month["expiry"] - date).days,
            "far_symbol": far["symbol"],
            "far_close": far["close"],
            "far_days": (far["expiry"] - date).days,
            "q1_symbol": quarter_1["symbol"] if quarter_1 is not None else None,
            "q1_close": quarter_1["close"] if quarter_1 is not None else None,
            "q1_days": (quarter_1["expiry"] - date).days if quarter_1 is not None else None,
            "q2_symbol": quarter_2["symbol"] if quarter_2 is not None else None,
            "q2_close": quarter_2["close"] if quarter_2 is not None else None,
            "q2_days": (quarter_2["expiry"] - date).days if quarter_2 is not None else None,
        }
        rows.append(row)
    ts = pd.DataFrame(rows)
    merged = frame.merge(ts, on="date", how="inner")
    merged["m1_m2_slope"] = (merged["next_close"] - merged["near_close"]) / merged["near_close"]
    merged["m1_m2_annualized"] = merged.apply(
        lambda row: _annualized_calendar_spread(
            row["near_close"],
            row["next_close"],
            row["near_days"],
            row["next_days"],
        ),
        axis=1,
    )
    merged["far_near_slope"] = (merged["far_close"] - merged["near_close"]) / merged["near_close"]
    merged["far_near_annualized"] = merged.apply(
        lambda row: _annualized_calendar_spread(
            row["near_close"],
            row["far_close"],
            row["near_days"],
            row["far_days"],
        ),
        axis=1,
    )
    merged["q1_q2_slope"] = (merged["q2_close"] - merged["q1_close"]) / merged["q1_close"]
    merged["q1_q2_annualized"] = merged.apply(
        lambda row: _annualized_calendar_spread(
            row["q1_close"],
            row["q2_close"],
            row["q1_days"],
            row["q2_days"],
        ) if pd.notna(row["q1_close"]) and pd.notna(row["q2_close"]) else pd.NA,
        axis=1,
    )
    merged["front_end_gap"] = merged["m1_m2_annualized"] - merged["q1_q2_annualized"]
    return merged


def _evaluate_window(df: pd.DataFrame, mask: pd.Series) -> dict[str, float | int]:
    sample = df[mask].copy()
    if sample.empty:
        return {
            "count": 0,
            "tradable_ret_5d": 0.0,
            "excess_ret_5d": 0.0,
        }
    tradable_5d = sample["tradable_return_1d"].shift(-1).rolling(5).sum()
    return {
        "count": int(len(sample)),
        "tradable_ret_5d": float(tradable_5d.mean()),
        "excess_ret_5d": float(sample["excess_ret_5d"].mean()),
    }


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


def _build_report(
    *,
    windows: dict[str, dict[str, float | int]],
    strategies: list[dict[str, object]],
    start: str,
    end: str,
    front_collapse_threshold: float,
    front_calm_threshold: float,
    q_anchor_median: float,
    q_anchor_band: float,
) -> str:
    ordered = sorted(strategies, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC期限结构验证报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 目标: 判断更贴近衍生品实战的期限结构口径，能否更早地筛出 IC 多头收缩信号。",
        "- 关键口径:",
        "  - `M1-M2 年化贴水差`: 近月/次月前端结构，偏向第一层预警。",
        "  - `Q1-Q2 远季跨期年化率`: 远季锚定结构，偏向分红季镇静剂 / 长端定价参考。",
        f"- 前端塌陷阈值: M1-M2 与 Q1-Q2 的年化差 >= {front_collapse_threshold:.2%}",
        f"- 前端平稳阈值: M1-M2 与 Q1-Q2 的年化差 <= {front_calm_threshold:.2%}",
        f"- 远季锚中位数: {q_anchor_median:.2%}",
        f"- 远季锚稳定带宽: ±{q_anchor_band:.2%}",
        "",
        "## 期限结构窗口统计",
        "",
        "| 窗口 | 样本数 | 可交易5日收益 | 相对现货超额收益 |",
        "| --- | ---: | ---: | ---: |",
    ]
    for label, stats in windows.items():
        lines.append(
            f"| {label} | {stats['count']} | {stats['tradable_ret_5d'] * 100:.2f}% | {stats['excess_ret_5d'] * 100:.2f}% |"
        )
    lines.extend([
        "",
        "## 策略比较",
        "",
        "| 方案 | 样本数 | 策略收益 | 持有主力收益 | 最大回撤 | Sharpe | 平均仓位 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for item in ordered:
        lines.append(
            f"| {item['label']} | {item['sample_count']} | {item['strategy_total_return'] * 100:.2f}% | "
            f"{item['buy_hold_total_return'] * 100:.2f}% | {item['strategy_max_drawdown'] * 100:.2f}% | "
            f"{item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} |"
        )
    best = ordered[0]
    lines.extend(["", "## 结论", ""])
    if windows["前端塌陷+趋势破坏"]["tradable_ret_5d"] < windows["前端平稳+趋势破坏"]["tradable_ret_5d"]:
        lines.append("- 当 M1-M2 前端塌陷叠加趋势破坏时，未来 5 日表现更差，说明“前端塌陷”更像该缩 IC 多头的先行告警。")
    else:
        lines.append("- 前端结构本身还不够稳定，单独拿来当第一触发器还不够强，仍需与趋势或盘中信号联动。")
    lines.append("- Q1-Q2 远季跨期更适合当“长端锚”，帮助区分分红季假性高贴水和真正的前端恐慌。")
    lines.append(f"- 当前最优策略是 `{best['label']}`，Sharpe {best['strategy_sharpe']:.2f}。")
    lines.append("- 这份报告更适合判断“期限结构里，到底是看 M1-M2 还是看远季锚”这个问题。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame = _build_term_structure_frame(args.start, args.end, args.data_cache_dir, args.refresh_data_cache)
    front_collapse_threshold = float(frame["front_end_gap"].dropna().quantile(0.80))
    front_calm_threshold = float(frame["front_end_gap"].dropna().quantile(0.20))
    q_anchor = frame["q1_q2_annualized"].dropna()
    q_anchor_median = float(q_anchor.median())
    q_anchor_band = float((q_anchor - q_anchor_median).abs().median())
    if q_anchor_band <= 0:
        q_anchor_band = float(q_anchor.std(ddof=0) * 0.5) if len(q_anchor) else 0.0
    if q_anchor_band <= 0:
        q_anchor_band = 0.01

    high_carry = frame["annualized_carry"] >= float(frame["annualized_carry"].dropna().quantile(0.80))
    base_signal = pd.Series(0.5, index=frame.index)
    base_signal[high_carry & (frame["trend_intact"] == 1)] = 1.0

    front_collapse = frame["front_end_gap"] >= front_collapse_threshold
    front_calm = frame["front_end_gap"] <= front_calm_threshold
    trend_broken = frame["trend_intact"] == 0
    q_anchor_stable = (frame["q1_q2_annualized"] - q_anchor_median).abs() <= q_anchor_band
    q_anchor_unstable = ~q_anchor_stable

    windows = {
        "前端塌陷(M1-M2拉阔)": _evaluate_window(frame, front_collapse),
        "前端平稳": _evaluate_window(frame, front_calm),
        "前端塌陷+趋势破坏": _evaluate_window(frame, front_collapse & trend_broken),
        "前端平稳+趋势破坏": _evaluate_window(frame, front_calm & trend_broken),
        "远季锚稳定+高贴水+趋势完整": _evaluate_window(frame, q_anchor_stable & high_carry & (frame["trend_intact"] == 1)),
        "远季锚失稳+趋势破坏": _evaluate_window(frame, q_anchor_unstable & trend_broken),
    }

    strategies = [
        _evaluate_strategy(frame, "高贴水满仓/普通半仓", base_signal),
        _evaluate_strategy(frame, "基线+前端塌陷时降一档", base_signal.mask(front_collapse, 0.0)),
        _evaluate_strategy(frame, "基线+前端塌陷且趋势破坏时降一档", base_signal.mask(front_collapse & trend_broken, 0.0)),
        _evaluate_strategy(frame, "基线+前端塌陷且远季锚失稳时降一档", base_signal.mask(front_collapse & q_anchor_unstable, 0.0)),
        _evaluate_strategy(frame, "基线+前端平稳且趋势破坏时降一档", base_signal.mask(front_calm & trend_broken, 0.0)),
    ]

    report = _build_report(
        windows=windows,
        strategies=strategies,
        start=args.start,
        end=args.end,
        front_collapse_threshold=front_collapse_threshold,
        front_calm_threshold=front_calm_threshold,
        q_anchor_median=q_anchor_median,
        q_anchor_band=q_anchor_band,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC期限结构验证报告.md"
    latest_path = output_dir / "latest_ic_term_structure_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC期限结构验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
