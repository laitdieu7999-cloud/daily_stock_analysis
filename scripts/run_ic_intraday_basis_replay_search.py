#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Replay historical intraday IC basis proxy signals on minute data."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay historical intraday IC basis proxy signals.")
    parser.add_argument("--ic-symbol", default="IC0", help="IC minute symbol for Sina futures API.")
    parser.add_argument("--spot-symbol", default="sz399905", help="Spot minute symbol for Akshare stock minute API.")
    parser.add_argument("--period", default="5", help="Minute bar period to replay.")
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "reports" / "backtests"),
        help="Directory where the markdown report will be written.",
    )
    return parser


def _load_futures_minute(symbol: str, period: str) -> pd.DataFrame:
    df = ak.futures_zh_minute_sina(symbol=symbol, period=period)
    if df is None or df.empty:
        raise RuntimeError(f"未获取到 {symbol} 分钟数据")
    frame = df.copy()
    frame["ts"] = pd.to_datetime(frame["datetime"])
    frame["futures_close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame[["ts", "futures_close"]].dropna()


def _load_spot_minute(symbol: str, period: str) -> pd.DataFrame:
    df = ak.stock_zh_a_minute(symbol=symbol, period=period, adjust="qfq")
    if df is None or df.empty:
        raise RuntimeError(f"未获取到 {symbol} 分钟数据")
    frame = df.copy()
    frame["ts"] = pd.to_datetime(frame["day"])
    frame["spot_close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame[["ts", "spot_close"]].dropna()


def _prepare_frame(ic_symbol: str, spot_symbol: str, period: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    fut = _load_futures_minute(ic_symbol, period)
    spot = _load_spot_minute(spot_symbol, period)
    merged = fut.merge(spot, on="ts", how="inner").sort_values("ts").reset_index(drop=True)
    if merged.empty:
        raise RuntimeError("期货与现货分钟数据未对齐出有效样本")

    merged["trade_date"] = merged["ts"].dt.normalize()
    merged["basis"] = merged["spot_close"] - merged["futures_close"]
    merged["basis_pct"] = merged["basis"] / merged["futures_close"] * 100.0
    merged["basis_jump"] = merged.groupby("trade_date")["basis_pct"].diff().fillna(0.0)
    merged["intraday_mean"] = merged.groupby("trade_date")["basis_pct"].expanding().mean().reset_index(level=0, drop=True)
    merged["intraday_std"] = (
        merged.groupby("trade_date")["basis_pct"].expanding().std().reset_index(level=0, drop=True).fillna(0.0)
    )
    merged["basis_zscore"] = (
        (merged["basis_pct"] - merged["intraday_mean"]) / merged["intraday_std"].replace(0.0, pd.NA)
    ).fillna(0.0)

    daily = (
        merged.groupby("trade_date", as_index=False)
        .agg(spot_close=("spot_close", "last"))
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    daily["t1_ret"] = daily["spot_close"].shift(-1) / daily["spot_close"] - 1.0
    daily["t3_ret"] = daily["spot_close"].shift(-3) / daily["spot_close"] - 1.0
    return merged, daily


def _search_thresholds(frame: pd.DataFrame, daily: pd.DataFrame) -> list[dict[str, float | int | str]]:
    jump_abs = frame["basis_jump"].abs()
    jump_thresholds = [float(jump_abs.quantile(q)) for q in (0.70, 0.80, 0.90)]
    zscore_thresholds = [1.5, 2.0, 2.5]
    cutoffs = [
        ("全天", None),
        ("14:30前", "14:30:00"),
        ("14:00前", "14:00:00"),
    ]

    rows: list[dict[str, float | int | str]] = []
    for z_thr in zscore_thresholds:
        for jump_thr in jump_thresholds:
            for cutoff_label, cutoff in cutoffs:
                scoped = frame.copy()
                if cutoff is not None:
                    scoped = scoped[scoped["ts"].dt.strftime("%H:%M:%S") <= cutoff]
                triggered = scoped[
                    (scoped["basis_zscore"] >= z_thr) & (scoped["basis_jump"].abs() >= jump_thr)
                ]
                trigger_days = (
                    triggered.groupby("trade_date", as_index=False)
                    .agg(
                        trigger_count=("ts", "size"),
                        max_basis_zscore=("basis_zscore", "max"),
                        max_basis_jump=("basis_jump", lambda s: float(s.abs().max())),
                        max_basis_pct=("basis_pct", "max"),
                    )
                )
                if trigger_days.empty:
                    continue
                merged_days = trigger_days.merge(daily, on="trade_date", how="left").dropna(subset=["t1_ret", "t3_ret"])
                if merged_days.empty:
                    continue
                rows.append(
                    {
                        "trigger_rule": f"z>={z_thr:.1f} & |jump|>={jump_thr:.3f} ({cutoff_label})",
                        "sample_days": int(len(merged_days)),
                        "avg_t1_ret": float(merged_days["t1_ret"].mean()),
                        "avg_t3_ret": float(merged_days["t3_ret"].mean()),
                        "t1_win_rate": float((merged_days["t1_ret"] > 0).mean()),
                        "t3_win_rate": float((merged_days["t3_ret"] > 0).mean()),
                        "avg_max_zscore": float(merged_days["max_basis_zscore"].mean()),
                        "avg_max_jump": float(merged_days["max_basis_jump"].mean()),
                        "avg_max_basis_pct": float(merged_days["max_basis_pct"].mean()),
                    }
                )
    return sorted(
        rows,
        key=lambda item: (item["avg_t1_ret"], item["avg_t3_ret"], item["sample_days"]),
        reverse=True,
    )


def _build_report(
    *,
    intraday_frame: pd.DataFrame,
    daily: pd.DataFrame,
    ranked_rows: list[dict[str, float | int | str]],
    ic_symbol: str,
    spot_symbol: str,
    period: str,
) -> str:
    start = intraday_frame["ts"].min().strftime("%Y-%m-%d %H:%M")
    end = intraday_frame["ts"].max().strftime("%Y-%m-%d %H:%M")
    robust_rows = [item for item in ranked_rows if int(item["sample_days"]) >= 4]
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} IC历史盘中基差回放搜索报告",
        "",
        f"- 回放区间: {start} 至 {end}",
        f"- 数据源: {ic_symbol} {period}分钟 + {spot_symbol} {period}分钟",
        "- 目标: 不等新样本自然积累，先直接回放历史盘中基差代理样本，搜索更早期的异常阈值",
        "- 口径: 以 `basis_pct` 的日内 zscore 与单根跳升作为盘中异常代理，用次日/3日现货表现做方向验证",
        "",
        "## 样本概览",
        "",
        f"- 分钟样本数: {len(intraday_frame)}",
        f"- 覆盖交易日: {daily['trade_date'].nunique()}",
        f"- basis_pct 区间: {intraday_frame['basis_pct'].min():.2f}% ~ {intraday_frame['basis_pct'].max():.2f}%",
        "",
        "## 候选阈值搜索（按次日/3日收益从好到差排序）",
        "",
        "| 规则 | 样本日数 | 次日均值 | 3日均值 | 次日胜率 | 3日胜率 | 平均最大zscore | 平均最大跳升 | 平均最大basis_pct |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    if not ranked_rows:
        lines.append("| 无可用样本 | 0 | - | - | - | - | - | - | - |")
    else:
        for item in ranked_rows[:12]:
            rule_label = str(item["trigger_rule"]).replace("|", "\\|")
            lines.append(
                f"| {rule_label} | {item['sample_days']} | {item['avg_t1_ret'] * 100:.2f}% | "
                f"{item['avg_t3_ret'] * 100:.2f}% | {item['t1_win_rate'] * 100:.1f}% | {item['t3_win_rate'] * 100:.1f}% | "
                f"{item['avg_max_zscore']:.2f} | {item['avg_max_jump']:.3f} | {item['avg_max_basis_pct']:.2f}% |"
            )
        best = ranked_rows[0]
        robust_best = robust_rows[0] if robust_rows else None
        lines.extend(
            [
                "",
                "## 结论",
                "",
                f"- 全量最优阈值是 `{best['trigger_rule']}`，但它只有 `{best['sample_days']}` 个样本日，更适合先当候选观察，不适合直接当主规则。",
            ]
        )
        if robust_best is not None:
            lines.extend(
                [
                    f"- 若要求至少 `4` 个样本日，当前更稳的候选是 `{robust_best['trigger_rule']}`。",
                    f"- 这条更稳候选对应的次日均值 `{robust_best['avg_t1_ret'] * 100:.2f}%`，3日均值 `{robust_best['avg_t3_ret'] * 100:.2f}%`。",
                ]
            )
        lines.extend(
            [
                "- 这份报告只是盘中异常代理的历史回放，不等于已经替代了 `趋势破坏 + 单日弱势`，但它已经能帮助我们先筛出最值得继续累积样本的早期盘中阈值。",
            ]
        )
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    intraday_frame, daily = _prepare_frame(args.ic_symbol, args.spot_symbol, args.period)
    ranked_rows = _search_thresholds(intraday_frame, daily)
    report = _build_report(
        intraday_frame=intraday_frame,
        daily=daily,
        ranked_rows=ranked_rows,
        ic_symbol=args.ic_symbol,
        spot_symbol=args.spot_symbol,
        period=args.period,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_IC历史盘中基差回放搜索报告.md"
    latest_path = output_dir / "latest_ic_intraday_basis_replay_search.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("IC历史盘中基差回放搜索完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
