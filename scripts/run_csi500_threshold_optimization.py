#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Threshold optimization for the CSI500 / IC line on 5-year daily data."""

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

from backtest_next_production_metaphysical_model import (  # noqa: E402
    _build_candidate_frame,
    _build_target,
    _compute_quant_and_resonance_features,
    _evaluate_probability_frame,
    _generate_walk_forward_probabilities,
)
from src.models.metaphysical import NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES  # noqa: E402
from src.services.qlib_local_history import (  # noqa: E402
    find_latest_bootstrapped_qlib_root,
    load_qlib_daily_ohlcv,
)


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="Optimize CSI500/IC thresholds on 5-year daily data.")
    parser.add_argument("--start", default=default_start)
    parser.add_argument("--end", default=default_end)
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "xgb_cache"),
        help="Cache directory for metaphysical feature calculations.",
    )
    parser.add_argument(
        "--probability-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "metaphysical_probabilities"),
        help="Directory used to cache walk-forward probability frames.",
    )
    parser.add_argument(
        "--refresh-probability-cache",
        action="store_true",
        help="Rebuild the walk-forward probability cache before grid search.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "reports" / "backtests"),
        help="Directory where the markdown report will be written.",
    )
    return parser


def _load_csi500_proxy(start: str, end: str) -> pd.DataFrame:
    qlib_root = find_latest_bootstrapped_qlib_root()
    if qlib_root is not None:
        frame = load_qlib_daily_ohlcv("000905", qlib_root)
        if frame is not None and not frame.empty:
            frame = frame.copy()
            frame["date"] = pd.to_datetime(frame["date"])
            frame = frame[(frame["date"] >= pd.Timestamp(start)) & (frame["date"] <= pd.Timestamp(end))].copy()
            if not frame.empty:
                return frame[["date", "open", "close", "high", "low", "volume"]].sort_values("date").reset_index(drop=True)

    import yfinance as yf

    ticker = yf.Ticker("510500.SS")
    frame = ticker.history(start=start, end=end, auto_adjust=True)
    if frame is None or frame.empty:
        raise RuntimeError("未获取到 510500.SS 的日线数据")
    frame = frame.reset_index().rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    frame = frame[["date", "open", "close", "high", "low", "volume"]].copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.tz_localize(None)
    return frame.sort_values("date").reset_index(drop=True)


def _prepare_frame(start: str, end: str, cache_dir: str) -> pd.DataFrame:
    frame = _load_csi500_proxy(start, end)
    feature_df = _compute_quant_and_resonance_features(frame)
    feature_df = _build_target(feature_df)
    candidate_df = _build_candidate_frame(feature_df, cache_dir=cache_dir)
    candidate_df = candidate_df.loc[:, ~candidate_df.columns.duplicated()].copy()
    for feature in NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES:
        if feature not in candidate_df.columns:
            candidate_df[feature] = 0.0
    return candidate_df


def _probability_cache_path(base_dir: Path, start: str, end: str, min_train_days: int, retrain_every: int) -> Path:
    return base_dir / f"csi500_threshold_{start}_{end}_min{min_train_days}_retrain{retrain_every}.pkl"


def _build_report(grid: pd.DataFrame, *, start: str, end: str) -> str:
    top = grid.head(8).copy()
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} 中证500阈值优化报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 基础特征池: NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES",
        "- 标的代理: 本地Qlib优先读取 000905，缺失时回退 510500.SS",
        "- 目标: 给中证500 / IC 这条线单独寻找更合适的 caution / risk_off 阈值",
        "",
        "## Top 阈值组合",
        "",
        "| 排名 | caution | risk_off | 样本数 | 策略收益 | 买入持有收益 | 最大回撤 | Sharpe | 平均仓位 | AUC | AP |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for idx, row in top.reset_index(drop=True).iterrows():
        lines.append(
            f"| {idx + 1} | {row['caution_threshold']:.2f} | {row['risk_off_threshold']:.2f} | {int(row['sample_count'])} | "
            f"{row['strategy_total_return'] * 100:.2f}% | {row['buy_hold_total_return'] * 100:.2f}% | "
            f"{row['strategy_max_drawdown'] * 100:.2f}% | {row['strategy_sharpe']:.2f} | {row['avg_position']:.2f} | "
            f"{row['auc']:.4f} | {row['ap']:.4f} |"
        )

    best = top.iloc[0]
    default_row = grid[(grid["caution_threshold"] == 0.40) & (grid["risk_off_threshold"] == 0.60)]
    lines.extend(["", "## 结论", ""])
    lines.append(
        f"- 当前最优阈值组合是 caution={best['caution_threshold']:.2f}, risk_off={best['risk_off_threshold']:.2f}，Sharpe {best['strategy_sharpe']:.2f}。"
    )
    if not default_row.empty:
        default = default_row.iloc[0]
        lines.append(
            f"- 当前默认阈值 0.40 / 0.60 的 Sharpe 是 {default['strategy_sharpe']:.2f}，策略收益 {default['strategy_total_return'] * 100:.2f}%。"
        )
        if float(best["strategy_sharpe"]) > float(default["strategy_sharpe"]):
            lines.append("- 说明中证500 / IC 这条线确实值得单独阈值优化，而不应完全沿用全局默认值。")
        else:
            lines.append("- 说明当前默认阈值已经接近最优，中证500线的问题更多在因子和标签，而不是阈值本身。")
    if float(best["avg_position"]) < 0.95:
        lines.append("- 最优组合会更积极地收缩仓位，说明这条线的核心价值更偏风控，而不是进攻增厚。")
    else:
        lines.append("- 最优组合平均仓位仍接近满仓，说明当前模型对中证500线的收缩能力还偏弱。")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame = _prepare_frame(args.start, args.end, args.cache_dir)
    cache_dir = Path(args.probability_cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = _probability_cache_path(cache_dir, args.start, args.end, 756, 42)
    usable = _generate_walk_forward_probabilities(
        frame,
        min_train_days=756,
        retrain_every=42,
        cache_path=cache_path,
        refresh_cache=args.refresh_probability_cache,
    )
    if usable.empty and not args.refresh_probability_cache:
        usable = _generate_walk_forward_probabilities(
            frame,
            min_train_days=756,
            retrain_every=42,
            cache_path=cache_path,
            refresh_cache=True,
        )
    if usable.empty:
        raise RuntimeError("walk-forward 概率为空，无法做阈值优化")

    rows = []
    for caution_threshold in [0.20, 0.25, 0.30, 0.35, 0.40, 0.45]:
        for risk_off_threshold in [0.45, 0.50, 0.55, 0.60, 0.65, 0.70]:
            if caution_threshold >= risk_off_threshold:
                continue
            _, metrics = _evaluate_probability_frame(
                usable,
                risk_off_threshold=risk_off_threshold,
                caution_threshold=caution_threshold,
            )
            rows.append(metrics)
    grid = pd.DataFrame(rows).sort_values(
        ["strategy_sharpe", "strategy_total_return", "strategy_max_drawdown"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    report = _build_report(grid, start=args.start, end=args.end)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_中证500阈值优化报告.md"
    latest_path = output_dir / "latest_csi500_threshold_optimization.md"
    csv_path = output_dir / f"{report_date}_中证500阈值优化表.csv"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")
    grid.to_csv(csv_path, index=False)

    print("=" * 72)
    print("中证500阈值优化完成")
    print(f"报告: {report_path}")
    print(f"表格: {csv_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
