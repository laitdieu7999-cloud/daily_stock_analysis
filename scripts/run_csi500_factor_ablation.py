#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Targeted factor ablation for the CSI500 / IC line on 5-year daily data."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, roc_auc_score

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
)
from src.services.qlib_local_history import (  # noqa: E402
    find_latest_bootstrapped_qlib_root,
    load_qlib_daily_ohlcv,
)
from src.models.metaphysical import (  # noqa: E402
    CURRENT_PRODUCTION_CANDIDATE_FEATURES,
    NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES,
    QUANT_FEATURES,
    REGIME_WEIGHTED_AUTHOR_FEATURES,
    SELECTED_TRIGGER_FEATURES,
)


CSI500_RESONANCE_CORE = [
    "bb_width_roc_3d",
    "bb_position_pct",
    "bb_width_ratio",
    "bb_breakout_strength",
    "triple_resonance_score",
    "bb_position_volume_interaction",
    "bb_breakout_volume_interaction",
    "is_bb_breakout",
]


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="CSI500/IC factor ablation on 5-year daily data.")
    parser.add_argument("--start", default=default_start)
    parser.add_argument("--end", default=default_end)
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "xgb_cache"),
        help="Cache directory for metaphysical feature calculations.",
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

    try:
        import yfinance as yf
    except Exception as exc:
        raise RuntimeError(f"未安装 yfinance: {exc}") from exc

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


def _fit_classifier(X_train: pd.DataFrame, y_train: pd.Series) -> RandomForestClassifier | None:
    if y_train.nunique() < 2:
        return None
    model = RandomForestClassifier(
        n_estimators=400,
        max_depth=8,
        min_samples_leaf=4,
        class_weight="balanced_subsample",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    return model


def _annualized_sharpe(returns: pd.Series) -> float:
    valid = returns.dropna()
    if len(valid) < 2 or valid.std() == 0:
        return 0.0
    return float(valid.mean() / valid.std() * np.sqrt(252))


def _max_drawdown(equity_curve: pd.Series) -> float:
    peak = equity_curve.cummax()
    drawdown = equity_curve / peak - 1.0
    return float(drawdown.min())


def _run_walk_forward(frame: pd.DataFrame, feature_list: list[str], *, min_train_days: int = 756, retrain_every: int = 42) -> dict[str, float | int]:
    usable = frame.dropna(subset=feature_list + ["target_extreme_volatility", "next_day_return"]).copy().reset_index(drop=True)
    if len(usable) <= min_train_days:
        raise RuntimeError(f"样本不足: {len(usable)} <= {min_train_days}")

    probabilities: list[float] = [np.nan] * len(usable)
    model = None
    for idx in range(len(usable)):
        if idx < min_train_days:
            continue
        if model is None or (idx - min_train_days) % retrain_every == 0:
            train = usable.iloc[:idx]
            X_train = train[feature_list].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            y_train = train["target_extreme_volatility"].astype(int)
            model = _fit_classifier(X_train, y_train)
            if model is None:
                continue

        X_live = usable.iloc[[idx]][feature_list].replace([np.inf, -np.inf], np.nan).fillna(0.0)
        probabilities[idx] = float(model.predict_proba(X_live)[:, 1][0])

    usable["tail_risk_probability"] = probabilities
    usable = usable.dropna(subset=["tail_risk_probability"]).copy()
    if usable.empty:
        raise RuntimeError("walk-forward 概率为空")

    caution_threshold = 0.40
    risk_off_threshold = 0.60
    usable["position"] = 1.0
    usable.loc[usable["tail_risk_probability"] >= caution_threshold, "position"] = 0.5
    usable.loc[usable["tail_risk_probability"] >= risk_off_threshold, "position"] = 0.0
    usable["strategy_return"] = usable["position"] * usable["next_day_return"]
    usable["buy_hold_return"] = usable["next_day_return"]
    usable["strategy_equity"] = (1.0 + usable["strategy_return"]).cumprod()
    usable["buy_hold_equity"] = (1.0 + usable["buy_hold_return"]).cumprod()

    y_true = usable["target_extreme_volatility"].astype(int)
    y_score = usable["tail_risk_probability"]
    return {
        "sample_count": int(len(usable)),
        "auc": float(roc_auc_score(y_true, y_score)),
        "ap": float(average_precision_score(y_true, y_score)),
        "strategy_total_return": float(usable["strategy_equity"].iloc[-1] - 1.0),
        "buy_hold_total_return": float(usable["buy_hold_equity"].iloc[-1] - 1.0),
        "strategy_max_drawdown": _max_drawdown(usable["strategy_equity"]),
        "strategy_sharpe": _annualized_sharpe(usable["strategy_return"]),
        "avg_position": float(usable["position"].mean()),
    }


def _build_report(results: list[dict[str, object]], start: str, end: str) -> str:
    ordered = sorted(results, key=lambda item: float(item["strategy_sharpe"]), reverse=True)
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} 中证500因子消融报告",
        "",
        f"- 验证区间: {start} 至 {end}",
        "- 标的代理: 本地Qlib优先读取 000905，缺失时回退 510500.SS",
        "- 目标: 找出中证500/IC 这条线当前真正有效和真正拖后腿的因子组",
        "",
        "## 消融结果",
        "",
        "| 方案 | 特征数 | 样本数 | AUC | AP | 策略收益 | 买入持有收益 | 最大回撤 | Sharpe | 平均仓位 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in ordered:
        lines.append(
            f"| {item['label']} | {item['feature_count']} | {item['sample_count']} | {item['auc']:.4f} | {item['ap']:.4f} | "
            f"{item['strategy_total_return'] * 100:.2f}% | {item['buy_hold_total_return'] * 100:.2f}% | "
            f"{item['strategy_max_drawdown'] * 100:.2f}% | {item['strategy_sharpe']:.2f} | {item['avg_position']:.2f} |"
        )

    best = ordered[0]
    worst = ordered[-1]
    lines.extend(["", "## 结论", ""])
    lines.append(f"- 当前最优方案是 `{best['label']}`，Sharpe `{best['strategy_sharpe']:.2f}`。")
    lines.append(f"- 当前最弱方案是 `{worst['label']}`，Sharpe `{worst['strategy_sharpe']:.2f}`。")

    weighted = next((item for item in ordered if item["label"] == "全量加权候选池"), None)
    production = next((item for item in ordered if item["label"] == "当前生产候选池"), None)
    quant_only = next((item for item in ordered if item["label"] == "纯量价"), None)
    no_trigger = next((item for item in ordered if item["label"] == "生产池去触发器"), None)

    if quant_only and production:
        if production["strategy_sharpe"] > quant_only["strategy_sharpe"]:
            lines.append("- 当前生产候选池整体上优于纯量价，说明共振和触发器并非全部无效。")
        else:
            lines.append("- 当前生产候选池没有跑赢纯量价，说明中证500这条线确实存在增强因子拖累。")
    if no_trigger and production:
        if no_trigger["strategy_sharpe"] > production["strategy_sharpe"]:
            lines.append("- 去掉精选触发器后表现更好，说明当前触发器在中证500线上偏噪音。")
        else:
            lines.append("- 保留精选触发器仍更好，说明触发器至少有一部分在中证500线上提供了防守价值。")
    if weighted and production:
        if weighted["strategy_sharpe"] > production["strategy_sharpe"]:
            lines.append("- 加权作者特征有正向帮助，可以继续保留为候选增强。")
        else:
            lines.append("- 加权作者特征没有带来增益，说明当前中证500线不宜急着增加长周期/作者因子权重。")

    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    frame = _prepare_frame(args.start, args.end, args.cache_dir)
    setups = [
        ("纯量价", QUANT_FEATURES),
        ("量价+共振核心", QUANT_FEATURES + CSI500_RESONANCE_CORE),
        ("当前生产候选池", CURRENT_PRODUCTION_CANDIDATE_FEATURES),
        ("生产池去触发器", [f for f in CURRENT_PRODUCTION_CANDIDATE_FEATURES if f not in SELECTED_TRIGGER_FEATURES]),
        ("全量加权候选池", NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES),
        ("加权池去作者权重", [f for f in NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES if f not in REGIME_WEIGHTED_AUTHOR_FEATURES]),
    ]

    results = []
    for label, feature_list in setups:
        print(f"[消融] {label} ...")
        metrics = _run_walk_forward(frame, feature_list)
        metrics["label"] = label
        metrics["feature_count"] = len(feature_list)
        results.append(metrics)

    report = _build_report(results, args.start, args.end)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_中证500因子消融报告.md"
    latest_path = output_dir / "latest_csi500_factor_ablation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("中证500因子消融完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
