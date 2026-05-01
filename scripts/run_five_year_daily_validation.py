#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run a 5-year daily-bar validation across the current core trading lines.

This script reuses the existing next-production walk-forward model path and
evaluates whether the current candidate feature pool is stable enough on
5-year daily data for:
- gold main futures
- silver main futures
- CSI500 index
- CSI500 ETF proxy
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[0]
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
from src.models.metaphysical import (  # noqa: E402
    NEXT_PRODUCTION_MODEL_DEFAULTS,
    NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES,
)
from src.market_data_fetcher import MarketDataFetcher  # noqa: E402
from src.services.qlib_local_history import (  # noqa: E402
    find_latest_bootstrapped_qlib_root,
    load_qlib_daily_ohlcv,
)
from src.services.local_etf_history import load_cached_etf_daily_ohlcv  # noqa: E402


def _default_start_end() -> tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=365 * 5)
    return start.isoformat(), end.isoformat()


def _build_parser() -> argparse.ArgumentParser:
    default_start, default_end = _default_start_end()
    parser = argparse.ArgumentParser(description="5-year daily validation for the current production candidate feature set.")
    parser.add_argument("--start", default=default_start, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end", default=default_end, help="End date in YYYY-MM-DD format.")
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "xgb_cache"),
        help="Optional cache directory for metaphysical astro calculations.",
    )
    parser.add_argument(
        "--probability-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "metaphysical_probabilities"),
        help="Directory used to cache walk-forward probability frames.",
    )
    parser.add_argument(
        "--refresh-probability-cache",
        action="store_true",
        help="Ignore cached probability frames and rebuild them.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "reports" / "backtests"),
        help="Directory where the markdown validation report will be written.",
    )
    parser.add_argument(
        "--min-train-days",
        type=int,
        default=NEXT_PRODUCTION_MODEL_DEFAULTS["min_train_days"],
        help="Minimum expanding-window training size before predictions start.",
    )
    parser.add_argument(
        "--retrain-every",
        type=int,
        default=NEXT_PRODUCTION_MODEL_DEFAULTS["retrain_every"],
        help="Retrain the model every N trading days.",
    )
    parser.add_argument(
        "--risk-off-threshold",
        type=float,
        default=NEXT_PRODUCTION_MODEL_DEFAULTS["risk_off_threshold"],
        help="Predicted tail-risk threshold above which exposure becomes 0.",
    )
    parser.add_argument(
        "--caution-threshold",
        type=float,
        default=NEXT_PRODUCTION_MODEL_DEFAULTS["caution_threshold"],
        help="Predicted tail-risk threshold above which exposure is cut to 0.5.",
    )
    return parser


def _load_yfinance_frame(symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
    try:
        import yfinance as yf
    except Exception:
        return None

    try:
        ticker = yf.Ticker(symbol)
        frame = ticker.history(start=start, end=end, auto_adjust=True)
    except Exception:
        return None
    if frame is None or frame.empty:
        return None

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
    if "date" not in frame.columns:
        return None
    frame = frame[["date", "open", "close", "high", "low", "volume"]].copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.tz_localize(None)
    return frame.sort_values("date").reset_index(drop=True)


def _load_etf_hist_sina(symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
    try:
        import akshare as ak
    except Exception:
        return None

    try:
        frame = ak.fund_etf_hist_sina(symbol=symbol)
    except Exception:
        return None
    if frame is None or frame.empty:
        return None

    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.rename(
        columns={
            "open": "open",
            "close": "close",
            "high": "high",
            "low": "low",
            "volume": "volume",
        }
    )
    frame = frame[(frame["date"] >= pd.Timestamp(start)) & (frame["date"] <= pd.Timestamp(end))].copy()
    return frame[["date", "open", "close", "high", "low", "volume"]].sort_values("date").reset_index(drop=True)


def _load_market_data_with_market_fetcher(symbol: str, source: str, start: str, end: str) -> Optional[pd.DataFrame]:
    fetcher = MarketDataFetcher("")
    try:
        frame = fetcher.get_historical_kline(symbol=symbol, source=source)
    finally:
        fetcher.close()
    if frame is None or frame.empty:
        return None
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame[(frame["date"] >= pd.Timestamp(start)) & (frame["date"] <= pd.Timestamp(end))].copy()
    return frame[["date", "open", "close", "high", "low", "volume"]].sort_values("date").reset_index(drop=True)


def _load_local_qlib_frame(symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
    qlib_root = find_latest_bootstrapped_qlib_root()
    if qlib_root is None:
        return None
    frame = load_qlib_daily_ohlcv(symbol, qlib_root)
    if frame is None or frame.empty:
        return None
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame[(frame["date"] >= pd.Timestamp(start)) & (frame["date"] <= pd.Timestamp(end))].copy()
    return frame[["date", "open", "close", "high", "low", "volume"]].sort_values("date").reset_index(drop=True)


def _load_local_etf_frame(symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
    frame = load_cached_etf_daily_ohlcv(symbol)
    if frame is None or frame.empty:
        return None
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame[(frame["date"] >= pd.Timestamp(start)) & (frame["date"] <= pd.Timestamp(end))].copy()
    return frame[["date", "open", "close", "high", "low", "volume"]].sort_values("date").reset_index(drop=True)


def _first_non_empty(loaders: list[Callable[[], Optional[pd.DataFrame]]]) -> Optional[pd.DataFrame]:
    for loader in loaders:
        frame = loader()
        if frame is not None and not frame.empty:
            return frame
    return None


def _load_asset_frame(asset: dict[str, str], start: str, end: str) -> Optional[pd.DataFrame]:
    kind = asset["kind"]
    if kind == "market_fetcher":
        return _first_non_empty(
            [
                lambda: _load_local_qlib_frame(asset["symbol"], start, end),
                lambda: _load_market_data_with_market_fetcher(asset["symbol"], asset["source"], start, end),
            ]
        )
    if kind == "multi":
        return _first_non_empty(
            [
                lambda: _load_local_qlib_frame(asset["code"], start, end),
                lambda: _load_local_etf_frame(asset["akshare_symbol"], start, end),
                lambda: _load_market_data_with_market_fetcher(asset["symbol"], asset["source"], start, end),
                lambda: _load_yfinance_frame(asset["yfinance_symbol"], start, end),
                lambda: _load_etf_hist_sina(asset["akshare_symbol"], start, end),
            ]
        )
    raise ValueError(f"unknown asset kind: {kind}")


def _probability_cache_path(base_dir: Path, asset_code: str, start: str, end: str, min_train_days: int, retrain_every: int) -> Path:
    safe_code = asset_code.replace("/", "_").replace(".", "_")
    return base_dir / f"{safe_code}_{start}_{end}_min{min_train_days}_retrain{retrain_every}.pkl"


def _run_validation_for_asset(asset: dict[str, str], args: argparse.Namespace) -> dict[str, object]:
    frame = _load_asset_frame(asset, args.start, args.end)
    if frame is None or frame.empty:
        return {
            "name": asset["name"],
            "code": asset["code"],
            "status": "failed",
            "reason": "未获取到日线数据",
        }

    feature_df = _compute_quant_and_resonance_features(frame)
    feature_df = _build_target(feature_df)
    candidate_df = _build_candidate_frame(feature_df, cache_dir=args.cache_dir)
    candidate_df = candidate_df.loc[:, ~candidate_df.columns.duplicated()].copy()
    for feature in NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES:
        if feature not in candidate_df.columns:
            candidate_df[feature] = 0.0

    cache_dir = Path(args.probability_cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = _probability_cache_path(
        cache_dir,
        asset["code"],
        args.start,
        args.end,
        args.min_train_days,
        args.retrain_every,
    )

    usable = _generate_walk_forward_probabilities(
        candidate_df,
        min_train_days=args.min_train_days,
        retrain_every=args.retrain_every,
        cache_path=cache_path,
        refresh_cache=args.refresh_probability_cache,
    )
    if usable.empty and not args.refresh_probability_cache:
        usable = _generate_walk_forward_probabilities(
            candidate_df,
            min_train_days=args.min_train_days,
            retrain_every=args.retrain_every,
            cache_path=cache_path,
            refresh_cache=True,
        )
    if usable.empty:
        return {
            "name": asset["name"],
            "code": asset["code"],
            "status": "failed",
            "reason": f"可回测样本不足(原始{len(frame)} / 候选{len(candidate_df)})",
        }

    backtest_df, metrics = _evaluate_probability_frame(
        usable,
        risk_off_threshold=args.risk_off_threshold,
        caution_threshold=args.caution_threshold,
    )
    latest = backtest_df.iloc[-1]
    return {
        "name": asset["name"],
        "code": asset["code"],
        "status": "ok",
        "rows": int(len(frame)),
        "sample_count": int(metrics["sample_count"]),
        "auc": float(metrics["auc"]),
        "ap": float(metrics["ap"]),
        "strategy_total_return": float(metrics["strategy_total_return"]),
        "buy_hold_total_return": float(metrics["buy_hold_total_return"]),
        "strategy_max_drawdown": float(metrics["strategy_max_drawdown"]),
        "buy_hold_max_drawdown": float(metrics["buy_hold_max_drawdown"]),
        "strategy_sharpe": float(metrics["strategy_sharpe"]),
        "buy_hold_sharpe": float(metrics["buy_hold_sharpe"]),
        "avg_position": float(metrics["avg_position"]),
        "risk_off_days": int(metrics["risk_off_days"]),
        "caution_days": int(metrics["caution_days"]),
        "full_risk_days": int(metrics["full_risk_days"]),
        "latest_date": pd.Timestamp(latest["date"]).date().isoformat(),
        "latest_probability": float(latest["tail_risk_probability"]),
        "latest_position": float(latest["position"]),
        "cache_path": str(cache_path),
    }


def _build_report(results: list[dict[str, object]], args: argparse.Namespace) -> str:
    ok_results = [item for item in results if item.get("status") == "ok"]
    failed_results = [item for item in results if item.get("status") != "ok"]

    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} 五年日线验证报告",
        "",
        f"- 验证区间: {args.start} 至 {args.end}",
        f"- 最小训练天数: {args.min_train_days}",
        f"- 重训间隔: {args.retrain_every}",
        f"- 风险收缩阈值: caution={args.caution_threshold:.2f}, risk_off={args.risk_off_threshold:.2f}",
        "",
        "## 核心结果",
        "",
        "| 资产 | 样本数 | AUC | AP | 策略收益 | 买入持有收益 | 策略最大回撤 | 策略Sharpe | 平均仓位 | 最新风险概率 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for item in ok_results:
        lines.append(
            f"| {item['name']} | {item['sample_count']} | {item['auc']:.4f} | {item['ap']:.4f} | "
            f"{item['strategy_total_return'] * 100:.2f}% | {item['buy_hold_total_return'] * 100:.2f}% | "
            f"{item['strategy_max_drawdown'] * 100:.2f}% | {item['strategy_sharpe']:.2f} | "
            f"{item['avg_position']:.2f} | {item['latest_probability']:.3f} |"
        )

    if failed_results:
        lines.extend(["", "## 未完成项", ""])
        for item in failed_results:
            lines.append(f"- {item['name']}({item['code']}): {item.get('reason', '未知错误')}")

    lines.extend(["", "## 观察结论", ""])
    if not ok_results:
        lines.append("- 当前没有可用结果，先检查数据源和依赖。")
        return "\n".join(lines)

    strongest = max(ok_results, key=lambda item: item["strategy_sharpe"])
    weakest = min(ok_results, key=lambda item: item["strategy_sharpe"])
    lines.append(
        f"- 当前 5 年窗口里，表现最稳的是 {strongest['name']}，策略 Sharpe {strongest['strategy_sharpe']:.2f}。"
    )
    lines.append(
        f"- 当前 5 年窗口里，最需要谨慎看待的是 {weakest['name']}，策略 Sharpe {weakest['strategy_sharpe']:.2f}。"
    )
    profitable = [item["name"] for item in ok_results if item["strategy_total_return"] > item["buy_hold_total_return"]]
    if profitable:
        lines.append(f"- 策略收益跑赢买入持有的资产: {', '.join(profitable)}。")
    else:
        lines.append("- 当前没有资产在 5 年窗口里明显跑赢买入持有，后续应优先看风控价值而不是进攻收益。")

    lines.extend(["", "## 最新信号", ""])
    for item in ok_results:
        lines.append(
            f"- {item['name']}: 最新日期 {item['latest_date']} | 风险概率 {item['latest_probability']:.3f} | 仓位 {item['latest_position']:.2f}"
        )

    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    assets = [
        {
            "name": "黄金主力",
            "code": "AU0",
            "kind": "market_fetcher",
            "symbol": "AU0",
            "source": "futures_sina",
        },
        {
            "name": "白银主力",
            "code": "AG0",
            "kind": "market_fetcher",
            "symbol": "AG0",
            "source": "futures_sina",
        },
        {
            "name": "中证500指数",
            "code": "000905",
            "kind": "market_fetcher",
            "symbol": "000905",
            "source": "index_zh_a",
        },
        {
            "name": "中证500ETF",
            "code": "510500",
            "kind": "multi",
            "symbol": "000905",
            "source": "index_zh_a",
            "yfinance_symbol": "510500.SS",
            "akshare_symbol": "sh510500",
        },
    ]

    results = []
    for asset in assets:
        print(f"[验证] {asset['name']} ({asset['code']}) ...")
        results.append(_run_validation_for_asset(asset, args))

    report = _build_report(results, args)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y-%m-%d")
    report_path = output_dir / f"{report_date}_五年日线验证报告.md"
    latest_path = output_dir / "latest_five_year_daily_validation.md"
    report_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")

    print("=" * 72)
    print("五年日线验证完成")
    print(f"报告: {report_path}")
    print("=" * 72)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
