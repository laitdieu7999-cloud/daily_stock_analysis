#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Minimal training example for the next-production metaphysical feature path.

This script demonstrates the standard workflow:
1. fetch daily market data
2. compute quant / resonance columns expected by the candidate pool
3. build the dynamic regime-aware metaphysical candidate slice
4. fit a small classifier and report AUC / AP
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.metaphysical import (  # noqa: E402
    NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES,
    archive_candidate_dataset,
    bb_breakout_strength,
    bollinger_bands,
    build_next_production_backtest_features,
    is_bb_breakout,
    is_bb_squeeze,
    is_triple_resonance,
    record_training_run,
    triple_resonance_score,
    volatility_plus_score,
)
from src.services.local_etf_history import load_cached_etf_daily_ohlcv  # noqa: E402
from src.services.qlib_local_history import (  # noqa: E402
    find_latest_bootstrapped_qlib_root,
    load_qlib_daily_ohlcv,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train a minimal classifier on the next-production metaphysical candidate path."
    )
    parser.add_argument(
        "--symbol",
        default="510500.SS",
        help="Target symbol. Local Qlib/ETF history is used first; yfinance is only a fallback.",
    )
    parser.add_argument("--start", default="2016-01-01", help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end", default="2026-04-20", help="End date in YYYY-MM-DD format.")
    parser.add_argument(
        "--cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "xgb_cache"),
        help="Optional cache directory for metaphysical astro calculations.",
    )
    parser.add_argument(
        "--record-run",
        action="store_true",
        help="Append this training result to the training-run JSONL ledger.",
    )
    parser.add_argument(
        "--training-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_training_runs.jsonl"),
        help="Path to the training-run JSONL ledger.",
    )
    parser.add_argument(
        "--archive-dataset",
        action="store_true",
        help="Persist a durable local copy of the training dataset snapshot.",
    )
    return parser


def _load_market_data(symbol: str, start: str, end: str) -> pd.DataFrame:
    qlib_root = find_latest_bootstrapped_qlib_root()
    if qlib_root is not None:
        local_qlib = load_qlib_daily_ohlcv(symbol, qlib_root)
        if local_qlib is not None and not local_qlib.empty:
            frame = local_qlib.copy()
            frame["date"] = pd.to_datetime(frame["date"])
            frame = frame[(frame["date"] >= pd.Timestamp(start)) & (frame["date"] <= pd.Timestamp(end))].copy()
            if not frame.empty:
                return frame[["date", "open", "close", "high", "low", "volume"]].sort_values("date").reset_index(drop=True)

    local_etf = load_cached_etf_daily_ohlcv(symbol)
    if local_etf is not None and not local_etf.empty:
        frame = local_etf.copy()
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame[(frame["date"] >= pd.Timestamp(start)) & (frame["date"] <= pd.Timestamp(end))].copy()
        if not frame.empty:
            return frame[["date", "open", "close", "high", "low", "volume"]].sort_values("date").reset_index(drop=True)

    import yfinance as yf

    ticker = yf.Ticker(symbol)
    frame = ticker.history(start=start, end=end, auto_adjust=True)
    if frame is None or frame.empty:
        raise RuntimeError(f"no market data returned for {symbol}")

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
    frame = frame.sort_values("date").reset_index(drop=True)
    return frame


def _compute_quant_and_resonance_features(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    df["quant_return_1d"] = df["close"].pct_change()
    df["quant_return_5d"] = df["close"].pct_change(5)
    df["quant_volume_change"] = df["volume"].pct_change()
    df["quant_volume_ratio_5d"] = df["volume"] / df["volume"].rolling(5).mean()

    df["prev_close"] = df["close"].shift(1)
    df["tr"] = df[["high", "prev_close"]].max(axis=1) - df[["low", "prev_close"]].min(axis=1)
    df["atr_20"] = df["tr"].rolling(20).mean()
    df["ma_20"] = df["close"].rolling(20).mean()
    df["std_20"] = df["close"].rolling(20).std()
    df["bollinger_width"] = (df["std_20"] / df["ma_20"]) * 100

    kline_for_bb = [
        {"date": row.date.strftime("%Y-%m-%d"), "close": float(row.close)}
        for row in df.itertuples()
    ]
    bb_series = bollinger_bands(kline_for_bb, period=20, stddev=2.0)
    df["bb_width_ratio"] = [item.get("bb_width_ratio") for item in bb_series]
    df["bb_position_pct"] = [item.get("bb_position_pct") for item in bb_series]
    df["bb_width_roc_3d"] = [item.get("bb_width_roc_3d") for item in bb_series]

    squeeze_flags = []
    breakout_flags = []
    breakout_strengths = []
    resonance_scores = []
    volatility_plus_scores = []
    for idx in range(len(bb_series)):
        partial = bb_series[: idx + 1]
        volume_ratio = df["quant_volume_ratio_5d"].iloc[idx]
        volume_value = None if pd.isna(volume_ratio) else float(volume_ratio)
        squeeze_flags.append(int(is_bb_squeeze(partial)))
        breakout_flags.append(int(is_bb_breakout(partial)))
        breakout_strengths.append(bb_breakout_strength(partial))
        resonance_scores.append(triple_resonance_score(partial, volume_ratio=volume_value))
        volatility_plus_scores.append(
            volatility_plus_score(partial, dt=df["date"].iloc[idx], volume_ratio=volume_value)
        )

    df["is_bb_squeeze"] = squeeze_flags
    df["is_bb_breakout"] = breakout_flags
    df["bb_breakout_strength"] = breakout_strengths
    df["triple_resonance_score"] = resonance_scores
    df["volatility_plus_score"] = volatility_plus_scores
    df["is_triple_resonance"] = [
        int(is_triple_resonance(bb_series[: idx + 1], volume_ratio=None if pd.isna(df["quant_volume_ratio_5d"].iloc[idx]) else float(df["quant_volume_ratio_5d"].iloc[idx])))
        for idx in range(len(bb_series))
    ]
    df["bb_position_volume_interaction"] = df["bb_position_pct"] * df["quant_volume_ratio_5d"]
    df["bb_breakout_volume_interaction"] = df["bb_breakout_strength"] * df["quant_volume_ratio_5d"]
    return df


def _build_target(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    df["next_day_amplitude"] = df["high"].shift(-1) - df["low"].shift(-1)
    df["target_extreme_volatility"] = np.where(df["next_day_amplitude"] > 1.5 * df["atr_20"], 1, 0)
    return df


def _build_training_frame(frame: pd.DataFrame, cache_dir: str) -> pd.DataFrame:
    candidate_df = build_next_production_backtest_features(
        frame,
        date_col="date",
        enabled=True,
        cache_dir=cache_dir,
        regime_mode="dynamic",
        candidate_only=True,
    )
    candidate_df = candidate_df.loc[:, ~candidate_df.columns.duplicated()].copy()

    merged = frame.reset_index(drop=True).copy()
    for column in candidate_df.columns:
        if column == "date" or column in merged.columns:
            continue
        merged[column] = candidate_df[column].reset_index(drop=True)

    merged = merged.loc[:, ~merged.columns.duplicated()].copy()
    for feature in NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES:
        if feature not in merged.columns:
            merged[feature] = 0.0
    return merged


def _fit_model(frame: pd.DataFrame) -> tuple[float, float, int]:
    usable = frame.dropna(subset=NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES + ["target_extreme_volatility"]).copy()
    X = usable[NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    y = usable["target_extreme_volatility"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )
    model = RandomForestClassifier(
        n_estimators=400,
        max_depth=8,
        min_samples_leaf=4,
        class_weight="balanced_subsample",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    y_pred = model.predict_proba(X_test)[:, 1]
    return roc_auc_score(y_test, y_pred), average_precision_score(y_test, y_pred), len(usable)


def main() -> int:
    args = _build_parser().parse_args()
    market_df = _load_market_data(args.symbol, args.start, args.end)
    feature_df = _compute_quant_and_resonance_features(market_df)
    feature_df = _build_target(feature_df)
    training_df = _build_training_frame(feature_df, cache_dir=args.cache_dir)
    if args.archive_dataset:
        archive_candidate_dataset(
            training_df,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            suffix="training_snapshot",
        )
    auc, ap, sample_count = _fit_model(training_df)

    print("=" * 60)
    print("Next Production Metaphysical Example")
    print("=" * 60)
    print(f"symbol: {args.symbol}")
    print(f"range:  {args.start} -> {args.end}")
    print(f"samples: {sample_count}")
    print(f"features: {len(NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES)}")
    print(f"AUC: {auc:.4f}")
    print(f"AP:  {ap:.4f}")
    if args.record_run:
        record_training_run(
            args.training_log_path,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            sample_count=sample_count,
            feature_count=len(NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES),
            auc=auc,
            ap=ap,
            params={"cache_dir": args.cache_dir},
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
