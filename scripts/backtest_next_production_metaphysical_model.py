#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Walk-forward backtest for the next-production metaphysical candidate path.

This script turns the dynamic Regime-aware author factors into a model-driven
risk filter instead of forcing them into the old rule-based resonance strategy.

Workflow:
1. fetch daily market data
2. compute quant / resonance columns required by the candidate pool
3. build `NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES`
4. run an expanding-window walk-forward classifier
5. map predicted tail-risk probabilities to position sizing
"""

from __future__ import annotations

import argparse
import itertools
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, roc_auc_score

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.metaphysical import (  # noqa: E402
    NEXT_PRODUCTION_MODEL_DEFAULTS,
    NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES,
    apply_next_production_position_sizing,
    bb_breakout_strength,
    bollinger_bands,
    build_next_production_backtest_features,
    is_bb_breakout,
    is_bb_squeeze,
    is_triple_resonance,
    latest_next_production_signal_with_report_overlay,
    latest_next_production_signal,
    record_stage_performance_run,
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
        description="Walk-forward backtest for the next-production metaphysical model."
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
    parser.add_argument(
        "--grid-search",
        action="store_true",
        help="Run a threshold grid search on top of a single walk-forward probability pass.",
    )
    parser.add_argument(
        "--probability-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "metaphysical_probabilities"),
        help="Directory used to cache walk-forward tail-risk probabilities.",
    )
    parser.add_argument(
        "--refresh-probability-cache",
        action="store_true",
        help="Ignore any cached walk-forward probability files and rebuild them.",
    )
    parser.add_argument(
        "--tactical-report-file",
        default=None,
        help="Optional UTF-8 text file containing the daily Google Doc archive text.",
    )
    parser.add_argument(
        "--record-stage-performance",
        action="store_true",
        help="Append the backtest metrics into a stage-performance JSONL ledger.",
    )
    parser.add_argument(
        "--stage",
        default="candidate",
        choices=["candidate", "shadow", "production"],
        help="Stage label used when recording backtest performance.",
    )
    parser.add_argument(
        "--stage-performance-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_stage_performance_runs.jsonl"),
        help="Path to the stage-performance JSONL ledger.",
    )
    return parser


def _load_tactical_report_text(path: str | None) -> str | None:
    if not path:
        return None
    return Path(path).read_text(encoding="utf-8").strip()


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
    triple_resonance_flags = []
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
        triple_resonance_flags.append(int(is_triple_resonance(partial, volume_ratio=volume_value)))

    df["is_bb_squeeze"] = squeeze_flags
    df["is_bb_breakout"] = breakout_flags
    df["bb_breakout_strength"] = breakout_strengths
    df["triple_resonance_score"] = resonance_scores
    df["volatility_plus_score"] = volatility_plus_scores
    df["is_triple_resonance"] = triple_resonance_flags
    df["bb_position_volume_interaction"] = df["bb_position_pct"] * df["quant_volume_ratio_5d"]
    df["bb_breakout_volume_interaction"] = df["bb_breakout_strength"] * df["quant_volume_ratio_5d"]
    return df


def _build_target(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    df["next_day_amplitude"] = df["high"].shift(-1) - df["low"].shift(-1)
    df["target_extreme_volatility"] = np.where(df["next_day_amplitude"] > 1.5 * df["atr_20"], 1, 0)
    df["next_day_return"] = df["close"].shift(-1) / df["close"] - 1.0
    return df


def _build_candidate_frame(frame: pd.DataFrame, cache_dir: str) -> pd.DataFrame:
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


def _sanitize_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def _probability_cache_path(
    cache_dir: str | Path,
    *,
    symbol: str,
    start: str,
    end: str,
    min_train_days: int,
    retrain_every: int,
) -> Path:
    base = Path(cache_dir)
    base.mkdir(parents=True, exist_ok=True)
    filename = (
        f"{_sanitize_token(symbol)}_{_sanitize_token(start)}_{_sanitize_token(end)}"
        f"_min{min_train_days}_retrain{retrain_every}.pkl"
    )
    return base / filename


def _fit_classifier(X_train: pd.DataFrame, y_train: pd.Series) -> RandomForestClassifier:
    return RandomForestClassifier(
        n_estimators=400,
        max_depth=8,
        min_samples_leaf=4,
        class_weight="balanced_subsample",
        random_state=42,
        n_jobs=-1,
    ).fit(X_train, y_train)


def _max_drawdown(equity_curve: pd.Series) -> float:
    rolling_peak = equity_curve.cummax()
    drawdown = equity_curve / rolling_peak - 1.0
    return float(drawdown.min())


def _annualized_sharpe(returns: pd.Series) -> float:
    valid = returns.dropna()
    if valid.std() == 0 or len(valid) < 2:
        return 0.0
    return float(valid.mean() / valid.std() * np.sqrt(252))


def _generate_walk_forward_probabilities(
    frame: pd.DataFrame,
    *,
    min_train_days: int,
    retrain_every: int,
    cache_path: str | Path | None = None,
    refresh_cache: bool = False,
) -> pd.DataFrame:
    if cache_path is not None:
        cache_file = Path(cache_path)
        if cache_file.exists() and not refresh_cache:
            return pd.read_pickle(cache_file)

    usable = frame.dropna(
        subset=NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES + ["target_extreme_volatility", "next_day_return"]
    ).copy()
    usable = usable.reset_index(drop=True)

    probabilities: list[float] = [np.nan] * len(usable)
    model = None

    for idx in range(len(usable)):
        if idx < min_train_days:
            continue
        if model is None or (idx - min_train_days) % retrain_every == 0:
            train = usable.iloc[:idx]
            X_train = train[NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            y_train = train["target_extreme_volatility"].astype(int)
            if y_train.nunique() < 2:
                continue
            model = _fit_classifier(X_train, y_train)

        X_live = (
            usable.iloc[[idx]][NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES]
            .replace([np.inf, -np.inf], np.nan)
            .fillna(0.0)
        )
        probabilities[idx] = float(model.predict_proba(X_live)[:, 1][0])

    usable["tail_risk_probability"] = probabilities
    usable = usable.dropna(subset=["tail_risk_probability"]).copy()
    if cache_path is not None:
        Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
        usable.to_pickle(cache_path)
    return usable


def _evaluate_probability_frame(
    usable: pd.DataFrame,
    *,
    risk_off_threshold: float,
    caution_threshold: float,
) -> tuple[pd.DataFrame, dict]:
    result = apply_next_production_position_sizing(
        usable,
        probability_col="tail_risk_probability",
        caution_threshold=caution_threshold,
        risk_off_threshold=risk_off_threshold,
        copy=True,
    )
    result["strategy_return"] = result["position"] * result["next_day_return"]
    result["buy_hold_return"] = result["next_day_return"]
    result["strategy_equity"] = (1.0 + result["strategy_return"]).cumprod()
    result["buy_hold_equity"] = (1.0 + result["buy_hold_return"]).cumprod()

    y_true = result["target_extreme_volatility"].astype(int)
    y_score = result["tail_risk_probability"]

    metrics = {
        "sample_count": int(len(result)),
        "auc": float(roc_auc_score(y_true, y_score)),
        "ap": float(average_precision_score(y_true, y_score)),
        "strategy_total_return": float(result["strategy_equity"].iloc[-1] - 1.0),
        "buy_hold_total_return": float(result["buy_hold_equity"].iloc[-1] - 1.0),
        "strategy_max_drawdown": _max_drawdown(result["strategy_equity"]),
        "buy_hold_max_drawdown": _max_drawdown(result["buy_hold_equity"]),
        "strategy_sharpe": _annualized_sharpe(result["strategy_return"]),
        "buy_hold_sharpe": _annualized_sharpe(result["buy_hold_return"]),
        "avg_position": float(result["position"].mean()),
        "risk_off_days": int((result["position"] == 0.0).sum()),
        "caution_days": int((result["position"] == 0.5).sum()),
        "full_risk_days": int((result["position"] == 1.0).sum()),
        "risk_off_threshold": float(risk_off_threshold),
        "caution_threshold": float(caution_threshold),
    }
    return result, metrics


def _run_walk_forward_backtest(
    frame: pd.DataFrame,
    *,
    min_train_days: int,
    retrain_every: int,
    risk_off_threshold: float,
    caution_threshold: float,
    cache_path: str | Path | None = None,
    refresh_cache: bool = False,
) -> tuple[pd.DataFrame, dict]:
    usable = _generate_walk_forward_probabilities(
        frame,
        min_train_days=min_train_days,
        retrain_every=retrain_every,
        cache_path=cache_path,
        refresh_cache=refresh_cache,
    )
    return _evaluate_probability_frame(
        usable,
        risk_off_threshold=risk_off_threshold,
        caution_threshold=caution_threshold,
    )


def _run_threshold_grid_search(
    usable: pd.DataFrame,
    *,
    caution_values: list[float],
    risk_off_values: list[float],
) -> pd.DataFrame:
    rows = []
    for caution_threshold, risk_off_threshold in itertools.product(caution_values, risk_off_values):
        if caution_threshold >= risk_off_threshold:
            continue
        _, metrics = _evaluate_probability_frame(
            usable,
            risk_off_threshold=risk_off_threshold,
            caution_threshold=caution_threshold,
        )
        rows.append(metrics)
    result = pd.DataFrame(rows)
    return result.sort_values(
        ["strategy_sharpe", "strategy_total_return", "strategy_max_drawdown"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def _time_slice_summary(result: pd.DataFrame, *, slices: int = 3) -> pd.DataFrame:
    rows = []
    boundaries = np.linspace(0, len(result), slices + 1, dtype=int)
    for idx in range(slices):
        start = boundaries[idx]
        end = boundaries[idx + 1]
        chunk = result.iloc[start:end].copy()
        if chunk.empty:
            continue
        rows.append(
            {
                "slice": idx + 1,
                "date_from": pd.Timestamp(chunk["date"].iloc[0]).date(),
                "date_to": pd.Timestamp(chunk["date"].iloc[-1]).date(),
                "days": int(len(chunk)),
                "strategy_return": float(chunk["strategy_equity"].iloc[-1] / chunk["strategy_equity"].iloc[0] - 1.0),
                "buy_hold_return": float(chunk["buy_hold_equity"].iloc[-1] / chunk["buy_hold_equity"].iloc[0] - 1.0),
                "strategy_max_drawdown": _max_drawdown(chunk["strategy_equity"] / chunk["strategy_equity"].iloc[0]),
                "buy_hold_max_drawdown": _max_drawdown(chunk["buy_hold_equity"] / chunk["buy_hold_equity"].iloc[0]),
                "strategy_sharpe": _annualized_sharpe(chunk["strategy_return"]),
                "buy_hold_sharpe": _annualized_sharpe(chunk["buy_hold_return"]),
                "avg_position": float(chunk["position"].mean()),
            }
        )
    return pd.DataFrame(rows)


def _run_training_schedule_grid(
    frame: pd.DataFrame,
    *,
    min_train_values: list[int],
    retrain_values: list[int],
    caution_threshold: float,
    risk_off_threshold: float,
    cache_dir: str | Path,
    symbol: str,
    start: str,
    end: str,
    refresh_cache: bool = False,
) -> pd.DataFrame:
    rows = []
    for min_train_days, retrain_every in itertools.product(min_train_values, retrain_values):
        cache_path = _probability_cache_path(
            cache_dir,
            symbol=symbol,
            start=start,
            end=end,
            min_train_days=min_train_days,
            retrain_every=retrain_every,
        )
        usable = _generate_walk_forward_probabilities(
            frame,
            min_train_days=min_train_days,
            retrain_every=retrain_every,
            cache_path=cache_path,
            refresh_cache=refresh_cache,
        )
        _, metrics = _evaluate_probability_frame(
            usable,
            risk_off_threshold=risk_off_threshold,
            caution_threshold=caution_threshold,
        )
        rows.append(
            {
                "min_train_days": int(min_train_days),
                "retrain_every": int(retrain_every),
                **metrics,
            }
        )
    result = pd.DataFrame(rows)
    return result.sort_values(
        ["strategy_sharpe", "strategy_total_return", "strategy_max_drawdown"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def main() -> int:
    args = _build_parser().parse_args()
    market_df = _load_market_data(args.symbol, args.start, args.end)
    feature_df = _compute_quant_and_resonance_features(market_df)
    feature_df = _build_target(feature_df)
    frame = _build_candidate_frame(feature_df, cache_dir=args.cache_dir)
    base_cache_path = _probability_cache_path(
        args.probability_cache_dir,
        symbol=args.symbol,
        start=args.start,
        end=args.end,
        min_train_days=args.min_train_days,
        retrain_every=args.retrain_every,
    )
    usable = _generate_walk_forward_probabilities(
        frame,
        min_train_days=args.min_train_days,
        retrain_every=args.retrain_every,
        cache_path=base_cache_path,
        refresh_cache=args.refresh_probability_cache,
    )
    backtest_df, metrics = _evaluate_probability_frame(
        usable,
        risk_off_threshold=args.risk_off_threshold,
        caution_threshold=args.caution_threshold,
    )

    print("=" * 72)
    print("Next Production Metaphysical Model Backtest")
    print("=" * 72)
    print(f"symbol:              {args.symbol}")
    print(f"range:               {args.start} -> {args.end}")
    print(f"samples:             {metrics['sample_count']}")
    print(f"candidate features:  {len(NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES)}")
    print(f"AUC:                 {metrics['auc']:.4f}")
    print(f"AP:                  {metrics['ap']:.4f}")
    print(f"strategy return:     {metrics['strategy_total_return'] * 100:.2f}%")
    print(f"buy&hold return:     {metrics['buy_hold_total_return'] * 100:.2f}%")
    print(f"strategy max DD:     {metrics['strategy_max_drawdown'] * 100:.2f}%")
    print(f"buy&hold max DD:     {metrics['buy_hold_max_drawdown'] * 100:.2f}%")
    print(f"strategy sharpe:     {metrics['strategy_sharpe']:.2f}")
    print(f"buy&hold sharpe:     {metrics['buy_hold_sharpe']:.2f}")
    print(f"avg position:        {metrics['avg_position']:.2f}")
    print(f"risk-off days:       {metrics['risk_off_days']}")
    print(f"caution days:        {metrics['caution_days']}")
    print(f"full-risk days:      {metrics['full_risk_days']}")

    preview = backtest_df[
        ["date", "close", "tail_risk_probability", "position", "next_day_return", "strategy_equity"]
    ].tail(10)
    print("\nRecent signals:")
    print(preview.to_string(index=False))
    latest_signal = latest_next_production_signal(
        backtest_df,
        probability_col="tail_risk_probability",
        date_col="date",
        caution_threshold=args.caution_threshold,
        risk_off_threshold=args.risk_off_threshold,
    )
    print("\nLatest signal:")
    print(latest_signal)
    report_text = _load_tactical_report_text(args.tactical_report_file)
    if report_text:
        overlay_signal = latest_next_production_signal_with_report_overlay(
            backtest_df,
            report_text=report_text,
            probability_col="tail_risk_probability",
            date_col="date",
            caution_threshold=args.caution_threshold,
            risk_off_threshold=args.risk_off_threshold,
        )
        print("\nLatest signal with tactical report overlay:")
        print(overlay_signal)

    if args.grid_search:
        grid = _run_threshold_grid_search(
            usable,
            caution_values=[0.20, 0.25, 0.30, 0.35, 0.40, 0.45],
            risk_off_values=[0.45, 0.50, 0.55, 0.60, 0.65, 0.70],
        )
        print("\nTop threshold combos:")
        print(
            grid[
                [
                    "caution_threshold",
                    "risk_off_threshold",
                    "strategy_total_return",
                    "strategy_max_drawdown",
                    "strategy_sharpe",
                    "avg_position",
                    "risk_off_days",
                    "caution_days",
                ]
            ]
            .head(10)
            .to_string(index=False)
        )

        schedule_grid = _run_training_schedule_grid(
            frame,
            min_train_values=[252, 378, 504, 756],
            retrain_values=[5, 10, 21, 42],
            caution_threshold=args.caution_threshold,
            risk_off_threshold=args.risk_off_threshold,
            cache_dir=args.probability_cache_dir,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            refresh_cache=args.refresh_probability_cache,
        )
        print("\nTop training schedules:")
        print(
            schedule_grid[
                [
                    "min_train_days",
                    "retrain_every",
                    "strategy_total_return",
                    "strategy_max_drawdown",
                    "strategy_sharpe",
                    "auc",
                    "ap",
                    "avg_position",
                ]
            ]
            .head(10)
            .to_string(index=False)
        )

    slice_report = _time_slice_summary(backtest_df, slices=3)
    print("\nTime-slice stability:")
    print(
        slice_report[
            [
                "slice",
                "date_from",
                "date_to",
                "strategy_return",
                "buy_hold_return",
                "strategy_max_drawdown",
                "buy_hold_max_drawdown",
                "strategy_sharpe",
                "buy_hold_sharpe",
                "avg_position",
            ]
        ].to_string(index=False)
    )
    if args.record_stage_performance:
        record_stage_performance_run(
            args.stage_performance_log_path,
            stage=args.stage,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            metrics=metrics,
        )
        print("\nStage performance log:")
        print(args.stage_performance_log_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
