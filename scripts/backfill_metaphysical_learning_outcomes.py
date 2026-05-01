#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backfill forward outcomes in the metaphysical learning snapshot ledger."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.metaphysical import (  # noqa: E402
    archive_price_history,
    backfill_learning_snapshot_outcomes,
)
from src.services.local_etf_history import load_cached_etf_daily_ohlcv  # noqa: E402
from src.services.qlib_local_history import (  # noqa: E402
    find_latest_bootstrapped_qlib_root,
    load_qlib_daily_ohlcv,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill 1/3/5/10-day outcomes in the metaphysical learning JSONL ledger."
    )
    parser.add_argument(
        "--symbol",
        default="510500.SS",
        help="Target symbol. Local Qlib/ETF history is used first; yfinance is only a fallback.",
    )
    parser.add_argument("--start", default="2016-01-01", help="Start date in YYYY-MM-DD format.")
    parser.add_argument(
        "--end",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date in YYYY-MM-DD format. Default: today.",
    )
    parser.add_argument(
        "--snapshot-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_learning_samples.jsonl"),
        help="Path to the daily learning snapshot JSONL ledger.",
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
                return frame[["date", "close"]].sort_values("date").reset_index(drop=True)

    local_etf = load_cached_etf_daily_ohlcv(symbol)
    if local_etf is not None and not local_etf.empty:
        frame = local_etf.copy()
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame[(frame["date"] >= pd.Timestamp(start)) & (frame["date"] <= pd.Timestamp(end))].copy()
        if not frame.empty:
            return frame[["date", "close"]].sort_values("date").reset_index(drop=True)

    import yfinance as yf

    ticker = yf.Ticker(symbol)
    frame = ticker.history(start=start, end=end, auto_adjust=True)
    if frame is None or frame.empty:
        raise RuntimeError(f"no market data returned for {symbol}")
    frame = frame.reset_index().rename(columns={"Date": "date", "Close": "close"})
    frame = frame[["date", "close"]].copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.tz_localize(None)
    return frame


def main() -> int:
    args = _build_parser().parse_args()
    prices = _load_market_data(args.symbol, args.start, args.end)
    archive_price_history(prices, symbol=args.symbol, suffix="backfill")
    output_path = backfill_learning_snapshot_outcomes(args.snapshot_path, prices)
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
