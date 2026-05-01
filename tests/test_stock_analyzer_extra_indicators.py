# -*- coding: utf-8 -*-
"""Tests for expanded technical indicators in StockTrendAnalyzer."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.stock_analyzer import StockTrendAnalyzer


def _sample_ohlcv(rows: int = 80) -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=rows, freq="D")
    base = np.linspace(10.0, 14.0, rows)
    wave = np.sin(np.linspace(0, 8, rows)) * 0.25
    close = base + wave
    open_ = close - 0.05
    high = close + 0.25
    low = close - 0.25
    volume = np.linspace(1_000_000, 1_500_000, rows)
    return pd.DataFrame(
        {
            "date": dates,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "amount": close * volume,
        }
    )


def test_prepare_indicator_frame_adds_expanded_columns() -> None:
    analyzer = StockTrendAnalyzer()
    frame = analyzer.prepare_indicator_frame(_sample_ohlcv())

    for column in [
        "ADX",
        "PLUS_DI",
        "MINUS_DI",
        "MFI",
        "CCI",
        "ROC",
        "DONCHIAN_UPPER",
        "WILLIAMS_R",
        "STOCH_RSI",
        "CMF",
        "VWAP",
        "VWAP_DISTANCE_PCT",
    ]:
        assert column in frame.columns

    latest = frame.iloc[-1]
    assert np.isfinite(float(latest["ADX"]))
    assert np.isfinite(float(latest["MFI"]))
    assert np.isfinite(float(latest["VWAP_DISTANCE_PCT"]))


def test_analyze_exports_expanded_indicator_values() -> None:
    analyzer = StockTrendAnalyzer()
    result = analyzer.analyze(_sample_ohlcv(), "600000")
    payload = result.to_dict()

    assert "adx_14" in payload
    assert "mfi_14" in payload
    assert "donchian_signal" in payload
    assert "vwap_signal" in payload
    assert "indicator_consensus_signal" in payload
    assert "indicator_consensus_score" in payload
    assert "indicator_consensus_details" in payload
    assert isinstance(result.adx_signal, str)
    assert isinstance(result.cmf_signal, str)
    assert isinstance(result.indicator_consensus_signal, str)
    assert isinstance(result.indicator_consensus_details, list)
    assert result.indicator_bullish_count + result.indicator_bearish_count + result.indicator_neutral_count > 0
