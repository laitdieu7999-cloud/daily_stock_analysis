# -*- coding: utf-8 -*-
"""Tests for local ETF history cache helpers."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from src.services.local_etf_history import (
    build_etf_cache_path,
    load_cached_etf_daily_ohlcv,
    normalize_cn_etf_symbol,
    save_cached_etf_daily_ohlcv,
)


class LocalEtfHistoryTestCase(unittest.TestCase):
    def test_normalize_cn_etf_symbol_handles_sh_and_sz(self) -> None:
        self.assertEqual(normalize_cn_etf_symbol("510500"), "sh510500")
        self.assertEqual(normalize_cn_etf_symbol("159937"), "sz159937")
        self.assertEqual(normalize_cn_etf_symbol("sh510300"), "sh510300")

    def test_save_and_load_cached_etf_daily_ohlcv_roundtrip(self) -> None:
        with TemporaryDirectory() as tmp:
            frame = pd.DataFrame(
                [
                    {"date": "2026-04-23", "open": 8.1, "close": 8.2, "high": 8.3, "low": 8.0, "volume": 1000},
                    {"date": "2026-04-24", "open": 8.2, "close": 8.25, "high": 8.28, "low": 8.15, "volume": 1100},
                ]
            )
            path = save_cached_etf_daily_ohlcv("510500", frame, tmp)
            self.assertTrue(path.exists())
            loaded = load_cached_etf_daily_ohlcv("sh510500", tmp)
            self.assertIsNotNone(loaded)
            self.assertEqual(len(loaded), 2)
            self.assertAlmostEqual(float(loaded.loc[1, "close"]), 8.25)

    def test_build_etf_cache_path_uses_normalized_symbol(self) -> None:
        with TemporaryDirectory() as tmp:
            path = build_etf_cache_path("159937", Path(tmp))
            self.assertEqual(path.name, "sz159937.csv")
