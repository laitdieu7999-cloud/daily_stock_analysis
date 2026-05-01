# -*- coding: utf-8 -*-
"""Regression tests for ambiguous index/special market code routing."""

import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_provider.base import DataFetcherManager, is_cn_index_code, normalize_stock_code


class TestSpecialMarketCodeRouting(unittest.TestCase):
    def test_ndx100_normalizes_to_supported_ndx_alias(self):
        self.assertEqual(normalize_stock_code("NDX100"), "NDX")

    def test_000905_is_forced_to_cn_index(self):
        self.assertTrue(is_cn_index_code("000905"))

    def test_000905_name_overrides_stale_stock_cache(self):
        manager = DataFetcherManager.__new__(DataFetcherManager)
        manager._fetchers = []
        manager._stock_name_cache = {"000905": "厦门港务"}

        name = DataFetcherManager.get_stock_name(manager, "000905", allow_realtime=False)

        self.assertEqual(name, "中证500指数")
        self.assertEqual(manager._stock_name_cache["000905"], "中证500指数")

    def test_000905_daily_data_uses_index_api_not_stock_fetchers(self):
        manager = DataFetcherManager.__new__(DataFetcherManager)
        manager._fetchers = [MagicMock(name="ShouldNotCallStockFetcher")]
        raw_df = pd.DataFrame(
            [
                {"date": "2026-01-02", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100},
                {"date": "2026-01-05", "open": 2, "high": 3, "low": 2, "close": 3, "volume": 120},
            ]
        )
        fake_akshare = SimpleNamespace(stock_zh_index_daily=MagicMock(return_value=raw_df))

        with patch.dict(sys.modules, {"akshare": fake_akshare}):
            df, source = DataFetcherManager.get_daily_data(
                manager,
                "000905",
                start_date="2026-01-01",
                end_date="2026-01-06",
            )

        fake_akshare.stock_zh_index_daily.assert_called_once_with(symbol="sh000905")
        self.assertEqual(source, "AkshareIndexFetcher")
        self.assertEqual(df["code"].iloc[0], "000905")
        self.assertIn("ma20", df.columns)

    def test_000905_realtime_uses_index_hub(self):
        manager = DataFetcherManager.__new__(DataFetcherManager)
        manager._fetchers = []
        manager.get_main_indices = MagicMock(
            return_value=[
                {
                    "code": "sh000905",
                    "name": "中证500",
                    "current": 6000.5,
                    "change": -12.3,
                    "change_pct": -0.2,
                    "open": 6010.0,
                    "high": 6020.0,
                    "low": 5990.0,
                    "prev_close": 6012.8,
                    "volume": 123456,
                    "amount": 987654321.0,
                }
            ]
        )
        fake_config = SimpleNamespace(enable_realtime_quote=True, realtime_source_priority="efinance,akshare_em")

        with patch("src.config.get_config", return_value=fake_config):
            quote = DataFetcherManager.get_realtime_quote(manager, "000905")

        self.assertIsNotNone(quote)
        self.assertEqual(quote.code, "000905")
        self.assertEqual(quote.name, "中证500")
        self.assertEqual(quote.price, 6000.5)
        manager.get_main_indices.assert_called_once_with(region="cn")


if __name__ == "__main__":
    unittest.main()
