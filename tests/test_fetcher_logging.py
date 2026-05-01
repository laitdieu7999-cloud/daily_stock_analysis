import logging
import os
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_provider.base import BaseFetcher, DataFetchError, DataFetcherManager
from data_provider.efinance_fetcher import EfinanceFetcher
from src.logging_config import setup_logging


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2026-03-06", "2026-03-07"],
            "open": [10.0, 10.2],
            "high": [10.5, 10.4],
            "low": [9.8, 10.1],
            "close": [10.3, 10.35],
            "volume": [1000, 1200],
            "amount": [10300, 12420],
            "pct_chg": [1.0, 0.49],
        }
    )


class _SuccessFetcher(BaseFetcher):
    name = "SuccessFetcher"
    priority = 1

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return _sample_df()

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        return df


class _FailureFetcher(BaseFetcher):
    name = "FailureFetcher"
    priority = 0

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        raise DataFetchError(
            "Eastmoney 历史K线接口失败: "
            "endpoint=push2his.eastmoney.com/api/qt/stock/kline/get, "
            "category=remote_disconnect"
        )

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        return df


class TestFetcherLogging(unittest.TestCase):
    def test_setup_logging_raises_urllib3_connectionpool_to_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            setup_logging(log_prefix="test_logging", log_dir=tmp_dir, debug=False)

        self.assertEqual(logging.getLogger("urllib3").level, logging.WARNING)
        self.assertEqual(
            logging.getLogger("urllib3.connectionpool").level,
            logging.ERROR,
        )

    def test_tushare_missing_token_warning_only_logs_once_at_warning_level(self):
        import data_provider.tushare_fetcher as tushare_module
        from data_provider.tushare_fetcher import TushareFetcher

        tushare_module._TUSHARE_MISSING_TOKEN_WARNED = False
        fake_config = SimpleNamespace(tushare_token="")

        with patch("data_provider.tushare_fetcher.get_config", return_value=fake_config):
            with self.assertLogs("data_provider.tushare_fetcher", level="WARNING") as captured:
                TushareFetcher()

            self.assertIn("Tushare Token 未配置，此数据源不可用", "\n".join(captured.output))

            with patch.object(tushare_module.logger, "warning") as warning_mock:
                TushareFetcher()
            warning_mock.assert_not_called()

    def test_default_fetcher_priority_summary_logs_info_only_once(self):
        import data_provider.base as base_module
        base_module._FETCHER_PRIORITY_INFO_LOGGED = False

        dummy_fetchers = {
            "data_provider.efinance_fetcher.EfinanceFetcher": SimpleNamespace(name="EfinanceFetcher", priority=0),
            "data_provider.akshare_fetcher.AkshareFetcher": SimpleNamespace(name="AkshareFetcher", priority=1),
            "data_provider.tushare_fetcher.TushareFetcher": SimpleNamespace(name="TushareFetcher", priority=2),
            "data_provider.pytdx_fetcher.PytdxFetcher": SimpleNamespace(name="PytdxFetcher", priority=2),
            "data_provider.baostock_fetcher.BaostockFetcher": SimpleNamespace(name="BaostockFetcher", priority=3),
            "data_provider.yfinance_fetcher.YfinanceFetcher": SimpleNamespace(name="YfinanceFetcher", priority=4),
            "data_provider.longbridge_fetcher.LongbridgeFetcher": SimpleNamespace(name="LongbridgeFetcher", priority=5),
        }

        with patch("data_provider.base.AkshareFundamentalAdapter", return_value=SimpleNamespace()):
            with patch.multiple(
                "data_provider.efinance_fetcher",
                EfinanceFetcher=lambda: dummy_fetchers["data_provider.efinance_fetcher.EfinanceFetcher"],
            ), patch.multiple(
                "data_provider.akshare_fetcher",
                AkshareFetcher=lambda: dummy_fetchers["data_provider.akshare_fetcher.AkshareFetcher"],
            ), patch.multiple(
                "data_provider.tushare_fetcher",
                TushareFetcher=lambda: dummy_fetchers["data_provider.tushare_fetcher.TushareFetcher"],
            ), patch.multiple(
                "data_provider.pytdx_fetcher",
                PytdxFetcher=lambda: dummy_fetchers["data_provider.pytdx_fetcher.PytdxFetcher"],
            ), patch.multiple(
                "data_provider.baostock_fetcher",
                BaostockFetcher=lambda: dummy_fetchers["data_provider.baostock_fetcher.BaostockFetcher"],
            ), patch.multiple(
                "data_provider.yfinance_fetcher",
                YfinanceFetcher=lambda: dummy_fetchers["data_provider.yfinance_fetcher.YfinanceFetcher"],
            ), patch.multiple(
                "data_provider.longbridge_fetcher",
                LongbridgeFetcher=lambda: dummy_fetchers["data_provider.longbridge_fetcher.LongbridgeFetcher"],
            ):
                with self.assertLogs("data_provider.base", level="INFO") as captured:
                    DataFetcherManager()

                self.assertIn("已初始化 7 个数据源（按优先级）", "\n".join(captured.output))

                with patch.object(base_module.logger, "info") as info_mock:
                    DataFetcherManager()
                info_mock.assert_not_called()

    def test_base_fetcher_logs_start_and_success(self):
        fetcher = _SuccessFetcher()

        with self.assertLogs("data_provider.base", level="INFO") as captured:
            df = fetcher.get_daily_data("600519", start_date="2026-03-01", end_date="2026-03-08")

        log_text = "\n".join(captured.output)
        self.assertFalse(df.empty)
        self.assertIn("[SuccessFetcher] 开始获取 600519 日线数据", log_text)
        self.assertIn("[SuccessFetcher] 600519 获取成功:", log_text)
        self.assertIn("rows=2", log_text)

    def test_manager_logs_fallback_and_final_success(self):
        manager = DataFetcherManager(fetchers=[_FailureFetcher(), _SuccessFetcher()])

        with self.assertLogs("data_provider.base", level="INFO") as captured:
            df, source = manager.get_daily_data("601006", start_date="2026-01-07", end_date="2026-03-08")

        log_text = "\n".join(captured.output)
        self.assertFalse(df.empty)
        self.assertEqual(source, "SuccessFetcher")
        self.assertIn("[数据源尝试 1/2] [FailureFetcher] 获取 601006...", log_text)
        self.assertIn("[数据源失败 1/2] [FailureFetcher] 601006:", log_text)
        self.assertIn("[数据源切换] 601006: [FailureFetcher] -> [SuccessFetcher]", log_text)
        self.assertIn("[数据源完成] 601006 使用 [SuccessFetcher] 获取成功:", log_text)

    def test_efinance_logs_eastmoney_endpoint_on_remote_disconnect(self):
        fetcher = EfinanceFetcher()
        fake_efinance = types.SimpleNamespace(
            stock=types.SimpleNamespace(
                get_quote_history=lambda **kwargs: (_ for _ in ()).throw(
                    requests.exceptions.ConnectionError("Remote end closed connection without response")
                )
            )
        )

        with patch.dict(sys.modules, {"efinance": fake_efinance}):
            with patch.object(fetcher, "_set_random_user_agent", return_value=None), patch.object(
                fetcher, "_enforce_rate_limit", return_value=None
            ):
                with self.assertLogs(level="INFO") as captured:
                    with self.assertRaises(DataFetchError):
                        fetcher.get_daily_data("601006", start_date="2026-01-07", end_date="2026-03-08")

        log_text = "\n".join(captured.output)
        self.assertIn("Eastmoney 历史K线接口失败:", log_text)
        self.assertIn("endpoint=push2his.eastmoney.com/api/qt/stock/kline/get", log_text)
        self.assertIn("category=remote_disconnect", log_text)
        self.assertIn("[EfinanceFetcher] 601006 获取失败:", log_text)


if __name__ == "__main__":
    unittest.main()
