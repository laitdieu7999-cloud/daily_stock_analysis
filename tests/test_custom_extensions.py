# -*- coding: utf-8 -*-
"""Regression tests for local custom extension modules."""

import asyncio
import json
import sys
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd


def _build_kline_df(rows: int, *, start_close: float = 100.0) -> pd.DataFrame:
    records = []
    for index in range(rows):
        open_price = start_close + index
        close_price = open_price + 1
        records.append(
            {
                "date": f"2026-04-{index + 1:02d}",
                "open": open_price,
                "high": close_price + 1,
                "low": open_price - 1,
                "close": close_price,
                "volume": 1000 + index * 10,
            }
        )
    return pd.DataFrame(records)


def _make_trend_result() -> SimpleNamespace:
    return SimpleNamespace(
        buy_signal=SimpleNamespace(value="BUY"),
        ma5=101.0,
        ma10=99.0,
        ma20=97.0,
        ma60=88.0,
        ma_alignment="MA5 > MA10 > MA20",
        macd_signal="金叉",
        macd_dif=1.2,
        macd_dea=0.8,
        boll_signal="接近上轨",
        boll_upper=110.0,
        boll_mid=100.0,
        boll_lower=90.0,
        kdj_signal="强势",
        kdj_k=75.0,
        kdj_d=68.0,
        kdj_j=89.0,
        rsi_signal="强势买入",
        rsi_6=66.0,
        rsi_12=59.0,
        atr_percent=2.5,
        atr_14=2.1,
        obv_signal="OBV上升",
        rsrs_signal="RSRS买入",
        rsrs_beta=1.02,
        rsrs_zscore=0.81,
        rsrs_r2=0.72,
        volume_trend="量能正常",
        signal_score=78,
        signal_reasons=["均线多头", "量价配合"],
        risk_factors=["短线波动"],
    )


class MarketDataFetcherTestCase(unittest.TestCase):
    def test_resolve_mcp_remote_command_returns_none_when_npx_missing(self):
        from src.market_data_fetcher import MarketDataFetcher

        with patch("src.market_data_fetcher.shutil.which", return_value=None):
            fetcher = MarketDataFetcher("jin10-secret")

        self.assertIsNone(fetcher._mcp_remote_cmd)

    def test_get_quote_returns_none_without_api_key(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("")

        self.assertIsNone(fetcher.get_quote("XAUUSD"))

    def test_get_quote_returns_none_when_npx_missing(self):
        from src.market_data_fetcher import MarketDataFetcher

        with patch("src.market_data_fetcher.shutil.which", return_value=None):
            fetcher = MarketDataFetcher("jin10-secret")

        self.assertIsNone(fetcher.get_quote("XAUUSD"))

    def test_list_flash_limits_and_normalizes_items_from_result_dict(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret")
        fetcher._call_tool = MagicMock(
            return_value={
                "data": {
                    "items": [
                        {"id": "1", "title": "a"},
                        {"id": "2", "title": "b"},
                        "ignored",
                    ]
                }
            }
        )

        items = fetcher.list_flash(limit=1)

        self.assertEqual(items, [{"id": "1", "title": "a"}])

    def test_get_quote_returns_none_when_tool_call_raises(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret")
        fetcher._call_tool = MagicMock(side_effect=RuntimeError("boom"))

        self.assertIsNone(fetcher.get_quote("XAUUSD"))

    def test_list_flash_returns_empty_list_on_tool_failure(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret")
        fetcher._call_tool = MagicMock(side_effect=RuntimeError("boom"))

        self.assertEqual(fetcher.list_flash(limit=5), [])

    def test_list_vip_watch_events_returns_empty_without_x_token(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret")

        self.assertEqual(fetcher.list_vip_watch_events(limit=5), [])

    def test_get_nasdaq_golden_dragon_snapshot_parses_official_page(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret")
        html = """
        <html>
          <div id="overview">
            <data class="change">1.21%</data>
            <span>Previous Close</span><span>6,843.70</span>
            <span>Today’s High</span><span>6,953.39</span>
            <tr><td>Last</td><td>6,926.41</td></tr>
            <tr><td>Net Change</td><td>81.20</td></tr>
          </div>
        </html>
        """
        response = MagicMock()
        response.text = html
        response.raise_for_status = MagicMock()

        with patch("src.market_data_fetcher.requests.get", return_value=response):
            snapshot = fetcher.get_nasdaq_golden_dragon_snapshot()

        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["code"], "HXC")
        self.assertAlmostEqual(snapshot["last"], 6926.41)
        self.assertAlmostEqual(snapshot["change_pct"], 1.21)
        self.assertAlmostEqual(snapshot["change"], 81.20)

    def test_get_vip_watch_indicator_resonance_uses_x_token(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret", "jin10-x-token")
        fetcher._vip_watch_get = MagicMock(
            return_value={"status": 200, "data": {"code": "000905", "weights": 87}}
        )

        data = fetcher.get_vip_watch_indicator_resonance("000905")

        self.assertEqual(data, {"code": "000905", "weights": 87})
        fetcher._vip_watch_get.assert_called_once_with(
            "/api/vip-watch/indicator/resonance",
            {"code": "000905"},
        )

    def test_list_vip_watch_products_returns_items(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret", "jin10-x-token")
        fetcher._vip_watch_get = MagicMock(
            return_value={"status": 200, "data": [{"code": "XAUUSD.GOODS"}, {"code": "XAGUSD.GOODS"}]}
        )

        items = fetcher.list_vip_watch_products()

        self.assertEqual(items, [{"code": "XAUUSD.GOODS"}, {"code": "XAGUSD.GOODS"}])

    def test_get_ic_basis_uses_spot_sina_fallback_and_chinese_fields(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret")
        spot_df = pd.DataFrame(
            [
                {"代码": "sh000001", "最新价": 3000.0},
                {"代码": "sh000905", "最新价": 5800.0},
            ]
        )
        futures_df = pd.DataFrame(
            [
                {
                    "日期": "2026-04-25",
                    "收盘价": 5750.0,
                    "合约名称": "IC2505",
                }
            ]
        )

        fake_ak = ModuleType("akshare")
        fake_ak.stock_zh_index_spot_sina = MagicMock(return_value=spot_df)
        fake_ak.stock_zh_index_daily = MagicMock(return_value=pd.DataFrame())
        fake_ak.futures_main_sina = MagicMock(return_value=futures_df)

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            basis = fetcher.get_ic_basis()

        self.assertIsNotNone(basis)
        self.assertAlmostEqual(basis.spot_price, 5800.0)
        self.assertAlmostEqual(basis.futures_price, 5750.0)
        self.assertEqual(basis.contract_code, "IC2505")
        self.assertGreater(basis.days_to_expiry, 0)

    def test_get_ic_basis_prefers_realtime_main_contract_when_available(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret")
        spot_df = pd.DataFrame([{"代码": "sh000905", "最新价": 5800.0}])
        realtime_df = pd.DataFrame(
            [
                {"symbol": "IF2606", "trade": 5000.0},
                {"symbol": "IC2606", "trade": 5755.0},
            ]
        )

        fake_ak = ModuleType("akshare")
        fake_ak.stock_zh_index_spot_sina = MagicMock(return_value=spot_df)
        fake_ak.stock_zh_index_daily = MagicMock(return_value=pd.DataFrame())
        fake_ak.match_main_contract = MagicMock(return_value="IF2606,IC2606,IM2606")
        fake_ak.futures_zh_realtime = MagicMock(return_value=realtime_df)
        fake_ak.futures_main_sina = MagicMock()

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            basis = fetcher.get_ic_basis()

        self.assertIsNotNone(basis)
        self.assertAlmostEqual(basis.futures_price, 5755.0)
        self.assertEqual(basis.contract_code, "IC2606")
        self.assertGreater(basis.days_to_expiry, 0)
        fake_ak.futures_main_sina.assert_not_called()

    def test_get_ic_term_structure_builds_front_end_and_q_anchor_snapshot(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret")
        realtime_df = pd.DataFrame(
            [
                {"symbol": "IC2605", "trade": 5700.0},
                {"symbol": "IC2606", "trade": 5712.0},
                {"symbol": "IC2609", "trade": 5750.0},
                {"symbol": "IC2612", "trade": 5790.0},
            ]
        )

        fake_ak = ModuleType("akshare")
        fake_ak.futures_zh_realtime = MagicMock(return_value=realtime_df)

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            data = fetcher.get_ic_term_structure()

        self.assertIsNotNone(data)
        self.assertEqual(data.near_symbol, "IC2605")
        self.assertEqual(data.next_symbol, "IC2606")
        self.assertIsNotNone(data.q1_q2_annualized_pct)
        self.assertIsNotNone(data.front_end_gap_pct)
        self.assertGreater(data.next_days, data.near_days)

    def test_get_ic_term_structure_uses_spot_fallback_when_realtime_list_is_empty(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret")
        empty_realtime = pd.DataFrame(
            columns=["symbol", "trade", "exchange", "name"]
        )

        def _fake_spot(symbol: str, market: str = "FF", adjust: str = "0"):
            values = {
                "IC2606": 8132.8,
                "IC2607": 8156.4,
                "IC2609": 8201.0,
                "IC2612": 8260.2,
            }
            if symbol not in values:
                return pd.DataFrame()
            return pd.DataFrame(
                [
                    {
                        "symbol": f"中证500指数期货{symbol[2:]}",
                        "current_price": values[symbol],
                    }
                ]
            )

        fake_ak = ModuleType("akshare")
        fake_ak.futures_zh_realtime = MagicMock(return_value=empty_realtime)
        fake_ak.match_main_contract = MagicMock(return_value="IF2606,TF2606,IH2606,IC2606,TS2606,IM2606")
        fake_ak.futures_zh_spot = MagicMock(side_effect=_fake_spot)

        with patch.dict(sys.modules, {"akshare": fake_ak}), \
             patch("src.market_data_fetcher.MarketDataFetcher._load_ic_contracts_via_sina_hq", return_value=[]):
            data = fetcher.get_ic_term_structure()

        self.assertIsNotNone(data)
        self.assertEqual(data.near_symbol, "IC2606")
        self.assertEqual(data.next_symbol, "IC2607")
        self.assertIsNotNone(data.front_end_gap_pct)
        self.assertGreater(data.next_price, data.near_price)

    def test_get_500etf_option_proxy_builds_qvix_and_put_skew_snapshot(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret")
        qvix_df = pd.DataFrame(
            [
                {"time": "14:00:00", "qvix": 20.0},
                {"time": "14:05:00", "qvix": 20.2},
                {"time": "14:10:00", "qvix": 20.4},
                {"time": "14:15:00", "qvix": 24.0},
            ]
        )
        board_df = pd.DataFrame(
            [
                {"日期": "20260426143000", "合约交易代码": "510500C2606M08250", "当前价": 0.35, "行权价": 8.25},
                {"日期": "20260426143000", "合约交易代码": "510500P2606M08250", "当前价": 0.32, "行权价": 8.25},
                {"日期": "20260426143000", "合约交易代码": "510500C2606M08750", "当前价": 0.62, "行权价": 8.75},
                {"日期": "20260426143000", "合约交易代码": "510500P2606M08750", "当前价": 0.60, "行权价": 8.75},
                {"日期": "20260426143000", "合约交易代码": "510500C2606M09000", "当前价": 0.80, "行权价": 9.00},
                {"日期": "20260426143000", "合约交易代码": "510500P2606M09000", "当前价": 0.88, "行权价": 9.00},
            ]
        )
        meta_df = pd.DataFrame(
            [
                {"合约编码": "1000", "合约交易代码": "510500P2606M08250", "标的券名称及代码": "500ETF(510500)", "到期日": "20260527"},
                {"合约编码": "1001", "合约交易代码": "510500C2606M08750", "标的券名称及代码": "500ETF(510500)"},
                {"合约编码": "1002", "合约交易代码": "510500P2606M08750", "标的券名称及代码": "500ETF(510500)"},
            ]
        )

        def _spot_payload(latest: float, volume: int, bid1: float, ask1: float) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {"字段": "最新价", "值": latest},
                    {"字段": "成交量", "值": volume},
                    {"字段": "行情时间", "值": "2026-04-26 14:29:00"},
                    {"字段": "申买价一", "值": bid1},
                    {"字段": "申卖价一", "值": ask1},
                ]
            )

        fake_ak = ModuleType("akshare")
        fake_ak.index_option_500etf_min_qvix = MagicMock(return_value=qvix_df)
        fake_ak.option_finance_board = MagicMock(return_value=board_df)
        fake_ak.option_current_day_sse = MagicMock(return_value=meta_df)
        fake_ak.option_sse_spot_price_sina = MagicMock(
            side_effect=[
                _spot_payload(0.60, 3200, 0.58, 0.61),
                _spot_payload(0.62, 1200, 0.60, 0.63),
                _spot_payload(0.32, 900, 0.31, 0.34),
            ]
        )

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            data = fetcher.get_500etf_option_proxy()

        self.assertIsNotNone(data)
        self.assertEqual(data.expiry_ym, "2606")
        self.assertEqual(data.expiry_style, "M")
        self.assertAlmostEqual(data.atm_strike, 8.75)
        self.assertAlmostEqual(data.otm_put_strike, 8.25)
        self.assertAlmostEqual(data.put_skew_ratio, round(0.32 / 0.60, 3))
        self.assertAlmostEqual(data.atm_put_call_volume_ratio, round(3200 / 1200, 2))
        self.assertGreater(data.qvix_zscore, 1.0)
        self.assertEqual(data.otm_put_price_source, "ask1")
        self.assertAlmostEqual(data.otm_put_price, 0.34)
        self.assertAlmostEqual(data.otm_put_ask1, 0.34)
        self.assertAlmostEqual(data.otm_put_bid1, 0.31)
        self.assertGreater(data.otm_put_days_to_expiry, 0)

    def test_get_500etf_option_proxy_skips_front_month_near_expiry(self):
        from src.market_data_fetcher import MarketDataFetcher

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 4, 28, 14, 30, 0)

        fetcher = MarketDataFetcher("jin10-secret")
        qvix_df = pd.DataFrame(
            [
                {"time": "14:00:00", "qvix": 20.0},
                {"time": "14:05:00", "qvix": 20.2},
                {"time": "14:10:00", "qvix": 20.4},
                {"time": "14:15:00", "qvix": 24.0},
            ]
        )
        board_df = pd.DataFrame(
            [
                {"日期": "20260428143000", "合约交易代码": "510500C2604M08250", "当前价": 0.25, "行权价": 8.25},
                {"日期": "20260428143000", "合约交易代码": "510500P2604M08250", "当前价": 0.22, "行权价": 8.25},
                {"日期": "20260428143000", "合约交易代码": "510500C2604M08750", "当前价": 0.45, "行权价": 8.75},
                {"日期": "20260428143000", "合约交易代码": "510500P2604M08750", "当前价": 0.43, "行权价": 8.75},
                {"日期": "20260428143000", "合约交易代码": "510500C2605M08250", "当前价": 0.35, "行权价": 8.25},
                {"日期": "20260428143000", "合约交易代码": "510500P2605M08250", "当前价": 0.32, "行权价": 8.25},
                {"日期": "20260428143000", "合约交易代码": "510500C2605M08750", "当前价": 0.62, "行权价": 8.75},
                {"日期": "20260428143000", "合约交易代码": "510500P2605M08750", "当前价": 0.60, "行权价": 8.75},
            ]
        )
        meta_df = pd.DataFrame(
            [
                {"合约编码": "2001", "合约交易代码": "510500C2604M08750", "标的券名称及代码": "500ETF(510500)", "到期日": "20260430"},
                {"合约编码": "2002", "合约交易代码": "510500P2604M08750", "标的券名称及代码": "500ETF(510500)", "到期日": "20260430"},
                {"合约编码": "2003", "合约交易代码": "510500P2604M08250", "标的券名称及代码": "500ETF(510500)", "到期日": "20260430"},
                {"合约编码": "3001", "合约交易代码": "510500C2605M08750", "标的券名称及代码": "500ETF(510500)", "到期日": "20260527"},
                {"合约编码": "3002", "合约交易代码": "510500P2605M08750", "标的券名称及代码": "500ETF(510500)", "到期日": "20260527"},
                {"合约编码": "3003", "合约交易代码": "510500P2605M08250", "标的券名称及代码": "500ETF(510500)", "到期日": "20260527"},
            ]
        )

        def _spot_payload(latest: float, volume: int, bid1: float, ask1: float) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {"字段": "最新价", "值": latest},
                    {"字段": "成交量", "值": volume},
                    {"字段": "行情时间", "值": "2026-04-28 14:29:00"},
                    {"字段": "申买价一", "值": bid1},
                    {"字段": "申卖价一", "值": ask1},
                ]
            )

        fake_ak = ModuleType("akshare")
        fake_ak.index_option_500etf_min_qvix = MagicMock(return_value=qvix_df)
        fake_ak.option_finance_board = MagicMock(return_value=board_df)
        fake_ak.option_current_day_sse = MagicMock(return_value=meta_df)
        fake_ak.option_sse_spot_price_sina = MagicMock(
            side_effect=[
                _spot_payload(0.60, 3200, 0.58, 0.61),
                _spot_payload(0.62, 1200, 0.60, 0.63),
                _spot_payload(0.32, 900, 0.31, 0.34),
            ]
        )

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            with patch("src.market_data_fetcher.datetime", FixedDateTime):
                data = fetcher.get_500etf_option_proxy()

        self.assertIsNotNone(data)
        self.assertEqual(data.expiry_ym, "2605")
        self.assertTrue(data.roll_window_shifted)
        self.assertGreater(data.expiry_days_to_expiry, 3)

    def test_get_historical_kline_prefers_stock_zh_index_daily_for_000905(self):
        from src.market_data_fetcher import MarketDataFetcher

        fetcher = MarketDataFetcher("jin10-secret")
        index_df = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-04-23"),
                    "open": 5800.0,
                    "high": 5820.0,
                    "low": 5790.0,
                    "close": 5810.0,
                    "volume": 123456789,
                }
            ]
        )

        fake_ak = ModuleType("akshare")
        fake_ak.stock_zh_index_daily = MagicMock(return_value=index_df)
        fake_ak.index_zh_a_hist = MagicMock()

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            df = fetcher.get_historical_kline("000905", source="index_zh_a")

        self.assertIsNotNone(df)
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df.iloc[0]["close"], 5810.0)
        fake_ak.index_zh_a_hist.assert_not_called()


class IntradaySnapshotCollectorTestCase(unittest.TestCase):
    def test_collect_intraday_snapshots_appends_snapshot_and_dedupes_events(self):
        from src import intraday_snapshot_collector as collector

        fake_fetcher = MagicMock()
        fake_fetcher.get_gold_silver_quotes.return_value = {
            "XAUUSD": SimpleNamespace(price=3300.5, change_pct=1.2, high=3310.0, low=3288.0, time="10:00"),
            "XAGUSD": SimpleNamespace(price=33.2, change_pct=-0.4, high=33.6, low=32.9, time="10:00"),
        }
        fake_fetcher.get_ic_basis.return_value = SimpleNamespace(
            spot_price=5800.0,
            futures_price=5750.0,
            basis=50.0,
            annualized_basis_pct=8.0,
            contract_code="IC2505",
            days_to_expiry=20,
        )
        fake_fetcher.get_nasdaq_golden_dragon_snapshot.return_value = {
            "code": "HXC",
            "change_pct": 1.1,
        }
        fake_fetcher.get_ic_term_structure.return_value = None
        fake_fetcher.get_500etf_option_proxy.return_value = None
        fake_fetcher.list_flash.return_value = [
            {"id": "", "title": "中东局势升级，原油拉升", "time": "22:15"},
            {"id": "", "title": "中东局势升级，原油拉升", "time": "22:15"},
            {"id": "", "title": "Anthropic获得新融资，估值走高", "time": "22:16"},
        ]

        with TemporaryDirectory() as tmp_dir:
            archive_dir = Path(tmp_dir) / "intraday"
            with patch.object(collector, "INTRADAY_ARCHIVE_DIR", archive_dir):
                with patch("src.intraday_snapshot_collector.MarketDataFetcher", return_value=fake_fetcher):
                    first = collector.collect_intraday_snapshots(
                        jin10_api_key="jin10-secret",
                        jin10_x_token="jin10-x-token",
                    )
                    second = collector.collect_intraday_snapshots(
                        jin10_api_key="jin10-secret",
                        jin10_x_token="jin10-x-token",
                    )

            self.assertIsNotNone(first)
            self.assertIsNotNone(second)
            snapshot_lines = Path(first["snapshot_path"]).read_text(encoding="utf-8").strip().splitlines()
            event_lines = Path(first["event_path"]).read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(snapshot_lines), 2)
            self.assertEqual(len(event_lines), 1)
            self.assertEqual(first["new_event_count"], 1)
            self.assertEqual(second["new_event_count"], 0)
            event = json.loads(event_lines[0])
            self.assertEqual(event["headline"], "中东局势升级，原油拉升")
            self.assertEqual(event["keywords"], ["原油", "中东"])

        fake_fetcher.close.assert_called()

    def test_collect_intraday_snapshots_records_basis_signal_when_intraday_jump_is_large(self):
        from src import intraday_snapshot_collector as collector

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 4, 25, 14, 0, 0)

        fake_fetcher = MagicMock()
        fake_fetcher.get_gold_silver_quotes.return_value = {}
        fake_fetcher.get_nasdaq_golden_dragon_snapshot.return_value = {"code": "HXC", "change_pct": 0.8}
        fake_fetcher.get_ic_term_structure.return_value = None
        fake_fetcher.get_500etf_option_proxy.return_value = None
        fake_fetcher.list_flash.return_value = []
        fake_fetcher.get_ic_basis.side_effect = [
            SimpleNamespace(spot_price=5800.0, futures_price=5785.0, basis=15.0, annualized_basis_pct=6.0, contract_code="IC2505", days_to_expiry=20),
            SimpleNamespace(spot_price=5801.0, futures_price=5786.0, basis=15.0, annualized_basis_pct=6.3, contract_code="IC2505", days_to_expiry=20),
            SimpleNamespace(spot_price=5802.0, futures_price=5787.0, basis=15.0, annualized_basis_pct=6.2, contract_code="IC2505", days_to_expiry=20),
            SimpleNamespace(spot_price=5795.0, futures_price=5750.0, basis=45.0, annualized_basis_pct=13.2, contract_code="IC2505", days_to_expiry=20),
            SimpleNamespace(spot_price=5788.0, futures_price=5710.0, basis=78.0, annualized_basis_pct=20.0, contract_code="IC2505", days_to_expiry=20),
            SimpleNamespace(spot_price=5780.0, futures_price=5680.0, basis=100.0, annualized_basis_pct=30.0, contract_code="IC2505", days_to_expiry=20),
        ]

        with TemporaryDirectory() as tmp_dir:
            archive_dir = Path(tmp_dir) / "intraday"
            with patch.object(collector, "INTRADAY_ARCHIVE_DIR", archive_dir):
                with patch.object(collector, "datetime", FixedDateTime):
                    with patch("src.intraday_snapshot_collector.MarketDataFetcher", return_value=fake_fetcher):
                        results = [
                            collector.collect_intraday_snapshots(jin10_api_key="jin10-secret", jin10_x_token="jin10-x-token")
                            for _ in range(6)
                        ]

            self.assertTrue(all(result is not None for result in results))
            basis_signal_path = Path(results[-1]["basis_signal_path"])
            shadow_signal_path = Path(results[-1]["shadow_signal_path"])
            self.assertTrue(basis_signal_path.exists())
            self.assertTrue(shadow_signal_path.exists())
            lines = basis_signal_path.read_text(encoding="utf-8").strip().splitlines()
            shadow_lines = shadow_signal_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 3)
            self.assertEqual(len(shadow_lines), 1)
            payload = json.loads(lines[-1])
            shadow_payload = json.loads(shadow_lines[0])
            self.assertEqual(payload["basis_signal"]["severity"], "critical")
            self.assertTrue(payload["basis_signal"]["triggered"])
            self.assertGreater(payload["basis_signal"]["zscore"], 2.0)
            self.assertEqual(results[-1]["new_basis_signal_count"], 1)
            self.assertTrue(shadow_payload["shadow_signal"]["candidate"])
            self.assertEqual(results[4]["new_shadow_signal_count"], 0)
            self.assertEqual(results[5]["new_shadow_signal_count"], 1)
            self.assertEqual(shadow_payload["shadow_signal"]["confirmation_required"], 3)
            self.assertEqual(shadow_payload["shadow_signal"]["confirmation_count"], 3)
            self.assertTrue(results[-1]["snapshot"]["csi500_basis_shadow_signal"]["cooldown_active"] is False)

    def test_build_basis_signal_ignores_near_zero_std_zscore_blowups(self):
        from src import intraday_snapshot_collector as collector

        snapshot = {
            "captured_at": "2026-04-28T10:00:00",
            "csi500_basis": {
                "spot_price": 5800.0,
                "futures_price": 5744.0,
                "basis": 56.0,
                "annualized_basis_pct": 9.73,
                "contract_code": "IC2606",
                "days_to_expiry": 55,
            },
        }
        prior_rows = [
            {"csi500_basis": {"annualized_basis_pct": 9.740000000000001}},
            {"csi500_basis": {"annualized_basis_pct": 9.74}},
            {"csi500_basis": {"annualized_basis_pct": 9.739999999999998}},
        ]

        payload = collector._build_basis_signal(snapshot, prior_rows)

        self.assertIsNotNone(payload)
        self.assertEqual(payload["zscore"], 0.0)
        self.assertTrue(payload["rolling_std_floor_applied"])

    def test_collect_intraday_snapshots_records_term_structure_shadow_signal(self):
        from src import intraday_snapshot_collector as collector

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 4, 25, 14, 5, 0)

        fake_fetcher = MagicMock()
        fake_fetcher.get_gold_silver_quotes.return_value = {}
        fake_fetcher.get_nasdaq_golden_dragon_snapshot.return_value = {"code": "HXC", "change_pct": 0.4}
        fake_fetcher.get_ic_basis.return_value = SimpleNamespace(
            spot_price=5800.0,
            futures_price=5750.0,
            basis=50.0,
            annualized_basis_pct=8.0,
            contract_code="IC2606",
            days_to_expiry=55,
        )
        fake_fetcher.get_ic_term_structure.return_value = SimpleNamespace(
            near_symbol="IC2605",
            near_price=5700.0,
            near_days=20,
            next_symbol="IC2606",
            next_price=5712.0,
            next_days=48,
            m1_m2_annualized_pct=2.9,
            q1_symbol="IC2609",
            q1_price=5750.0,
            q1_days=111,
            q2_symbol="IC2612",
            q2_price=5790.0,
            q2_days=202,
            q1_q2_annualized_pct=0.4,
            front_end_gap_pct=2.5,
        )
        fake_fetcher.get_500etf_option_proxy.return_value = None
        fake_fetcher.list_flash.return_value = []

        with TemporaryDirectory() as tmp_dir:
            archive_dir = Path(tmp_dir) / "intraday"
            with patch.object(collector, "INTRADAY_ARCHIVE_DIR", archive_dir):
                with patch.object(collector, "datetime", FixedDateTime):
                    with patch("src.intraday_snapshot_collector.MarketDataFetcher", return_value=fake_fetcher):
                        results = [
                            collector.collect_intraday_snapshots(jin10_api_key="jin10-secret", jin10_x_token="jin10-x-token")
                            for _ in range(4)
                        ]

            result = results[-1]
            self.assertIsNotNone(result)
            term_path = Path(result["term_shadow_signal_path"])
            self.assertTrue(term_path.exists())
            rows = term_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(rows), 1)
            payload = json.loads(rows[0])
            self.assertTrue(payload["term_structure_signal"]["candidate"])
            self.assertEqual(payload["term_structure_signal"]["rule_name"], "M1-M2前端塌陷>=2.05% (14:30前)")
            self.assertEqual(results[2]["new_term_shadow_signal_count"], 1)
            self.assertEqual(results[3]["new_term_shadow_signal_count"], 0)
            self.assertEqual(payload["term_structure_signal"]["confirmation_required"], 3)
            self.assertEqual(payload["term_structure_signal"]["confirmation_count"], 3)
            self.assertTrue(results[3]["snapshot"]["csi500_term_structure_shadow_signal"]["cooldown_active"])

    def test_collect_intraday_snapshots_records_option_proxy_shadow_signal(self):
        from src import intraday_snapshot_collector as collector

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 4, 25, 14, 10, 0)

        fake_fetcher = MagicMock()
        fake_fetcher.get_gold_silver_quotes.return_value = {}
        fake_fetcher.get_nasdaq_golden_dragon_snapshot.return_value = {"code": "HXC", "change_pct": 0.4}
        fake_fetcher.get_ic_basis.return_value = SimpleNamespace(
            spot_price=5800.0,
            futures_price=5750.0,
            basis=50.0,
            annualized_basis_pct=8.0,
            contract_code="IC2606",
            days_to_expiry=55,
        )
        fake_fetcher.get_ic_term_structure.return_value = None
        fake_fetcher.get_500etf_option_proxy.return_value = SimpleNamespace(
            board_timestamp="20260425141000",
            expiry_ym="2606",
            expiry_style="M",
            qvix_latest=26.8,
            qvix_prev=24.9,
            qvix_jump_pct=7.63,
            qvix_zscore=2.45,
            atm_strike=8.75,
            atm_call_trade_code="510500C2606M08750",
            atm_call_price=0.62,
            atm_put_trade_code="510500P2606M08750",
            atm_put_price=0.60,
            atm_put_last_price=0.60,
            atm_put_bid1=0.59,
            atm_put_ask1=0.61,
            atm_put_quote_time="2026-04-25 14:09:58",
            atm_put_days_to_expiry=33,
            atm_put_price_source="ask1",
            otm_put_trade_code="510500P2606M08250",
            otm_put_strike=8.25,
            otm_put_price=0.37,
            otm_put_last_price=0.36,
            otm_put_bid1=0.35,
            otm_put_ask1=0.37,
            otm_put_quote_time="2026-04-25 14:09:58",
            otm_put_days_to_expiry=33,
            otm_put_price_source="ask1",
            put_skew_ratio=0.60,
            atm_put_call_volume_ratio=2.40,
            atm_put_volume=3200,
            atm_call_volume=1200,
            source="akshare_public",
        )
        fake_fetcher.list_flash.return_value = []

        with TemporaryDirectory() as tmp_dir:
            archive_dir = Path(tmp_dir) / "intraday"
            with patch.object(collector, "INTRADAY_ARCHIVE_DIR", archive_dir):
                with patch.object(collector, "datetime", FixedDateTime):
                    with patch("src.intraday_snapshot_collector.MarketDataFetcher", return_value=fake_fetcher):
                        results = [
                            collector.collect_intraday_snapshots(jin10_api_key="jin10-secret", jin10_x_token="jin10-x-token")
                            for _ in range(4)
                        ]

            result = results[-1]
            self.assertIsNotNone(result)
            option_path = Path(result["option_shadow_signal_path"])
            self.assertTrue(option_path.exists())
            rows = option_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(rows), 1)
            payload = json.loads(rows[0])
            self.assertTrue(payload["option_proxy_signal"]["candidate"])
            self.assertEqual(payload["option_proxy_signal"]["rule_name"], "500ETF期权代理共振 (14:30前)")
            self.assertEqual(results[2]["new_option_shadow_signal_count"], 1)
            self.assertEqual(results[3]["new_option_shadow_signal_count"], 0)
            self.assertEqual(payload["csi500_option_proxy"]["otm_put_price_source"], "ask1")
            self.assertAlmostEqual(payload["csi500_option_proxy"]["otm_put_ask1"], 0.37)
            self.assertEqual(payload["option_proxy_signal"]["confirmation_required"], 3)
            self.assertTrue(results[3]["snapshot"]["csi500_option_proxy_shadow_signal"]["cooldown_active"])

    def test_collect_intraday_snapshots_silences_shadow_candidates_during_auction_windows(self):
        from src import intraday_snapshot_collector as collector

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 4, 25, 9, 27, 0)

        fake_fetcher = MagicMock()
        fake_fetcher.get_gold_silver_quotes.return_value = {}
        fake_fetcher.get_nasdaq_golden_dragon_snapshot.return_value = {"code": "HXC", "change_pct": 0.4}
        fake_fetcher.get_ic_basis.return_value = SimpleNamespace(
            spot_price=5800.0,
            futures_price=5750.0,
            basis=50.0,
            annualized_basis_pct=8.0,
            contract_code="IC2606",
            days_to_expiry=55,
        )
        fake_fetcher.get_ic_term_structure.return_value = SimpleNamespace(
            near_symbol="IC2605",
            near_price=5700.0,
            near_days=20,
            next_symbol="IC2606",
            next_price=5712.0,
            next_days=48,
            m1_m2_annualized_pct=2.9,
            q1_symbol="IC2609",
            q1_price=5750.0,
            q1_days=111,
            q2_symbol="IC2612",
            q2_price=5790.0,
            q2_days=202,
            q1_q2_annualized_pct=0.4,
            front_end_gap_pct=2.5,
        )
        fake_fetcher.get_500etf_option_proxy.return_value = None
        fake_fetcher.list_flash.return_value = []

        with TemporaryDirectory() as tmp_dir:
            archive_dir = Path(tmp_dir) / "intraday"
            with patch.object(collector, "INTRADAY_ARCHIVE_DIR", archive_dir):
                with patch.object(collector, "datetime", FixedDateTime):
                    with patch("src.intraday_snapshot_collector.MarketDataFetcher", return_value=fake_fetcher):
                        result = collector.collect_intraday_snapshots(jin10_api_key="jin10-secret", jin10_x_token="jin10-x-token")

            self.assertIsNotNone(result)
            term_signal = result["snapshot"]["csi500_term_structure_shadow_signal"]
            self.assertFalse(term_signal["candidate"])
            self.assertTrue(term_signal["silent_window_active"])
            self.assertEqual(result["new_term_shadow_signal_count"], 0)
            self.assertFalse(Path(result["term_shadow_signal_path"]).exists())

    def test_collect_intraday_snapshots_refreshes_m1_m2_shadow_monitoring_outputs(self):
        from src import intraday_snapshot_collector as collector

        fake_fetcher = MagicMock()
        fake_fetcher.get_gold_silver_quotes.return_value = {}
        fake_fetcher.get_nasdaq_golden_dragon_snapshot.return_value = {"code": "HXC", "change_pct": 0.1}
        fake_fetcher.get_ic_basis.return_value = SimpleNamespace(
            spot_price=5800.0,
            futures_price=5750.0,
            basis=50.0,
            annualized_basis_pct=8.0,
            contract_code="IC2606",
            days_to_expiry=55,
        )
        fake_fetcher.get_ic_term_structure.return_value = None
        fake_fetcher.get_500etf_option_proxy.return_value = None
        fake_fetcher.list_flash.return_value = []

        shadow_payload = {
            "candidate_count": 2,
            "event_cluster_count": 1,
            "ledger_path": "/tmp/ledger.jsonl",
            "summary_path": "/tmp/summary.md",
            "latest_summary_path": "/tmp/latest.md",
        }

        with TemporaryDirectory() as tmp_dir:
            archive_dir = Path(tmp_dir) / "intraday"
            with patch.object(collector, "INTRADAY_ARCHIVE_DIR", archive_dir):
                with patch("src.intraday_snapshot_collector.MarketDataFetcher", return_value=fake_fetcher):
                    with patch.object(
                        collector,
                        "_refresh_term_structure_shadow_monitoring",
                        return_value=shadow_payload,
                    ) as refresh_mock:
                        result = collector.collect_intraday_snapshots(
                            jin10_api_key="jin10-secret",
                            jin10_x_token="jin10-x-token",
                        )

        self.assertIsNotNone(result)
        self.assertEqual(result["shadow_monitoring_payload"], shadow_payload)
        refresh_mock.assert_called_once()


class IntradayBasisValidationScriptTestCase(unittest.TestCase):
    def test_intraday_basis_validation_reports_snapshot_coverage_without_signals(self):
        import scripts.run_ic_intraday_basis_signal_validation as validation

        with TemporaryDirectory() as tmp_dir:
            archive_dir = Path(tmp_dir) / "intraday"
            archive_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path = archive_dir / "2026-04-25_market_snapshots.jsonl"
            snapshot_payload = {
                "captured_at": "2026-04-25T11:55:20",
                "csi500_basis": {
                    "spot_price": 8247.87,
                    "futures_price": 8132.80,
                    "basis": 115.08,
                    "annualized_basis_pct": 17.22,
                    "contract_code": "IC主力",
                    "days_to_expiry": 30,
                },
                "csi500_basis_signal": {
                    "severity": "normal",
                    "triggered": False,
                    "delta_vs_prev": 0.0,
                    "zscore": 0.0,
                },
            }
            snapshot_path.write_text(json.dumps(snapshot_payload, ensure_ascii=False) + "\n", encoding="utf-8")

            spot = pd.DataFrame(
                [
                    {"date": "2026-04-25", "close": 8247.87, "open": 8200, "high": 8260, "low": 8180, "volume": 100},
                    {"date": "2026-04-28", "close": 8300.00, "open": 8250, "high": 8310, "low": 8240, "volume": 100},
                ]
            )
            with patch.object(validation, "_load_spot_history", return_value=spot):
                signal_rows = validation._load_intraday_signal_days(archive_dir)
                snapshot_rows = validation._load_intraday_snapshot_days(archive_dir)
                signal_days = validation._build_daily_signal_summary(signal_rows)
                snapshot_days = validation._build_daily_snapshot_summary(snapshot_rows)
                shadow_days = validation._build_daily_shadow_summary(snapshot_rows)
                term_shadow_days = validation._build_daily_term_shadow_summary(snapshot_rows)
                option_shadow_days = validation._build_daily_option_shadow_summary(snapshot_rows)
                merged = snapshot_days.merge(
                    validation._load_spot_returns(Path(tmp_dir)),
                    left_on="report_date",
                    right_on="date",
                    how="left",
                ).drop(columns=["date"])
                shadow_merged = shadow_days.merge(
                    validation._load_spot_returns(Path(tmp_dir)),
                    left_on="report_date",
                    right_on="date",
                    how="left",
                ).drop(columns=["date"])
                term_shadow_merged = term_shadow_days.copy()
                option_shadow_merged = option_shadow_days.copy()
                report = validation._build_report(
                    signal_days,
                    snapshot_days,
                    shadow_days,
                    term_shadow_days,
                    option_shadow_days,
                    merged,
                    shadow_merged,
                    term_shadow_merged,
                    option_shadow_merged,
                )

        self.assertIn("盘中基差覆盖日数: 1", report)
        self.assertIn("已触发异常日数: 0", report)
        self.assertIn("2026-04-25", report)

    def test_intraday_basis_validation_reports_shadow_candidate_samples(self):
        import scripts.run_ic_intraday_basis_signal_validation as validation

        with TemporaryDirectory() as tmp_dir:
            archive_dir = Path(tmp_dir) / "intraday"
            archive_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path = archive_dir / "2026-04-25_market_snapshots.jsonl"
            snapshot_payload = {
                "captured_at": "2026-04-25T14:00:00",
                "csi500_basis": {
                    "spot_price": 8247.87,
                    "futures_price": 8132.80,
                    "basis": 115.08,
                    "annualized_basis_pct": 17.22,
                    "contract_code": "IC2606",
                    "days_to_expiry": 55,
                },
                "csi500_basis_signal": {
                    "severity": "warning",
                    "triggered": False,
                    "delta_vs_prev": 0.20,
                    "zscore": 2.30,
                },
            }
            snapshot_path.write_text(json.dumps(snapshot_payload, ensure_ascii=False) + "\n", encoding="utf-8")

            spot = pd.DataFrame(
                [
                    {"date": "2026-04-25", "close": 8247.87, "open": 8200, "high": 8260, "low": 8180, "volume": 100},
                    {"date": "2026-04-28", "close": 8320.00, "open": 8250, "high": 8330, "low": 8240, "volume": 100},
                    {"date": "2026-04-29", "close": 8340.00, "open": 8320, "high": 8350, "low": 8310, "volume": 100},
                    {"date": "2026-04-30", "close": 8360.00, "open": 8340, "high": 8370, "low": 8330, "volume": 100},
                ]
            )
            with patch.object(validation, "_load_spot_history", return_value=spot):
                signal_rows = validation._load_intraday_signal_days(archive_dir)
                snapshot_rows = validation._load_intraday_snapshot_days(archive_dir)
                signal_days = validation._build_daily_signal_summary(signal_rows)
                snapshot_days = validation._build_daily_snapshot_summary(snapshot_rows)
                shadow_days = validation._build_daily_shadow_summary(snapshot_rows)
                term_shadow_days = validation._build_daily_term_shadow_summary(snapshot_rows)
                option_shadow_days = validation._build_daily_option_shadow_summary(snapshot_rows)
                merged = snapshot_days.merge(
                    validation._load_spot_returns(Path(tmp_dir)),
                    left_on="report_date",
                    right_on="date",
                    how="left",
                ).drop(columns=["date"])
                shadow_merged = shadow_days.merge(
                    validation._load_spot_returns(Path(tmp_dir)),
                    left_on="report_date",
                    right_on="date",
                    how="left",
                ).drop(columns=["date"])
                term_shadow_merged = term_shadow_days.copy()
                option_shadow_merged = option_shadow_days.copy()
                report = validation._build_report(
                    signal_days,
                    snapshot_days,
                    shadow_days,
                    term_shadow_days,
                    option_shadow_days,
                    merged,
                    shadow_merged,
                    term_shadow_merged,
                    option_shadow_merged,
                )

        self.assertIn("已触发影子候选日数: 1", report)
        self.assertIn("影子规则: `z>=2.0 & |jump|>=0.114 (14:30前)`", report)
        self.assertIn("可评估样本数: 1", report)

    def test_intraday_basis_validation_reports_term_structure_shadow_samples(self):
        import scripts.run_ic_intraday_basis_signal_validation as validation

        with TemporaryDirectory() as tmp_dir:
            archive_dir = Path(tmp_dir) / "intraday"
            archive_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path = archive_dir / "2026-04-25_market_snapshots.jsonl"
            snapshot_payload = {
                "captured_at": "2026-04-25T14:05:00",
                "csi500_basis": {
                    "spot_price": 8247.87,
                    "futures_price": 8132.80,
                    "basis": 115.08,
                    "annualized_basis_pct": 17.22,
                    "contract_code": "IC2606",
                    "days_to_expiry": 55,
                },
                "csi500_basis_signal": {
                    "severity": "normal",
                    "triggered": False,
                    "delta_vs_prev": 0.05,
                    "zscore": 0.20,
                },
                "csi500_term_structure_shadow_signal": {
                    "rule_name": "M1-M2前端塌陷>=2.05% (14:30前)",
                    "candidate": True,
                    "front_end_gap_pct": 2.50,
                    "anchor_stable": True,
                },
            }
            snapshot_path.write_text(json.dumps(snapshot_payload, ensure_ascii=False) + "\n", encoding="utf-8")

            spot = pd.DataFrame(
                [
                    {"date": "2026-04-25", "close": 8247.87, "open": 8200, "high": 8260, "low": 8180, "volume": 100},
                    {"date": "2026-04-28", "close": 8320.00, "open": 8250, "high": 8330, "low": 8240, "volume": 100},
                    {"date": "2026-04-29", "close": 8340.00, "open": 8320, "high": 8350, "low": 8310, "volume": 100},
                    {"date": "2026-04-30", "close": 8360.00, "open": 8340, "high": 8370, "low": 8330, "volume": 100},
                ]
            )
            with patch.object(validation, "_load_spot_history", return_value=spot):
                signal_rows = validation._load_intraday_signal_days(archive_dir)
                snapshot_rows = validation._load_intraday_snapshot_days(archive_dir)
                signal_days = validation._build_daily_signal_summary(signal_rows)
                snapshot_days = validation._build_daily_snapshot_summary(snapshot_rows)
                shadow_days = validation._build_daily_shadow_summary(snapshot_rows)
                term_shadow_days = validation._build_daily_term_shadow_summary(snapshot_rows)
                option_shadow_days = validation._build_daily_option_shadow_summary(snapshot_rows)
                merged = snapshot_days.merge(
                    validation._load_spot_returns(Path(tmp_dir)),
                    left_on="report_date",
                    right_on="date",
                    how="left",
                ).drop(columns=["date"])
                shadow_merged = shadow_days.merge(
                    validation._load_spot_returns(Path(tmp_dir)),
                    left_on="report_date",
                    right_on="date",
                    how="left",
                ).drop(columns=["date"])
                term_shadow_merged = term_shadow_days.merge(
                    validation._load_spot_returns(Path(tmp_dir)),
                    left_on="report_date",
                    right_on="date",
                    how="left",
                ).drop(columns=["date"])
                option_shadow_merged = option_shadow_days.copy()
                report = validation._build_report(
                    signal_days,
                    snapshot_days,
                    shadow_days,
                    term_shadow_days,
                    option_shadow_days,
                    merged,
                    shadow_merged,
                    term_shadow_merged,
                    option_shadow_merged,
                )

        self.assertIn("已触发前端塌陷候选日数: 1", report)
        self.assertIn("影子规则: `M1-M2前端塌陷>=2.05% (14:30前)`", report)
        self.assertIn("最大前端塌陷差", report)

    def test_intraday_basis_validation_reports_option_shadow_samples(self):
        import scripts.run_ic_intraday_basis_signal_validation as validation

        with TemporaryDirectory() as tmp_dir:
            archive_dir = Path(tmp_dir) / "intraday"
            archive_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path = archive_dir / "2026-04-25_market_snapshots.jsonl"
            snapshot_payload = {
                "captured_at": "2026-04-25T14:10:00",
                "csi500_basis": {
                    "spot_price": 8247.87,
                    "futures_price": 8132.80,
                    "basis": 115.08,
                    "annualized_basis_pct": 17.22,
                    "contract_code": "IC2606",
                    "days_to_expiry": 55,
                },
                "csi500_basis_signal": {
                    "severity": "normal",
                    "triggered": False,
                    "delta_vs_prev": 0.05,
                    "zscore": 0.20,
                },
                "csi500_option_proxy_shadow_signal": {
                    "rule_name": "500ETF期权代理共振 (14:30前)",
                    "candidate": True,
                    "qvix_zscore": 2.40,
                    "qvix_jump_pct": 7.50,
                    "put_skew_ratio": 0.62,
                    "atm_put_call_volume_ratio": 2.30,
                },
            }
            snapshot_path.write_text(json.dumps(snapshot_payload, ensure_ascii=False) + "\n", encoding="utf-8")

            spot = pd.DataFrame(
                [
                    {"date": "2026-04-25", "close": 8247.87, "open": 8200, "high": 8260, "low": 8180, "volume": 100},
                    {"date": "2026-04-28", "close": 8320.00, "open": 8250, "high": 8330, "low": 8240, "volume": 100},
                    {"date": "2026-04-29", "close": 8340.00, "open": 8320, "high": 8350, "low": 8310, "volume": 100},
                    {"date": "2026-04-30", "close": 8360.00, "open": 8340, "high": 8370, "low": 8330, "volume": 100},
                ]
            )
            with patch.object(validation, "_load_spot_history", return_value=spot):
                signal_rows = validation._load_intraday_signal_days(archive_dir)
                snapshot_rows = validation._load_intraday_snapshot_days(archive_dir)
                signal_days = validation._build_daily_signal_summary(signal_rows)
                snapshot_days = validation._build_daily_snapshot_summary(snapshot_rows)
                shadow_days = validation._build_daily_shadow_summary(snapshot_rows)
                term_shadow_days = validation._build_daily_term_shadow_summary(snapshot_rows)
                option_shadow_days = validation._build_daily_option_shadow_summary(snapshot_rows)
                merged = snapshot_days.merge(
                    validation._load_spot_returns(Path(tmp_dir)),
                    left_on="report_date",
                    right_on="date",
                    how="left",
                ).drop(columns=["date"])
                shadow_merged = shadow_days.merge(
                    validation._load_spot_returns(Path(tmp_dir)),
                    left_on="report_date",
                    right_on="date",
                    how="left",
                ).drop(columns=["date"])
                term_shadow_merged = term_shadow_days.merge(
                    validation._load_spot_returns(Path(tmp_dir)),
                    left_on="report_date",
                    right_on="date",
                    how="left",
                ).drop(columns=["date"])
                option_shadow_merged = option_shadow_days.merge(
                    validation._load_spot_returns(Path(tmp_dir)),
                    left_on="report_date",
                    right_on="date",
                    how="left",
                ).drop(columns=["date"])
                report = validation._build_report(
                    signal_days,
                    snapshot_days,
                    shadow_days,
                    term_shadow_days,
                    option_shadow_days,
                    merged,
                    shadow_merged,
                    term_shadow_merged,
                    option_shadow_merged,
                )

        self.assertIn("已触发期权代理候选日数: 1", report)
        self.assertIn("影子规则: `500ETF期权代理共振 (14:30前)`", report)
        self.assertIn("最大Qvix zscore", report)

    def test_intraday_basis_validation_reports_shadow_state_lock_qa_ledger(self):
        import scripts.run_ic_intraday_basis_signal_validation as validation

        with TemporaryDirectory() as tmp_dir:
            archive_dir = Path(tmp_dir) / "intraday"
            archive_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path = archive_dir / "2026-04-25_market_snapshots.jsonl"
            payloads = [
                {
                    "captured_at": "2026-04-25T10:00:00",
                    "csi500_basis": {
                        "spot_price": 8247.87,
                        "futures_price": 8132.80,
                        "basis": 115.08,
                        "annualized_basis_pct": 17.22,
                        "contract_code": "IC2606",
                        "days_to_expiry": 55,
                    },
                    "csi500_basis_signal": {
                        "severity": "warning",
                        "triggered": False,
                        "delta_vs_prev": 0.20,
                        "zscore": 2.30,
                    },
                    "csi500_basis_shadow_signal": {
                        "rule_name": "z>=2.0 & |jump|>=0.114 (14:30前)",
                        "candidate": False,
                        "raw_candidate": True,
                        "confirmation_count": 1,
                        "confirmation_required": 3,
                        "cooldown_active": False,
                        "silent_window_active": False,
                        "zscore": 2.3,
                        "abs_jump": 0.2,
                    },
                    "csi500_term_structure_shadow_signal": {
                        "rule_name": "M1-M2前端塌陷>=2.05% (14:30前)",
                        "candidate": False,
                        "raw_candidate": True,
                        "confirmation_count": 0,
                        "confirmation_required": 3,
                        "cooldown_active": True,
                        "silent_window_active": False,
                        "front_end_gap_pct": 2.4,
                        "anchor_stable": True,
                    },
                    "csi500_option_proxy": {
                        "roll_window_shifted": True,
                    },
                    "csi500_option_proxy_shadow_signal": {
                        "rule_name": "500ETF期权代理共振 (14:30前)",
                        "candidate": False,
                        "raw_candidate": True,
                        "confirmation_count": 0,
                        "confirmation_required": 3,
                        "cooldown_active": False,
                        "silent_window_active": True,
                        "qvix_zscore": 2.5,
                        "qvix_jump_pct": 6.0,
                        "put_skew_ratio": 0.61,
                        "atm_put_call_volume_ratio": 2.2,
                    },
                }
            ]
            snapshot_path.write_text(
                "\n".join(json.dumps(item, ensure_ascii=False) for item in payloads) + "\n",
                encoding="utf-8",
            )

            spot = pd.DataFrame(
                [
                    {"date": "2026-04-25", "close": 8247.87, "open": 8200, "high": 8260, "low": 8180, "volume": 100},
                    {"date": "2026-04-28", "close": 8300.00, "open": 8250, "high": 8310, "low": 8240, "volume": 100},
                    {"date": "2026-04-29", "close": 8320.00, "open": 8300, "high": 8330, "low": 8290, "volume": 100},
                    {"date": "2026-04-30", "close": 8340.00, "open": 8320, "high": 8350, "low": 8310, "volume": 100},
                ]
            )
            with patch.object(validation, "_load_spot_history", return_value=spot):
                signal_rows = validation._load_intraday_signal_days(archive_dir)
                snapshot_rows = validation._load_intraday_snapshot_days(archive_dir)
                signal_days = validation._build_daily_signal_summary(signal_rows)
                snapshot_days = validation._build_daily_snapshot_summary(snapshot_rows)
                shadow_days = validation._build_daily_shadow_summary(snapshot_rows)
                term_shadow_days = validation._build_daily_term_shadow_summary(snapshot_rows)
                option_shadow_days = validation._build_daily_option_shadow_summary(snapshot_rows)
                spot_returns = validation._load_spot_returns(Path(tmp_dir))
                merged = snapshot_days.merge(spot_returns, left_on="report_date", right_on="date", how="left").drop(columns=["date"])
                shadow_merged = shadow_days.merge(spot_returns, left_on="report_date", right_on="date", how="left").drop(columns=["date"])
                term_shadow_merged = term_shadow_days.merge(spot_returns, left_on="report_date", right_on="date", how="left").drop(columns=["date"])
                option_shadow_merged = option_shadow_days.merge(spot_returns, left_on="report_date", right_on="date", how="left").drop(columns=["date"])
                report = validation._build_report(
                    signal_days,
                    snapshot_days,
                    shadow_days,
                    term_shadow_days,
                    option_shadow_days,
                    merged,
                    shadow_merged,
                    term_shadow_merged,
                    option_shadow_merged,
                )

        self.assertIn("## 影子QA验收账本", report)
        self.assertIn("基差影子被三连确认拦住日数: 1", report)
        self.assertIn("前端塌陷被冷却压制日数: 1", report)
        self.assertIn("期权代理被静默窗口压制日数: 1", report)
        self.assertIn("期权腿换月平移发生日数: 1", report)


class DailyPushPipelineTestCase(unittest.TestCase):
    def test_record_vip_watch_signal_history_appends_jsonl(self):
        from src.daily_push_pipeline import DailyPushPipeline

        pipeline = DailyPushPipeline(notifier=MagicMock(), ai_enabled=False)
        signal_payload = {
            "score": 1.2,
            "strength": "强利多",
            "signals": ["资金偏多(61.5%/38.5%)", "临近支撑4676.14"],
        }

        with TemporaryDirectory() as tmp_dir:
            history_path = Path(tmp_dir) / "vip_watch_signal_history.jsonl"
            with patch("src.daily_push_pipeline.VIP_WATCH_SIGNAL_HISTORY_PATH", history_path):
                pipeline._record_vip_watch_signal_history("黄金期货", signal_payload)

            lines = history_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 1)
            record = json.loads(lines[0])
            self.assertEqual(record["asset_name"], "黄金期货")
            self.assertEqual(record["score"], 1.2)
            self.assertEqual(record["strength"], "强利多")
            self.assertEqual(record["signals"], signal_payload["signals"])

    def test_summarize_recent_vip_watch_history_describes_strengthening_sequence(self):
        from src.daily_push_pipeline import DailyPushPipeline

        pipeline = DailyPushPipeline(notifier=MagicMock(), ai_enabled=False)
        records = [
            {"asset_name": "黄金期货", "score": -0.6, "strength": "弱利空"},
            {"asset_name": "黄金期货", "score": 0.1, "strength": "中性"},
        ]

        with patch.object(pipeline, "_load_recent_vip_watch_signal_history", return_value=records):
            summary = pipeline._summarize_recent_vip_watch_history(
                "黄金期货",
                current_signal_payload={"score": 0.8, "strength": "弱利多", "signals": []},
            )

        self.assertIsNotNone(summary)
        self.assertIn("近3次=", summary)
        self.assertIn("弱利空(-0.6) -> 中性(+0.1) -> 弱利多(+0.8)", summary)
        self.assertIn("最新较前次转强", summary)
        self.assertIn("连续走强", summary)

    def test_summarize_macro_bias_uses_vip_trend_to_resolve_conflict(self):
        from src.daily_push_pipeline import DailyPushPipeline

        pipeline = DailyPushPipeline(notifier=MagicMock(), ai_enabled=False)

        result = pipeline._summarize_macro_bias(
            "白银期货",
            vip_watch_context="会员盯盘信号: 强利多 | 弱利多 | 趋势偏强",
            vip_trend_note="最新转弱，连续走弱",
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["label"], "利空")
        self.assertLess(result["trend_score_adjustment"], 0)
        self.assertIn("连续走弱", result["reasons"])

    def test_summarize_macro_bias_keeps_single_trend_change_as_adjustment_only(self):
        from src.daily_push_pipeline import DailyPushPipeline

        pipeline = DailyPushPipeline(notifier=MagicMock(), ai_enabled=False)

        result = pipeline._summarize_macro_bias(
            "黄金期货",
            vip_watch_context="会员盯盘信号: 强利多 | 弱利多 | 趋势偏强",
            vip_trend_note="最新转弱",
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["label"], "利多")
        self.assertEqual(result["keyword_score"], 8)
        self.assertEqual(result["trend_score_adjustment"], -3)

    def test_analyze_asset_uses_fallback_kline_when_primary_is_insufficient(self):
        from src.daily_push_pipeline import DailyPushPipeline

        short_df = _build_kline_df(10)
        fallback_df = _build_kline_df(30)
        fetcher = MagicMock()
        fetcher.get_quote.return_value = None
        fetcher.get_historical_kline.side_effect = [short_df, fallback_df]

        pipeline = DailyPushPipeline(notifier=MagicMock(), ai_enabled=False)

        with patch("src.stock_analyzer.StockTrendAnalyzer") as mock_analyzer_cls:
            mock_analyzer_cls.return_value.analyze.return_value = _make_trend_result()
            result = pipeline._analyze_asset(
                fetcher,
                {
                    "code": "000905",
                    "source": "index_zh_a",
                    "name": "中证500指数",
                    "unit": "点",
                    "fallback_code": "IC0",
                    "fallback_source": "futures_sina",
                    "fallback_name": "IC期货(中证500替代)",
                },
            )

        self.assertIsNotNone(result)
        self.assertIn("IC期货(中证500替代)", result)
        self.assertEqual(fetcher.get_historical_kline.call_count, 2)

    def test_push_market_summary_skips_ai_when_disabled(self):
        from src.daily_push_pipeline import DailyPushPipeline

        notifier = MagicMock()
        pipeline = DailyPushPipeline(notifier=notifier, jin10_api_key="jin10-secret", ai_enabled=False)
        fake_fetcher = MagicMock()

        akshare_module = ModuleType("akshare")
        akshare_module.futures_main_sina = MagicMock(
            side_effect=[
                pd.DataFrame([{"收盘价": 700.0}]),
                pd.DataFrame([{"收盘价": 7000.0}]),
            ]
        )

        with patch("src.market_data_fetcher.MarketDataFetcher", return_value=fake_fetcher):
            with patch.object(
                pipeline,
                "_analyze_asset",
                side_effect=["黄金分析", "白银分析", None],
            ):
                with patch.object(pipeline, "_ai_predict", side_effect=AssertionError("should not call AI")):
                    with patch.dict(sys.modules, {"akshare": akshare_module}):
                        pipeline.push_market_summary()

        notifier.send.assert_called_once()
        sent_text = notifier.send.call_args[0][0]
        self.assertIn("黄金分析", sent_text)
        self.assertIn("白银分析", sent_text)
        self.assertNotIn("AI预测", sent_text)
        fake_fetcher.close.assert_called_once()

    def test_push_market_summary_does_not_notify_when_all_sections_fail(self):
        from src.daily_push_pipeline import DailyPushPipeline

        notifier = MagicMock()
        pipeline = DailyPushPipeline(notifier=notifier, ai_enabled=False)
        fake_fetcher = MagicMock()

        with patch("src.market_data_fetcher.MarketDataFetcher", return_value=fake_fetcher):
            with patch.object(pipeline, "_analyze_asset", return_value=None):
                pipeline.push_market_summary()

        notifier.send.assert_not_called()
        fake_fetcher.close.assert_called_once()

    def test_push_market_summary_passes_jin10_context_to_ai_without_rendering_flash_section(self):
        from src.daily_push_pipeline import DailyPushPipeline

        notifier = MagicMock()
        pipeline = DailyPushPipeline(
            notifier=notifier,
            jin10_api_key="jin10-secret",
            jin10_x_token="jin10-x-token",
            ai_enabled=True,
        )
        fake_fetcher = MagicMock()
        fake_fetcher.list_flash.return_value = [
            {"id": "301", "title": "美联储释放降息信号，黄金走强", "time": "09:31"},
            {"id": "302", "title": "中证500期指震荡回升", "time": "09:33"},
            {"id": "303", "title": "原油价格回落", "time": "09:35"},
        ]
        fake_fetcher.list_calendar.return_value = [
            {
                "data_id": 78,
                "indicator_id": 78,
                "country": "美国",
                "indicator_name": "ADP就业人数",
                "pub_time": "2026-04-24 20:15",
                "actual": "18.4",
                "consensus": "15.0",
                "previous": "12.0",
            }
        ]
        fake_fetcher.list_calendar_indicators.return_value = [
            {"id": 78, "tags": ["美元", "黄金", "白银"]}
        ]
        fake_fetcher.get_calendar_interpretation.return_value = {
            "impact": "数据>前值=利空黄金，利空白银"
        }
        fake_fetcher.get_vip_watch_indicator_resonance.return_value = {
            "price": 4694.47,
            "hl": [
                {"type": "7", "low": 4099.11, "high": 5419.29, "weights": 13},
                {"type": "5", "low": 4664.29, "high": 4753.44, "weights": 4},
            ],
            "classPivotPoint": [
                {"type": "5", "alia": "日枢轴", "value": 4703.54, "support1": 4653.64, "resistance1": 4742.79}
            ],
            "woodiePivotPoint": [
                {"type": "5", "alia": "日枢轴", "value": 4700.87, "support1": 4648.30, "resistance1": 4737.45}
            ],
            "cycleRange": [
                {"type": "21", "min": 4592.11, "max": 4796.89}
            ],
            "fibonacci": [
                {"type": "20", "levels": [4682.91, 4694.43, 4703.75, 4713.06]}
            ],
            "boll": [
                {"type": "20", "middleBand": 4700.95, "upperBand": 4725.75, "lowerBand": 4676.14}
            ],
            "vpc": [
                {"type": "20", "value": [4689.27, 4766.74, 4792.86]}
            ],
            "averages": [
                {"type": "20", "value": [4693.76, 4693.75, 4700.95, 4711.39, 4714.71, 4728.09]}
            ],
            "optionKey": [
                {"minPrice": 3863, "rankWriting": "较重要位"},
                {"minPrice": 3893, "maxPrice": 3898, "rankWriting": "集中处"},
            ],
        }
        fake_fetcher.list_vip_watch_events.return_value = [
            {
                "data": {
                    "title": "触发资金炸弹，一分钟成交量1500手",
                    "extra": {"longOrder": 61.5, "shortOrder": 38.5},
                }
            }
        ]
        fake_fetcher.get_nasdaq_golden_dragon_snapshot.return_value = {
            "code": "HXC",
            "name": "纳斯达克中国金龙指数",
            "last": 6926.41,
            "change": 81.20,
            "change_pct": 1.21,
        }

        akshare_module = ModuleType("akshare")
        akshare_module.futures_main_sina = MagicMock(
            side_effect=[
                pd.DataFrame([{"收盘价": 700.0}]),
                pd.DataFrame([{"收盘价": 7000.0}]),
            ]
        )

        with patch("src.market_data_fetcher.MarketDataFetcher", return_value=fake_fetcher):
            with patch.object(
                pipeline,
                "_analyze_asset",
                side_effect=["黄金分析", "白银分析", "中证500分析"],
            ):
                with patch.object(
                    pipeline,
                    "_ai_predict",
                    side_effect=["黄金AI", "白银AI", "中证500AI"],
                ) as mock_ai:
                    with patch.object(
                        pipeline,
                        "_is_overnight_flash_time",
                        side_effect=lambda time_text, now=None: time_text in {"09:31", "09:33"},
                    ):
                        with patch.object(
                            pipeline,
                            "_load_recent_vip_watch_signal_history",
                            return_value=[
                                {"asset_name": "黄金期货", "score": -0.4, "strength": "弱利空"},
                                {"asset_name": "黄金期货", "score": 0.1, "strength": "中性"},
                            ],
                        ):
                            with patch.dict(sys.modules, {"akshare": akshare_module}):
                                pipeline.push_market_summary()

        self.assertEqual(mock_ai.call_count, 3)
        self.assertIn("黄金期货参考倾向:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("美联储释放降息信号，黄金走强", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("利空黄金，利空白银", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("会员盯盘信号:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("历史序列:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("弱利多", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("均线", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("临近支撑", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("资金偏多", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("指标共振:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("高低区:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("枢轴带:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("周期区间:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("斐波那契:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("布林带:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("VPC筹码:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("均线簇:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("期权关键位:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("资金炸弹:", mock_ai.call_args_list[0].kwargs["jin10_context"])
        self.assertIn("隔夜中概金龙指数", mock_ai.call_args_list[2].kwargs["jin10_context"])
        self.assertIn("隔夜金十事件:", mock_ai.call_args_list[2].kwargs["jin10_context"])

        notifier.send.assert_called_once()
        sent_text = notifier.send.call_args[0][0]
        self.assertIn("宏观利多利空摘要", sent_text)
        self.assertIn("黄金期货", sent_text)
        self.assertIn("弱利多", sent_text)
        self.assertIn("资金偏多", sent_text)
        self.assertIn("趋势: 最新转强", sent_text)
        self.assertIn("会员盯盘建议", sent_text)
        self.assertIn("回踩支撑再考虑跟随", sent_text)
        self.assertIn("黄金AI", sent_text)
        self.assertNotIn("金十快讯摘要", sent_text)
        self.assertNotIn("美联储释放降息信号，黄金走强", sent_text)


class EventMonitorCustomAlertsTestCase(unittest.TestCase):
    def test_check_ic_basis_triggers_on_deep_contango(self):
        from src.agent.events import EventMonitor, ICBasisAlert

        monitor = EventMonitor()
        rule = ICBasisAlert(stock_code="IC", deep_threshold=10.0, change_threshold=2.0)

        result_payload = {
            "contract": "IC2505",
            "spot_price": 5800.0,
            "futures_price": 5600.0,
            "basis": 200.0,
            "annualized": 12.5,
            "days_to_expiry": 20,
            "open_interest": 123456,
        }

        with patch("src.agent.events.asyncio.to_thread", new=AsyncMock(return_value=result_payload)):
            triggered = asyncio.run(monitor._check_ic_basis(rule))

        self.assertIsNotNone(triggered)
        self.assertIn("年化贴水", triggered.message)
        self.assertEqual(rule._prev_annualized, 12.5)

    def test_check_price_spike_uses_jin10_fetcher_and_triggers_after_baseline(self):
        from src.agent.events import EventMonitor, PriceSpikeAlert

        prices = [
            SimpleNamespace(price=100.0),
            SimpleNamespace(price=102.0),
        ]
        fake_fetcher = MagicMock()
        fake_fetcher.get_quote.side_effect = prices

        monitor = EventMonitor(jin10_api_key="jin10-secret")
        rule = PriceSpikeAlert(stock_code="XAUUSD", change_pct=1.0, direction="both", window_minutes=30)

        with patch("src.market_data_fetcher.MarketDataFetcher", return_value=fake_fetcher):
            with patch("src.agent.events.time.time", side_effect=[1000.0, 1120.0]):
                baseline = asyncio.run(monitor._check_price_spike(rule))
                triggered = asyncio.run(monitor._check_price_spike(rule))

        self.assertIsNone(baseline)
        self.assertIsNotNone(triggered)
        self.assertIn("暴涨", triggered.message)
        self.assertIn("变化幅度", triggered.message)
        self.assertEqual(fake_fetcher.close.call_count, 2)

    def test_check_price_spike_does_not_trigger_when_window_expired(self):
        from src.agent.events import EventMonitor, PriceSpikeAlert

        prices = [
            SimpleNamespace(price=100.0),
            SimpleNamespace(price=102.0),
        ]
        fake_fetcher = MagicMock()
        fake_fetcher.get_quote.side_effect = prices

        monitor = EventMonitor(jin10_api_key="jin10-secret")
        rule = PriceSpikeAlert(stock_code="XAUUSD", change_pct=1.0, direction="both", window_minutes=1)

        with patch("src.market_data_fetcher.MarketDataFetcher", return_value=fake_fetcher):
            with patch("src.agent.events.time.time", side_effect=[1000.0, 1120.0]):
                baseline = asyncio.run(monitor._check_price_spike(rule))
                triggered = asyncio.run(monitor._check_price_spike(rule))

        self.assertIsNone(baseline)
        self.assertIsNone(triggered)

    def test_check_price_spike_falls_back_to_sina_without_jin10_key(self):
        from src.agent.events import EventMonitor, PriceSpikeAlert

        monitor = EventMonitor(jin10_api_key="")
        rule = PriceSpikeAlert(stock_code="XAUUSD", change_pct=1.0, direction="up", window_minutes=30)

        first_response = SimpleNamespace(text='var hq_str_hf_GC="100,99,101,98";', encoding="")
        second_response = SimpleNamespace(text='var hq_str_hf_GC="102,100,103,99";', encoding="")

        with patch("src.agent.events.time.time", side_effect=[1000.0, 1120.0]):
            with patch("requests.get", side_effect=[first_response, second_response]):
                baseline = asyncio.run(monitor._check_price_spike(rule))
                triggered = asyncio.run(monitor._check_price_spike(rule))

        self.assertIsNone(baseline)
        self.assertIsNotNone(triggered)
        self.assertIn("暴涨", triggered.message)

    def test_check_news_impact_deduplicates_latest_flash_id(self):
        from src.agent.events import EventMonitor, NewsImpactAlert

        fake_fetcher = MagicMock()
        fake_fetcher.list_flash.return_value = [
            {"id": "101", "title": "美联储释放降息信号", "time": "09:31"},
            {"id": "102", "title": "黄金大涨，避险情绪升温", "time": "09:32"},
        ]

        monitor = EventMonitor(jin10_api_key="jin10-secret")
        rule = NewsImpactAlert(stock_code="NEWS", keywords=["黄金", "美联储"])

        with patch("src.market_data_fetcher.MarketDataFetcher", return_value=fake_fetcher):
            first = asyncio.run(monitor._check_news_impact(rule))
            second = asyncio.run(monitor._check_news_impact(rule))

        self.assertIsNotNone(first)
        self.assertIn("匹配到 **2** 条", first.message)
        self.assertIsNone(second)

    def test_check_news_impact_returns_none_when_no_keywords_match(self):
        from src.agent.events import EventMonitor, NewsImpactAlert

        fake_fetcher = MagicMock()
        fake_fetcher.list_flash.return_value = [
            {"id": "201", "title": "原油震荡", "time": "10:01"},
        ]

        monitor = EventMonitor(jin10_api_key="jin10-secret")
        rule = NewsImpactAlert(stock_code="NEWS", keywords=["黄金", "美联储"])

        with patch("src.market_data_fetcher.MarketDataFetcher", return_value=fake_fetcher):
            result = asyncio.run(monitor._check_news_impact(rule))

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
