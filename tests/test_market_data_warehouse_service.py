# -*- coding: utf-8 -*-
"""Tests for local market-data warehouse refresh service."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from src.config import Config
from src.services.market_data_warehouse_service import MarketDataWarehouseService
from src.storage import DatabaseManager, PortfolioAccount, PortfolioPosition, StockDaily


class _FakeFetcherManager:
    def __init__(self) -> None:
        self.calls = []

    def get_daily_data(self, stock_code, start_date=None, end_date=None, days=30):
        self.calls.append(
            {
                "stock_code": stock_code,
                "start_date": start_date,
                "end_date": end_date,
                "days": days,
            }
        )
        return (
            pd.DataFrame(
                [
                    {
                        "date": "2026-04-28",
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.0,
                        "close": 10.5,
                        "volume": 1000,
                        "amount": 10500,
                        "pct_chg": 1.0,
                    },
                    {
                        "date": "2026-04-29",
                        "open": 10.5,
                        "high": 12.0,
                        "low": 10.0,
                        "close": 11.0,
                        "volume": 1200,
                        "amount": 13200,
                        "pct_chg": 4.76,
                    },
                ]
            ),
            "fake",
        )

    def close(self):
        return None


class MarketDataWarehouseServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        os.environ["DATABASE_PATH"] = str(self.root / "stock_analysis.db")
        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        os.environ.pop("DATABASE_PATH", None)
        self.temp_dir.cleanup()

    def test_refresh_collects_holdings_and_watchlist_and_writes_ledger(self) -> None:
        with self.db.get_session() as session:
            account = PortfolioAccount(name="主账户", market="cn", is_active=True)
            session.add(account)
            session.flush()
            session.add_all(
                [
                    PortfolioPosition(account_id=account.id, symbol="600519", market="cn", quantity=100),
                    PortfolioPosition(account_id=account.id, symbol="000001", market="cn", quantity=0),
                    StockDaily(
                        code="600519",
                        date=date(2026, 4, 25),
                        close=10.0,
                        data_source="seed",
                        created_at=datetime(2026, 4, 25, 15, 0),
                    ),
                ]
            )
            session.commit()

        fake_fetcher = _FakeFetcherManager()
        service = MarketDataWarehouseService(
            self.db,
            fetcher_manager=fake_fetcher,
            project_root=self.root,
        )
        config = SimpleNamespace(
            stock_list=["300750"],
            watchlist_stock_list=["159326", "600519"],
            market_data_warehouse_lookback_days=60,
            market_data_warehouse_refresh_overlap_days=3,
            market_data_warehouse_max_symbols=10,
        )

        result = service.run_refresh(
            config=config,
            end_date=date(2026, 4, 29),
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["totals"]["target_count"], 3)
        self.assertEqual(result["totals"]["succeeded"], 3)
        self.assertTrue(Path(result["ledger_path"]).exists())

        calls_by_code = {call["stock_code"]: call for call in fake_fetcher.calls}
        self.assertEqual(calls_by_code["600519"]["start_date"], "2026-04-22")
        self.assertEqual(calls_by_code["300750"]["start_date"], "2026-02-28")
        self.assertEqual(calls_by_code["159326"]["end_date"], "2026-04-29")

        with self.db.get_session() as session:
            saved_count = session.query(StockDaily).count()
            latest = session.query(StockDaily).filter(StockDaily.code == "600519").order_by(StockDaily.date.desc()).first()
        self.assertEqual(saved_count, 7)
        self.assertEqual(latest.date, date(2026, 4, 29))

        latest_run = service.latest_run()
        self.assertEqual(latest_run["status"], "ok")
        self.assertEqual(latest_run["totals"]["target_count"], 3)

    def test_normalize_code_handles_hk_and_common_index_aliases(self) -> None:
        self.assertEqual(MarketDataWarehouseService._normalize_code("01810"), "HK01810")
        self.assertEqual(MarketDataWarehouseService._normalize_code("02319"), "HK02319")
        self.assertEqual(MarketDataWarehouseService._normalize_code("NDX100"), "NDX")

    def test_refresh_skips_known_unattended_unsupported_symbols(self) -> None:
        fake_fetcher = _FakeFetcherManager()
        service = MarketDataWarehouseService(
            self.db,
            fetcher_manager=fake_fetcher,
            project_root=self.root,
        )
        config = SimpleNamespace(
            stock_list=[],
            watchlist_stock_list=[],
            market_data_warehouse_lookback_days=60,
            market_data_warehouse_refresh_overlap_days=3,
            market_data_warehouse_max_symbols=10,
        )

        result = service.run_refresh(
            config=config,
            symbols=["000300", "600519"],
            end_date=date(2026, 4, 29),
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["totals"]["target_count"], 2)
        self.assertEqual(result["totals"]["succeeded"], 1)
        self.assertEqual(result["totals"]["skipped"], 1)
        self.assertEqual(result["totals"]["failed"], 0)
        self.assertEqual([call["stock_code"] for call in fake_fetcher.calls], ["600519"])


if __name__ == "__main__":
    unittest.main()
