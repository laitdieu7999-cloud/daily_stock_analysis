# -*- coding: utf-8 -*-
"""Tests for local data center inventory service."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

from src.config import Config
from src.services.data_center_service import DataCenterService
from src.storage import (
    AnalysisHistory,
    BacktestResult,
    BacktestSummary,
    DatabaseManager,
    FundamentalSnapshot,
    NewsIntel,
    PortfolioAccount,
    PortfolioDailySnapshot,
    PortfolioPosition,
    PortfolioTrade,
    StockDaily,
)


class DataCenterServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "stock_analysis.db"
        os.environ["DATABASE_PATH"] = str(self.db_path)

        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        os.environ.pop("DATABASE_PATH", None)
        self.temp_dir.cleanup()

    def test_build_overview_counts_local_assets(self) -> None:
        with self.db.get_session() as session:
            session.add_all(
                [
                    StockDaily(
                        code="600519",
                        date=date(2026, 4, 24),
                        close=1600.0,
                        data_source="unit-test",
                    ),
                    StockDaily(
                        code="000001",
                        date=date(2026, 4, 25),
                        close=10.0,
                        data_source="unit-test",
                    ),
                ]
            )
            history = AnalysisHistory(
                query_id="q1",
                code="600519",
                name="贵州茅台",
                report_type="simple",
                created_at=datetime(2026, 4, 25, 9, 30),
            )
            session.add(history)
            session.flush()
            session.add(
                BacktestResult(
                    analysis_history_id=history.id,
                    code="600519",
                    analysis_date=date(2026, 4, 25),
                    eval_window_days=10,
                    engine_version="v1",
                    eval_status="completed",
                    evaluated_at=datetime(2026, 4, 26, 10, 0),
                )
            )
            session.add(
                BacktestSummary(
                    scope="overall",
                    code=None,
                    eval_window_days=10,
                    engine_version="v1",
                    computed_at=datetime(2026, 4, 26, 10, 5),
                    total_evaluations=1,
                )
            )
            account = PortfolioAccount(name="主账户", market="cn", is_active=True)
            session.add(account)
            session.flush()
            session.add_all(
                [
                    PortfolioPosition(account_id=account.id, symbol="600519", quantity=100, updated_at=datetime(2026, 4, 26, 11, 0)),
                    PortfolioTrade(account_id=account.id, symbol="600519", trade_date=date(2026, 4, 20), side="buy", quantity=100, price=10),
                    PortfolioDailySnapshot(account_id=account.id, snapshot_date=date(2026, 4, 26), total_equity=1000),
                ]
            )
            session.add(
                NewsIntel(
                    code="600519",
                    name="贵州茅台",
                    dimension="latest_news",
                    title="测试新闻",
                    url="https://example.com/news",
                    fetched_at=datetime(2026, 4, 26, 12, 0),
                )
            )
            session.add(
                FundamentalSnapshot(
                    query_id="q1",
                    code="600519",
                    payload="{}",
                    created_at=datetime(2026, 4, 26, 12, 5),
                )
            )
            session.commit()

        service = DataCenterService(self.db)
        overview = service.build_overview()

        self.assertEqual(overview["market_data"]["bar_count"], 2)
        self.assertEqual(overview["market_data"]["stock_count"], 2)
        self.assertIn("quality", overview["market_data"])
        self.assertIn("maintenance", overview)
        self.assertIn("ai_routing", overview)
        self.assertEqual(overview["analysis"]["report_count"], 1)
        self.assertEqual(overview["backtests"]["result_count"], 1)
        self.assertEqual(overview["portfolio"]["position_count"], 1)
        self.assertEqual(overview["news"]["item_count"], 1)
        self.assertEqual(overview["fundamentals"]["snapshot_count"], 1)
        self.assertTrue(overview["database"]["exists"])
        self.assertTrue(any(item["key"] == "database" for item in overview["files"]))
        self.assertEqual(overview["warnings"], [])

    def test_format_bytes_uses_readable_units(self) -> None:
        self.assertEqual(DataCenterService._format_bytes(0), "0 B")
        self.assertEqual(DataCenterService._format_bytes(1536), "1.5 KB")

    def test_run_portfolio_backtests_uses_current_holdings(self) -> None:
        with self.db.get_session() as session:
            account = PortfolioAccount(name="主账户", market="cn", is_active=True)
            session.add(account)
            session.flush()
            session.add_all(
                [
                    PortfolioPosition(account_id=account.id, symbol="600519", market="cn", quantity=100),
                    PortfolioPosition(account_id=account.id, symbol="000001", market="cn", quantity=0),
                    PortfolioPosition(account_id=account.id, symbol="AAPL", market="us", quantity=10),
                ]
            )
            session.commit()

        fake_backtest = patch("src.services.data_center_service.BacktestService").start()
        self.addCleanup(patch.stopall)
        instance = fake_backtest.return_value
        instance.run_backtest.return_value = {
            "candidate_count": 1,
            "processed": 1,
            "saved": 1,
            "completed": 1,
            "insufficient": 0,
            "errors": 0,
        }
        instance.get_stock_summary.return_value = {"win_rate_pct": 60.0}

        result = DataCenterService(self.db).run_portfolio_backtests(limit_per_symbol=20)

        self.assertEqual(result["holding_count"], 1)
        self.assertEqual(result["processed_symbols"], 1)
        self.assertEqual(result["items"][0]["code"], "600519")
        self.assertEqual(result["totals"]["saved"], 1)
        instance.run_backtest.assert_called_once_with(
            code="600519",
            force=False,
            eval_window_days=None,
            min_age_days=None,
            limit=20,
        )

    def test_portfolio_risk_radar_classifies_current_holdings(self) -> None:
        with self.db.get_session() as session:
            account = PortfolioAccount(name="主账户", market="cn", is_active=True)
            session.add(account)
            session.flush()
            session.add_all(
                [
                    PortfolioPosition(
                        account_id=account.id,
                        symbol="600519",
                        market="cn",
                        quantity=100,
                        market_value_base=10000,
                    ),
                    PortfolioPosition(
                        account_id=account.id,
                        symbol="000001",
                        market="cn",
                        quantity=200,
                        market_value_base=5000,
                    ),
                ]
            )
            session.commit()

        fake_backtest = patch("src.services.data_center_service.BacktestService").start()
        self.addCleanup(patch.stopall)
        instance = fake_backtest.return_value

        def _summary(code: str):
            if code == "600519":
                return {
                    "total_evaluations": 3,
                    "completed_count": 3,
                    "insufficient_count": 0,
                    "win_rate_pct": 66.67,
                    "avg_simulated_return_pct": 5.2,
                }
            return None

        instance.get_stock_summary.side_effect = _summary

        result = DataCenterService(self.db).get_portfolio_risk_radar()

        self.assertEqual(result["holding_count"], 2)
        items = {item["code"]: item for item in result["items"]}
        self.assertEqual(items["600519"]["tone"], "strong")
        self.assertEqual(items["600519"]["label"], "优先关注")
        self.assertEqual(items["000001"]["tone"], "empty")
        self.assertEqual(items["000001"]["label"], "先分析")

    def test_portfolio_risk_radar_distinguishes_immature_backtests(self) -> None:
        with self.db.get_session() as session:
            account = PortfolioAccount(name="主账户", market="cn", is_active=True)
            session.add(account)
            session.flush()
            session.add(
                PortfolioPosition(
                    account_id=account.id,
                    symbol="600918",
                    market="cn",
                    quantity=100,
                    market_value_base=10000,
                )
            )
            session.commit()

        fake_backtest = patch("src.services.data_center_service.BacktestService").start()
        self.addCleanup(patch.stopall)
        instance = fake_backtest.return_value
        instance.get_stock_summary.return_value = {
            "total_evaluations": 1,
            "completed_count": 0,
            "insufficient_count": 1,
        }

        result = DataCenterService(self.db).get_portfolio_risk_radar()

        self.assertEqual(result["items"][0]["code"], "600918")
        self.assertEqual(result["items"][0]["label"], "待成熟")
        self.assertIn("已有分析", result["items"][0]["title"])
