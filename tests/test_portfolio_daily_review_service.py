# -*- coding: utf-8 -*-
"""Tests for daily portfolio review generation."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.config import Config
from src.services.portfolio_daily_review_service import PortfolioDailyReviewService
from src.storage import DatabaseManager, PortfolioAccount, PortfolioPosition, StockDaily


class PortfolioDailyReviewServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        os.environ["DATABASE_PATH"] = str(self.root / "data" / "stock_analysis.db")
        (self.root / "data").mkdir()
        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        os.environ.pop("DATABASE_PATH", None)
        self.temp_dir.cleanup()

    def test_run_writes_json_and_markdown_report(self) -> None:
        with self.db.get_session() as session:
            account = PortfolioAccount(name="主账户", market="cn", is_active=True)
            session.add(account)
            session.flush()
            session.add(PortfolioPosition(account_id=account.id, symbol="600519", market="cn", quantity=100))
            session.add(StockDaily(code="600519", date=date(2026, 4, 29), close=1600, data_source="unit"))
            session.commit()

        fake_backtest = patch("src.services.data_center_service.BacktestService").start()
        self.addCleanup(patch.stopall)
        fake_backtest.return_value.run_backtest.return_value = {
            "candidate_count": 0,
            "processed": 0,
            "saved": 0,
            "completed": 0,
            "insufficient": 0,
            "errors": 0,
        }
        fake_backtest.return_value.get_stock_summary.return_value = None

        config = SimpleNamespace(
            portfolio_daily_review_limit_per_symbol=50,
            portfolio_daily_review_notify_enabled=False,
        )
        result = PortfolioDailyReviewService(
            self.db,
            config=config,
            project_root=self.root,
        ).run(report_date=date(2026, 4, 29))

        self.assertEqual(result["status"], "ok")
        self.assertTrue(Path(result["json_path"]).exists())
        self.assertTrue(Path(result["markdown_path"]).exists())
        self.assertIn("持仓复盘", Path(result["markdown_path"]).read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
