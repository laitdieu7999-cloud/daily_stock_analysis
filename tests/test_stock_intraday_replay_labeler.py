from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

from src.config import Config
from src.services.stock_intraday_replay_labeler import StockIntradayReplayLabeler
from src.storage import DatabaseManager, StockDaily


class StockIntradayReplayLabelerTestCase(unittest.TestCase):
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

    def test_backfills_forward_labels_from_local_daily_bars(self) -> None:
        with self.db.get_session() as session:
            session.add_all(
                [
                    StockDaily(code="600519", date=date(2026, 4, 28), close=102.0, high=103.0, low=99.0),
                    StockDaily(code="600519", date=date(2026, 4, 29), close=101.0, high=104.0, low=100.0),
                    StockDaily(code="600519", date=date(2026, 4, 30), close=98.0, high=102.0, low=97.0),
                    StockDaily(code="600519", date=date(2026, 5, 6), close=105.0, high=106.0, low=101.0),
                    StockDaily(code="600519", date=date(2026, 5, 7), close=110.0, high=111.0, low=104.0),
                ]
            )
            session.commit()

        ledger_path = self.root / "stock_intraday_replay_ledger.jsonl"
        ledger_path.write_text(
            json.dumps(
                {
                    "schema_version": 2,
                    "event_time": "2026-04-27T14:35:00+08:00",
                    "code": "600519",
                    "name": "贵州茅台",
                    "signal_type": "BUY_SETUP",
                    "current_price": 100.0,
                    "forward_labels": {"t_plus_1": None, "t_plus_3": None, "t_plus_5": None},
                    "outcome_reference_window": {},
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )

        result = StockIntradayReplayLabeler(
            db_manager=self.db,
            ledger_path=ledger_path,
        ).run()

        rows = [
            json.loads(line)
            for line in ledger_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

        self.assertEqual(result["totals"]["updated"], 1)
        self.assertEqual(rows[0]["forward_labels"]["t_plus_1"]["return_pct"], 2.0)
        self.assertEqual(rows[0]["forward_labels"]["t_plus_3"]["return_pct"], -2.0)
        self.assertEqual(rows[0]["forward_labels"]["t_plus_5"]["return_pct"], 10.0)
        self.assertEqual(rows[0]["outcome_reference_window"]["outcome_max_adverse_1d"], -3.0)
        self.assertEqual(rows[0]["outcome_reference_window"]["outcome_max_favorable_1d"], 11.0)
        self.assertEqual(rows[0]["label_source"], "local_stock_daily")

    def test_dry_run_does_not_rewrite_ledger(self) -> None:
        ledger_path = self.root / "stock_intraday_replay_ledger.jsonl"
        original = (
            json.dumps(
                {
                    "event_time": "2026-04-27T14:35:00+08:00",
                    "code": "600519",
                    "current_price": 100.0,
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        ledger_path.write_text(original, encoding="utf-8")

        result = StockIntradayReplayLabeler(
            db_manager=self.db,
            ledger_path=ledger_path,
        ).run(dry_run=True)

        self.assertEqual(result["totals"]["missing_bars"], 1)
        self.assertEqual(ledger_path.read_text(encoding="utf-8"), original)


if __name__ == "__main__":
    unittest.main()
