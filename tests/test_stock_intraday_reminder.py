from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.services.stock_intraday_reminder import (
    HoldingRealtimeRadarConfig,
    _is_cn_intraday_session,
    collect_actionable_stock_items,
    run_stock_intraday_reminder_cycle,
)


@dataclass
class _Row:
    code: str
    name: str
    operation_advice: str
    trend_prediction: str
    stop_loss: float | None
    take_profit: float | None
    created_at: datetime


class _FakeRepo:
    def __init__(self, mapping):
        self.mapping = mapping

    def get_list(self, code=None, days=30, limit=50):
        row = self.mapping.get(code)
        return [row] if row else []


class _FakeStockService:
    def __init__(self, quotes=None):
        self.quotes = quotes or {}

    def get_realtime_quote(self, stock_code: str):
        return self.quotes.get(stock_code)


class _FakeNotifier:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send(self, content: str) -> bool:
        self.messages.append(content)
        return True


class _FakeConfig:
    stock_list = ["600529", "300251", "159326", "600519"]
    stock_intraday_reminder_analysis_max_age_days = 3
    stock_intraday_reminder_max_items = 6
    stock_intraday_watchlist_buy_start_time = "14:30"
    stock_intraday_watchlist_buy_end_time = "14:55"
    stock_intraday_watchlist_min_change_pct = -1.5
    stock_intraday_watchlist_max_change_pct = 2.5
    stock_intraday_watchlist_max_stop_loss_distance_pct = 3.5
    stock_intraday_watchlist_ma_proximity_pct = 1.5
    stock_intraday_watchlist_require_quote = True
    stock_intraday_watchlist_require_stop_loss = True
    stock_intraday_holding_cooldown_minutes = 30
    stock_intraday_watchlist_daily_limit = 1
    stock_intraday_systemic_batch_threshold = 3
    stock_intraday_bad_tick_max_abs_change_pct = 25.0


class StockIntradayReminderTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = _FakeRepo(
            {
                "600529": _Row("600529", "山东药玻", "买入", "看多", 22.8, 25.5, datetime(2026, 4, 27, 9, 30)),
                "300251": _Row("300251", "光线传媒", "卖出", "强烈看空", 14.5, 15.5, datetime(2026, 4, 27, 9, 31)),
                "159326": _Row("159326", "华夏中证电网设备主题ETF", "买入", "看多", 1.1, 1.3, datetime(2026, 4, 27, 9, 32)),
                "600519": _Row("600519", "贵州茅台", "观望", "震荡", 1400.0, 1440.0, datetime(2026, 4, 27, 9, 33)),
            }
        )
        self.stock_service = _FakeStockService(
            {
                "600529": {"current_price": 23.2, "change_percent": 1.8},
                "300251": {"current_price": 14.2, "change_percent": -3.4},
                "159326": {"current_price": 1.12, "change_percent": 0.4},
            }
        )

    def test_collect_actionable_items_includes_etf_buy_sell(self) -> None:
        items = collect_actionable_stock_items(
            stock_codes=["600529", "300251", "159326", "600519"],
            repo=self.repo,
            stock_service=self.stock_service,
        )
        self.assertEqual([item.code for item in items], ["300251", "159326", "600529"])
        self.assertEqual([item.action for item in items], ["卖", "买", "买"])

    def test_collect_actionable_items_can_filter_buy_only(self) -> None:
        items = collect_actionable_stock_items(
            stock_codes=["600529", "300251", "159326", "600519"],
            repo=self.repo,
            stock_service=self.stock_service,
            allowed_actions={"买"},
        )
        self.assertEqual([item.code for item in items], ["159326", "600529"])
        self.assertEqual([item.action for item in items], ["买", "买"])

    def test_collect_holding_realtime_radar_flags_ma20_break_without_saved_sell(self) -> None:
        row = _Row("600519", "贵州茅台", "观望", "震荡", 1380.0, 1450.0, datetime(2026, 4, 27, 9, 33))
        row.ma20 = 1400.0
        repo = _FakeRepo({"600519": row})
        stock_service = _FakeStockService(
            {"600519": {"current_price": 1395.0, "change_percent": -0.8}}
        )

        items = collect_actionable_stock_items(
            stock_codes=["600519"],
            repo=repo,
            stock_service=stock_service,
            allowed_actions={"卖"},
            holding_realtime_radar=HoldingRealtimeRadarConfig(),
            now=datetime(2026, 4, 27, 10, 5),
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].action, "卖")
        self.assertIn("MA20", items[0].trigger_reason)

    def test_cycle_sends_during_cn_market_hours(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            notifier = _FakeNotifier()
            state_path = Path(tmpdir) / "stock_state.json"
            result = run_stock_intraday_reminder_cycle(
                state_path=state_path,
                config=_FakeConfig(),
                now=datetime(2026, 4, 27, 10, 5),
                repo=self.repo,
                stock_service=self.stock_service,
                notifier_factory=lambda: notifier,
            )
            self.assertTrue(result["market_open"])
            self.assertEqual(result["item_count"], 1)
            self.assertTrue(result["sent"])
            self.assertEqual(len(notifier.messages), 1)
            self.assertIn("持仓盘中实时风控", notifier.messages[0])
            self.assertIn("光线传媒(300251)", notifier.messages[0])
            self.assertIn("实时跌破止损位", notifier.messages[0])
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["last_sent_date"], "2026-04-27")
            self.assertEqual(saved["last_active_codes"], ["300251"])
            self.assertEqual(len(saved["last_signal_keys"]), 1)
            self.assertEqual(saved["groups"]["holding_risk"]["last_active_codes"], ["300251"])
            route_state_path = Path(result["route_state_path"])
            self.assertTrue(route_state_path.exists())

    def test_cycle_suppresses_duplicate_same_day_signals(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            notifier = _FakeNotifier()
            state_path = Path(tmpdir) / "stock_state.json"
            first = run_stock_intraday_reminder_cycle(
                state_path=state_path,
                config=_FakeConfig(),
                now=datetime(2026, 4, 27, 10, 5),
                repo=self.repo,
                stock_service=self.stock_service,
                notifier_factory=lambda: notifier,
            )
            second = run_stock_intraday_reminder_cycle(
                state_path=state_path,
                config=_FakeConfig(),
                now=datetime(2026, 4, 27, 10, 10),
                repo=self.repo,
                stock_service=self.stock_service,
                notifier_factory=lambda: notifier,
            )

            self.assertTrue(first["sent"])
            self.assertFalse(first["suppressed_duplicate"])
            self.assertFalse(second["sent"])
            self.assertTrue(second["suppressed_duplicate"])
            self.assertEqual(len(notifier.messages), 1)
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["last_suppressed_at"], "2026-04-27T10:10:00+08:00")

    def test_cycle_uses_watchlist_and_sends_buy_only(self) -> None:
        class _WatchlistConfig(_FakeConfig):
            watchlist_stock_list = ["600529", "300251", "159326"]

        with tempfile.TemporaryDirectory() as tmpdir:
            notifier = _FakeNotifier()
            state_path = Path(tmpdir) / "stock_state.json"
            result = run_stock_intraday_reminder_cycle(
                state_path=state_path,
                config=_WatchlistConfig(),
                now=datetime(2026, 4, 27, 14, 35),
                repo=self.repo,
                stock_service=self.stock_service,
                notifier_factory=lambda: notifier,
            )

            self.assertTrue(result["market_open"])
            self.assertEqual(result["scope"], "mixed")
            self.assertEqual(result["group_counts"], {"holding_risk": 1, "watchlist_buy": 2})
            self.assertEqual(result["item_count"], 3)
            self.assertTrue(result["sent"])
            self.assertEqual(len(notifier.messages), 2)
            combined = "\n".join(notifier.messages)
            self.assertIn("持仓盘中实时风控", combined)
            self.assertIn("自选股盘中买入提醒", combined)
            self.assertIn("山东药玻(600529)", combined)
            self.assertIn("华夏中证电网设备主题ETF(159326)", combined)
            ledger_path = Path(result["replay_ledger_path"])
            self.assertTrue(ledger_path.exists())
            ledger_rows = [
                json.loads(line)
                for line in ledger_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(len(ledger_rows), 3)
            self.assertEqual(
                {row["scope"] for row in ledger_rows},
                {"holding_risk", "watchlist_buy"},
            )
            self.assertTrue(all("forward_labels" in row for row in ledger_rows))
            self.assertTrue(all(row["schema_version"] == 2 for row in ledger_rows))
            self.assertTrue(all(row.get("signal_id") for row in ledger_rows))
            self.assertTrue(all(row.get("rule_version") == "stock_intraday_replay_v2" for row in ledger_rows))
            self.assertTrue(all("trigger_condition_snapshot" in row for row in ledger_rows))
            self.assertTrue(all("outcome_reference_window" in row for row in ledger_rows))
            self.assertEqual(
                {row["signal_type"] for row in ledger_rows},
                {"RISK_STOP", "BUY_SETUP"},
            )

    def test_watchlist_route_state_limits_same_symbol_once_per_day(self) -> None:
        class _WatchlistConfig(_FakeConfig):
            stock_list = []
            watchlist_stock_list = ["600529"]

        with tempfile.TemporaryDirectory() as tmpdir:
            notifier = _FakeNotifier()
            state_path = Path(tmpdir) / "stock_state.json"
            first = run_stock_intraday_reminder_cycle(
                state_path=state_path,
                config=_WatchlistConfig(),
                now=datetime(2026, 4, 27, 14, 35),
                repo=self.repo,
                stock_service=self.stock_service,
                notifier_factory=lambda: notifier,
            )
            # Simulate a changed analysis signal so the legacy exact-key
            # duplicate check does not hide the route-level daily limit.
            changed_repo = _FakeRepo(
                {
                    "600529": _Row(
                        "600529",
                        "山东药玻",
                        "买入",
                        "强烈看多",
                        22.8,
                        25.5,
                        datetime(2026, 4, 27, 14, 40),
                    )
                }
            )
            second = run_stock_intraday_reminder_cycle(
                state_path=state_path,
                config=_WatchlistConfig(),
                now=datetime(2026, 4, 27, 14, 50),
                repo=changed_repo,
                stock_service=self.stock_service,
                notifier_factory=lambda: notifier,
            )

            self.assertTrue(first["sent"])
            self.assertFalse(second["sent"])
            self.assertFalse(second["suppressed_duplicate"])
            self.assertIn("daily limit", second["route_decision"]["reason"])
            self.assertEqual(len(notifier.messages), 1)

    def test_watchlist_buy_filter_suppresses_bad_tick_quote(self) -> None:
        class _WatchlistConfig(_FakeConfig):
            stock_list = []
            watchlist_stock_list = ["600529"]

        bad_quote = _FakeStockService(
            {"600529": {"current_price": 0.01, "change_percent": -99.0}}
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            notifier = _FakeNotifier()
            result = run_stock_intraday_reminder_cycle(
                state_path=Path(tmpdir) / "stock_state.json",
                config=_WatchlistConfig(),
                now=datetime(2026, 4, 27, 14, 35),
                repo=self.repo,
                stock_service=bad_quote,
                notifier_factory=lambda: notifier,
            )

            self.assertEqual(result["item_count"], 0)
            self.assertFalse(result["sent"])
            self.assertEqual(len(notifier.messages), 0)

    def test_cycle_watchlist_buy_filter_suppresses_outside_tail_window(self) -> None:
        class _WatchlistConfig(_FakeConfig):
            stock_list = []
            watchlist_stock_list = ["600529", "300251", "159326"]

        with tempfile.TemporaryDirectory() as tmpdir:
            notifier = _FakeNotifier()
            state_path = Path(tmpdir) / "stock_state.json"
            result = run_stock_intraday_reminder_cycle(
                state_path=state_path,
                config=_WatchlistConfig(),
                now=datetime(2026, 4, 27, 10, 5),
                repo=self.repo,
                stock_service=self.stock_service,
                notifier_factory=lambda: notifier,
            )

            self.assertTrue(result["market_open"])
            self.assertEqual(result["scope"], "watchlist_buy")
            self.assertEqual(result["item_count"], 0)
            self.assertFalse(result["sent"])
            self.assertEqual(len(notifier.messages), 0)

    def test_cycle_watchlist_buy_filter_suppresses_poor_stop_distance(self) -> None:
        class _WatchlistConfig(_FakeConfig):
            stock_list = []
            watchlist_stock_list = ["600529"]

        overextended_quote = _FakeStockService(
            {"600529": {"current_price": 25.0, "change_percent": 1.8}}
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            notifier = _FakeNotifier()
            state_path = Path(tmpdir) / "stock_state.json"
            result = run_stock_intraday_reminder_cycle(
                state_path=state_path,
                config=_WatchlistConfig(),
                now=datetime(2026, 4, 27, 14, 35),
                repo=self.repo,
                stock_service=overextended_quote,
                notifier_factory=lambda: notifier,
            )

            self.assertTrue(result["market_open"])
            self.assertEqual(result["item_count"], 0)
            self.assertFalse(result["sent"])
            self.assertEqual(len(notifier.messages), 0)

    def test_cycle_suppresses_legacy_same_day_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            notifier = _FakeNotifier()
            state_path = Path(tmpdir) / "stock_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "last_sent_at": "2026-04-27T10:00:00",
                        "last_active_codes": ["300251"],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            result = run_stock_intraday_reminder_cycle(
                state_path=state_path,
                config=_FakeConfig(),
                now=datetime(2026, 4, 27, 10, 5),
                repo=self.repo,
                stock_service=self.stock_service,
                notifier_factory=lambda: notifier,
            )

            self.assertFalse(result["sent"])
            self.assertTrue(result["suppressed_duplicate"])
            self.assertEqual(len(notifier.messages), 0)
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["last_sent_date"], "2026-04-27")
            self.assertEqual(len(saved["last_signal_keys"]), 1)

    def test_cycle_skips_outside_cn_market_hours(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            notifier = _FakeNotifier()
            state_path = Path(tmpdir) / "stock_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "last_sent_at": "2026-04-27T10:00:00",
                        "last_sent_date": "2026-04-27",
                        "last_active_codes": ["300251"],
                        "last_signal_keys": ["300251|卖|卖出|看空|14.5000|15.5000"],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            result = run_stock_intraday_reminder_cycle(
                state_path=state_path,
                config=_FakeConfig(),
                now=datetime(2026, 4, 27, 12, 10),
                repo=self.repo,
                stock_service=self.stock_service,
                notifier_factory=lambda: notifier,
            )
            self.assertFalse(result["market_open"])
            self.assertFalse(result["sent"])
            self.assertEqual(len(notifier.messages), 0)
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertFalse(saved["market_open"])

    def test_session_uses_cn_market_timezone(self) -> None:
        utc_time = datetime(2026, 4, 29, 1, 35, tzinfo=timezone.utc)
        self.assertTrue(_is_cn_intraday_session(utc_time))


if __name__ == "__main__":
    unittest.main()
