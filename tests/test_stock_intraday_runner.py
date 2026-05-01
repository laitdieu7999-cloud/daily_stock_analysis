from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from scripts.run_stock_intraday_reminder import _append_urgent_error, _maybe_send_self_check, _write_heartbeat


class _FakeNotifier:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send(self, content: str) -> bool:
        self.messages.append(content)
        return True


class StockIntradayRunnerTestCase(unittest.TestCase):
    def test_self_check_sends_once_in_preopen_window(self) -> None:
        config = SimpleNamespace(
            stock_intraday_self_check_enabled=True,
            stock_intraday_reminder_enabled=True,
            stock_intraday_self_check_time="09:25",
            stock_list=["600519", "300251"],
            watchlist_stock_list=["159326"],
            feishu_webhook_url="https://open.feishu.cn/test",
        )
        notifier = _FakeNotifier()

        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "self_check.json"
            first = _maybe_send_self_check(
                config,
                now=datetime(2026, 4, 30, 9, 25),
                state_path=state_path,
                notifier_factory=lambda: notifier,
            )
            second = _maybe_send_self_check(
                config,
                now=datetime(2026, 4, 30, 9, 26),
                state_path=state_path,
                notifier_factory=lambda: notifier,
            )

        self.assertTrue(first["sent"])
        self.assertFalse(second["sent"])
        self.assertIn("already sent", second["reason"])
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("盘中实时监控自检", notifier.messages[0])
        self.assertIn("持仓监控: 2 个标的", notifier.messages[0])

    def test_self_check_skips_outside_window(self) -> None:
        config = SimpleNamespace(
            stock_intraday_self_check_enabled=True,
            stock_intraday_reminder_enabled=True,
            stock_intraday_self_check_time="09:25",
            stock_list=[],
            watchlist_stock_list=[],
            feishu_webhook_url=None,
        )

        result = _maybe_send_self_check(
            config,
            now=datetime(2026, 4, 30, 10, 5),
            state_path=Path(tempfile.gettempdir()) / "unused_self_check.json",
            notifier_factory=_FakeNotifier,
        )

        self.assertFalse(result["sent"])
        self.assertIn("outside", result["reason"])

    def test_heartbeat_and_error_log_are_structured(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            heartbeat_path = Path(tmpdir) / "heartbeat.json"
            error_path = Path(tmpdir) / "errors.jsonl"
            _write_heartbeat(
                run_id="run-1",
                status="running",
                started_at="2026-04-30T09:30:00",
                details={"market_open": True},
                heartbeat_path=heartbeat_path,
            )
            _append_urgent_error(
                run_id="run-1",
                error_type="TIMEOUT",
                message="exceeded",
                started_at="2026-04-30T09:30:00",
                error_log_path=error_path,
            )

            heartbeat = json.loads(heartbeat_path.read_text(encoding="utf-8"))
            errors = [json.loads(line) for line in error_path.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(heartbeat["schema_version"], 1)
        self.assertEqual(heartbeat["status"], "running")
        self.assertEqual(heartbeat["details"]["market_open"], True)
        self.assertEqual(errors[0]["level"], "URGENT")
        self.assertEqual(errors[0]["error_type"], "TIMEOUT")


if __name__ == "__main__":
    unittest.main()
