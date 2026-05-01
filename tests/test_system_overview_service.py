from __future__ import annotations

import os
import re
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.services.system_overview_service import SystemOverviewService


class SystemOverviewServiceTestCase(unittest.TestCase):
    def test_build_overview_reports_routing_modules_and_files(self) -> None:
        with tempfile.TemporaryDirectory() as project_tmp, tempfile.TemporaryDirectory() as home_tmp:
            project_root = Path(project_tmp)
            reports = project_root / "reports"
            signal_events = reports / "signal_events"
            signal_events.mkdir(parents=True)
            (project_root / "docs").mkdir()
            (project_root / "docs" / "SIGNAL_ROUTING.md").write_text("# routing\n", encoding="utf-8")
            (reports / "gemini_daily.md").write_text("2026-04-28 daily\n", encoding="utf-8")
            (reports / "gemini_black_swan.md").write_text("2026-04-28 black swan\n", encoding="utf-8")
            (signal_events / "ic_shadow_events.jsonl").write_text('{"event": {"priority": "P3"}}\n', encoding="utf-8")
            (reports / "ic_m1_m2_shadow_monitoring_events.jsonl").write_text('{"event_key": "a"}\n', encoding="utf-8")
            (reports / "stock_intraday_replay_ledger.jsonl").write_text('{"code":"600519"}\n', encoding="utf-8")
            (reports / "sniper_point_downgrade_audit.jsonl").write_text(
                '{"event":"sniper_point_context_mismatch"}\n',
                encoding="utf-8",
            )
            (reports / "sniper_point_downgrade_summary.md").write_text("# 狙击点位保护降级摘要\n", encoding="utf-8")
            (reports / "system_health_archive").mkdir()
            (reports / "system_health_archive" / "2026-04-28_workstation_health.jsonl").write_text(
                '{"status":"ok"}\n',
                encoding="utf-8",
            )
            data_dir = project_root / "data"
            data_dir.mkdir()
            (data_dir / "stock_analysis.db").write_bytes(b"not-a-real-db")

            config = SimpleNamespace(
                schedule_enabled=True,
                schedule_time="09:40",
                nightly_market_outlook_enabled=True,
                nightly_market_outlook_time="22:30",
                stock_intraday_reminder_enabled=True,
                stock_list=["600519"],
                watchlist_stock_list=["159326"],
                database_path="./data/stock_analysis.db",
            )

            payload = SystemOverviewService(
                project_root=project_root,
                home_dir=home_tmp,
                config=config,
            ).build_overview()

        priorities = {item["priority"]: item for item in payload["priorities"]}
        modules = {item["key"]: item for item in payload["modules"]}
        files = {item["key"]: item for item in payload["files"]}
        services = {item["key"]: item for item in payload["services"]}

        self.assertIn("P0", priorities)
        self.assertIn("P4", priorities)
        self.assertEqual(modules["black_swan"]["priority"], "P0")
        self.assertEqual(modules["ic_shadow"]["priority"], "P3")
        self.assertEqual(modules["gemini_external"]["priority"], "P4")
        self.assertEqual(modules["sniper_point_guard"]["status"], "active")
        self.assertIn("已拦截 1 条", modules["sniper_point_guard"]["detail"])
        self.assertIn("统一P3账本 1 条", modules["ic_shadow"]["detail"])
        self.assertTrue(files["signal_contract"]["exists"])
        self.assertTrue(files["stock_intraday_replay_ledger"]["exists"])
        self.assertTrue(files["sniper_point_downgrade_summary"]["exists"])
        self.assertTrue(payload["scheduler"]["nightly_market_outlook_enabled"])
        self.assertIn("services", payload)
        self.assertIn("stock_intraday_realtime", services)
        self.assertEqual(services["stock_intraday_realtime"]["name"], "盘中实时监控")
        self.assertIn("data_warehouse", payload)
        self.assertIn("alerts", payload)
        self.assertTrue(payload["data_warehouse"]["database"]["exists"])
        self.assertIn("disk", payload["data_warehouse"])

    def test_closed_desktop_backend_is_warning_not_critical(self) -> None:
        with tempfile.TemporaryDirectory() as project_tmp, tempfile.TemporaryDirectory() as home_tmp:
            project_root = Path(project_tmp)
            (project_root / "reports").mkdir(parents=True)
            (project_root / "data").mkdir()
            config = SimpleNamespace(
                schedule_enabled=True,
                schedule_time="09:40",
                nightly_market_outlook_enabled=True,
                nightly_market_outlook_time="22:30",
                stock_intraday_reminder_enabled=False,
                stock_list=[],
                watchlist_stock_list=[],
                database_path="./data/stock_analysis.db",
            )

            payload = SystemOverviewService(
                project_root=project_root,
                home_dir=home_tmp,
                config=config,
            ).build_overview()

        services = {item["key"]: item for item in payload["services"]}
        self.assertEqual(services["desktop_backend"]["status"], "warning")
        self.assertFalse(
            any(item["title"] == "桌面端服务异常" and item["level"] == "critical" for item in payload["alerts"])
        )

    def test_stock_intraday_realtime_is_critical_when_heartbeat_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as project_tmp, tempfile.TemporaryDirectory() as home_tmp:
            project_root = Path(project_tmp)
            home_root = Path(home_tmp)
            reports = project_root / "reports"
            reports.mkdir(parents=True)
            heartbeat = reports / "stock_intraday_heartbeat.json"
            heartbeat.write_text('{"status":"ok"}\n', encoding="utf-8")
            old_ts = (datetime.now() - timedelta(minutes=10)).timestamp()
            os.utime(heartbeat, (old_ts, old_ts))

            plist_dir = home_root / "Library" / "LaunchAgents"
            plist_dir.mkdir(parents=True)
            (plist_dir / "com.laitdieu.daily-stock-analysis.stock-intraday.plist").write_text(
                "<plist></plist>\n",
                encoding="utf-8",
            )

            config = SimpleNamespace(
                schedule_enabled=True,
                schedule_time="09:40",
                nightly_market_outlook_enabled=True,
                nightly_market_outlook_time="22:30",
                stock_intraday_reminder_enabled=True,
                stock_list=[],
                watchlist_stock_list=[],
                database_path="./data/stock_analysis.db",
            )

            payload = SystemOverviewService(
                project_root=project_root,
                home_dir=home_root,
                config=config,
            ).build_overview()

        services = {item["key"]: item for item in payload["services"]}
        self.assertEqual(services["stock_intraday_realtime"]["status"], "critical")
        self.assertIn("超过 5 分钟", services["stock_intraday_realtime"]["detail"])
        self.assertTrue(
            any(item["title"] == "盘中实时监控异常" and item["level"] == "critical" for item in payload["alerts"])
        )

    def test_find_process_pid_can_identify_current_process(self) -> None:
        current_command = " ".join([sys.executable, *sys.argv])
        pattern = re.escape(current_command)

        with patch("subprocess.run") as run_mock:
            pid = SystemOverviewService._find_process_pid(pattern)

        self.assertEqual(pid, os.getpid())
        run_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
