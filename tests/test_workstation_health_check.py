from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts import run_workstation_health_check as health_check


class _FakeOverviewService:
    def build_overview(self):
        return {
            "generated_at": "2026-04-30T09:35:00",
            "alerts": [
                {
                    "level": "critical",
                    "title": "盘中实时监控异常",
                    "description": "最近心跳/日志超过 5 分钟未更新",
                }
            ],
            "services": [
                {
                    "key": "stock_intraday_realtime",
                    "status": "critical",
                    "pid": None,
                    "detail": "最近心跳/日志超过 5 分钟未更新",
                }
            ],
            "data_warehouse": {"database": {}, "disk": {}},
        }


class WorkstationHealthCheckTestCase(unittest.TestCase):
    def test_critical_snapshot_sends_project_and_macos_notification(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            health_check,
            "SystemOverviewService",
            return_value=_FakeOverviewService(),
        ), patch("src.notification.NotificationService") as notification_service, patch(
            "subprocess.run"
        ) as subprocess_run:
            notification_service.return_value.send.return_value = {"sent": True}
            subprocess_run.return_value = MagicMock(returncode=0)

            result = health_check.run(
                notify=True,
                output_dir=Path(tmpdir),
                cooldown_minutes=1,
            )

            rows = [
                json.loads(line)
                for line in Path(result["written_path"]).read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

        self.assertEqual(result["status"], "critical")
        self.assertTrue(result["notification_sent"])
        self.assertEqual(rows[0]["status"], "critical")
        notification_service.return_value.send.assert_called_once()
        subprocess_run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
