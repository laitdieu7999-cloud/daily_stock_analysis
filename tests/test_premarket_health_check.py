from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.services.premarket_health_check import build_premarket_health_check


class _FakeOverviewService:
    def __init__(self, *args, **kwargs):
        pass

    def build_overview(self):
        return {
            "alerts": [],
            "scheduler": {
                "launch_agent_alive": True,
                "launch_agent_pid": 123,
                "schedule_enabled": True,
                "nightly_market_outlook_enabled": True,
            },
            "services": [
                {"name": "桌面端服务", "status": "active", "pid": 456},
                {"name": "股票定时任务", "status": "active", "pid": 123},
            ],
            "files": [
                {"key": "gemini_daily", "modified_at": "2026-04-29T08:00:00"},
                {"key": "gemini_black_swan", "modified_at": "2026-04-29T08:01:00"},
                {"key": "latest_nightly_outlook", "path": "/tmp/nightly.md"},
                {"key": "latest_daily_push", "path": "/tmp/daily.md"},
            ],
        }


def test_build_premarket_health_check_writes_report_and_observation(tmp_path: Path):
    home = tmp_path / "home"
    log_dir = home / "Library" / "Application Support" / "Daily Stock Analysis" / "logs"
    log_dir.mkdir(parents=True)
    (log_dir / "stock_analysis_20260429.log").write_text(
        "飞书消息发送成功\n非A股盘中交易时段\n", encoding="utf-8"
    )

    with patch("src.services.premarket_health_check.SystemOverviewService", _FakeOverviewService):
        payload = build_premarket_health_check(
            config=SimpleNamespace(),
            project_root=tmp_path,
            home_dir=home,
            now=datetime(2026, 4, 29, 8, 50),
        )

    assert payload["status"] == "ok"
    assert Path(payload["report_path"]).exists()
    assert Path(payload["json_path"]).exists()
    assert "系统已就绪" in Path(payload["report_path"]).read_text(encoding="utf-8")
    assert payload["observation"]["counts"]["feishu_success"] == 1
