#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run a lightweight local workstation health check.

This is intentionally not a backup job. It only writes small JSONL health
snapshots and can send a macOS notification when critical failures appear.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.system_overview_service import SystemOverviewService  # noqa: E402


def _reports_dir() -> Path:
    reports = PROJECT_ROOT / "reports"
    try:
        return reports.resolve()
    except Exception:
        return reports


def _snapshot_path(output_dir: Path | None = None) -> Path:
    archive_dir = output_dir or (_reports_dir() / "system_health_archive")
    archive_dir.mkdir(parents=True, exist_ok=True)
    return archive_dir / f"{date.today().isoformat()}_workstation_health.jsonl"


def _summarize(payload: Dict[str, Any]) -> Dict[str, Any]:
    alerts = payload.get("alerts") or []
    services = payload.get("services") or []
    warehouse = payload.get("data_warehouse") or {}
    return {
        "generated_at": payload.get("generated_at") or datetime.now().isoformat(timespec="seconds"),
        "status": _overall_status(alerts),
        "alert_count": len(alerts),
        "alerts": alerts,
        "services": [
            {
                "key": item.get("key"),
                "status": item.get("status"),
                "pid": item.get("pid"),
                "detail": item.get("detail"),
            }
            for item in services
        ],
        "database": warehouse.get("database"),
        "disk": warehouse.get("disk"),
    }


def _overall_status(alerts: List[Dict[str, Any]]) -> str:
    if any(item.get("level") == "critical" for item in alerts):
        return "critical"
    if any(item.get("level") == "warning" for item in alerts):
        return "warning"
    return "ok"


def _write_snapshot(snapshot: Dict[str, Any], output_dir: Path | None = None) -> Path:
    path = _snapshot_path(output_dir)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(snapshot, ensure_ascii=False, sort_keys=True) + "\n")
    return path


def _notification_state_path(output_dir: Path | None = None) -> Path:
    archive_dir = output_dir or (_reports_dir() / "system_health_archive")
    archive_dir.mkdir(parents=True, exist_ok=True)
    return archive_dir / "notification_state.json"


def _should_notify(snapshot: Dict[str, Any], output_dir: Path | None, cooldown_minutes: int) -> bool:
    if snapshot.get("status") != "critical":
        return False
    state_path = _notification_state_path(output_dir)
    now = datetime.now()
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            last = datetime.fromisoformat(state.get("last_notified_at"))
            if now - last < timedelta(minutes=cooldown_minutes):
                return False
        except Exception:
            pass
    state_path.write_text(
        json.dumps({"last_notified_at": now.isoformat(timespec="seconds")}, ensure_ascii=False),
        encoding="utf-8",
    )
    return True


def _send_notification(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    first_alert = (snapshot.get("alerts") or [{}])[0]
    title = "股票工作站异常"
    message = str(first_alert.get("title") or "本地工作站需要检查")
    alerts = snapshot.get("alerts") or []
    alert_lines = []
    for item in alerts[:5]:
        alert_lines.append(
            f"- {item.get('level', 'unknown')}: {item.get('title', '未知异常')} - {item.get('description', '')}"
        )
    content = "\n".join(alert_lines) or message
    push_result: Any = None
    push_error = ""
    try:
        from src.notification import NotificationBuilder, NotificationService

        payload = NotificationBuilder.build_simple_alert(
            title=title,
            content=content,
            alert_type="error",
        )
        push_result = NotificationService().send(payload)
    except Exception as exc:
        push_error = str(exc)

    script = f'display notification {json.dumps(message)} with title {json.dumps(title)}'
    macos_sent = False
    macos_error = ""
    try:
        result = subprocess.run(["osascript", "-e", script], check=False, timeout=5)
        macos_sent = result.returncode == 0
    except Exception as exc:
        macos_error = str(exc)
    return {
        "project_notification_result": push_result,
        "project_notification_error": push_error,
        "macos_notification_sent": macos_sent,
        "macos_notification_error": macos_error,
    }


def run(*, notify: bool, output_dir: Path | None, cooldown_minutes: int) -> Dict[str, Any]:
    payload = SystemOverviewService().build_overview()
    snapshot = _summarize(payload)
    written_path = _write_snapshot(snapshot, output_dir)
    snapshot["written_path"] = str(written_path)
    if notify and _should_notify(snapshot, output_dir, cooldown_minutes):
        snapshot["notification"] = _send_notification(snapshot)
        snapshot["notification_sent"] = True
    else:
        snapshot["notification_sent"] = False
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local workstation health check")
    parser.add_argument("--notify", action="store_true", help="Send macOS notification for critical failures")
    parser.add_argument("--no-notify", action="store_true", help="Disable notification even if --notify is set")
    parser.add_argument("--output-dir", type=Path, default=None, help="Override health archive directory")
    parser.add_argument("--cooldown-minutes", type=int, default=60, help="Notification cooldown")
    args = parser.parse_args()

    snapshot = run(
        notify=bool(args.notify and not args.no_notify),
        output_dir=args.output_dir,
        cooldown_minutes=max(args.cooldown_minutes, 1),
    )
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
