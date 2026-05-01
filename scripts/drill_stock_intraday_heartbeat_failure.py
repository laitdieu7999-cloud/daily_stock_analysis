#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Dry-run drill for stale stock-intraday heartbeat detection."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_stock_intraday_reminder import HEARTBEAT_PATH, _save_json  # noqa: E402
from scripts.run_workstation_health_check import run as run_health_check  # noqa: E402
from src.services.system_overview_service import SystemOverviewService  # noqa: E402

INTRADAY_STDOUT_LOG = (
    Path.home()
    / "Library"
    / "Application Support"
    / "Daily Stock Analysis"
    / "logs"
    / "stock-intraday.stdout.log"
)


def _snapshot(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"exists": False}
    return {
        "exists": True,
        "content": path.read_text(encoding="utf-8", errors="ignore"),
        "mtime": path.stat().st_mtime,
    }


def _restore(path: Path, snapshot: Dict[str, Any]) -> None:
    if not snapshot.get("exists"):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(snapshot.get("content") or ""), encoding="utf-8")
    mtime = float(snapshot.get("mtime") or datetime.now().timestamp())
    os.utime(path, (mtime, mtime))


def _force_stale_heartbeat(path: Path, *, stale_minutes: int) -> None:
    stale_at = datetime.now() - timedelta(minutes=stale_minutes)
    _save_json(
        path,
        {
            "schema_version": 1,
            "run_id": "drill-stale-heartbeat",
            "status": "ok",
            "started_at": stale_at.isoformat(timespec="seconds"),
            "updated_at": stale_at.isoformat(timespec="seconds"),
            "pid": os.getpid(),
            "details": {"drill": True},
        },
    )
    old_ts = stale_at.timestamp()
    os.utime(path, (old_ts, old_ts))


def _force_stale_file(path: Path, *, stale_minutes: int) -> None:
    if not path.exists():
        return
    stale_at = datetime.now() - timedelta(minutes=stale_minutes)
    old_ts = stale_at.timestamp()
    os.utime(path, (old_ts, old_ts))


def main() -> int:
    parser = argparse.ArgumentParser(description="Drill stale heartbeat detection")
    parser.add_argument("--stale-minutes", type=int, default=10)
    parser.add_argument("--notify", action="store_true", help="Actually send notifications during drill")
    parser.add_argument("--keep-stale", action="store_true", help="Do not restore heartbeat after drill")
    args = parser.parse_args()

    snapshot = _snapshot(HEARTBEAT_PATH)
    log_snapshot = _snapshot(INTRADAY_STDOUT_LOG)
    try:
        _force_stale_heartbeat(HEARTBEAT_PATH, stale_minutes=max(args.stale_minutes, 6))
        _force_stale_file(INTRADAY_STDOUT_LOG, stale_minutes=max(args.stale_minutes, 6))
        overview = SystemOverviewService().build_overview()
        service = next(
            (item for item in overview.get("services", []) if item.get("key") == "stock_intraday_realtime"),
            {},
        )
        health = run_health_check(notify=bool(args.notify), output_dir=None, cooldown_minutes=1)
        result = {
            "ok": True,
            "heartbeat_path": str(HEARTBEAT_PATH),
            "service_status": service.get("status"),
            "service_detail": service.get("detail"),
            "health_status": health.get("status"),
            "health_alert_count": health.get("alert_count"),
            "notification_sent": health.get("notification_sent"),
            "restored": not args.keep_stale,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if service.get("status") == "critical" and health.get("status") == "critical" else 2
    finally:
        if not args.keep_stale:
            _restore(HEARTBEAT_PATH, snapshot)
            _restore(INTRADAY_STDOUT_LOG, log_snapshot)


if __name__ == "__main__":
    raise SystemExit(main())
