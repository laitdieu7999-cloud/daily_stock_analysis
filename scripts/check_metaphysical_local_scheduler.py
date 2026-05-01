#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Inspect the local metaphysical scheduler status and latest artifacts."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LAUNCH_AGENT_LABEL = "com.laitdieu.daily-stock-analysis.metaphysical"
LAUNCH_AGENT_PLIST = Path.home() / "Library" / "LaunchAgents" / f"{LAUNCH_AGENT_LABEL}.plist"
LOG_DIR = Path.home() / "Library" / "Application Support" / "Daily Stock Analysis" / "logs"


def _read_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt_ts(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(sep=" ", timespec="seconds")
def _launchctl_print(label: str) -> str:
    uid = subprocess.check_output(["id", "-u"], text=True).strip()
    result = subprocess.run(
        ["launchctl", "print", f"gui/{uid}/{label}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return (result.stdout or result.stderr or "").strip()


def main() -> int:
    latest_signal = _read_json(PROJECT_ROOT / "reports" / "metaphysical_latest_signal.json")
    daily_desktop = Path.home() / "Desktop" / "玄学治理日报"
    latest_desktop = sorted(daily_desktop.glob("*_玄学治理日报.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    stdout_log = LOG_DIR / "metaphysical-launchagent.stdout.log"
    stderr_log = LOG_DIR / "metaphysical-launchagent.stderr.log"

    payload = {
        "launch_agent_plist_exists": LAUNCH_AGENT_PLIST.exists(),
        "launch_agent_status": _launchctl_print(LAUNCH_AGENT_LABEL),
        "latest_signal_path": str(PROJECT_ROOT / "reports" / "metaphysical_latest_signal.json"),
        "latest_signal_exists": latest_signal is not None,
        "latest_signal_date": (latest_signal.get("final_signal", {}).get("signal_date") if isinstance(latest_signal, dict) else None),
        "latest_signal_file_mtime": _fmt_ts(PROJECT_ROOT / "reports" / "metaphysical_latest_signal.json"),
        "latest_desktop_report": str(latest_desktop[0]) if latest_desktop else None,
        "latest_desktop_report_mtime": _fmt_ts(latest_desktop[0]) if latest_desktop else None,
        "stdout_log_exists": stdout_log.exists(),
        "stderr_log_exists": stderr_log.exists(),
        "stdout_log_mtime": _fmt_ts(stdout_log),
        "stderr_log_mtime": _fmt_ts(stderr_log),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
