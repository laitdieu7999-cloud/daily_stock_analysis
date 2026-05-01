#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run the premarket health check and optionally push the result."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_config, setup_env  # noqa: E402
from src.logging_config import setup_logging  # noqa: E402
from src.services.premarket_health_check import run_premarket_health_check  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Daily Stock Analysis premarket health check")
    parser.add_argument("--no-notify", action="store_true", help="Only write local reports, do not push")
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT, help="Project root override")
    args = parser.parse_args()

    setup_env()
    config = get_config()
    setup_logging(log_prefix="premarket_health_check", debug=False, log_dir=config.log_dir)
    payload = run_premarket_health_check(
        config=config,
        notify=not args.no_notify,
        project_root=args.project_root,
        now=datetime.now(),
    )
    printable = {
        "status": payload.get("status"),
        "report_path": payload.get("report_path"),
        "json_path": payload.get("json_path"),
        "notification_sent": payload.get("notification_sent", False),
    }
    print(json.dumps(printable, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
