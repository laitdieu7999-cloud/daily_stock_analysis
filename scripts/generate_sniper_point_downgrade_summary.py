#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a compact report for protectively downgraded sniper levels."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.sniper_point_downgrade_report import (  # noqa: E402
    DEFAULT_AUDIT_PATH,
    DEFAULT_REPORT_PATH,
    build_sniper_point_downgrade_summary,
    render_sniper_point_downgrade_summary,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a compact report for protectively downgraded sniper levels."
    )
    parser.add_argument(
        "--audit-path",
        default=str(DEFAULT_AUDIT_PATH),
        help="Path to the sniper point downgrade audit JSONL ledger.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_REPORT_PATH),
        help="Markdown output path.",
    )
    parser.add_argument(
        "--max-recent-rows",
        type=int,
        default=20,
        help="How many recent downgrade events to show.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of Markdown.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    summary = build_sniper_point_downgrade_summary(
        args.audit_path,
        max_recent_rows=args.max_recent_rows,
    )
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    rendered = render_sniper_point_downgrade_summary(summary)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    print(f"written: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
