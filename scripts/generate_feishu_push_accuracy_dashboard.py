#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a compact dashboard that links real Feishu pushes to later outcomes."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.metaphysical import (  # noqa: E402
    build_feishu_push_accuracy_dashboard,
    render_feishu_push_accuracy_dashboard,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a compact dashboard linking Feishu pushes to later outcomes."
    )
    parser.add_argument(
        "--audit-path",
        default=str(PROJECT_ROOT / "reports" / "feishu_push_audit.jsonl"),
        help="Path to the Feishu push audit ledger.",
    )
    parser.add_argument(
        "--snapshot-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_learning_samples.jsonl"),
        help="Path to the daily learning snapshot JSONL ledger.",
    )
    parser.add_argument(
        "--max-recent-rows",
        type=int,
        default=5,
        help="How many recently validated push rows to show.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of markdown.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    summary = build_feishu_push_accuracy_dashboard(
        audit_path=args.audit_path,
        snapshot_path=args.snapshot_path,
        max_recent_rows=args.max_recent_rows,
    )
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(render_feishu_push_accuracy_dashboard(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
