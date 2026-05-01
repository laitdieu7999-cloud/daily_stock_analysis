#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build a shadow-monitoring ledger for the M1-M2 front-end collapse signal."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.ic_term_structure_shadow_monitor import (  # noqa: E402
    refresh_term_structure_shadow_monitoring_outputs,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record and summarize shadow-monitoring events for M1-M2 front-end collapse."
    )
    parser.add_argument(
        "--intraday-archive-dir",
        default=str(PROJECT_ROOT / "reports" / "intraday_archive"),
        help="Directory that stores intraday market snapshots.",
    )
    parser.add_argument(
        "--ledger-path",
        default=str(PROJECT_ROOT / "reports" / "ic_m1_m2_shadow_monitoring_events.jsonl"),
        help="Path to the derived shadow-monitoring event ledger.",
    )
    parser.add_argument(
        "--summary-path",
        default=None,
        help="Path to the dated markdown summary. Defaults to the latest snapshot date.",
    )
    parser.add_argument(
        "--latest-summary-path",
        default=str(PROJECT_ROOT / "reports" / "backtests" / "latest_ic_m1_m2_shadow_monitoring.md"),
        help="Path to the rolling latest summary markdown.",
    )
    parser.add_argument(
        "--data-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "ic_basis_history"),
        help="Directory used to cache CSI500 spot and CFFEX IC panel history for shadow scorecard enrichment.",
    )
    parser.add_argument(
        "--refresh-data-cache",
        action="store_true",
        help="Ignore local IC history cache and rebuild it from data sources when enriching scorecard fields.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON payload.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    payload = refresh_term_structure_shadow_monitoring_outputs(
        intraday_archive_dir=args.intraday_archive_dir,
        ledger_path=args.ledger_path,
        latest_summary_path=args.latest_summary_path,
        summary_path=args.summary_path,
        data_cache_dir=args.data_cache_dir,
        refresh_data_cache=args.refresh_data_cache,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"candidate_count: {payload['candidate_count']}")
        print(f"paper_trade_count: {payload['paper_trade_count']}")
        print(f"ledger_path: {payload['ledger_path']}")
        print(f"summary_path: {payload['summary_path']}")
        print(f"latest_summary_path: {payload['latest_summary_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
