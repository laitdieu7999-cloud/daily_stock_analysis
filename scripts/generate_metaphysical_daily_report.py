#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a standalone daily metaphysical governance report."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.metaphysical import (  # noqa: E402
    build_daily_governance_summary,
    render_daily_governance_summary,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a standalone daily metaphysical governance report."
    )
    parser.add_argument(
        "--symbol",
        default="510500.SS",
        help="Target symbol used to read the cached model and governance artifacts.",
    )
    parser.add_argument("--start", default="2016-01-01", help="Backtest start date.")
    parser.add_argument("--end", default="2026-04-20", help="Backtest end date.")
    parser.add_argument(
        "--probability-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "metaphysical_probabilities"),
        help="Probability cache directory.",
    )
    parser.add_argument(
        "--governance-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_governance_runs.jsonl"),
        help="Governance ledger path.",
    )
    parser.add_argument(
        "--lifecycle-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_lifecycle_runs.jsonl"),
        help="Lifecycle ledger path.",
    )
    parser.add_argument(
        "--stage-performance-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_stage_performance_runs.jsonl"),
        help="Stage-performance ledger path.",
    )
    parser.add_argument(
        "--switch-proposal-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_version_switch_proposals.jsonl"),
        help="Switch-proposal ledger path.",
    )
    parser.add_argument(
        "--tactical-report-file",
        default=None,
        help="Optional UTF-8 tactical report text file.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of markdown.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    report_text = None
    report_sync_status = {}
    if args.tactical_report_file:
        report_text = Path(args.tactical_report_file).read_text(encoding="utf-8").strip()
    sync_status_path = PROJECT_ROOT / "reports" / "metaphysical_latest_report_sync.json"
    if sync_status_path.exists():
        report_sync_status = json.loads(sync_status_path.read_text(encoding="utf-8"))
    summary = build_daily_governance_summary(
        cache_dir=args.probability_cache_dir,
        symbol=args.symbol,
        start=args.start,
        end=args.end,
        governance_path=args.governance_log_path,
        lifecycle_path=args.lifecycle_log_path,
        stage_performance_path=args.stage_performance_log_path,
        switch_proposal_path=args.switch_proposal_log_path,
        report_text=report_text,
        report_sync_status=report_sync_status,
    )
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(render_daily_governance_summary(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
