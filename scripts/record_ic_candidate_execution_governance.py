#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Record and render governance artifacts for the IC second-confirmation candidate rule."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
import re

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.ic_candidate_governance import (  # noqa: E402
    append_ic_candidate_execution_record,
    build_ic_candidate_execution_record,
    render_ic_candidate_execution_summary,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record governance evidence for the IC second-confirmation flat-2d candidate."
    )
    parser.add_argument(
        "--execution-report-path",
        default=str(PROJECT_ROOT / "reports" / "backtests" / "2026-04-26_IC第二确认执行细节验证报告.md"),
        help="Path to the execution-detail validation report.",
    )
    parser.add_argument(
        "--reentry-report-path",
        default=str(PROJECT_ROOT / "reports" / "backtests" / "2026-04-26_IC第二确认冷静期与回补搜索报告.md"),
        help="Path to the cooldown-and-reentry validation report.",
    )
    parser.add_argument(
        "--strategy-path",
        default=str(PROJECT_ROOT / "strategies" / "ic_basis_roll_framework.yaml"),
        help="Path to the IC strategy definition.",
    )
    parser.add_argument(
        "--docs-path",
        default=str(PROJECT_ROOT / "docs" / "CUSTOM_EXTENSIONS.md"),
        help="Path to the custom extension documentation.",
    )
    parser.add_argument(
        "--ledger-path",
        default=str(PROJECT_ROOT / "reports" / "ic_candidate_execution_governance_runs.jsonl"),
        help="Path to the JSONL governance ledger.",
    )
    parser.add_argument(
        "--summary-path",
        default=None,
        help="Path to the dated governance summary markdown.",
    )
    parser.add_argument(
        "--latest-summary-path",
        default=str(PROJECT_ROOT / "reports" / "backtests" / "latest_ic_candidate_execution_governance.md"),
        help="Path to the rolling latest governance summary markdown.",
    )
    parser.add_argument(
        "--current-stage",
        default="candidate",
        choices=["research", "candidate", "shadow", "production"],
        help="Current governance stage.",
    )
    parser.add_argument(
        "--review-status",
        default="pending_review",
        help="Review status label.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON to stdout.")
    return parser


def _infer_report_date(execution_report_path: str) -> str:
    match = re.search(r"(\d{4}-\d{2}-\d{2})", Path(execution_report_path).name)
    if match:
        return match.group(1)
    return "latest"


def main() -> int:
    args = _build_parser().parse_args()
    report_date = _infer_report_date(args.execution_report_path)
    summary_path_value = args.summary_path or str(
        PROJECT_ROOT / "reports" / "backtests" / f"{report_date}_IC候选执行治理摘要.md"
    )
    record = build_ic_candidate_execution_record(
        execution_report_path=args.execution_report_path,
        reentry_report_path=args.reentry_report_path,
        strategy_path=args.strategy_path,
        docs_path=args.docs_path,
        current_stage=args.current_stage,
        review_status=args.review_status,
    )
    append_ic_candidate_execution_record(args.ledger_path, record)
    summary = render_ic_candidate_execution_summary(record)

    summary_path = Path(summary_path_value)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(summary + "\n", encoding="utf-8")

    latest_summary_path = Path(args.latest_summary_path)
    latest_summary_path.parent.mkdir(parents=True, exist_ok=True)
    latest_summary_path.write_text(summary + "\n", encoding="utf-8")

    payload = {
        "record": record,
        "ledger_path": args.ledger_path,
        "summary_path": summary_path_value,
        "latest_summary_path": args.latest_summary_path,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"candidate: {record['candidate_display_name']}")
        print(f"current_stage: {record['current_stage']}")
        print(f"review_status: {record['review_status']}")
        print(f"ledger_path: {args.ledger_path}")
        print(f"summary_path: {summary_path_value}")
        print(f"latest_summary_path: {args.latest_summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
