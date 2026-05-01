#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a one-page weekly governance summary for the metaphysical model."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.metaphysical import (  # noqa: E402
    build_weekly_governance_summary,
    render_weekly_governance_summary,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a compact weekly governance summary for the metaphysical model."
    )
    parser.add_argument(
        "--snapshot-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_learning_samples.jsonl"),
        help="Path to the daily learning snapshot JSONL ledger.",
    )
    parser.add_argument(
        "--training-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_training_runs.jsonl"),
        help="Path to the training-run JSONL ledger.",
    )
    parser.add_argument(
        "--governance-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_governance_runs.jsonl"),
        help="Path to the governance-run JSONL ledger.",
    )
    parser.add_argument(
        "--lifecycle-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_lifecycle_runs.jsonl"),
        help="Path to the lifecycle-run JSONL ledger.",
    )
    parser.add_argument(
        "--stage-performance-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_stage_performance_runs.jsonl"),
        help="Path to the stage-performance JSONL ledger.",
    )
    parser.add_argument(
        "--switch-proposal-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_version_switch_proposals.jsonl"),
        help="Path to the version-switch proposal JSONL ledger.",
    )
    parser.add_argument(
        "--current-stage",
        default="candidate",
        choices=["research", "candidate", "shadow", "production"],
        help="Fallback stage when no lifecycle ledger exists.",
    )
    parser.add_argument("--recent-n", type=int, default=3, help="Recent window size.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of markdown.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    summary = build_weekly_governance_summary(
        snapshot_path=args.snapshot_path,
        training_path=args.training_log_path,
        governance_path=args.governance_log_path,
        lifecycle_path=args.lifecycle_log_path,
        stage_performance_path=args.stage_performance_log_path,
        switch_proposal_path=args.switch_proposal_log_path,
        current_stage=args.current_stage,
        recent_n=args.recent_n,
    )
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(render_weekly_governance_summary(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
