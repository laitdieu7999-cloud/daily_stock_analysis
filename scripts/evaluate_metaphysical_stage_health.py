#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Evaluate stage-performance guardrails for candidate/shadow/production."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.metaphysical import (  # noqa: E402
    evaluate_stage_guardrail,
    latest_stage_performance_run,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate recent stage-performance health for metaphysical model governance."
    )
    parser.add_argument(
        "--stage-performance-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_stage_performance_runs.jsonl"),
        help="Path to the stage-performance JSONL ledger.",
    )
    parser.add_argument(
        "--stage",
        default="production",
        choices=["candidate", "shadow", "production"],
        help="Stage to evaluate.",
    )
    parser.add_argument("--recent-n", type=int, default=3, help="Recent window size.")
    parser.add_argument("--min-runs", type=int, default=2, help="Minimum recent records before health is trusted.")
    parser.add_argument(
        "--min-strategy-sharpe",
        type=float,
        default=0.20,
        help="Minimum mean strategy sharpe floor.",
    )
    parser.add_argument(
        "--min-excess-return",
        type=float,
        default=-0.05,
        help="Minimum mean excess-return floor.",
    )
    parser.add_argument(
        "--max-drawdown-gap",
        type=float,
        default=0.05,
        help="Maximum allowed mean drawdown gap. Lower is better.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    latest = latest_stage_performance_run(args.stage_performance_log_path, stage=args.stage)
    guardrail = evaluate_stage_guardrail(
        args.stage_performance_log_path,
        stage=args.stage,
        recent_n=args.recent_n,
        min_runs=args.min_runs,
        min_strategy_sharpe=args.min_strategy_sharpe,
        min_excess_return=args.min_excess_return,
        max_drawdown_gap=args.max_drawdown_gap,
    )
    payload = {
        "latest_stage_run": latest,
        "guardrail": guardrail,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"stage: {args.stage}")
        print(f"guardrail_action: {guardrail['action']}")
        print(f"healthy: {guardrail['healthy']}")
        print(f"run_count: {guardrail['run_count']}")
        print(f"mean_strategy_sharpe: {guardrail['mean_strategy_sharpe']}")
        print(f"mean_excess_return: {guardrail['mean_excess_return']}")
        print(f"mean_drawdown_gap: {guardrail['mean_drawdown_gap']}")
        print("reasons: " + "；".join(guardrail["reasons"]))
        if latest:
            print(f"latest_run_end: {latest.get('end')}")
            print(f"latest_strategy_sharpe: {latest.get('strategy_sharpe')}")
            print(f"latest_excess_return: {latest.get('excess_return')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
