#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Evaluate matured sample readiness and candidate promotion status."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.metaphysical import (  # noqa: E402
    build_version_switch_change_request,
    build_version_switch_confirmation_draft,
    build_version_switch_execution_plan,
    build_version_switch_proposal,
    evaluate_governance_action,
    evaluate_candidate_promotion_readiness,
    evaluate_governance_stage_flow,
    evaluate_release_lifecycle,
    record_governance_run,
    record_lifecycle_run,
    record_version_switch_proposal,
    select_matured_learning_samples,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate matured metaphysical samples and promotion readiness."
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
    parser.add_argument("--recent-n", type=int, default=3, help="Recent training window size.")
    parser.add_argument("--min-runs", type=int, default=2, help="Minimum training runs before promotion.")
    parser.add_argument("--auc-floor", type=float, default=0.60, help="Minimum mean AUC threshold.")
    parser.add_argument("--ap-floor", type=float, default=0.18, help="Minimum mean AP threshold.")
    parser.add_argument(
        "--current-stage",
        default="candidate",
        choices=["research", "candidate", "shadow", "production"],
        help="Current governance stage to evaluate from.",
    )
    parser.add_argument(
        "--min-matured-samples-for-candidate",
        type=int,
        default=2,
        help="Minimum matured_10d samples before recommending candidate promotion.",
    )
    parser.add_argument(
        "--min-matured-samples-for-shadow",
        type=int,
        default=5,
        help="Minimum matured_10d samples before recommending shadow promotion.",
    )
    parser.add_argument(
        "--min-matured-samples-for-production",
        type=int,
        default=12,
        help="Minimum matured_10d samples before recommending production promotion.",
    )
    parser.add_argument("--shadow-auc-floor", type=float, default=0.62, help="Shadow mean AUC threshold.")
    parser.add_argument("--shadow-ap-floor", type=float, default=0.20, help="Shadow mean AP threshold.")
    parser.add_argument(
        "--production-auc-floor",
        type=float,
        default=0.64,
        help="Production mean AUC threshold.",
    )
    parser.add_argument(
        "--production-ap-floor",
        type=float,
        default=0.22,
        help="Production mean AP threshold.",
    )
    parser.add_argument(
        "--production-min-runs",
        type=int,
        default=3,
        help="Minimum recent runs before recommending production.",
    )
    parser.add_argument(
        "--record-governance",
        action="store_true",
        help="Append the evaluated governance decision into a JSONL ledger.",
    )
    parser.add_argument(
        "--governance-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_governance_runs.jsonl"),
        help="Path to the governance decision JSONL ledger.",
    )
    parser.add_argument(
        "--record-lifecycle",
        action="store_true",
        help="Append the evaluated lifecycle decision into a JSONL ledger.",
    )
    parser.add_argument(
        "--lifecycle-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_lifecycle_runs.jsonl"),
        help="Path to the lifecycle decision JSONL ledger.",
    )
    parser.add_argument(
        "--record-switch-proposal",
        action="store_true",
        help="Append a draft version-switch proposal into a JSONL ledger.",
    )
    parser.add_argument(
        "--switch-proposal-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_version_switch_proposals.jsonl"),
        help="Path to the version-switch proposal JSONL ledger.",
    )
    parser.add_argument(
        "--current-profile",
        default="next_production_candidate",
        help="Current live/review profile label used when generating switch proposals.",
    )
    parser.add_argument(
        "--stage-performance-log-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_stage_performance_runs.jsonl"),
        help="Path to the stage-performance JSONL ledger.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    matured = select_matured_learning_samples(args.snapshot_path)
    readiness = evaluate_candidate_promotion_readiness(
        args.training_log_path,
        recent_n=args.recent_n,
        min_runs=args.min_runs,
        auc_floor=args.auc_floor,
        ap_floor=args.ap_floor,
    )
    stage_flow = evaluate_governance_stage_flow(
        args.snapshot_path,
        args.training_log_path,
        current_stage=args.current_stage,
        recent_n=args.recent_n,
        min_runs=args.min_runs,
        auc_floor=args.auc_floor,
        ap_floor=args.ap_floor,
        min_matured_samples_for_candidate=args.min_matured_samples_for_candidate,
        min_matured_samples_for_shadow=args.min_matured_samples_for_shadow,
        min_matured_samples_for_production=args.min_matured_samples_for_production,
        shadow_auc_floor=args.shadow_auc_floor,
        shadow_ap_floor=args.shadow_ap_floor,
        production_auc_floor=args.production_auc_floor,
        production_ap_floor=args.production_ap_floor,
        production_min_runs=args.production_min_runs,
    )
    governance = evaluate_governance_action(
        args.snapshot_path,
        args.training_log_path,
        current_stage=args.current_stage,
        recent_n=args.recent_n,
        min_runs=args.min_runs,
        auc_floor=args.auc_floor,
        ap_floor=args.ap_floor,
        min_matured_samples_for_candidate=args.min_matured_samples_for_candidate,
        min_matured_samples_for_shadow=args.min_matured_samples_for_shadow,
        min_matured_samples_for_production=args.min_matured_samples_for_production,
        shadow_auc_floor=args.shadow_auc_floor,
        shadow_ap_floor=args.shadow_ap_floor,
        production_auc_floor=args.production_auc_floor,
        production_ap_floor=args.production_ap_floor,
        production_min_runs=args.production_min_runs,
    )
    lifecycle = evaluate_release_lifecycle(
        args.snapshot_path,
        args.training_log_path,
        args.stage_performance_log_path,
        current_stage=args.current_stage,
        recent_n=args.recent_n,
        min_runs=args.min_runs,
        auc_floor=args.auc_floor,
        ap_floor=args.ap_floor,
        min_matured_samples_for_candidate=args.min_matured_samples_for_candidate,
        min_matured_samples_for_shadow=args.min_matured_samples_for_shadow,
        min_matured_samples_for_production=args.min_matured_samples_for_production,
        shadow_auc_floor=args.shadow_auc_floor,
        shadow_ap_floor=args.shadow_ap_floor,
        production_auc_floor=args.production_auc_floor,
        production_ap_floor=args.production_ap_floor,
        production_min_runs=args.production_min_runs,
    )
    switch_proposal = build_version_switch_proposal(
        lifecycle=lifecycle,
        current_profile=args.current_profile,
    )
    switch_execution_plan = build_version_switch_execution_plan(
        proposal=switch_proposal,
    )
    switch_confirmation_draft = build_version_switch_confirmation_draft(
        proposal=switch_proposal,
        execution_plan=switch_execution_plan,
    )
    switch_change_request = build_version_switch_change_request(
        proposal=switch_proposal,
        execution_plan=switch_execution_plan,
        confirmation_draft=switch_confirmation_draft,
    )
    payload = {
        "matured_sample_count": len(matured),
        "promotion_readiness": readiness,
        "stage_flow": stage_flow,
        "governance": governance,
        "lifecycle": lifecycle,
        "switch_proposal": switch_proposal,
        "switch_execution_plan": switch_execution_plan,
        "switch_confirmation_draft": switch_confirmation_draft,
        "switch_change_request": switch_change_request,
    }
    if args.record_governance:
        record_governance_run(
            args.governance_log_path,
            governance=stage_flow,
        )
        payload["governance_log_path"] = args.governance_log_path
    if args.record_lifecycle:
        record_lifecycle_run(
            args.lifecycle_log_path,
            lifecycle=lifecycle,
        )
        payload["lifecycle_log_path"] = args.lifecycle_log_path
    if args.record_switch_proposal:
        record_version_switch_proposal(
            args.switch_proposal_log_path,
            lifecycle=lifecycle,
            current_profile=args.current_profile,
        )
        payload["switch_proposal_log_path"] = args.switch_proposal_log_path
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"matured_sample_count: {len(matured)}")
        print(f"promotion_ready: {readiness['promotion_ready']}")
        print(f"latest_auc: {readiness['latest_auc']}")
        print(f"latest_ap: {readiness['latest_ap']}")
        print("reasons: " + "；".join(readiness["reasons"]))
        print(f"current_stage: {stage_flow['current_stage']}")
        print(f"target_stage: {stage_flow['target_stage']}")
        print(f"stage_flow_action: {stage_flow['action']}")
        print(f"stage_flow_reason: {stage_flow['reason']}")
        print(f"governance_action: {governance['action']}")
        print(f"governance_reason: {governance['reason']}")
        print(f"lifecycle_action: {lifecycle['lifecycle_action']}")
        print(f"lifecycle_target_stage: {lifecycle['lifecycle_target_stage']}")
        print(f"lifecycle_reason: {lifecycle['reason']}")
        print(f"switch_proposal_status: {switch_proposal['proposal_status']}")
        print(f"switch_proposal_action: {switch_proposal['proposal_action']}")
        print(f"switch_proposed_profile: {switch_proposal['proposed_profile']}")
        print("switch_review_checks: " + "；".join(switch_execution_plan["review_checks"]))
        print(f"switch_confirmation_state: {switch_confirmation_draft['confirmation_state']}")
        print(f"switch_confirmation_summary: {switch_confirmation_draft['summary']}")
        print(f"switch_change_request_state: {switch_change_request['request_state']}")
        print(f"switch_change_request_title: {switch_change_request['title']}")
        if args.record_governance:
            print(f"governance_log_path: {args.governance_log_path}")
        if args.record_lifecycle:
            print(f"lifecycle_log_path: {args.lifecycle_log_path}")
        if args.record_switch_proposal:
            print(f"switch_proposal_log_path: {args.switch_proposal_log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
