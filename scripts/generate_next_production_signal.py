#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate the latest next-production signal from cached probabilities.

This lightweight entrypoint avoids rerunning the full walk-forward backtest.
It reads the cached probability frame, emits the raw model signal, and
optionally applies the daily tactical-report overlay from the Google Doc
archive text.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.metaphysical import (  # noqa: E402
    NEXT_PRODUCTION_MODEL_DEFAULTS,
    assess_tactical_report_freshness,
    build_tactical_report_optimization_notes,
    record_daily_learning_snapshot,
    latest_next_production_signal,
    latest_next_production_signal_with_report_overlay,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate the latest next-production signal from cached probabilities."
    )
    parser.add_argument(
        "--symbol",
        default="510500.SS",
        help="Target symbol used to locate the cached signal artifacts. Default: 510500.SS",
    )
    parser.add_argument("--start", default="2016-01-01", help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end", default="2026-04-20", help="End date in YYYY-MM-DD format.")
    parser.add_argument(
        "--min-train-days",
        type=int,
        default=NEXT_PRODUCTION_MODEL_DEFAULTS["min_train_days"],
        help="Training window used by the cached probability frame.",
    )
    parser.add_argument(
        "--retrain-every",
        type=int,
        default=NEXT_PRODUCTION_MODEL_DEFAULTS["retrain_every"],
        help="Retrain cadence used by the cached probability frame.",
    )
    parser.add_argument(
        "--probability-cache-dir",
        default=str(PROJECT_ROOT / ".cache" / "metaphysical_probabilities"),
        help="Directory used to cache walk-forward tail-risk probabilities.",
    )
    parser.add_argument(
        "--tactical-report-file",
        default=None,
        help="Optional UTF-8 text file containing the daily Google Doc archive text.",
    )
    parser.add_argument(
        "--expected-report-date",
        default=None,
        help="Expected report date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of a human-readable summary.",
    )
    parser.add_argument(
        "--record-snapshot",
        action="store_true",
        help="Append or update the daily learning snapshot ledger when a tactical report is provided.",
    )
    parser.add_argument(
        "--snapshot-path",
        default=str(PROJECT_ROOT / "reports" / "metaphysical_learning_samples.jsonl"),
        help="Path to the daily learning snapshot JSONL ledger.",
    )
    return parser


def _sanitize_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def _probability_cache_path(
    cache_dir: str | Path,
    *,
    symbol: str,
    start: str,
    end: str,
    min_train_days: int,
    retrain_every: int,
) -> Path:
    base = Path(cache_dir)
    filename = (
        f"{_sanitize_token(symbol)}_{_sanitize_token(start)}_{_sanitize_token(end)}"
        f"_min{min_train_days}_retrain{retrain_every}.pkl"
    )
    return base / filename


def _load_tactical_report_text(path: str | None) -> str | None:
    if not path:
        return None
    return Path(path).read_text(encoding="utf-8").strip()


def _load_probability_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"cached probability frame not found: {path}. "
            "Run backtest_next_production_metaphysical_model.py first to build it."
        )
    return pd.read_pickle(path)


def _human_summary(raw_signal: dict[str, object], final_signal: dict[str, object], cache_path: Path) -> str:
    lines = [
        "Next Production Daily Signal",
        f"cache: {cache_path}",
        f"raw: {raw_signal['position_regime']} | action={raw_signal['action']} | prob={raw_signal['tail_risk_probability']:.4f}",
    ]
    if bool(final_signal.get("overlay_active")):
        lines.extend(
            [
                f"final: {final_signal['position_regime']} | action={final_signal['action']}",
                f"report_risk_score: {final_signal['report_risk_score']}",
                f"alignment: {final_signal['report_alignment']}",
                f"reason: {final_signal['overlay_reason']}",
            ]
        )
    elif bool(final_signal.get("report_overlay_skipped")):
        lines.extend(
            [
                "final: same_as_raw",
                f"report_freshness: stale | reason={final_signal.get('report_overlay_skip_reason')}",
            ]
        )
    else:
        lines.append("final: same_as_raw")
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    cache_path = _probability_cache_path(
        args.probability_cache_dir,
        symbol=args.symbol,
        start=args.start,
        end=args.end,
        min_train_days=args.min_train_days,
        retrain_every=args.retrain_every,
    )
    frame = _load_probability_frame(cache_path)
    raw_signal = latest_next_production_signal(frame)
    report_text = _load_tactical_report_text(args.tactical_report_file)
    expected_report_date = args.expected_report_date or date.today().isoformat()
    report_freshness = (
        assess_tactical_report_freshness(report_text, expected_date_iso=expected_report_date)
        if report_text
        else {}
    )
    final_signal = (
        latest_next_production_signal_with_report_overlay(
            frame,
            report_text=report_text,
            expected_report_date=expected_report_date,
        )
        if report_text
        else dict(raw_signal)
    )
    duplicate_check = {}
    if report_text:
        duplicate_check = (
            build_tactical_report_optimization_notes(
                report_text,
                archive_dir=PROJECT_ROOT / "reports" / "gemini_daily_archive",
            ).get("duplicate_check")
            or {}
        )

    payload = {
        "cache_path": str(cache_path),
        "raw_signal": raw_signal,
        "final_signal": final_signal,
        "report_freshness": report_freshness,
        "duplicate_check": duplicate_check,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        summary = _human_summary(raw_signal, final_signal, cache_path)
        if duplicate_check:
            summary += (
                "\n"
                f"duplicate_check: has_duplicates={duplicate_check.get('has_duplicates')} "
                f"duplicate_line_count={duplicate_check.get('duplicate_line_count')}"
            )
        print(summary)
    if args.record_snapshot and report_text and bool(final_signal.get("report_is_fresh", True)):
        record_daily_learning_snapshot(
            args.snapshot_path,
            report_text=report_text,
            final_signal=final_signal,
            symbol=args.symbol,
            report_path=args.tactical_report_file,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
