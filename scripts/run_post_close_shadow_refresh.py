#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Refresh post-close theory scorecard and stock signal shadow ledger."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BACKTEST_DIR = PROJECT_ROOT / "reports" / "backtests"
DEFAULT_WINDOWS = [3, 5, 10]


def _run_command(command: list[str], *, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )


def _parse_key_value_stdout(stdout: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for line in (stdout or "").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _render_summary(payload: dict[str, Any]) -> str:
    scorecard = payload.get("scorecard") or {}
    ledger = payload.get("ledger") or {}
    ledger_counts = ledger.get("parsed_stdout") or {}
    intraday_labels = payload.get("intraday_replay_labels") or {}
    intraday_totals = intraday_labels.get("totals") or {}
    lines = [
        f"# {payload['run_date']} 收盘后理论回测刷新",
        "",
        "- 范围: 刷新理论评分表、Shadow 纸面账本，并回填盘中提醒 replay ledger 标签。",
        "- 实盘影响: 不下单、不推送、不改变持仓。",
        f"- 状态: {payload['status']}",
        "",
        "## 输出文件",
        "",
        f"- 理论评分表: `{scorecard.get('report_path', '')}`",
        f"- 理论评分 JSON: `{scorecard.get('json_path', '')}`",
        f"- Shadow 账本摘要: `{ledger_counts.get('summary_path', '')}`",
        f"- Shadow 账本文件: `{ledger_counts.get('ledger_path', '')}`",
        f"- 盘中提醒回放账本: `{intraday_labels.get('ledger_path', '')}`",
        "",
        "## Shadow 账本计数",
        "",
        f"- 本次新增: {ledger_counts.get('new_entry_count', '--')}",
        f"- 账本总数: {ledger_counts.get('total_entry_count', '--')}",
        f"- 未结算: {ledger_counts.get('open_entry_count', '--')}",
        f"- 已结算: {ledger_counts.get('settled_entry_count', '--')}",
        "",
        "## 盘中提醒 replay ledger 标签",
        "",
        f"- 标签回填状态: {intraday_labels.get('status', '--')}",
        f"- 账本行数: {intraday_totals.get('rows', '--')}",
        f"- 可回填行数: {intraday_totals.get('eligible', '--')}",
        f"- 本次已更新: {intraday_totals.get('updated', '--')}",
        f"- 缺少价格: {intraday_totals.get('missing_price', '--')}",
        f"- 缺少后续行情: {intraday_totals.get('missing_bars', '--')}",
    ]
    return "\n".join(lines) + "\n"


def run_post_close_shadow_refresh(
    *,
    output_dir: Path = DEFAULT_BACKTEST_DIR,
    windows: list[int] | None = None,
    min_samples: int = 50,
    rule_set: str = "core",
    max_rows_per_code: int = 1600,
    permutation_iterations: int = 300,
    timeout_seconds: int = 900,
    rebuild_ledger: bool = False,
    skip_ic: bool = False,
    backfill_intraday_replay_labels: bool = True,
) -> dict[str, Any]:
    """Run the post-close research refresh pipeline.

    The pipeline intentionally remains paper-only: it produces files for review
    and never sends notifications or writes trade instructions.
    """

    output_dir = Path(output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_date = datetime.now().date().isoformat()
    safe_windows = sorted({int(item) for item in (windows or DEFAULT_WINDOWS) if int(item) > 0})
    if not safe_windows:
        raise ValueError("windows must include at least one positive integer")

    scorecard_json = output_dir / f"{run_date}_theory_signal_scorecard.json"
    scorecard_command = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "run_theory_signal_scorecard.py"),
        "--output-dir",
        str(output_dir),
        "--windows",
        *[str(item) for item in safe_windows],
        "--min-samples",
        str(int(min_samples)),
        "--rule-set",
        rule_set,
        "--max-rows-per-code",
        str(int(max_rows_per_code)),
        "--permutation-iterations",
        str(int(permutation_iterations)),
    ]
    if skip_ic:
        scorecard_command.append("--skip-ic")

    scorecard_result = _run_command(scorecard_command, timeout_seconds=timeout_seconds)
    scorecard_stdout = _parse_key_value_stdout(scorecard_result.stdout)

    ledger_command = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "run_stock_signal_shadow_ledger.py"),
        "--scorecard-json",
        str(scorecard_json),
        "--windows",
        *[str(item) for item in safe_windows],
        "--rule-set",
        rule_set,
        "--max-rows-per-code",
        str(int(max_rows_per_code)),
    ]
    if rebuild_ledger:
        ledger_command.append("--rebuild")

    ledger_result = _run_command(ledger_command, timeout_seconds=timeout_seconds)
    ledger_stdout = _parse_key_value_stdout(ledger_result.stdout)

    intraday_replay_labels: dict[str, Any]
    if backfill_intraday_replay_labels:
        from src.services.stock_intraday_replay_labeler import StockIntradayReplayLabeler

        intraday_replay_labels = StockIntradayReplayLabeler().run(dry_run=False)
    else:
        intraday_replay_labels = {
            "status": "skipped",
            "dry_run": False,
            "ledger_path": str(PROJECT_ROOT / "reports" / "stock_intraday_replay_ledger.jsonl"),
            "totals": {},
        }

    payload: dict[str, Any] = {
        "status": "ok",
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "output_dir": str(output_dir),
        "windows": safe_windows,
        "scorecard": {
            "command": scorecard_command,
            "returncode": scorecard_result.returncode,
            "stdout": scorecard_result.stdout,
            "stderr": scorecard_result.stderr,
            "parsed_stdout": scorecard_stdout,
            "report_path": scorecard_stdout.get("generated", str(output_dir / f"{run_date}_理论信号准确率评分表.md")),
            "json_path": scorecard_stdout.get("json", str(scorecard_json)),
            "latest_path": scorecard_stdout.get("latest", str(output_dir / "latest_theory_signal_scorecard.md")),
        },
        "ledger": {
            "command": ledger_command,
            "returncode": ledger_result.returncode,
            "stdout": ledger_result.stdout,
            "stderr": ledger_result.stderr,
            "parsed_stdout": ledger_stdout,
        },
        "intraday_replay_labels": intraday_replay_labels,
    }

    summary_path = output_dir / f"{run_date}_post_close_shadow_refresh.md"
    latest_summary_path = output_dir / "latest_post_close_shadow_refresh.md"
    json_path = output_dir / f"{run_date}_post_close_shadow_refresh.json"
    summary = _render_summary(payload)
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary_path.write_text(summary, encoding="utf-8")
    payload["summary_path"] = str(summary_path)
    payload["latest_summary_path"] = str(latest_summary_path)
    payload["json_path"] = str(json_path)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_BACKTEST_DIR), help="Output directory.")
    parser.add_argument("--windows", nargs="+", type=int, default=DEFAULT_WINDOWS, help="Forward windows.")
    parser.add_argument("--min-samples", type=int, default=50, help="Minimum samples for scorecard promotion.")
    parser.add_argument("--rule-set", choices=["core", "full"], default="core", help="Signal rule universe.")
    parser.add_argument("--max-rows-per-code", type=int, default=1600, help="History rows per symbol.")
    parser.add_argument("--permutation-iterations", type=int, default=300, help="Permutation checks per signal.")
    parser.add_argument("--timeout-seconds", type=int, default=900, help="Timeout for each child refresh step.")
    parser.add_argument("--rebuild-ledger", action="store_true", help="Rebuild shadow ledger instead of preserving existing rows.")
    parser.add_argument("--skip-ic", action="store_true", help="Skip IC theory section in scorecard.")
    parser.add_argument(
        "--skip-intraday-replay-labels",
        action="store_true",
        help="Skip T+1/T+3/T+5 label backfill for stock intraday replay ledger.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON payload.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    payload = run_post_close_shadow_refresh(
        output_dir=Path(args.output_dir),
        windows=args.windows,
        min_samples=int(args.min_samples),
        rule_set=args.rule_set,
        max_rows_per_code=int(args.max_rows_per_code),
        permutation_iterations=int(args.permutation_iterations),
        timeout_seconds=max(1, int(args.timeout_seconds)),
        rebuild_ledger=bool(args.rebuild_ledger),
        skip_ic=bool(args.skip_ic),
        backfill_intraday_replay_labels=not bool(args.skip_intraday_replay_labels),
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"status: {payload['status']}")
        print(f"summary_path: {payload['summary_path']}")
        print(f"latest_summary_path: {payload['latest_summary_path']}")
        print(f"json_path: {payload['json_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
