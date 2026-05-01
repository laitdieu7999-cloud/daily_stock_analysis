#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Record and settle paper trades for promoted stock/ETF shadow signals."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import run_theory_signal_scorecard as scorecard  # noqa: E402


DEFAULT_WINDOWS = [3, 5, 10]


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows)
    path.write_text(content, encoding="utf-8")


def _load_scorecard_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"scorecard json not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _promoted_signal_keys(payload: dict[str, Any]) -> set[tuple[str, str, str]]:
    keys: set[tuple[str, str, str]] = set()
    for row in payload.get("graduation_scorecard", {}).get("rows", []):
        if row.get("final_decision") != "可进Shadow":
            continue
        if row.get("direction_type") != "offensive":
            continue
        keys.add((str(row.get("module")), str(row.get("direction_type")), str(row.get("rule"))))
    return keys


def _entry_key(row: dict[str, Any]) -> str:
    return "|".join([str(row.get("signal_date")), str(row.get("code")), str(row.get("module")), str(row.get("rule"))])


def _find_entry_position(group: pd.DataFrame, signal_date: str) -> int | None:
    dates = pd.to_datetime(group["date"]).dt.date.astype(str).tolist()
    try:
        return dates.index(signal_date)
    except ValueError:
        return None


def _settlement_for_window(group: pd.DataFrame, pos: int, entry_price: float, window: int) -> dict[str, Any] | None:
    if pos + window >= len(group):
        return None
    future = group.iloc[pos + 1 : pos + window + 1]
    if len(future) < window:
        return None
    end_row = future.iloc[-1]
    end_price = scorecard._num(end_row, "close")
    if entry_price <= 0 or end_price <= 0:
        return None
    max_high = float(pd.to_numeric(future["high"], errors="coerce").max())
    min_low = float(pd.to_numeric(future["low"], errors="coerce").min())
    return {
        "window": int(window),
        "settle_date": pd.to_datetime(end_row["date"]).date().isoformat(),
        "exit_price": float(end_price),
        "return_pct": float((end_price - entry_price) / entry_price * 100.0),
        "mfe_pct": float((max_high - entry_price) / entry_price * 100.0) if max_high > 0 else None,
        "mae_pct": float((min_low - entry_price) / entry_price * 100.0) if min_low > 0 else None,
    }


def settle_entries(entries: list[dict[str, Any]], indicator_df: pd.DataFrame, windows: list[int]) -> list[dict[str, Any]]:
    if not entries:
        return []
    by_code = {str(code): group.sort_values("date").reset_index(drop=True) for code, group in indicator_df.groupby("code")}
    settled: list[dict[str, Any]] = []
    for entry in entries:
        current = dict(entry)
        group = by_code.get(str(current.get("code")))
        if group is None or group.empty:
            settled.append(current)
            continue
        pos = _find_entry_position(group, str(current.get("signal_date")))
        if pos is None:
            settled.append(current)
            continue
        entry_price = float(current.get("entry_price") or 0.0)
        settlements = dict(current.get("settlements") or {})
        for window in windows:
            key = f"T+{int(window)}"
            if key in settlements:
                continue
            result = _settlement_for_window(group, pos, entry_price, int(window))
            if result is not None:
                settlements[key] = result
        current["settlements"] = settlements
        current["status"] = "settled" if all(f"T+{int(window)}" in settlements for window in windows) else "open"
        current["updated_at"] = datetime.now().isoformat(timespec="seconds")
        settled.append(current)
    return settled


def build_shadow_entries(
    indicator_df: pd.DataFrame,
    promoted_keys: set[tuple[str, str, str]],
    *,
    windows: list[int],
    as_of_date: str | None = None,
    rule_set: str = "core",
    max_signal_lag_days: int = 3,
) -> list[dict[str, Any]]:
    if not promoted_keys:
        return []
    rules = {
        (rule.module, rule.direction_type, rule.name): rule
        for rule in scorecard._build_rules(rule_set)
        if (rule.module, rule.direction_type, rule.name) in promoted_keys
    }
    if not rules:
        return []

    entries: list[dict[str, Any]] = []
    generated_at = datetime.now().isoformat(timespec="seconds")
    global_latest_date = pd.to_datetime(indicator_df["date"]).dt.date.max()
    min_allowed_date = global_latest_date - timedelta(days=max(0, int(max_signal_lag_days)))
    for code, group in indicator_df.groupby("code"):
        group = group.sort_values("date").reset_index(drop=True)
        if group.empty:
            continue
        if as_of_date:
            matched = group[pd.to_datetime(group["date"]).dt.date.astype(str) == as_of_date]
            if matched.empty:
                continue
            row = matched.iloc[-1]
        else:
            row = group.iloc[-1]
        row_date = pd.to_datetime(row["date"]).date()
        if not as_of_date and row_date < min_allowed_date:
            continue
        entry_price = scorecard._num(row, "close")
        if entry_price <= 0:
            continue
        signal_date = row_date.isoformat()
        for key, rule in rules.items():
            signal = rule.signal_fn(row)
            if signal != "bullish":
                continue
            entries.append(
                {
                    "entry_id": "|".join([signal_date, str(code), rule.module, rule.name]),
                    "status": "open",
                    "signal_date": signal_date,
                    "code": str(code),
                    "module": rule.module,
                    "direction_type": rule.direction_type,
                    "rule": rule.name,
                    "description": rule.description,
                    "entry_price": float(entry_price),
                    "market_regime": str(row.get("market_regime") or "未知"),
                    "windows": [int(window) for window in windows],
                    "settlements": {},
                    "created_at": generated_at,
                    "updated_at": generated_at,
                }
            )
    return entries


def merge_entries(existing: list[dict[str, Any]], new_entries: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, set[str]]:
    by_key = {_entry_key(row): dict(row) for row in existing}
    added = 0
    added_keys: set[str] = set()
    for row in new_entries:
        key = _entry_key(row)
        if key in by_key:
            continue
        by_key[key] = row
        added += 1
        added_keys.add(key)
    merged = sorted(by_key.values(), key=lambda item: (str(item.get("signal_date")), str(item.get("code")), str(item.get("rule"))))
    return merged, added, added_keys


def render_summary(payload: dict[str, Any]) -> str:
    lines = [
        f"# {payload['run_date']} 股票信号 Shadow 账本",
        "",
        "- 范围: 只记录纸面交易，不触发实盘、不推送。",
        f"- 晋升信号数: {payload['promoted_signal_count']}",
        f"- 本次新增记录: {payload['new_entry_count']}",
        f"- 账本总记录: {payload['total_entry_count']}",
        f"- 未结算记录: {payload['open_entry_count']}",
        f"- 已完全结算记录: {payload['settled_entry_count']}",
        f"- 信号新鲜度: 跳过落后全局最新日期超过 {payload.get('max_signal_lag_days')} 天的标的。",
        f"- 账本路径: `{payload['ledger_path']}`",
        "",
        "## 新增纸面交易",
        "",
        "| 日期 | 标的 | 信号 | 入场价 | 环境 |",
        "| --- | --- | --- | ---: | --- |",
    ]
    if not payload["new_entries"]:
        lines.append("| - | - | 本次无新增 | - | - |")
    else:
        for row in payload["new_entries"]:
            lines.append(
                f"| {row['signal_date']} | {row['code']} | {row['rule']} | {row['entry_price']:.4f} | {row.get('market_regime', '未知')} |"
            )

    lines.extend(
        [
            "",
            "## 最近结算结果",
            "",
            "| 日期 | 标的 | 信号 | T+3 | T+5 | T+10 | 状态 |",
            "| --- | --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    recent = payload["recent_entries"][-20:]
    if not recent:
        lines.append("| - | - | 暂无记录 | - | - | - | - |")
    else:
        for row in recent:
            settlements = row.get("settlements") or {}
            values = []
            for window in DEFAULT_WINDOWS:
                item = settlements.get(f"T+{window}")
                values.append("--" if not item else f"{float(item['return_pct']):.2f}%")
            lines.append(
                f"| {row['signal_date']} | {row['code']} | {row['rule']} | {values[0]} | {values[1]} | {values[2]} | {row.get('status')} |"
            )
    return "\n".join(lines) + "\n"


def refresh_shadow_ledger(
    *,
    scorecard_json_path: Path,
    ledger_path: Path,
    summary_path: Path,
    latest_summary_path: Path,
    db_path: Path,
    focus_codes: list[str],
    windows: list[int],
    as_of_date: str | None = None,
    rule_set: str = "core",
    use_qlib: bool = True,
    use_etf_cache: bool = True,
    max_rows_per_code: int | None = 1600,
    max_signal_lag_days: int = 3,
    rebuild: bool = False,
) -> dict[str, Any]:
    scorecard_payload = _load_scorecard_payload(scorecard_json_path)
    promoted_keys = _promoted_signal_keys(scorecard_payload)

    daily, daily_meta = scorecard.load_theory_daily_frame(
        db_path=db_path,
        focus_codes=focus_codes,
        use_qlib=use_qlib,
        use_etf_cache=use_etf_cache,
        max_rows_per_code=max_rows_per_code,
    )
    indicator_df = scorecard._prepare_indicator_frame(daily, "510300")

    existing = [] if rebuild else _load_jsonl(ledger_path)
    settled_existing = settle_entries(existing, indicator_df, windows)
    new_entries = build_shadow_entries(
        indicator_df,
        promoted_keys,
        windows=windows,
        as_of_date=as_of_date,
        rule_set=rule_set,
        max_signal_lag_days=max_signal_lag_days,
    )
    merged, added, added_keys = merge_entries(settled_existing, new_entries)
    final_entries = settle_entries(merged, indicator_df, windows)
    _write_jsonl(ledger_path, final_entries)

    new_added_entries = [row for row in final_entries if _entry_key(row) in added_keys]
    run_date = datetime.now().date().isoformat()
    payload = {
        "status": "ok",
        "run_date": run_date,
        "scorecard_json_path": str(scorecard_json_path),
        "max_signal_lag_days": int(max_signal_lag_days),
        "rebuild": bool(rebuild),
        "ledger_path": str(ledger_path),
        "summary_path": str(summary_path),
        "latest_summary_path": str(latest_summary_path),
        "daily_meta": daily_meta,
        "promoted_signal_count": len(promoted_keys),
        "new_entry_count": added,
        "total_entry_count": len(final_entries),
        "open_entry_count": sum(1 for row in final_entries if row.get("status") != "settled"),
        "settled_entry_count": sum(1 for row in final_entries if row.get("status") == "settled"),
        "new_entries": new_added_entries,
        "recent_entries": final_entries[-50:],
    }
    summary = render_summary(payload)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    latest_summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary_path.write_text(summary, encoding="utf-8")
    return payload


def _parse_codes(raw: str | None) -> list[str]:
    if not raw:
        return scorecard._default_focus_codes()
    return [scorecard._normalize_focus_code(item) for item in raw.split(",") if scorecard._normalize_focus_code(item)]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scorecard-json",
        default=str(PROJECT_ROOT / "reports" / "backtests" / f"{datetime.now().date().isoformat()}_theory_signal_scorecard.json"),
        help="Theory scorecard JSON path.",
    )
    parser.add_argument(
        "--ledger-path",
        default=str(PROJECT_ROOT / "reports" / "backtests" / "stock_signal_shadow_ledger.jsonl"),
        help="Shadow ledger JSONL path.",
    )
    parser.add_argument("--summary-path", default=None, help="Dated summary markdown path.")
    parser.add_argument(
        "--latest-summary-path",
        default=str(PROJECT_ROOT / "reports" / "backtests" / "latest_stock_signal_shadow_ledger.md"),
        help="Rolling latest summary markdown path.",
    )
    parser.add_argument("--db", default=None, help="SQLite database path.")
    parser.add_argument("--codes", default=None, help="Comma-separated focus codes.")
    parser.add_argument("--windows", nargs="+", type=int, default=DEFAULT_WINDOWS, help="Forward windows.")
    parser.add_argument("--as-of-date", default=None, help="Signal date to record. Defaults to latest date per code.")
    parser.add_argument("--max-signal-lag-days", type=int, default=3, help="Skip stale symbol rows older than this many calendar days versus the global latest date.")
    parser.add_argument("--rebuild", action="store_true", help="Rebuild the ledger from current candidates instead of preserving existing rows.")
    parser.add_argument("--rule-set", choices=["core", "full"], default="core", help="Rule universe.")
    parser.add_argument("--no-qlib", action="store_true", help="Do not use bootstrapped Qlib daily history.")
    parser.add_argument("--no-etf-cache", action="store_true", help="Do not use local ETF history cache.")
    parser.add_argument("--max-rows-per-code", type=int, default=1600, help="Limit rows per symbol after loading caches.")
    parser.add_argument("--json", action="store_true", help="Emit JSON payload.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    scorecard_json_path = Path(args.scorecard_json)
    ledger_path = Path(args.ledger_path)
    latest_summary_path = Path(args.latest_summary_path)
    summary_path = Path(args.summary_path) if args.summary_path else (
        PROJECT_ROOT / "reports" / "backtests" / f"{datetime.now().date().isoformat()}_stock_signal_shadow_ledger.md"
    )
    payload = refresh_shadow_ledger(
        scorecard_json_path=scorecard_json_path,
        ledger_path=ledger_path,
        summary_path=summary_path,
        latest_summary_path=latest_summary_path,
        db_path=scorecard._resolve_db_path(args.db),
        focus_codes=_parse_codes(args.codes),
        windows=sorted({int(window) for window in args.windows if int(window) > 0}),
        as_of_date=args.as_of_date,
        rule_set=args.rule_set,
        use_qlib=not args.no_qlib,
        use_etf_cache=not args.no_etf_cache,
        max_rows_per_code=args.max_rows_per_code,
        max_signal_lag_days=max(0, int(args.max_signal_lag_days)),
        rebuild=bool(args.rebuild),
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"new_entry_count: {payload['new_entry_count']}")
        print(f"total_entry_count: {payload['total_entry_count']}")
        print(f"open_entry_count: {payload['open_entry_count']}")
        print(f"settled_entry_count: {payload['settled_entry_count']}")
        print(f"ledger_path: {payload['ledger_path']}")
        print(f"summary_path: {payload['summary_path']}")
        print(f"latest_summary_path: {payload['latest_summary_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
