# -*- coding: utf-8 -*-
"""Read-only dashboard data for theory scorecard and shadow ledger outputs."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BACKTEST_DIR = PROJECT_ROOT / "reports" / "backtests"
USER_REPORTS_BACKTEST_DIR = Path.home() / "Reports" / "projects" / "daily_stock_analysis" / "backtests"
BACKTEST_ARTIFACT_PATTERNS = (
    "*_theory_signal_scorecard.json",
    "stock_signal_shadow_ledger.jsonl",
    "latest_stock_signal_shadow_ledger.md",
)


def _has_backtest_artifacts(directory: Path) -> bool:
    if not directory.exists() or not directory.is_dir():
        return False
    return any(next(directory.glob(pattern), None) is not None for pattern in BACKTEST_ARTIFACT_PATTERNS)


def _default_backtest_candidates() -> List[Path]:
    candidates: List[Path] = []
    env_path = os.getenv("DSA_BACKTEST_DIR") or os.getenv("DAILY_STOCK_ANALYSIS_BACKTEST_DIR")
    if env_path:
        candidates.append(Path(env_path).expanduser())
    candidates.extend([USER_REPORTS_BACKTEST_DIR, DEFAULT_BACKTEST_DIR])
    return candidates


def _resolve_backtest_dir(backtest_dir: Optional[Path] = None) -> Path:
    if backtest_dir is not None:
        return Path(backtest_dir).expanduser().resolve()

    candidates = _default_backtest_candidates()
    for candidate in candidates:
        candidate = candidate.expanduser()
        if _has_backtest_artifacts(candidate):
            return candidate.resolve()

    for candidate in candidates:
        candidate = candidate.expanduser()
        if candidate.exists():
            return candidate.resolve()

    return DEFAULT_BACKTEST_DIR.expanduser().resolve()


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _latest_file(directory: Path, pattern: str) -> Optional[Path]:
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item.stat().st_mtime, item.name))


def _scorecard_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(row.get("module") or ""),
        str(row.get("direction_type") or ""),
        str(row.get("rule") or ""),
    )


def _safe_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return parsed


def _forward_return(row: Dict[str, Any], key: str) -> Optional[float]:
    labels = row.get("forward_labels")
    if not isinstance(labels, dict):
        return None
    item = labels.get(key)
    if not isinstance(item, dict):
        return None
    return _safe_float(item.get("return_pct"))


def _primary_forward_return(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    for key in ("t_plus_5", "t_plus_3", "t_plus_1"):
        value = _forward_return(row, key)
        if value is not None:
            return key, value
    return None, None


def _is_effective_intraday_signal(signal_type: str, return_pct: Optional[float]) -> Optional[bool]:
    if return_pct is None:
        return None
    if signal_type == "BUY_SETUP":
        return return_pct > 0
    if signal_type.startswith("RISK_"):
        return return_pct < 0
    return None


class ShadowDashboardService:
    """Build a compact UI payload from already-generated research artifacts."""

    def __init__(self, backtest_dir: Optional[Path] = None, project_root: Optional[Path] = None):
        self.backtest_dir = _resolve_backtest_dir(backtest_dir)
        self.project_root = Path(project_root).expanduser().resolve() if project_root else PROJECT_ROOT

    def get_dashboard(self, *, limit: int = 50) -> Dict[str, Any]:
        scorecard = self._load_scorecard()
        ledger = self._load_ledger(limit=max(1, int(limit)))
        intraday_replay = self._load_intraday_replay(limit=max(1, int(limit)))
        return {
            "status": "ok"
            if scorecard["status"] != "missing" or ledger["status"] != "missing" or intraday_replay["status"] != "missing"
            else "missing",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "backtest_dir": str(self.backtest_dir),
            "scorecard": scorecard,
            "ledger": ledger,
            "intraday_replay": intraday_replay,
        }

    def _load_scorecard(self) -> Dict[str, Any]:
        path = _latest_file(self.backtest_dir, "*_theory_signal_scorecard.json")
        if path is None:
            return {
                "status": "missing",
                "json_path": None,
                "report_path": None,
                "generated_at": None,
                "primary_window": None,
                "min_samples": None,
                "daily_meta": {},
                "candidates": [],
                "all_rows": [],
            }

        payload = _read_json(path)
        report_path = self.backtest_dir / f"{path.name.replace('_theory_signal_scorecard.json', '_理论信号准确率评分表.md')}"
        rows = list((payload.get("graduation_scorecard") or {}).get("rows") or [])
        attribution_rows = list((payload.get("symbol_attribution") or {}).get("summary") or [])
        attribution_by_key = {_scorecard_key(row): row for row in attribution_rows}

        for row in rows:
            attribution = attribution_by_key.get(_scorecard_key(row))
            if attribution:
                row["symbol_attribution"] = attribution

        candidates = [
            row for row in rows
            if row.get("final_decision") == "可进Shadow" and row.get("direction_type") == "offensive"
        ]
        return {
            "status": "ok",
            "json_path": str(path),
            "report_path": str(report_path) if report_path.exists() else None,
            "generated_at": payload.get("generated_at"),
            "primary_window": payload.get("primary_window"),
            "min_samples": payload.get("min_samples"),
            "daily_meta": payload.get("daily_meta") or {},
            "candidates": candidates,
            "all_rows": rows,
        }

    def _load_ledger(self, *, limit: int) -> Dict[str, Any]:
        path = self.backtest_dir / "stock_signal_shadow_ledger.jsonl"
        if not path.exists():
            return {
                "status": "missing",
                "ledger_path": str(path),
                "summary_path": None,
                "total_count": 0,
                "open_count": 0,
                "settled_count": 0,
                "rule_counts": [],
                "entries": [],
            }

        rows = _read_jsonl(path)
        rows = sorted(
            rows,
            key=lambda item: (
                str(item.get("signal_date") or ""),
                str(item.get("code") or ""),
                str(item.get("rule") or ""),
            ),
            reverse=True,
        )
        open_count = sum(1 for row in rows if row.get("status") != "settled")
        settled_count = sum(1 for row in rows if row.get("status") == "settled")
        counts: Dict[str, int] = {}
        for row in rows:
            rule = str(row.get("rule") or "未知信号")
            counts[rule] = counts.get(rule, 0) + 1
        rule_counts = [
            {"rule": rule, "count": count}
            for rule, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        ]
        summary_path = self.backtest_dir / "latest_stock_signal_shadow_ledger.md"
        return {
            "status": "ok",
            "ledger_path": str(path),
            "summary_path": str(summary_path) if summary_path.exists() else None,
            "total_count": len(rows),
            "open_count": open_count,
            "settled_count": settled_count,
            "rule_counts": rule_counts,
            "entries": rows[:limit],
        }

    def _load_intraday_replay(self, *, limit: int) -> Dict[str, Any]:
        path = self.project_root / "reports" / "stock_intraday_replay_ledger.jsonl"
        if not path.exists():
            return {
                "status": "missing",
                "ledger_path": str(path),
                "total_count": 0,
                "labeled_count": 0,
                "pending_count": 0,
                "effective_count": 0,
                "effective_rate_pct": None,
                "avg_primary_return_pct": None,
                "avg_mfe_pct": None,
                "avg_mae_pct": None,
                "signal_type_counts": [],
                "entries": [],
            }

        raw_rows = _read_jsonl(path)
        rows = sorted(
            raw_rows,
            key=lambda item: str(item.get("trigger_timestamp") or item.get("event_time") or ""),
            reverse=True,
        )
        total_count = len(rows)
        labeled_count = 0
        effective_count = 0
        primary_returns: List[float] = []
        mfe_values: List[float] = []
        mae_values: List[float] = []
        by_type: Dict[str, Dict[str, Any]] = {}
        entries: List[Dict[str, Any]] = []

        for row in rows:
            signal_type = str(row.get("signal_type") or "UNKNOWN")
            bucket = by_type.setdefault(
                signal_type,
                {
                    "signal_type": signal_type,
                    "count": 0,
                    "labeled_count": 0,
                    "effective_count": 0,
                    "avg_primary_return_pct": None,
                    "effective_rate_pct": None,
                    "_returns": [],
                },
            )
            bucket["count"] += 1

            primary_horizon, primary_return = _primary_forward_return(row)
            effective = _is_effective_intraday_signal(signal_type, primary_return)
            if primary_return is not None:
                labeled_count += 1
                primary_returns.append(primary_return)
                bucket["labeled_count"] += 1
                bucket["_returns"].append(primary_return)
                if effective is True:
                    effective_count += 1
                    bucket["effective_count"] += 1

            outcome = row.get("outcome_reference_window") if isinstance(row.get("outcome_reference_window"), dict) else {}
            mfe = _safe_float(outcome.get("outcome_max_favorable_1d"))
            mae = _safe_float(outcome.get("outcome_max_adverse_1d"))
            if mfe is not None:
                mfe_values.append(mfe)
            if mae is not None:
                mae_values.append(mae)

            if len(entries) < limit:
                entries.append(
                    {
                        "signal_id": row.get("signal_id"),
                        "trigger_timestamp": row.get("trigger_timestamp") or row.get("event_time"),
                        "code": row.get("code") or row.get("symbol"),
                        "name": row.get("name") or row.get("stock_name"),
                        "scope": row.get("scope"),
                        "signal_type": signal_type,
                        "entry_price": _safe_float(
                            row.get("current_price")
                            or (
                                row.get("trigger_condition_snapshot", {}).get("current_price")
                                if isinstance(row.get("trigger_condition_snapshot"), dict)
                                else None
                            )
                        ),
                        "primary_horizon": primary_horizon,
                        "primary_return_pct": primary_return,
                        "effective": effective,
                        "t_plus_1_return_pct": _forward_return(row, "t_plus_1"),
                        "t_plus_3_return_pct": _forward_return(row, "t_plus_3"),
                        "t_plus_5_return_pct": _forward_return(row, "t_plus_5"),
                        "mfe_pct": mfe,
                        "mae_pct": mae,
                    }
                )

        signal_type_counts: List[Dict[str, Any]] = []
        for bucket in by_type.values():
            returns = bucket.pop("_returns", [])
            bucket["avg_primary_return_pct"] = round(sum(returns) / len(returns), 4) if returns else None
            bucket["effective_rate_pct"] = (
                round(bucket["effective_count"] / bucket["labeled_count"] * 100, 2)
                if bucket["labeled_count"]
                else None
            )
            signal_type_counts.append(bucket)
        signal_type_counts.sort(key=lambda item: (-int(item.get("count") or 0), str(item.get("signal_type") or "")))

        return {
            "status": "ok",
            "ledger_path": str(path),
            "total_count": total_count,
            "labeled_count": labeled_count,
            "pending_count": max(total_count - labeled_count, 0),
            "effective_count": effective_count,
            "effective_rate_pct": round(effective_count / labeled_count * 100, 2) if labeled_count else None,
            "avg_primary_return_pct": round(sum(primary_returns) / len(primary_returns), 4) if primary_returns else None,
            "avg_mfe_pct": round(sum(mfe_values) / len(mfe_values), 4) if mfe_values else None,
            "avg_mae_pct": round(sum(mae_values) / len(mae_values), 4) if mae_values else None,
            "signal_type_counts": signal_type_counts,
            "entries": entries,
        }
