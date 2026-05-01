# -*- coding: utf-8 -*-
"""Backfill forward labels for stock intraday replay ledger."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from src.storage import DatabaseManager, StockDaily


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REPLAY_LEDGER_PATH = PROJECT_ROOT / "reports" / "stock_intraday_replay_ledger.jsonl"
HORIZONS = (1, 3, 5)


@contextmanager
def _file_lock(path: Path, *, exclusive: bool):
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f"{path.name}.lock")
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        except ImportError:
            pass
        yield
    finally:
        try:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        handle.close()


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with _file_lock(path, exclusive=False):
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _write_jsonl_atomic(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _file_lock(path, exclusive=True):
        tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        tmp_path.replace(path)


def _parse_event_date(row: Dict[str, Any]) -> Optional[date]:
    value = row.get("trigger_timestamp") or row.get("event_time")
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        try:
            return datetime.combine(date.fromisoformat(str(value)[:10]), time.min).date()
        except ValueError:
            return None


def _entry_price(row: Dict[str, Any]) -> Optional[float]:
    for value in (
        (row.get("trigger_condition_snapshot") or {}).get("current_price")
        if isinstance(row.get("trigger_condition_snapshot"), dict)
        else None,
        row.get("current_price"),
    ):
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    return None


def _fetch_forward_bars(
    *,
    db_manager: DatabaseManager,
    code: str,
    event_date: date,
    max_horizon: int,
) -> List[StockDaily]:
    with db_manager.get_session() as session:
        return (
            session.query(StockDaily)
            .filter(StockDaily.code == code)
            .filter(StockDaily.date > event_date)
            .filter(StockDaily.close.isnot(None))
            .order_by(StockDaily.date.asc())
            .limit(max_horizon)
            .all()
        )


def _build_forward_labels(
    *,
    row: Dict[str, Any],
    bars: List[StockDaily],
    entry_price: float,
) -> Dict[str, Any]:
    labels: Dict[str, Any] = dict(row.get("forward_labels") or {})
    for horizon in HORIZONS:
        key = f"t_plus_{horizon}"
        if len(bars) < horizon:
            labels.setdefault(key, None)
            continue
        bar = bars[horizon - 1]
        close = float(bar.close)
        labels[key] = {
            "date": bar.date.isoformat(),
            "close": close,
            "return_pct": round((close - entry_price) / entry_price * 100, 4),
            "trading_day_offset": horizon,
        }
    return labels


def _build_outcome_window(
    *,
    bars: List[StockDaily],
    entry_price: float,
) -> Dict[str, Any]:
    usable = bars[: max(HORIZONS)]
    if not usable:
        return {
            "outcome_max_adverse_1h": None,
            "outcome_max_favorable_1h": None,
            "outcome_max_adverse_1d": None,
            "outcome_max_favorable_1d": None,
            "outcome_hit_target": None,
        }
    lows = [float(bar.low if bar.low is not None else bar.close) for bar in usable]
    highs = [float(bar.high if bar.high is not None else bar.close) for bar in usable]
    closes = [float(bar.close) for bar in usable]
    return {
        "outcome_max_adverse_1h": None,
        "outcome_max_favorable_1h": None,
        "outcome_max_adverse_1d": round((min(lows) - entry_price) / entry_price * 100, 4),
        "outcome_max_favorable_1d": round((max(highs) - entry_price) / entry_price * 100, 4),
        "outcome_close_return_5d": round((closes[-1] - entry_price) / entry_price * 100, 4),
        "outcome_hit_target": None,
    }


class StockIntradayReplayLabeler:
    """Fill forward labels for existing replay-ledger events from local OHLCV."""

    def __init__(
        self,
        *,
        db_manager: Optional[DatabaseManager] = None,
        ledger_path: str | Path = DEFAULT_REPLAY_LEDGER_PATH,
    ) -> None:
        self.db_manager = db_manager or DatabaseManager.get_instance()
        self.ledger_path = Path(ledger_path)

    def run(self, *, dry_run: bool = False, max_horizon: int = 5) -> Dict[str, Any]:
        rows = _read_jsonl(self.ledger_path)
        updated_rows: List[Dict[str, Any]] = []
        totals = {
            "rows": len(rows),
            "eligible": 0,
            "updated": 0,
            "missing_price": 0,
            "missing_bars": 0,
            "invalid_rows": 0,
        }

        for row in rows:
            updated = dict(row)
            code = str(updated.get("code") or updated.get("symbol") or "").strip()
            event_date = _parse_event_date(updated)
            entry_price = _entry_price(updated)
            if not code or event_date is None:
                totals["invalid_rows"] += 1
                updated_rows.append(updated)
                continue
            if entry_price is None:
                totals["missing_price"] += 1
                updated_rows.append(updated)
                continue

            totals["eligible"] += 1
            bars = _fetch_forward_bars(
                db_manager=self.db_manager,
                code=code,
                event_date=event_date,
                max_horizon=max_horizon,
            )
            if not bars:
                totals["missing_bars"] += 1
                updated_rows.append(updated)
                continue

            updated["forward_labels"] = _build_forward_labels(
                row=updated,
                bars=bars,
                entry_price=entry_price,
            )
            updated["outcome_reference_window"] = _build_outcome_window(
                bars=bars,
                entry_price=entry_price,
            )
            updated["label_updated_at"] = datetime.now().isoformat(timespec="seconds")
            updated["label_source"] = "local_stock_daily"
            totals["updated"] += 1
            updated_rows.append(updated)

        if not dry_run and rows:
            _write_jsonl_atomic(self.ledger_path, updated_rows)

        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "ok",
            "dry_run": bool(dry_run),
            "ledger_path": str(self.ledger_path),
            "totals": totals,
        }
