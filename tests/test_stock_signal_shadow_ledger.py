from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_stock_signal_shadow_ledger.py"
SPEC = importlib.util.spec_from_file_location("run_stock_signal_shadow_ledger", SCRIPT_PATH)
ledger = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules["run_stock_signal_shadow_ledger"] = ledger
SPEC.loader.exec_module(ledger)


def _daily_frame(days: int = 12):
    rows = []
    start = date(2026, 1, 1)
    for idx in range(days):
        close = 10.0 + idx
        rows.append(
            {
                "code": "600519",
                "date": ledger.pd.Timestamp(start + timedelta(days=idx)),
                "open": close,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
            }
        )
    return ledger.pd.DataFrame(rows)


def test_settle_entries_fills_forward_windows() -> None:
    entries = [
        {
            "signal_date": "2026-01-01",
            "code": "600519",
            "module": "日线技术信号",
            "rule": "test",
            "entry_price": 10.0,
            "settlements": {},
            "status": "open",
        }
    ]

    result = ledger.settle_entries(entries, _daily_frame(), [3, 5, 10])

    item = result[0]
    assert item["status"] == "settled"
    assert round(item["settlements"]["T+3"]["return_pct"], 6) == 30.0
    assert round(item["settlements"]["T+5"]["return_pct"], 6) == 50.0
    assert round(item["settlements"]["T+10"]["return_pct"], 6) == 100.0


def test_merge_entries_deduplicates_same_signal() -> None:
    base = {
        "signal_date": "2026-01-01",
        "code": "600519",
        "module": "日线技术信号",
        "rule": "test",
        "entry_price": 10.0,
    }

    merged, added, added_keys = ledger.merge_entries([base], [dict(base)])

    assert added == 0
    assert added_keys == set()
    assert len(merged) == 1


def test_promoted_signal_keys_reads_shadow_candidates_only() -> None:
    payload = {
        "graduation_scorecard": {
            "rows": [
                {"module": "日线技术信号", "direction_type": "offensive", "rule": "A", "final_decision": "可进Shadow"},
                {"module": "日线技术信号", "direction_type": "defensive", "rule": "B", "final_decision": "可进Shadow"},
                {"module": "日线技术信号", "direction_type": "offensive", "rule": "C", "final_decision": "不升级"},
            ]
        }
    }

    keys = ledger._promoted_signal_keys(payload)

    assert keys == {("日线技术信号", "offensive", "A")}


def test_jsonl_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "ledger.jsonl"
        rows = [{"a": 1}, {"b": "二"}]

        ledger._write_jsonl(path, rows)

        assert ledger._load_jsonl(path) == rows
        assert json.loads(path.read_text(encoding="utf-8").splitlines()[1]) == {"b": "二"}
