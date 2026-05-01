from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_post_close_shadow_refresh.py"
SPEC = importlib.util.spec_from_file_location("run_post_close_shadow_refresh", SCRIPT_PATH)
refresh = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules["run_post_close_shadow_refresh"] = refresh
SPEC.loader.exec_module(refresh)


def test_run_post_close_shadow_refresh_runs_scorecard_then_ledger(monkeypatch) -> None:
    commands: list[list[str]] = []

    def fake_run_command(command, *, timeout_seconds):
        commands.append(command)
        if "run_theory_signal_scorecard.py" in command[1]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=(
                    "generated: /tmp/report.md\n"
                    "json: /tmp/scorecard.json\n"
                    "latest: /tmp/latest.md\n"
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=(
                "new_entry_count: 2\n"
                "total_entry_count: 5\n"
                "open_entry_count: 3\n"
                "settled_entry_count: 2\n"
                "ledger_path: /tmp/ledger.jsonl\n"
                "summary_path: /tmp/ledger.md\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(refresh, "_run_command", fake_run_command)

    class FakeLabeler:
        def run(self, *, dry_run=False):
            return {
                "status": "ok",
                "dry_run": dry_run,
                "ledger_path": "/tmp/stock_intraday_replay_ledger.jsonl",
                "totals": {
                    "rows": 3,
                    "eligible": 2,
                    "updated": 2,
                    "missing_price": 0,
                    "missing_bars": 1,
                    "invalid_rows": 0,
                },
            }

    monkeypatch.setattr(
        "src.services.stock_intraday_replay_labeler.StockIntradayReplayLabeler",
        lambda: FakeLabeler(),
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        payload = refresh.run_post_close_shadow_refresh(
            output_dir=Path(tmpdir),
            windows=[5, 3, 5],
            min_samples=12,
            permutation_iterations=7,
            timeout_seconds=123,
            rebuild_ledger=True,
            skip_ic=True,
        )

        assert payload["status"] == "ok"
        assert payload["windows"] == [3, 5]
        assert len(commands) == 2
        assert "run_theory_signal_scorecard.py" in commands[0][1]
        assert "--skip-ic" in commands[0]
        assert "run_stock_signal_shadow_ledger.py" in commands[1][1]
        assert "--rebuild" in commands[1]
        assert "--scorecard-json" in commands[1]
        assert payload["intraday_replay_labels"]["totals"]["updated"] == 2
        assert Path(payload["summary_path"]).exists()
        assert Path(payload["json_path"]).exists()
        summary = Path(payload["summary_path"]).read_text(encoding="utf-8")
        assert "本次新增: 2" in summary
        assert "本次已更新: 2" in summary


def test_run_post_close_shadow_refresh_can_skip_intraday_labels(monkeypatch) -> None:
    def fake_run_command(command, *, timeout_seconds):
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(refresh, "_run_command", fake_run_command)

    with tempfile.TemporaryDirectory() as tmpdir:
        payload = refresh.run_post_close_shadow_refresh(
            output_dir=Path(tmpdir),
            backfill_intraday_replay_labels=False,
        )

    assert payload["intraday_replay_labels"]["status"] == "skipped"
