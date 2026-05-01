from __future__ import annotations

import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "sync_gemini_drive_report.py"
)
SPEC = importlib.util.spec_from_file_location("sync_gemini_drive_report", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_build_selection_metadata_prefers_same_day_and_title_score():
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        low_score = root / "2026-04-29 战术分析报告V1.gdoc"
        high_score = root / "2026-04-29 战区中枢收盘战术归档V1.gdoc"
        older = root / "2026-04-28 战区中枢报告.gdoc"
        for item in (low_score, high_score, older):
            item.write_text("{}", encoding="utf-8")

        selection = MODULE._build_selection_metadata(
            [low_score, high_score, older],
            expected_report_date="2026-04-29",
        )

        assert selection["selected_from_same_day"] is True
        assert selection["same_day_candidate_count"] == 2
        assert selection["selected_candidate_name"] == high_score.name
        assert "已按固定规则选用" in str(selection["selection_reason"])


def test_build_selection_metadata_reports_missing_same_day():
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        older = root / "2026-04-28 战区中枢报告.gdoc"
        older.write_text("{}", encoding="utf-8")

        selection = MODULE._build_selection_metadata(
            [older],
            expected_report_date="2026-04-29",
        )

        assert selection["selected_from_same_day"] is False
        assert selection["same_day_candidate_count"] == 0
        assert selection["selected_candidate_name"] == older.name
        assert "今天没有新的 Gemini 归档" in str(selection["selection_reason"])


def test_main_skips_export_when_auto_detected_source_is_stale(monkeypatch, tmp_path, capsys):
    stale = tmp_path / "2026-04-28 战区中枢报告.gdoc"
    stale.write_text('{"doc_id":"fake"}', encoding="utf-8")
    target = tmp_path / "gemini_daily.md"
    archive = tmp_path / "archive"

    monkeypatch.setattr(
        MODULE,
        "_auto_detect_google_drive_root",
        lambda: tmp_path,
    )
    monkeypatch.setattr(
        MODULE,
        "_export_google_doc_text",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not export stale source")),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "sync_gemini_drive_report.py",
            "--target-file",
            str(target),
            "--archive-dir",
            str(archive),
            "--expected-report-date",
            "2026-04-30",
            "--json",
        ],
    )

    assert MODULE.main() == 0
    payload = __import__("json").loads(capsys.readouterr().out)
    assert payload["status"] == "stale_source"
    assert payload["synced"] is False
    assert payload["freshness"]["is_fresh"] is False
