# -*- coding: utf-8 -*-
"""Tests for the local SQLite storage module."""

from __future__ import annotations

import sqlite3
import json
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import inspect, select

from src.local_storage import (
    DailyPredictionLog,
    Direction,
    DocumentKind,
    LocalDocumentArchive,
    ShadowAction,
    ShadowLedger,
    SignalPushArchive,
    SignalType,
    init_local_storage,
)


def test_init_local_storage_creates_database_directories_and_enables_wal() -> None:
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        db_path = root / "nested" / "local_storage.db"
        document_root = root / "docs"

        storage = init_local_storage(
            database_path=db_path,
            document_root=document_root,
            busy_timeout_ms=1234,
        )

        assert db_path.exists()
        assert document_root.exists()
        with storage.session_scope() as session:
            journal_mode = session.connection().exec_driver_sql("PRAGMA journal_mode").scalar()
            busy_timeout = session.connection().exec_driver_sql("PRAGMA busy_timeout").scalar()
            table_names = set(inspect(session.connection()).get_table_names())

        assert str(journal_mode).lower() == "wal"
        assert int(busy_timeout) == 1234
        assert {
            "signal_push_archive",
            "shadow_ledger",
            "daily_prediction_log",
            "local_document_archive",
        }.issubset(table_names)


def test_local_storage_records_core_rows() -> None:
    with TemporaryDirectory() as tmpdir:
        storage = init_local_storage(
            database_path=Path(tmpdir) / "local_storage.db",
            document_root=Path(tmpdir) / "documents",
        )

        signal = storage.record_signal_push(
            push_date_time=datetime(2026, 4, 30, 9, 30),
            target_symbol="510500.SS",
            signal_type="BUY",
            trigger_reason="tail risk cleared",
            price_at_push=1.234,
        )
        shadow = storage.record_shadow_ledger(
            ledger_date=date(2026, 4, 30),
            symbol="510500.SS",
            virtual_position=100.0,
            virtual_avg_cost=1.2,
            action=ShadowAction.BUY,
        )
        prediction = storage.record_daily_prediction(
            predict_date="2026-04-30",
            target_index="000905",
            local_direction=Direction.BULLISH,
            gemini_direction="NEUTRAL",
        )

        assert signal.id is not None
        assert shadow.id is not None
        assert prediction.id is not None
        with storage.session_scope() as session:
            saved_signal = session.scalars(select(SignalPushArchive)).one()
            saved_shadow = session.scalars(select(ShadowLedger)).one()
            saved_prediction = session.scalars(select(DailyPredictionLog)).one()

        assert saved_signal.signal_type == SignalType.BUY
        assert saved_signal.is_executed is False
        assert saved_shadow.action == ShadowAction.BUY
        assert saved_prediction.actual_result is None
        assert saved_prediction.gemini_direction == Direction.NEUTRAL


def test_archive_document_text_writes_yyyy_mm_dd_file_and_uri_index() -> None:
    with TemporaryDirectory() as tmpdir:
        document_root = Path(tmpdir) / "documents"
        storage = init_local_storage(
            database_path=Path(tmpdir) / "local_storage.db",
            document_root=document_root,
        )

        row = storage.archive_document_text(
            text="# Gemini Daily\n\n结论: 防守",
            title="2026-04-30 Gemini Daily",
            document_kind=DocumentKind.GEMINI_DAILY,
            document_date="2026-04-30",
        )

        expected_parent = (document_root / "2026" / "04" / "30").resolve()
        saved_file = Path(row.file_path)
        assert saved_file.exists()
        assert saved_file.parent == expected_parent
        assert row.file_uri == saved_file.as_uri()
        assert "# Gemini Daily" in saved_file.read_text(encoding="utf-8")

        with storage.session_scope() as session:
            saved_row = session.scalars(select(LocalDocumentArchive)).one()

        assert saved_row.document_kind == DocumentKind.GEMINI_DAILY
        assert saved_row.file_uri.startswith("file://")


def test_local_storage_database_is_plain_sqlite_readable() -> None:
    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "local_storage.db"
        storage = init_local_storage(database_path=db_path, document_root=Path(tmpdir) / "documents")
        storage.record_signal_push(
            target_symbol="IC",
            signal_type="CLEAR",
            trigger_reason="manual sanity check",
        )

        with sqlite3.connect(db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM signal_push_archive").fetchone()[0]

        assert count == 1


def test_import_metaphysical_learning_samples_upserts_prediction_rows() -> None:
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        snapshot_path = root / "learning.jsonl"
        row = {
            "report_date": "2026年4月24日",
            "symbol": "510500.SS",
            "raw_position_regime": "full_risk",
            "final_position_regime": "risk_off",
            "overlay_active": True,
            "future_outcomes": {"next_1d_return": -0.012},
        }
        snapshot_path.write_text(json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8")
        storage = init_local_storage(database_path=root / "local_storage.db", document_root=root / "documents")

        first = storage.import_metaphysical_learning_samples(snapshot_path)
        second = storage.import_metaphysical_learning_samples(snapshot_path)

        assert first["imported_predictions"] == 1
        assert second["imported_predictions"] == 1
        with storage.session_scope() as session:
            rows = list(session.scalars(select(DailyPredictionLog)))

        assert len(rows) == 1
        assert rows[0].local_direction == Direction.BULLISH
        assert rows[0].gemini_direction == Direction.BEARISH
        assert rows[0].actual_result == "DOWN"


def test_import_metaphysical_daily_archives_indexes_markdown_documents() -> None:
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        archive_dir = root / "metaphysical_daily_archive"
        archive_dir.mkdir()
        (archive_dir / "2026-04-30_玄学治理日报.md").write_text("## 玄学治理日报\n", encoding="utf-8")
        storage = init_local_storage(database_path=root / "local_storage.db", document_root=root / "documents")

        result = storage.import_metaphysical_daily_archives(archive_dir)

        assert result["source_files"] == 1
        assert result["imported_documents"] == 1
        with storage.session_scope() as session:
            docs = list(session.scalars(select(LocalDocumentArchive)))

        assert len(docs) == 1
        assert docs[0].document_date == date(2026, 4, 30)
        assert docs[0].document_kind == DocumentKind.TACTICAL_REPORT
        assert Path(docs[0].file_path).exists()


def test_import_feishu_push_audit_archives_successful_metaphysical_push_once() -> None:
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        push_file = root / "push.md"
        push_file.write_text("## 玄学治理日报\n\n- 结论: 偏防守，减少风险暴露\n", encoding="utf-8")
        audit_path = root / "feishu_push_audit.jsonl"
        audit_row = {
            "sent_at": "2026-04-30T09:00:00",
            "channel": "feishu",
            "success": True,
            "push_kind": "metaphysical_daily",
            "content_preview": "结论: 偏防守，减少风险暴露",
            "archive_path": str(push_file),
        }
        audit_path.write_text(json.dumps(audit_row, ensure_ascii=False) + "\n", encoding="utf-8")
        storage = init_local_storage(database_path=root / "local_storage.db", document_root=root / "documents")

        first = storage.import_feishu_push_audit(audit_path)
        second = storage.import_feishu_push_audit(audit_path)

        assert first["imported_documents"] == 1
        assert first["imported_signals"] == 1
        assert second["imported_documents"] == 1
        assert second["imported_signals"] == 1
        with storage.session_scope() as session:
            docs = list(session.scalars(select(LocalDocumentArchive)))
            signals = list(session.scalars(select(SignalPushArchive)))

        assert len(docs) == 1
        assert len(signals) == 1
        assert docs[0].document_kind == DocumentKind.FEISHU_PUSH
        assert signals[0].signal_type == SignalType.CLEAR
        assert signals[0].target_symbol == "510500.SS"
