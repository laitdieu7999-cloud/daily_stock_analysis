# -*- coding: utf-8 -*-
"""Local SQLite-backed archive for signals, shadow ledger, and daily predictions."""

from __future__ import annotations

import enum
import hashlib
import json
import logging
import re
import time
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar

from sqlalchemy import Boolean, Date, DateTime, Enum, Float, Index, Integer, String, Text, create_engine, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

logger = logging.getLogger(__name__)
T = TypeVar("T")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOCAL_STORAGE_ROOT = PROJECT_ROOT / "data" / "local_storage"
DEFAULT_DATABASE_PATH = DEFAULT_LOCAL_STORAGE_ROOT / "local_storage.db"
DEFAULT_DOCUMENT_ROOT = DEFAULT_LOCAL_STORAGE_ROOT / "documents"


class SignalType(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    CLEAR = "CLEAR"
    HOLD = "HOLD"
    HEDGE = "HEDGE"


class ShadowAction(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    CLEAR = "CLEAR"
    HOLD = "HOLD"
    REBALANCE = "REBALANCE"


class Direction(str, enum.Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"
    VOLATILE = "VOLATILE"
    UNKNOWN = "UNKNOWN"


class DocumentKind(str, enum.Enum):
    GEMINI_DAILY = "GEMINI_DAILY"
    TACTICAL_REPORT = "TACTICAL_REPORT"
    FEISHU_PUSH = "FEISHU_PUSH"
    OTHER = "OTHER"


class Base(DeclarativeBase):
    pass


class SignalPushArchive(Base):
    """Archive of actually emitted signal recommendations."""

    __tablename__ = "signal_push_archive"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    push_date_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    target_symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    signal_type: Mapped[SignalType] = mapped_column(Enum(SignalType, native_enum=False), nullable=False, index=True)
    trigger_reason: Mapped[str] = mapped_column(Text, nullable=False)
    price_at_push: Mapped[float | None] = mapped_column(Float)
    is_executed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (
        Index("ix_signal_push_symbol_time", "target_symbol", "push_date_time"),
    )


class ShadowLedger(Base):
    """Parallel paper ledger for the perfect-position path."""

    __tablename__ = "shadow_ledger"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    virtual_position: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    virtual_avg_cost: Mapped[float | None] = mapped_column(Float)
    action: Mapped[ShadowAction] = mapped_column(Enum(ShadowAction, native_enum=False), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (
        Index("ix_shadow_ledger_symbol_date", "symbol", "date"),
    )


class DailyPredictionLog(Base):
    """Daily local-vs-external prediction record with later outcome backfill."""

    __tablename__ = "daily_prediction_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    predict_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    target_index: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    local_direction: Mapped[Direction] = mapped_column(Enum(Direction, native_enum=False), nullable=False)
    gemini_direction: Mapped[Direction | None] = mapped_column(Enum(Direction, native_enum=False))
    actual_result: Mapped[str | None] = mapped_column(String(64), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        Index("ix_prediction_index_date", "target_index", "predict_date"),
    )


class LocalDocumentArchive(Base):
    """Index for local Markdown/text artifacts stored outside SQLite."""

    __tablename__ = "local_document_archive"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    document_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    document_kind: Mapped[DocumentKind] = mapped_column(Enum(DocumentKind, native_enum=False), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_uri: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (
        Index("ix_document_kind_date", "document_kind", "document_date"),
    )


def _coerce_date(value: date | datetime | str | None) -> date:
    if value is None:
        return datetime.now().date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _coerce_datetime(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now()
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _parse_loose_date(value: str | None) -> date | None:
    if not value:
        return None
    text = str(value).strip()
    if "T" in text:
        text = text.split("T", 1)[0]
    match = re.search(r"([0-9]{4})[年-]([0-9]{1,2})[月-]([0-9]{1,2})", text)
    if match:
        year, month, day = match.groups()
        return date(int(year), int(month), int(day))
    text = text.replace("年", "-").replace("月", "-").replace("日", "")
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _coerce_enum(enum_cls: type[enum.Enum], value: enum.Enum | str) -> enum.Enum:
    if isinstance(value, enum_cls):
        return value
    return enum_cls(str(value).upper())


def _read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    source = Path(path)
    if not source.exists():
        return []
    rows: list[dict[str, Any]] = []
    for raw_line in source.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _direction_from_regime(value: str | None) -> Direction:
    normalized = str(value or "").lower()
    if normalized in {"full_risk", "risk_on", "buy", "bullish"}:
        return Direction.BULLISH
    if normalized in {"risk_off", "clear", "sell", "bearish"}:
        return Direction.BEARISH
    if normalized in {"caution", "hold", "neutral"}:
        return Direction.NEUTRAL
    if normalized in {"volatile", "volatility"}:
        return Direction.VOLATILE
    return Direction.UNKNOWN


def _actual_result_from_outcomes(outcomes: dict[str, Any] | None) -> str | None:
    value = (outcomes or {}).get("next_1d_return")
    if value is None:
        return None
    ret = float(value)
    if ret > 0.001:
        return "UP"
    if ret < -0.001:
        return "DOWN"
    return "FLAT"


def _signal_type_from_text(text: str) -> SignalType:
    normalized = text.lower()
    if any(token in normalized for token in ("risk_off", "防守", "减仓", "离场", "clear")):
        return SignalType.CLEAR
    if any(token in normalized for token in ("sell", "卖出")):
        return SignalType.SELL
    if any(token in normalized for token in ("buy", "买入", "加仓")):
        return SignalType.BUY
    if "hedge" in normalized or "对冲" in normalized:
        return SignalType.HEDGE
    return SignalType.HOLD


def _is_sqlite_locked(exc: OperationalError) -> bool:
    return "database is locked" in str(exc).lower() or "database table is locked" in str(exc).lower()


class LocalStorageManager:
    """Small dedicated SQLite manager for local trading-system archives."""

    def __init__(
        self,
        *,
        database_path: str | Path = DEFAULT_DATABASE_PATH,
        document_root: str | Path = DEFAULT_DOCUMENT_ROOT,
        busy_timeout_ms: int = 5000,
        write_retry_max: int = 3,
        write_retry_base_delay: float = 0.1,
    ) -> None:
        self.database_path = Path(database_path).expanduser().resolve()
        self.document_root = Path(document_root).expanduser().resolve()
        self.busy_timeout_ms = int(busy_timeout_ms)
        self.write_retry_max = int(write_retry_max)
        self.write_retry_base_delay = float(write_retry_base_delay)
        self._ensure_directories()
        self.engine = self._create_engine()
        self.SessionLocal = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, expire_on_commit=False)
        Base.metadata.create_all(self.engine)

    def _ensure_directories(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.document_root.mkdir(parents=True, exist_ok=True)

    def _create_engine(self) -> Engine:
        engine = create_engine(
            f"sqlite:///{self.database_path}",
            pool_pre_ping=True,
            connect_args={
                "timeout": self.busy_timeout_ms / 1000,
                "check_same_thread": False,
            },
        )

        @event.listens_for(engine, "connect")
        def _configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute("PRAGMA foreign_keys=ON")
            finally:
                cursor.close()

        return engine

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def run_write(self, operation_name: str, operation: Callable[[Session], T]) -> T:
        for attempt in range(self.write_retry_max + 1):
            session = self.SessionLocal()
            try:
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
                result = operation(session)
                session.commit()
                return result
            except OperationalError as exc:
                session.rollback()
                if _is_sqlite_locked(exc) and attempt < self.write_retry_max:
                    delay = self.write_retry_base_delay * (2 ** attempt)
                    logger.warning(
                        "SQLite local storage write locked: %s (%s/%s, %.2fs)",
                        operation_name,
                        attempt + 1,
                        self.write_retry_max,
                        delay,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue
                raise
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
        raise RuntimeError(f"write operation did not complete: {operation_name}")

    def record_signal_push(
        self,
        *,
        target_symbol: str,
        signal_type: SignalType | str,
        trigger_reason: str,
        price_at_push: float | None = None,
        push_date_time: datetime | str | None = None,
        is_executed: bool = False,
    ) -> SignalPushArchive:
        def _write(session: Session) -> SignalPushArchive:
            row = SignalPushArchive(
                push_date_time=_coerce_datetime(push_date_time),
                target_symbol=str(target_symbol),
                signal_type=_coerce_enum(SignalType, signal_type),
                trigger_reason=str(trigger_reason),
                price_at_push=float(price_at_push) if price_at_push is not None else None,
                is_executed=bool(is_executed),
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return row

        return self.run_write("record_signal_push", _write)

    def record_shadow_ledger(
        self,
        *,
        symbol: str,
        action: ShadowAction | str,
        virtual_position: float,
        virtual_avg_cost: float | None = None,
        ledger_date: date | datetime | str | None = None,
    ) -> ShadowLedger:
        def _write(session: Session) -> ShadowLedger:
            row = ShadowLedger(
                date=_coerce_date(ledger_date),
                symbol=str(symbol),
                virtual_position=float(virtual_position),
                virtual_avg_cost=float(virtual_avg_cost) if virtual_avg_cost is not None else None,
                action=_coerce_enum(ShadowAction, action),
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return row

        return self.run_write("record_shadow_ledger", _write)

    def record_daily_prediction(
        self,
        *,
        target_index: str,
        local_direction: Direction | str,
        gemini_direction: Direction | str | None = None,
        actual_result: str | None = None,
        predict_date: date | datetime | str | None = None,
    ) -> DailyPredictionLog:
        def _write(session: Session) -> DailyPredictionLog:
            row = DailyPredictionLog(
                predict_date=_coerce_date(predict_date),
                target_index=str(target_index),
                local_direction=_coerce_enum(Direction, local_direction),
                gemini_direction=_coerce_enum(Direction, gemini_direction) if gemini_direction is not None else None,
                actual_result=actual_result,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return row

        return self.run_write("record_daily_prediction", _write)

    def archive_document_text(
        self,
        *,
        text: str,
        title: str,
        document_kind: DocumentKind | str = DocumentKind.OTHER,
        document_date: date | datetime | str | None = None,
        extension: str = ".md",
    ) -> LocalDocumentArchive:
        doc_date = _coerce_date(document_date)
        suffix = extension if extension.startswith(".") else f".{extension}"
        safe_title = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(title)).strip("_")
        safe_title = safe_title[:80] or "document"
        target_dir = self.document_root / f"{doc_date:%Y}" / f"{doc_date:%m}" / f"{doc_date:%d}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = (target_dir / f"{safe_title}{suffix}").resolve()
        normalized = text.strip() + "\n"
        target_path.write_text(normalized, encoding="utf-8")
        content_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()

        def _write(session: Session) -> LocalDocumentArchive:
            existing = session.scalars(
                select(LocalDocumentArchive).where(
                    LocalDocumentArchive.file_path == str(target_path),
                )
            ).first()
            if existing is not None:
                existing.content_hash = content_hash
                existing.file_uri = target_path.as_uri()
                return existing
            row = LocalDocumentArchive(
                document_date=doc_date,
                document_kind=_coerce_enum(DocumentKind, document_kind),
                title=str(title),
                file_path=str(target_path),
                file_uri=target_path.as_uri(),
                content_hash=content_hash,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return row

        return self.run_write("archive_document_text", _write)

    def archive_document_file(
        self,
        source_path: str | Path,
        *,
        title: str | None = None,
        document_kind: DocumentKind | str = DocumentKind.OTHER,
        document_date: date | datetime | str | None = None,
    ) -> LocalDocumentArchive:
        source = Path(source_path)
        return self.archive_document_text(
            text=source.read_text(encoding="utf-8"),
            title=title or source.stem,
            document_kind=document_kind,
            document_date=document_date,
            extension=source.suffix or ".md",
        )

    def upsert_daily_prediction(
        self,
        *,
        target_index: str,
        local_direction: Direction | str,
        gemini_direction: Direction | str | None = None,
        actual_result: str | None = None,
        predict_date: date | datetime | str | None = None,
    ) -> DailyPredictionLog:
        row_date = _coerce_date(predict_date)

        def _write(session: Session) -> DailyPredictionLog:
            row = session.scalars(
                select(DailyPredictionLog).where(
                    DailyPredictionLog.predict_date == row_date,
                    DailyPredictionLog.target_index == str(target_index),
                )
            ).first()
            if row is None:
                row = DailyPredictionLog(
                    predict_date=row_date,
                    target_index=str(target_index),
                    local_direction=_coerce_enum(Direction, local_direction),
                    gemini_direction=_coerce_enum(Direction, gemini_direction) if gemini_direction is not None else None,
                    actual_result=actual_result,
                )
                session.add(row)
            else:
                row.local_direction = _coerce_enum(Direction, local_direction)
                row.gemini_direction = _coerce_enum(Direction, gemini_direction) if gemini_direction is not None else None
                row.actual_result = actual_result
                row.updated_at = datetime.now()
            session.flush()
            session.refresh(row)
            return row

        return self.run_write("upsert_daily_prediction", _write)

    def record_signal_push_if_absent(
        self,
        *,
        target_symbol: str,
        signal_type: SignalType | str,
        trigger_reason: str,
        price_at_push: float | None = None,
        push_date_time: datetime | str | None = None,
        is_executed: bool = False,
    ) -> SignalPushArchive:
        row_time = _coerce_datetime(push_date_time)
        row_signal = _coerce_enum(SignalType, signal_type)

        def _write(session: Session) -> SignalPushArchive:
            existing = session.scalars(
                select(SignalPushArchive).where(
                    SignalPushArchive.push_date_time == row_time,
                    SignalPushArchive.target_symbol == str(target_symbol),
                    SignalPushArchive.signal_type == row_signal,
                    SignalPushArchive.trigger_reason == str(trigger_reason),
                )
            ).first()
            if existing is not None:
                return existing
            row = SignalPushArchive(
                push_date_time=row_time,
                target_symbol=str(target_symbol),
                signal_type=row_signal,
                trigger_reason=str(trigger_reason),
                price_at_push=float(price_at_push) if price_at_push is not None else None,
                is_executed=bool(is_executed),
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return row

        return self.run_write("record_signal_push_if_absent", _write)

    def import_feishu_push_audit(
        self,
        audit_path: str | Path,
        *,
        default_symbol: str = "510500.SS",
    ) -> dict[str, int]:
        rows = _read_jsonl(audit_path)
        imported_documents = 0
        imported_signals = 0
        skipped = 0
        for row in rows:
            if str(row.get("channel") or "") != "feishu" or not bool(row.get("success")):
                skipped += 1
                continue
            sent_at = _coerce_datetime(row.get("sent_at"))
            archive_path = Path(str(row.get("archive_path") or ""))
            content = archive_path.read_text(encoding="utf-8") if archive_path.exists() else str(row.get("content_preview") or "")
            if not content.strip():
                skipped += 1
                continue
            self.archive_document_text(
                text=content,
                title=f"{sent_at:%Y-%m-%d_%H%M%S}_feishu_{row.get('push_kind') or 'push'}",
                document_kind=DocumentKind.FEISHU_PUSH,
                document_date=sent_at,
            )
            imported_documents += 1
            if str(row.get("push_kind") or "") == "metaphysical_daily":
                preview = str(row.get("content_preview") or content)
                self.record_signal_push_if_absent(
                    push_date_time=sent_at,
                    target_symbol=default_symbol,
                    signal_type=_signal_type_from_text(preview),
                    trigger_reason=preview,
                    price_at_push=None,
                    is_executed=False,
                )
                imported_signals += 1
        return {
            "source_rows": len(rows),
            "imported_documents": imported_documents,
            "imported_signals": imported_signals,
            "skipped": skipped,
        }

    def import_metaphysical_daily_archives(self, archive_dir: str | Path) -> dict[str, int]:
        root = Path(archive_dir)
        imported = 0
        skipped = 0
        if not root.exists():
            return {"source_files": 0, "imported_documents": 0, "skipped": 0}
        files = sorted(root.glob("*.md"))
        for path in files:
            doc_date = _parse_loose_date(path.name[:10])
            if doc_date is None:
                skipped += 1
                continue
            self.archive_document_file(
                path,
                title=path.stem,
                document_kind=DocumentKind.TACTICAL_REPORT,
                document_date=doc_date,
            )
            imported += 1
        return {"source_files": len(files), "imported_documents": imported, "skipped": skipped}

    def import_metaphysical_learning_samples(self, snapshot_path: str | Path) -> dict[str, int]:
        rows = _read_jsonl(snapshot_path)
        imported = 0
        skipped = 0
        for row in rows:
            report_date = _parse_loose_date(row.get("report_date"))
            if report_date is None:
                skipped += 1
                continue
            local_direction = _direction_from_regime(row.get("raw_position_regime"))
            gemini_direction = _direction_from_regime(row.get("final_position_regime")) if row.get("overlay_active") else None
            self.upsert_daily_prediction(
                predict_date=report_date,
                target_index=str(row.get("symbol") or "510500.SS"),
                local_direction=local_direction,
                gemini_direction=gemini_direction,
                actual_result=_actual_result_from_outcomes(row.get("future_outcomes") or {}),
            )
            imported += 1
        return {"source_rows": len(rows), "imported_predictions": imported, "skipped": skipped}


def init_local_storage(
    *,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    document_root: str | Path = DEFAULT_DOCUMENT_ROOT,
    busy_timeout_ms: int = 5000,
) -> LocalStorageManager:
    """Create directories, initialize SQLite tables, and enable WAL mode."""
    return LocalStorageManager(
        database_path=database_path,
        document_root=document_root,
        busy_timeout_ms=busy_timeout_ms,
    )


__all__ = [
    "Base",
    "SignalType",
    "ShadowAction",
    "Direction",
    "DocumentKind",
    "SignalPushArchive",
    "ShadowLedger",
    "DailyPredictionLog",
    "LocalDocumentArchive",
    "LocalStorageManager",
    "init_local_storage",
    "DEFAULT_LOCAL_STORAGE_ROOT",
    "DEFAULT_DATABASE_PATH",
    "DEFAULT_DOCUMENT_ROOT",
]
