# -*- coding: utf-8 -*-
"""Long-running local market data warehouse refresh service."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import func

from data_provider import DataFetcherManager
from data_provider.base import canonical_stock_code, normalize_stock_code
from src.config import get_config
from src.storage import DatabaseManager, PortfolioPosition, StockDaily

logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WAREHOUSE_SKIPPED_CODES = {"000300", "160723", "838394"}


@dataclass(frozen=True)
class MarketDataTarget:
    code: str
    sources: List[str]


@dataclass(frozen=True)
class LocalCoverage:
    code: str
    bar_count: int
    first_date: Optional[date]
    latest_date: Optional[date]


class MarketDataWarehouseService:
    """Refresh local OHLCV data for holdings and configured watchlists."""

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        *,
        fetcher_manager: Optional[DataFetcherManager] = None,
        project_root: str | Path | None = None,
    ) -> None:
        self.db_manager = db_manager or DatabaseManager.get_instance()
        self.fetcher_manager = fetcher_manager
        self.project_root = Path(project_root) if project_root is not None else self._default_project_root(self.db_manager)

    def close(self) -> None:
        close = getattr(self.fetcher_manager, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:  # pragma: no cover - best effort cleanup
                logger.debug("关闭行情数据源管理器失败: %s", exc)

    def collect_targets(self, config: Any = None, symbols: Optional[Sequence[str]] = None) -> List[MarketDataTarget]:
        """Return deduplicated symbols from explicit input, holdings and config pools."""
        cfg = config or get_config()
        target_sources: Dict[str, List[str]] = {}

        def add(raw_code: str, source: str) -> None:
            code = self._normalize_code(raw_code)
            if not code:
                return
            bucket = target_sources.setdefault(code, [])
            if source not in bucket:
                bucket.append(source)

        if symbols:
            for raw in symbols:
                add(raw, "manual")
        else:
            for raw in self._current_holding_symbols():
                add(raw, "portfolio")
            for raw in list(getattr(cfg, "stock_list", []) or []):
                add(raw, "stock_list")
            for raw in list(getattr(cfg, "watchlist_stock_list", []) or []):
                add(raw, "watchlist")

        return [
            MarketDataTarget(code=code, sources=sources)
            for code, sources in sorted(target_sources.items(), key=lambda item: item[0])
        ]

    def preview_targets(self, config: Any = None) -> Dict[str, Any]:
        targets = self.collect_targets(config=config)
        return {
            "target_count": len(targets),
            "targets": [
                {
                    "code": target.code,
                    "sources": target.sources,
                    "coverage": self._coverage(target.code).__dict__,
                }
                for target in targets
            ],
        }

    def run_refresh(
        self,
        *,
        config: Any = None,
        symbols: Optional[Sequence[str]] = None,
        lookback_days: Optional[int] = None,
        refresh_overlap_days: Optional[int] = None,
        max_symbols: Optional[int] = None,
        force: bool = False,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """Fetch missing/recent bars and upsert them into the local database."""
        cfg = config or get_config()
        effective_lookback = int(
            lookback_days
            if lookback_days is not None
            else getattr(cfg, "market_data_warehouse_lookback_days", 1825)
        )
        effective_overlap = int(
            refresh_overlap_days
            if refresh_overlap_days is not None
            else getattr(cfg, "market_data_warehouse_refresh_overlap_days", 7)
        )
        effective_max_symbols = int(
            max_symbols
            if max_symbols is not None
            else getattr(cfg, "market_data_warehouse_max_symbols", 200)
        )
        effective_lookback = max(30, effective_lookback)
        effective_overlap = max(0, min(effective_overlap, 365))
        effective_max_symbols = max(1, effective_max_symbols)
        run_end_date = end_date or date.today()

        targets = self.collect_targets(config=cfg, symbols=symbols)[:effective_max_symbols]
        items: List[Dict[str, Any]] = []
        totals = {
            "target_count": len(targets),
            "processed": 0,
            "succeeded": 0,
            "skipped": 0,
            "failed": 0,
            "rows_fetched": 0,
            "rows_inserted": 0,
        }

        for target in targets:
            totals["processed"] += 1
            item = self._refresh_one(
                target,
                lookback_days=effective_lookback,
                refresh_overlap_days=effective_overlap,
                force=force,
                end_date=run_end_date,
            )
            items.append(item)
            if item["status"] == "ok":
                totals["succeeded"] += 1
                totals["rows_fetched"] += int(item.get("rows_fetched") or 0)
                totals["rows_inserted"] += int(item.get("rows_inserted") or 0)
            elif item["status"] == "skipped":
                totals["skipped"] += 1
            else:
                totals["failed"] += 1

        status = "ok" if totals["failed"] == 0 else ("partial" if totals["succeeded"] else "error")
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": status,
            "lookback_days": effective_lookback,
            "refresh_overlap_days": effective_overlap,
            "max_symbols": effective_max_symbols,
            "force": bool(force),
            "end_date": run_end_date.isoformat(),
            "totals": totals,
            "items": items,
        }
        payload["ledger_path"] = self._write_ledger(payload)
        return payload

    def latest_run(self) -> Dict[str, Any]:
        ledger_dir = self._ledger_dir()
        latest = self._latest_ledger_file(ledger_dir)
        if latest is None:
            return {
                "status": "missing",
                "path": str(ledger_dir),
                "generated_at": None,
                "totals": {},
            }
        last_payload: Optional[Dict[str, Any]] = None
        for line in latest.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            try:
                last_payload = json.loads(line)
            except json.JSONDecodeError:
                continue
        if not last_payload:
            return {
                "status": "unreadable",
                "path": str(latest),
                "generated_at": None,
                "totals": {},
            }
        return {
            "status": last_payload.get("status", "unknown"),
            "path": str(latest),
            "generated_at": last_payload.get("generated_at"),
            "totals": last_payload.get("totals") or {},
            "end_date": last_payload.get("end_date"),
        }

    def _refresh_one(
        self,
        target: MarketDataTarget,
        *,
        lookback_days: int,
        refresh_overlap_days: int,
        force: bool,
        end_date: date,
    ) -> Dict[str, Any]:
        before = self._coverage(target.code)
        if target.code in DEFAULT_WAREHOUSE_SKIPPED_CODES:
            return {
                "code": target.code,
                "status": "skipped",
                "sources": target.sources,
                "message": "当前数据源不适合自动沉淀，已跳过以避免拖慢每日任务",
                "before": self._coverage_payload(before),
                "after": self._coverage_payload(before),
            }
        start_date = self._resolve_start_date(
            latest_date=before.latest_date,
            end_date=end_date,
            lookback_days=lookback_days,
            refresh_overlap_days=refresh_overlap_days,
            force=force,
        )
        request_days = max(30, (end_date - start_date).days + 1)

        try:
            logger.info(
                "[MarketDataWarehouse] 刷新 %s: %s ~ %s sources=%s",
                target.code,
                start_date,
                end_date,
                ",".join(target.sources),
            )
            df, source_name = self._get_fetcher_manager().get_daily_data(
                target.code,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                days=request_days,
            )
            if df is None or df.empty:
                raise ValueError("数据源返回空行情")
            rows_fetched = int(len(df))
            rows_inserted = int(self.db_manager.save_daily_data(df, target.code, source_name))
            after = self._coverage(target.code)
            return {
                "code": target.code,
                "status": "ok",
                "sources": target.sources,
                "data_source": source_name,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "rows_fetched": rows_fetched,
                "rows_inserted": rows_inserted,
                "before": self._coverage_payload(before),
                "after": self._coverage_payload(after),
            }
        except Exception as exc:
            logger.warning("[MarketDataWarehouse] %s 刷新失败: %s", target.code, exc)
            return {
                "code": target.code,
                "status": "error",
                "sources": target.sources,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "message": str(exc),
                "before": self._coverage_payload(before),
                "after": self._coverage_payload(self._coverage(target.code)),
            }

    def _current_holding_symbols(self) -> List[str]:
        with self.db_manager.get_session() as session:
            rows = (
                session.query(PortfolioPosition.symbol)
                .filter(
                    PortfolioPosition.market == "cn",
                    PortfolioPosition.quantity > 0,
                )
                .group_by(PortfolioPosition.symbol)
                .order_by(PortfolioPosition.symbol.asc())
                .all()
            )
        return [str(row[0]).strip() for row in rows if row and str(row[0]).strip()]

    def _get_fetcher_manager(self) -> DataFetcherManager:
        if self.fetcher_manager is None:
            self.fetcher_manager = self._build_fast_fetcher_manager()
        return self.fetcher_manager

    @staticmethod
    def _build_fast_fetcher_manager() -> DataFetcherManager:
        """Use data sources suited for unattended daily refreshes.

        The full analysis manager includes slower exhaustive fallbacks. For a
        daily warehouse job, fast partial success is better than spending tens
        of seconds on a known-slow source for each symbol.
        """
        from data_provider.baostock_fetcher import BaostockFetcher
        from data_provider.longbridge_fetcher import LongbridgeFetcher
        from data_provider.tushare_fetcher import TushareFetcher
        from data_provider.yfinance_fetcher import YfinanceFetcher
        from data_provider.akshare_fetcher import AkshareFetcher

        def with_priority(fetcher: Any, priority: int) -> Any:
            fetcher.priority = priority
            return fetcher

        fetchers = []
        try:
            if getattr(get_config(), "tushare_token", None):
                fetchers.append(with_priority(TushareFetcher(), 0))
        except Exception:
            pass

        fetchers.extend(
            [
                with_priority(BaostockFetcher(), 1),
                with_priority(YfinanceFetcher(), 2),
                with_priority(AkshareFetcher(), 3),
                with_priority(LongbridgeFetcher(), 4),
            ]
        )
        return DataFetcherManager(fetchers=fetchers)

    def _coverage(self, code: str) -> LocalCoverage:
        with self.db_manager.get_session() as session:
            row = (
                session.query(
                    func.count(StockDaily.id),
                    func.min(StockDaily.date),
                    func.max(StockDaily.date),
                )
                .filter(StockDaily.code == code)
                .one()
            )
        return LocalCoverage(
            code=code,
            bar_count=int(row[0] or 0),
            first_date=row[1],
            latest_date=row[2],
        )

    @staticmethod
    def _resolve_start_date(
        *,
        latest_date: Optional[date],
        end_date: date,
        lookback_days: int,
        refresh_overlap_days: int,
        force: bool,
    ) -> date:
        floor = end_date - timedelta(days=lookback_days)
        if force or latest_date is None:
            return floor
        start = latest_date - timedelta(days=refresh_overlap_days)
        if start < floor:
            start = floor
        if start > end_date:
            start = end_date
        return start

    def _write_ledger(self, payload: Dict[str, Any]) -> str:
        ledger_dir = self._ledger_dir()
        ledger_dir.mkdir(parents=True, exist_ok=True)
        path = ledger_dir / f"{date.today().isoformat()}_market_data_refresh.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        return str(path)

    def _ledger_dir(self) -> Path:
        return self.project_root / "reports" / "market_data_refresh"

    @staticmethod
    def _latest_ledger_file(root: Path) -> Optional[Path]:
        if not root.exists():
            return None
        candidates = [path for path in root.glob("*_market_data_refresh.jsonl") if path.is_file()]
        if not candidates:
            return None
        return max(candidates, key=lambda item: item.stat().st_mtime)

    @staticmethod
    def _default_project_root(db_manager: DatabaseManager) -> Path:
        database = getattr(db_manager._engine.url, "database", None)
        if database and database != ":memory:":
            db_path = Path(database).expanduser().resolve()
            if db_path.parent.name == "data":
                return db_path.parent.parent
        return PROJECT_ROOT

    @staticmethod
    def _normalize_code(raw_code: str) -> str:
        raw = str(raw_code or "").strip().upper()
        if raw == "NDX100":
            raw = "NDX"
        normalized = canonical_stock_code(normalize_stock_code(raw))
        if normalized.isdigit() and len(normalized) == 5:
            return f"HK{normalized}"
        return normalized

    @staticmethod
    def _coverage_payload(coverage: LocalCoverage) -> Dict[str, Any]:
        return {
            "code": coverage.code,
            "bar_count": coverage.bar_count,
            "first_date": coverage.first_date.isoformat() if coverage.first_date else None,
            "latest_date": coverage.latest_date.isoformat() if coverage.latest_date else None,
        }
