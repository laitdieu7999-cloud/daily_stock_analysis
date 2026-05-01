# -*- coding: utf-8 -*-
"""Local financial data center inventory service."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import func

from src.config import get_config
from src.storage import (
    AnalysisHistory,
    BacktestResult,
    BacktestSummary,
    DatabaseManager,
    FundamentalSnapshot,
    NewsIntel,
    PortfolioAccount,
    PortfolioDailySnapshot,
    PortfolioPosition,
    PortfolioTrade,
    StockDaily,
)
from src.services.backtest_service import BacktestService
from src.services.ai_workload_routing_service import AIWorkloadRoutingService
from src.services.market_data_warehouse_service import DEFAULT_WAREHOUSE_SKIPPED_CODES, MarketDataWarehouseService
from src.services.workstation_cleanup_service import WorkstationCleanupService


class DataCenterService:
    """Build a read-only overview of local finance data assets."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        self.db_manager = db_manager or DatabaseManager.get_instance()

    def build_overview(self) -> Dict[str, Any]:
        warnings: List[str] = []

        with self.db_manager.get_session() as session:
            market_data = self._safe_section(
                warnings,
                "行情数据",
                lambda: self._market_data_inventory(session),
            )
            analysis = self._safe_section(
                warnings,
                "分析历史",
                lambda: {
                    "report_count": self._count_rows(session, AnalysisHistory),
                    "stock_count": self._count_distinct(session, AnalysisHistory.code),
                    "latest_created_at": self._to_text(session.query(func.max(AnalysisHistory.created_at)).scalar()),
                },
            )
            backtests = self._safe_section(
                warnings,
                "回测结果",
                lambda: {
                    "result_count": self._count_rows(session, BacktestResult),
                    "summary_count": self._count_rows(session, BacktestSummary),
                    "stock_count": self._count_distinct(session, BacktestResult.code),
                    "latest_evaluated_at": self._to_text(session.query(func.max(BacktestResult.evaluated_at)).scalar()),
                },
            )
            portfolio = self._safe_section(
                warnings,
                "持仓资产",
                lambda: {
                    "account_count": self._count_rows(session, PortfolioAccount),
                    "active_account_count": int(
                        session.query(func.count(PortfolioAccount.id))
                        .filter(PortfolioAccount.is_active.is_(True))
                        .scalar()
                        or 0
                    ),
                    "position_count": self._count_rows(session, PortfolioPosition),
                    "trade_count": self._count_rows(session, PortfolioTrade),
                    "snapshot_count": self._count_rows(session, PortfolioDailySnapshot),
                    "latest_updated_at": self._to_text(session.query(func.max(PortfolioPosition.updated_at)).scalar()),
                },
            )
            news = self._safe_section(
                warnings,
                "新闻情报",
                lambda: {
                    "item_count": self._count_rows(session, NewsIntel),
                    "stock_count": self._count_distinct(session, NewsIntel.code),
                    "latest_fetched_at": self._to_text(session.query(func.max(NewsIntel.fetched_at)).scalar()),
                },
            )
            fundamentals = self._safe_section(
                warnings,
                "基本面快照",
                lambda: {
                    "snapshot_count": self._count_rows(session, FundamentalSnapshot),
                    "stock_count": self._count_distinct(session, FundamentalSnapshot.code),
                    "latest_created_at": self._to_text(session.query(func.max(FundamentalSnapshot.created_at)).scalar()),
                },
            )

        database = self._database_inventory()
        files = self._file_inventories(database.get("path"))
        project_root = self._project_root()
        from src.services.portfolio_daily_review_service import PortfolioDailyReviewService

        maintenance = {
            "portfolio_daily_review": PortfolioDailyReviewService(
                self.db_manager,
                project_root=project_root,
            ).latest_run(),
            "cleanup": WorkstationCleanupService(project_root=project_root).latest_run(),
        }
        ai_routing = AIWorkloadRoutingService().build_status()

        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "database": database,
            "market_data": market_data,
            "analysis": analysis,
            "backtests": backtests,
            "portfolio": portfolio,
            "news": news,
            "fundamentals": fundamentals,
            "files": files,
            "maintenance": maintenance,
            "ai_routing": ai_routing,
            "recommendations": self._recommendations(
                market_data=market_data,
                analysis=analysis,
                backtests=backtests,
                portfolio=portfolio,
                maintenance=maintenance,
                ai_routing=ai_routing,
                warnings=warnings,
            ),
            "warnings": warnings,
        }

    def run_portfolio_backtests(
        self,
        *,
        force: bool = False,
        eval_window_days: Optional[int] = None,
        min_age_days: Optional[int] = None,
        limit_per_symbol: int = 50,
    ) -> Dict[str, Any]:
        """Run backtests for currently held CN symbols."""
        symbols = self._current_holding_symbols()
        backtest_service = BacktestService(self.db_manager)

        totals = {
            "candidate_count": 0,
            "processed": 0,
            "saved": 0,
            "completed": 0,
            "insufficient": 0,
            "errors": 0,
        }
        items: List[Dict[str, Any]] = []

        for symbol in symbols:
            try:
                stats = backtest_service.run_backtest(
                    code=symbol,
                    force=force,
                    eval_window_days=eval_window_days,
                    min_age_days=min_age_days,
                    limit=limit_per_symbol,
                )
                summary = backtest_service.get_stock_summary(
                    symbol,
                    eval_window_days=eval_window_days,
                )
                for key in totals:
                    totals[key] += int(stats.get(key) or 0)
                message = "已完成"
                if int(stats.get("candidate_count") or 0) == 0:
                    message = "暂无可回测的历史分析样本"
                elif int(stats.get("saved") or 0) == 0:
                    message = "本次没有新增回测结果"
                items.append(
                    {
                        "code": symbol,
                        "status": "ok",
                        "message": message,
                        "candidate_count": int(stats.get("candidate_count") or 0),
                        "processed": int(stats.get("processed") or 0),
                        "saved": int(stats.get("saved") or 0),
                        "completed": int(stats.get("completed") or 0),
                        "insufficient": int(stats.get("insufficient") or 0),
                        "errors": int(stats.get("errors") or 0),
                        "summary": summary,
                    }
                )
            except Exception as exc:  # pragma: no cover - each symbol should fail independently
                totals["errors"] += 1
                items.append(
                    {
                        "code": symbol,
                        "status": "error",
                        "message": str(exc),
                        "candidate_count": 0,
                        "processed": 0,
                        "saved": 0,
                        "completed": 0,
                        "insufficient": 0,
                        "errors": 1,
                        "summary": None,
                    }
                )

        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "holding_count": len(symbols),
            "processed_symbols": len(items),
            "totals": totals,
            "items": items,
        }

    def get_portfolio_risk_radar(self) -> Dict[str, Any]:
        """Build a read-only radar from current holdings and stored backtest summaries."""
        positions = self._current_holding_positions()
        backtest_service = BacktestService(self.db_manager)
        items: List[Dict[str, Any]] = []

        for position in positions:
            summary = backtest_service.get_stock_summary(position["code"])
            total = int((summary or {}).get("total_evaluations") or 0)
            completed = int((summary or {}).get("completed_count") or 0)
            insufficient = int((summary or {}).get("insufficient_count") or 0)
            win_rate = (summary or {}).get("win_rate_pct")
            avg_return = (summary or {}).get("avg_simulated_return_pct")
            tone, label, title, message = self._classify_radar_item(
                code=position["code"],
                total=total,
                completed=completed,
                insufficient=insufficient,
                win_rate=win_rate,
                avg_return=avg_return,
            )
            items.append(
                {
                    **position,
                    "tone": tone,
                    "label": label,
                    "title": title,
                    "message": message,
                    "total_evaluations": total,
                    "completed_count": completed,
                    "insufficient_count": insufficient,
                    "win_rate_pct": win_rate if isinstance(win_rate, (int, float)) else None,
                    "avg_simulated_return_pct": avg_return if isinstance(avg_return, (int, float)) else None,
                    "summary": summary,
                }
            )

        tone_order = {"weak": 0, "watch": 1, "strong": 2, "empty": 3, "error": 4}
        items.sort(key=lambda item: (tone_order.get(item["tone"], 9), -float(item.get("market_value_base") or 0)))

        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "holding_count": len(items),
            "items": items,
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

    def _current_holding_positions(self) -> List[Dict[str, Any]]:
        with self.db_manager.get_session() as session:
            rows = (
                session.query(
                    PortfolioPosition.symbol,
                    func.sum(PortfolioPosition.quantity),
                    func.sum(PortfolioPosition.market_value_base),
                    func.max(PortfolioPosition.updated_at),
                )
                .filter(
                    PortfolioPosition.market == "cn",
                    PortfolioPosition.quantity > 0,
                )
                .group_by(PortfolioPosition.symbol)
                .order_by(func.sum(PortfolioPosition.market_value_base).desc())
                .all()
            )
        return [
            {
                "code": str(symbol).strip(),
                "quantity": float(quantity or 0),
                "market_value_base": float(market_value or 0),
                "updated_at": self._to_text(updated_at),
            }
            for symbol, quantity, market_value, updated_at in rows
            if str(symbol).strip()
        ]

    def _market_data_inventory(self, session) -> Dict[str, Any]:
        warehouse = MarketDataWarehouseService(
            self.db_manager,
            project_root=self._project_root(),
        )
        target_preview = warehouse.preview_targets()
        latest_date = session.query(func.max(StockDaily.date)).scalar()
        return {
            "stock_count": self._count_distinct(session, StockDaily.code),
            "bar_count": self._count_rows(session, StockDaily),
            "first_date": self._to_text(session.query(func.min(StockDaily.date)).scalar()),
            "latest_date": self._to_text(latest_date),
            "data_sources": self._stock_data_sources(session),
            "warehouse": warehouse.latest_run(),
            "warehouse_targets": {
                "target_count": target_preview.get("target_count", 0),
                "targets": target_preview.get("targets", []),
            },
            "quality": self._market_data_quality(target_preview.get("targets", []), latest_date),
        }

    def _market_data_quality(self, targets: List[Dict[str, Any]], latest_date: Any) -> Dict[str, Any]:
        latest = latest_date if isinstance(latest_date, date) else None
        stale_cutoff = latest - timedelta(days=1) if latest else None
        items: List[Dict[str, Any]] = []
        summary = {"fresh": 0, "stale": 0, "missing": 0, "skipped": 0}

        for target in targets:
            code = str(target.get("code") or "").strip()
            coverage = target.get("coverage") or {}
            bar_count = int(coverage.get("bar_count") or 0)
            target_latest = coverage.get("latest_date")
            if isinstance(target_latest, str):
                try:
                    target_latest = date.fromisoformat(target_latest[:10])
                except ValueError:
                    target_latest = None

            if code in DEFAULT_WAREHOUSE_SKIPPED_CODES:
                status, label, message = (
                    "skipped",
                    "已跳过",
                    "当前数据源不适合无人值守自动沉淀，已避免拖慢后台任务。",
                )
            elif bar_count <= 0:
                status, label, message = ("missing", "缺失", "本地还没有这只标的的可用日线数据。")
            elif latest and isinstance(target_latest, date) and stale_cutoff and target_latest < stale_cutoff:
                status, label, message = ("stale", "滞后", "本地日线晚于当前数据集最新日期，需要后续刷新补齐。")
            else:
                status, label, message = ("fresh", "正常", "本地日线覆盖正常，可用于回测和复盘。")

            summary[status] += 1
            days_behind = None
            if latest and isinstance(target_latest, date):
                days_behind = max(0, (latest - target_latest).days)
            items.append(
                {
                    "code": code,
                    "status": status,
                    "label": label,
                    "message": message,
                    "bar_count": bar_count,
                    "latest_date": self._to_text(target_latest),
                    "days_behind": days_behind,
                    "sources": target.get("sources") or [],
                }
            )

        priority = {"missing": 0, "stale": 1, "skipped": 2, "fresh": 3}
        items.sort(key=lambda item: (priority.get(item["status"], 9), item["code"]))
        return {
            "status": "ok" if summary["missing"] == 0 and summary["stale"] == 0 else "attention",
            "latest_date": self._to_text(latest),
            "stale_cutoff_date": self._to_text(stale_cutoff),
            "summary": summary,
            "items": items,
        }

    @staticmethod
    def _classify_radar_item(
        *,
        code: str,
        total: int,
        completed: int,
        insufficient: int,
        win_rate: Any,
        avg_return: Any,
    ) -> Tuple[str, str, str, str]:
        if total <= 0:
            return (
                "empty",
                "先分析",
                f"{code}：暂无历史分析",
                "这只持仓还没有可用于回测的历史分析记录，先做几次分析后再看雷达更有意义。",
            )

        if completed <= 0 and insufficient > 0:
            return (
                "empty",
                "待成熟",
                f"{code}：已有分析但回测未成熟",
                "这只持仓已有历史分析记录，但后续行情时间或数据还不够，暂时不能计算胜率和收益。",
            )

        if completed <= 0:
            return (
                "empty",
                "待补齐",
                f"{code}：回测数据不足",
                "这只持仓已有记录，但缺少可完成评估的数据，暂不强行给判断。",
            )

        win = float(win_rate) if isinstance(win_rate, (int, float)) else None
        avg = float(avg_return) if isinstance(avg_return, (int, float)) else None

        if (win is not None and win >= 60) and (avg is None or avg >= 0):
            return (
                "strong",
                "优先关注",
                f"{code}：历史表现较好",
                "已有回测结果偏正向，可作为重点持仓继续跟踪。",
            )

        if (win is not None and win < 45) or (avg is not None and avg < 0):
            return (
                "weak",
                "需要谨慎",
                f"{code}：历史表现偏弱",
                "已有回测结果偏弱，后续更适合谨慎观察仓位风险。",
            )

        return (
            "watch",
            "继续观察",
            f"{code}：结果中性",
            "回测没有明显优势或劣势，继续结合趋势、成本和仓位观察。",
        )

    def _safe_section(self, warnings: List[str], label: str, loader) -> Dict[str, Any]:
        try:
            return loader()
        except Exception as exc:  # pragma: no cover - defensive for old local DB files
            warnings.append(f"{label}读取失败：{exc}")
            return {}

    @staticmethod
    def _count_rows(session, model) -> int:
        return int(session.query(func.count(model.id)).scalar() or 0)

    @staticmethod
    def _count_distinct(session, column) -> int:
        return int(session.query(func.count(func.distinct(column))).scalar() or 0)

    @staticmethod
    def _to_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def _stock_data_sources(self, session) -> List[Dict[str, Any]]:
        rows = (
            session.query(StockDaily.data_source, func.count(StockDaily.id))
            .group_by(StockDaily.data_source)
            .order_by(func.count(StockDaily.id).desc())
            .limit(8)
            .all()
        )
        return [
            {"name": source or "未知来源", "count": int(count or 0)}
            for source, count in rows
        ]

    def _database_inventory(self) -> Dict[str, Any]:
        path = self._sqlite_database_path()
        size_bytes = self._path_size(path) if path else 0
        return {
            "path": str(path) if path else None,
            "exists": bool(path and path.exists()),
            "size_bytes": size_bytes,
            "size_label": self._format_bytes(size_bytes),
        }

    def _sqlite_database_path(self) -> Optional[Path]:
        database = getattr(self.db_manager._engine.url, "database", None)
        if not database or database == ":memory:":
            return None
        return Path(database).expanduser().resolve()

    def _file_inventories(self, database_path: Optional[str]) -> List[Dict[str, Any]]:
        config = get_config()
        candidates: List[Tuple[str, str, Optional[Path]]] = [
            ("database", "本地数据库", Path(database_path) if database_path else None),
            ("reports", "报告文件", self._resolve_project_path("reports")),
            ("data", "数据目录", self._resolve_project_path("data")),
            ("cache", "本地缓存", self._resolve_project_path(".cache")),
        ]

        report_paths = [
            self._resolve_project_path(raw.strip())
            for raw in str(getattr(config, "external_tactical_report_path", "") or "").split(",")
            if raw.strip()
        ]
        for index, path in enumerate(report_paths, start=1):
            candidates.append((f"tactical_report_{index}", "外部战术报告", path))

        seen: set[str] = set()
        files: List[Dict[str, Any]] = []
        for key, label, path in candidates:
            if path is None:
                continue
            resolved = path.expanduser().resolve()
            marker = str(resolved)
            if marker in seen:
                continue
            seen.add(marker)
            file_count, size_bytes = self._path_inventory(resolved)
            files.append(
                {
                    "key": key,
                    "label": label,
                    "path": marker,
                    "exists": resolved.exists(),
                    "file_count": file_count,
                    "size_bytes": size_bytes,
                    "size_label": self._format_bytes(size_bytes),
                }
            )
        return files

    def _resolve_project_path(self, raw_path: str) -> Path:
        path = Path(raw_path).expanduser()
        if path.is_absolute():
            return path
        return self._project_root() / path

    def _project_root(self) -> Path:
        db_path = self._sqlite_database_path()
        if db_path and db_path.parent.name == "data":
            return db_path.parent.parent

        cwd = Path.cwd().resolve()
        if (cwd / "src").exists() and (cwd / "api").exists():
            return cwd
        source_root = Path(__file__).resolve().parents[2]
        if (source_root / "src").exists() and (source_root / "api").exists():
            return source_root
        return cwd

    def _path_inventory(self, path: Path) -> Tuple[int, int]:
        if not path.exists():
            return 0, 0
        if path.is_file():
            return 1, self._path_size(path)

        file_count = 0
        size_bytes = 0
        for root, dirs, files in os.walk(path):
            dirs[:] = [name for name in dirs if name not in {".git", "node_modules", ".venv", ".venv311"}]
            for name in files:
                file_path = Path(root) / name
                file_count += 1
                size_bytes += self._path_size(file_path)
        return file_count, size_bytes

    @staticmethod
    def _path_size(path: Optional[Path]) -> int:
        if path is None or not path.exists() or not path.is_file():
            return 0
        try:
            return int(path.stat().st_size)
        except OSError:
            return 0

    @staticmethod
    def _format_bytes(size_bytes: int) -> str:
        size = float(size_bytes or 0)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1024 or unit == "TB":
                if unit == "B":
                    return f"{int(size)} {unit}"
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"

    def _recommendations(
        self,
        *,
        market_data: Dict[str, Any],
        analysis: Dict[str, Any],
        backtests: Dict[str, Any],
        portfolio: Dict[str, Any],
        maintenance: Dict[str, Any],
        ai_routing: Dict[str, Any],
        warnings: Iterable[str],
    ) -> List[Dict[str, str]]:
        items: List[Dict[str, str]] = []

        if warnings:
            items.append(
                {
                    "level": "warning",
                    "title": "有部分本地数据读取失败",
                    "description": "先不做自动修复，避免误动旧数据；需要时可以逐项排查。",
                }
            )
        if int(market_data.get("bar_count") or 0) == 0:
            items.append(
                {
                    "level": "warning",
                    "title": "本地行情数据还没有沉淀",
                    "description": "后续可以把常用股票、ETF、IC指数行情缓存到本机，减少重复联网。",
                }
            )
        warehouse = market_data.get("warehouse") or {}
        warehouse_status = str(warehouse.get("status") or "")
        if warehouse_status in {"missing", "unreadable", "error"}:
            items.append(
                {
                    "level": "warning",
                    "title": "自动行情沉淀还没有形成稳定记录",
                    "description": "需要先跑一次持仓/自选历史行情补齐，之后每天收盘后自动追加到本地数据库。",
                }
            )
        elif warehouse_status == "partial":
            items.append(
                {
                    "level": "warning",
                    "title": "最近一次行情沉淀部分成功",
                    "description": "部分股票接口失败，已保留成功数据；下次任务会继续补齐缺口。",
                }
            )
        elif warehouse_status == "ok":
            items.append(
                {
                    "level": "success",
                    "title": "自动行情沉淀已启用",
                    "description": "持仓和自选的历史行情会持续写入本机数据库，后续回测和复盘可以直接复用。",
                }
            )
        if int(portfolio.get("position_count") or 0) > 0 and int(backtests.get("result_count") or 0) == 0:
            items.append(
                {
                    "level": "info",
                    "title": "持仓已有数据，回测还可以继续补强",
                    "description": "下一步适合让持仓股自动回测，并把结果固定显示在首页。",
                }
            )
        if int(analysis.get("report_count") or 0) > 0 and int(backtests.get("summary_count") or 0) > 0:
            items.append(
                {
                    "level": "success",
                    "title": "分析与回测链路已经开始闭环",
                    "description": "这台机器可以继续承担本地数据沉淀、批量回测和长期复盘。",
                }
            )
        portfolio_review = maintenance.get("portfolio_daily_review") or {}
        if portfolio_review.get("status") == "missing":
            items.append(
                {
                    "level": "info",
                    "title": "每日持仓复盘将在收盘后沉淀",
                    "description": "已配置为本地生成报告，先写入本机，不默认推送，避免无效打扰。",
                }
            )
        cleanup = maintenance.get("cleanup") or {}
        if cleanup.get("status") in {"missing", "unreadable"}:
            items.append(
                {
                    "level": "info",
                    "title": "旧日志和临时缓存会自动瘦身",
                    "description": "清理范围只覆盖旧日志和临时缓存，不碰数据库、报告和持仓数据。",
                }
            )
        if (ai_routing.get("cloud_analysis") or {}).get("enabled"):
            items.append(
                {
                    "level": "success",
                    "title": "云端 AI 与本地工作站分工已明确",
                    "description": "本机负责数据和回测，复杂判断继续走云端模型，本地模型不作为默认分析入口。",
                }
            )
        if not items:
            items.append(
                {
                    "level": "info",
                    "title": "当前适合先做数据沉淀",
                    "description": "高配置机器最有价值的方向是长期保存行情、持仓、回测和报告，而不是只运行本地大模型。",
                }
            )
        return items
