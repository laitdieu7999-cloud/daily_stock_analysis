from __future__ import annotations

import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from src.config import get_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEDULER_LABEL = "com.laitdieu.daily-stock-analysis.scheduler"
WORKSTATION_HEALTH_LABEL = "com.laitdieu.daily-stock-analysis.workstation-health"
STOCK_INTRADAY_LABEL = "com.laitdieu.daily-stock-analysis.stock-intraday"


def _looks_like_project_root(path: Path) -> bool:
    return (path / "main.py").exists() and (path / "src").is_dir()


def _resolve_default_project_root() -> Path:
    """Prefer the editable workspace over the packaged app internals."""
    candidates: List[Path] = []
    for raw in (
        os.getenv("DSA_PROJECT_ROOT"),
        os.getenv("PWD"),
    ):
        if raw:
            candidates.append(Path(raw).expanduser())

    candidates.extend(
        [
            Path.cwd(),
            Path.home() / "Documents" / "github" / "daily_stock_analysis",
            PROJECT_ROOT,
        ]
    )

    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        if _looks_like_project_root(resolved):
            return resolved
    return PROJECT_ROOT


def _is_process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _size_label(size_bytes: int) -> str:
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size_bytes} B"


def _resolve_path(base: Path, raw_path: str | Path | None, fallback: Path) -> Path:
    if raw_path:
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = base / path
        return path
    return fallback


def _file_info(key: str, label: str, path: Path) -> Dict[str, Any]:
    exists = path.exists()
    if path.is_dir():
        files = [item for item in path.rglob("*") if item.is_file()]
        size = sum(item.stat().st_size for item in files)
        file_count = len(files)
    elif path.is_file():
        size = path.stat().st_size
        file_count = 1
    else:
        size = 0
        file_count = 0
    return {
        "key": key,
        "label": label,
        "path": str(path),
        "exists": exists,
        "file_count": file_count,
        "size_bytes": size,
        "size_label": _size_label(size),
        "modified_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds") if exists else None,
    }


def _count_jsonl(path: Path) -> int:
    if not path.exists() or not path.is_file():
        return 0
    count = 0
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if line.strip():
            count += 1
    return count


def _latest_file(root: Path, patterns: Iterable[str]) -> Optional[Path]:
    candidates: List[Path] = []
    for pattern in patterns:
        candidates.extend(path for path in root.glob(pattern) if path.is_file())
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.stat().st_mtime)


def _modified_at(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


def _age_minutes(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    modified = datetime.fromtimestamp(path.stat().st_mtime)
    return int(max((datetime.now() - modified).total_seconds(), 0) // 60)


def _age_seconds(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    modified = datetime.fromtimestamp(path.stat().st_mtime)
    return int(max((datetime.now() - modified).total_seconds(), 0))


def _is_recent(path: Path, *, max_age_minutes: int) -> bool:
    age = _age_minutes(path)
    return age is not None and age <= max_age_minutes


class SystemOverviewService:
    """Build a read-only control-plane overview for the local trading system."""

    def __init__(
        self,
        *,
        project_root: str | Path | None = None,
        home_dir: str | Path | None = None,
        config: Any = None,
    ) -> None:
        self.project_root = Path(project_root) if project_root is not None else _resolve_default_project_root()
        self.home_dir = Path(home_dir) if home_dir is not None else Path.home()
        self.config = config

    def build_overview(self) -> Dict[str, Any]:
        config = self.config or get_config()
        reports_dir = self.project_root / "reports"
        signal_dir = reports_dir / "signal_events"
        desktop_dir = self.home_dir / "Desktop"
        desktop_history_dir = desktop_dir / "每日分析报告"

        priorities = self._build_priorities()
        scheduler = self._build_scheduler_status(config)
        files = self._build_files(reports_dir, signal_dir, desktop_dir, desktop_history_dir)
        modules = self._build_modules(config, reports_dir, signal_dir, files)
        services = self._build_services(config)
        data_warehouse = self._build_data_warehouse(config, reports_dir)
        alerts = self._build_alerts(
            modules=modules,
            scheduler=scheduler,
            services=services,
            data_warehouse=data_warehouse,
        )

        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "scheduler": scheduler,
            "services": services,
            "data_warehouse": data_warehouse,
            "alerts": alerts,
            "priorities": priorities,
            "modules": modules,
            "files": files,
            "recommendations": self._build_recommendations(
                modules=modules,
                scheduler=scheduler,
                services=services,
                data_warehouse=data_warehouse,
                alerts=alerts,
            ),
        }

    def _build_priorities(self) -> List[Dict[str, Any]]:
        return [
            {
                "priority": "P0",
                "label": "黑天鹅 / 系统级风险",
                "notify_rule": "必须提醒",
                "archive_rule": "长期归档",
                "status": "active",
            },
            {
                "priority": "P1",
                "label": "持仓风控",
                "notify_rule": "必须提醒",
                "archive_rule": "长期归档",
                "status": "active",
            },
            {
                "priority": "P2",
                "label": "自选股买入",
                "notify_rule": "仅买入提醒",
                "archive_rule": "必要时归档",
                "status": "active",
            },
            {
                "priority": "P3",
                "label": "IC/期权 Shadow",
                "notify_rule": "不提醒",
                "archive_rule": "只写账本",
                "status": "shadow",
            },
            {
                "priority": "P4",
                "label": "Gemini 外部观点",
                "notify_rule": "不提醒",
                "archive_rule": "只做对比归档",
                "status": "silent",
            },
        ]

    def _build_scheduler_status(self, config: Any) -> Dict[str, Any]:
        lock_path = self.home_dir / ".dsa_schedule.lock"
        lock_pid: Optional[int] = None
        if lock_path.exists():
            try:
                lock_pid = int(lock_path.read_text(encoding="utf-8").strip() or "0")
            except Exception:
                lock_pid = None
        scheduler_launch = self._launch_agent_status(SCHEDULER_LABEL)
        return {
            "schedule_enabled": bool(getattr(config, "schedule_enabled", False)),
            "schedule_time": str(getattr(config, "schedule_time", "")),
            "nightly_market_outlook_enabled": bool(getattr(config, "nightly_market_outlook_enabled", False)),
            "nightly_market_outlook_time": str(getattr(config, "nightly_market_outlook_time", "22:30")),
            "stock_intraday_reminder_enabled": bool(getattr(config, "stock_intraday_reminder_enabled", False)),
            "lock_path": str(lock_path),
            "lock_pid": lock_pid,
            "lock_alive": _is_process_alive(lock_pid or 0),
            "launch_agent_label": SCHEDULER_LABEL,
            "launch_agent_alive": bool(scheduler_launch.get("running")),
            "launch_agent_pid": scheduler_launch.get("pid"),
        }

    def _build_services(self, config: Any) -> List[Dict[str, Any]]:
        scheduler_launch = self._launch_agent_status(SCHEDULER_LABEL)
        health_launch = self._launch_agent_status(WORKSTATION_HEALTH_LABEL)
        stock_intraday_launch = self._launch_agent_status(STOCK_INTRADAY_LABEL)
        app_support = self.home_dir / "Library" / "Application Support" / "Daily Stock Analysis"
        latest_app_log = _latest_file(app_support / "logs", ["stock_analysis_*.log", "launchagent.stderr.log"])
        stock_intraday_log = app_support / "logs" / "stock-intraday.stdout.log"
        stock_intraday_heartbeat = self.project_root / "reports" / "stock_intraday_heartbeat.json"
        health_plist = self.home_dir / "Library" / "LaunchAgents" / f"{WORKSTATION_HEALTH_LABEL}.plist"
        stock_intraday_plist = self.home_dir / "Library" / "LaunchAgents" / f"{STOCK_INTRADAY_LABEL}.plist"
        latest_health_ledger = _latest_file(
            self.project_root / "reports" / "system_health_archive",
            ["*_workstation_health.jsonl"],
        )
        health_ready = bool(
            health_launch.get("running")
            or (health_plist.exists() and latest_health_ledger and _is_recent(latest_health_ledger, max_age_minutes=45))
        )
        stock_intraday_recent = _is_recent(stock_intraday_heartbeat, max_age_minutes=5) or _is_recent(
            stock_intraday_log,
            max_age_minutes=5,
        )
        stock_intraday_enabled = bool(getattr(config, "stock_intraday_reminder_enabled", False))
        stock_intraday_detail = (
            str(stock_intraday_heartbeat)
            if stock_intraday_heartbeat.exists()
            else (str(stock_intraday_log) if stock_intraday_log.exists() else STOCK_INTRADAY_LABEL)
        )
        desktop_backend_open = self._is_port_open("127.0.0.1", 8000)
        if not stock_intraday_enabled:
            stock_intraday_status = "warning"
            stock_intraday_detail = "盘中实时监控未启用"
        elif not stock_intraday_plist.exists() and not stock_intraday_launch.get("running"):
            stock_intraday_status = "critical"
            stock_intraday_detail = f"LaunchAgent 未安装或未加载: {STOCK_INTRADAY_LABEL}"
        elif not stock_intraday_recent:
            stock_intraday_status = "critical"
            stock_intraday_detail = (
                "最近心跳/日志超过 5 分钟未更新；"
                f"heartbeat_age_sec={_age_seconds(stock_intraday_heartbeat)}, "
                f"log_age_sec={_age_seconds(stock_intraday_log)}"
            )
        else:
            stock_intraday_status = "active"
        return [
            {
                "key": "desktop_backend",
                "name": "桌面端服务",
                "status": "active" if desktop_backend_open else "warning",
                "detail": (
                    "127.0.0.1:8000 本地接口已启动"
                    if desktop_backend_open
                    else "桌面窗口未打开；后台定时任务和盘中监控可独立运行"
                ),
                "pid": self._find_process_pid("stock_analysis.*--serve-only"),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            },
            {
                "key": "scheduler",
                "name": "股票定时任务",
                "status": "active" if scheduler_launch.get("running") else "critical",
                "detail": SCHEDULER_LABEL,
                "pid": scheduler_launch.get("pid"),
                "updated_at": scheduler_launch.get("checked_at"),
            },
            {
                "key": "stock_intraday_realtime",
                "name": "盘中实时监控",
                "status": stock_intraday_status,
                "detail": stock_intraday_detail,
                "pid": stock_intraday_launch.get("pid"),
                "updated_at": (
                    _modified_at(stock_intraday_heartbeat)
                    if stock_intraday_heartbeat.exists()
                    else (_modified_at(stock_intraday_log) if stock_intraday_log.exists() else stock_intraday_launch.get("checked_at"))
                ),
            },
            {
                "key": "workstation_health",
                "name": "工作站健康巡检",
                "status": "active" if health_ready else "warning",
                "detail": str(latest_health_ledger) if latest_health_ledger else "每 15 分钟写入健康账本，严重异常时发 macOS 通知",
                "pid": health_launch.get("pid"),
                "updated_at": _modified_at(latest_health_ledger) if latest_health_ledger else health_launch.get("checked_at"),
            },
            {
                "key": "latest_log",
                "name": "最近运行日志",
                "status": "active" if latest_app_log and _is_recent(latest_app_log, max_age_minutes=180) else "warning",
                "detail": str(latest_app_log) if latest_app_log else "未找到最近日志",
                "pid": None,
                "updated_at": _modified_at(latest_app_log) if latest_app_log else None,
            },
        ]

    def _build_data_warehouse(self, config: Any, reports_dir: Path) -> Dict[str, Any]:
        database_path = _resolve_path(
            self.project_root,
            getattr(config, "database_path", None),
            self.project_root / "data" / "stock_analysis.db",
        )
        database = self._database_status(database_path)
        report_archive = self._archive_status(reports_dir)
        health_archive = self._health_archive_status(reports_dir)
        disk = self._disk_status(database_path.parent if database_path.parent.exists() else self.project_root)
        return {
            "database": database,
            "reports": report_archive,
            "health_archive": health_archive,
            "disk": disk,
        }

    def _database_status(self, database_path: Path) -> Dict[str, Any]:
        status: Dict[str, Any] = {
            "path": str(database_path),
            "exists": database_path.exists(),
            "size_bytes": database_path.stat().st_size if database_path.exists() else 0,
            "size_label": _size_label(database_path.stat().st_size) if database_path.exists() else "0 B",
            "modified_at": _modified_at(database_path),
            "age_minutes": _age_minutes(database_path),
            "tables": {},
            "latest_stock_date": None,
            "latest_analysis_at": None,
            "latest_backtest_at": None,
            "latest_portfolio_at": None,
        }
        if not database_path.exists():
            status["status"] = "critical"
            return status

        try:
            conn = sqlite3.connect(str(database_path))
            cur = conn.cursor()
            table_queries = {
                "analysis_history": "select count(*) from analysis_history",
                "backtest_results": "select count(*) from backtest_results",
                "backtest_summaries": "select count(*) from backtest_summaries",
                "portfolio_positions": "select count(*) from portfolio_positions",
                "stock_daily": "select count(*) from stock_daily",
            }
            for key, query in table_queries.items():
                try:
                    cur.execute(query)
                    status["tables"][key] = int(cur.fetchone()[0] or 0)
                except sqlite3.Error:
                    status["tables"][key] = 0
            for target, query in {
                "latest_stock_date": "select max(date) from stock_daily",
                "latest_analysis_at": "select max(created_at) from analysis_history",
                "latest_backtest_at": "select max(evaluated_at) from backtest_results",
                "latest_portfolio_at": "select max(updated_at) from portfolio_positions",
            }.items():
                try:
                    cur.execute(query)
                    status[target] = cur.fetchone()[0]
                except sqlite3.Error:
                    status[target] = None
            conn.close()
            status["status"] = "active" if status["tables"].get("stock_daily", 0) > 0 else "warning"
        except sqlite3.Error as exc:
            status["status"] = "critical"
            status["error"] = str(exc)
        return status

    def _archive_status(self, reports_dir: Path) -> Dict[str, Any]:
        files = [item for item in reports_dir.rglob("*") if item.is_file()] if reports_dir.exists() else []
        latest = max(files, key=lambda item: item.stat().st_mtime) if files else None
        return {
            "path": str(reports_dir),
            "exists": reports_dir.exists(),
            "file_count": len(files),
            "size_bytes": sum(item.stat().st_size for item in files),
            "size_label": _size_label(sum(item.stat().st_size for item in files)),
            "latest_file": str(latest) if latest else None,
            "modified_at": _modified_at(latest) if latest else None,
            "status": "active" if files else "warning",
        }

    def _health_archive_status(self, reports_dir: Path) -> Dict[str, Any]:
        archive_dir = reports_dir / "system_health_archive"
        files = sorted(archive_dir.glob("*_workstation_health.jsonl")) if archive_dir.exists() else []
        latest = max(files, key=lambda item: item.stat().st_mtime) if files else None
        return {
            "path": str(archive_dir),
            "exists": archive_dir.exists(),
            "file_count": len(files),
            "latest_file": str(latest) if latest else None,
            "modified_at": _modified_at(latest) if latest else None,
            "status": "active" if latest and _is_recent(latest, max_age_minutes=45) else "warning",
        }

    def _disk_status(self, path: Path) -> Dict[str, Any]:
        usage = shutil.disk_usage(path)
        free_ratio = usage.free / usage.total if usage.total else 0.0
        status = "active"
        if free_ratio < 0.05:
            status = "critical"
        elif free_ratio < 0.15:
            status = "warning"
        return {
            "path": str(path),
            "total_bytes": usage.total,
            "used_bytes": usage.used,
            "free_bytes": usage.free,
            "total_label": _size_label(usage.total),
            "used_label": _size_label(usage.used),
            "free_label": _size_label(usage.free),
            "free_pct": round(free_ratio * 100, 2),
            "status": status,
        }

    def _build_alerts(
        self,
        *,
        modules: List[Dict[str, Any]],
        scheduler: Dict[str, Any],
        services: List[Dict[str, Any]],
        data_warehouse: Dict[str, Any],
    ) -> List[Dict[str, str]]:
        alerts: List[Dict[str, str]] = []
        for item in services:
            if item.get("status") == "critical":
                alerts.append(
                    {
                        "level": "critical",
                        "title": f"{item['name']}异常",
                        "description": str(item.get("detail") or "需要检查服务是否仍在运行。"),
                    }
                )
            elif item.get("status") == "warning":
                alerts.append(
                    {
                        "level": "warning",
                        "title": f"{item['name']}需要确认",
                        "description": str(item.get("detail") or "状态不是完全就绪。"),
                    }
                )
        if not scheduler.get("launch_agent_alive"):
            alerts.append(
                {
                    "level": "critical",
                    "title": "股票定时任务未运行",
                    "description": "LaunchAgent 未显示为 running，早盘/盘中自动任务可能不会执行。",
                }
            )
        database = data_warehouse.get("database", {})
        if database.get("status") == "critical":
            alerts.append(
                {
                    "level": "critical",
                    "title": "本地数据库不可用",
                    "description": str(database.get("error") or database.get("path") or "数据库文件缺失或无法读取。"),
                }
            )
        disk = data_warehouse.get("disk", {})
        if disk.get("status") in {"warning", "critical"}:
            alerts.append(
                {
                    "level": str(disk.get("status")),
                    "title": "磁盘空间需要关注",
                    "description": f"剩余 {disk.get('free_label')}，约 {disk.get('free_pct')}%。",
                }
            )
        if any(item.get("status") == "warning" for item in modules):
            alerts.append(
                {
                    "level": "warning",
                    "title": "部分业务模块未完全就绪",
                    "description": "系统总览中仍有 warning 模块，建议优先处理持仓、自选或外部观点缓存缺口。",
                }
            )
        return alerts

    def _build_files(
        self,
        reports_dir: Path,
        signal_dir: Path,
        desktop_dir: Path,
        desktop_history_dir: Path,
    ) -> List[Dict[str, Any]]:
        latest_nightly = _latest_file(reports_dir / "nightly_market_outlook_archive", ["*_明日大盘预判.md"])
        latest_comparison = _latest_file(reports_dir / "nightly_market_outlook_comparison", ["*_明日大盘预判对比.md"])
        return [
            _file_info("signal_contract", "信号路由契约", self.project_root / "docs" / "SIGNAL_ROUTING.md"),
            _file_info("gemini_daily", "Gemini日常缓存", reports_dir / "gemini_daily.md"),
            _file_info("gemini_black_swan", "Gemini黑天鹅缓存", reports_dir / "gemini_black_swan.md"),
            _file_info("latest_nightly_outlook", "最新明日大盘预判", latest_nightly or reports_dir / "nightly_market_outlook_archive"),
            _file_info("latest_gemini_comparison", "最新本机vsGemini对比", latest_comparison or reports_dir / "nightly_market_outlook_comparison"),
            _file_info("black_swan_signals", "P0黑天鹅信号账本", signal_dir / "black_swan_events.jsonl"),
            _file_info("gemini_external_signals", "P4外部观点账本", signal_dir / "gemini_external_views.jsonl"),
            _file_info("ic_shadow_signals", "P3 IC Shadow账本", signal_dir / "ic_shadow_events.jsonl"),
            _file_info("stock_intraday_replay_ledger", "盘中提醒回放账本", reports_dir / "stock_intraday_replay_ledger.jsonl"),
            _file_info("stock_intraday_heartbeat", "盘中实时监控心跳", reports_dir / "stock_intraday_heartbeat.json"),
            _file_info("stock_intraday_errors", "盘中实时监控异常账本", reports_dir / "stock_intraday_errors.jsonl"),
            _file_info("sniper_point_downgrade_audit", "狙击点位保护账本", reports_dir / "sniper_point_downgrade_audit.jsonl"),
            _file_info("sniper_point_downgrade_summary", "狙击点位保护摘要", reports_dir / "sniper_point_downgrade_summary.md"),
            _file_info("desktop_reports", "桌面最新报告", desktop_dir),
            _file_info("desktop_report_history", "桌面历史报告文件夹", desktop_history_dir),
        ]

    def _build_modules(
        self,
        config: Any,
        reports_dir: Path,
        signal_dir: Path,
        files: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        file_map = {item["key"]: item for item in files}
        watchlist_count = len(list(getattr(config, "watchlist_stock_list", []) or []))
        holding_count = len(list(getattr(config, "stock_list", []) or []))
        return [
            {
                "key": "routing_gateway",
                "name": "路由与通知网关",
                "priority": "control",
                "status": "active" if file_map["signal_contract"]["exists"] else "warning",
                "notify_rule": "统一裁决P0-P4是否提醒",
                "archive_path": file_map["signal_contract"]["path"],
                "detail": "SignalEvent 已成为持仓、自选、Gemini、IC Shadow 的统一入口。",
            },
            {
                "key": "black_swan",
                "name": "黑天鹅监控",
                "priority": "P0",
                "status": "active" if file_map["gemini_black_swan"]["exists"] else "warning",
                "notify_rule": "当天段落明确已触发才强提醒",
                "archive_path": file_map["black_swan_signals"]["path"],
                "detail": f"已归档 {_count_jsonl(signal_dir / 'black_swan_events.jsonl')} 条P0事件。",
            },
            {
                "key": "holding_risk",
                "name": "持仓风控",
                "priority": "P1",
                "status": "active" if holding_count > 0 else "warning",
                "notify_rule": "持仓破位/风控信号必须提醒",
                "archive_path": file_map["stock_intraday_replay_ledger"]["path"],
                "detail": f"当前主监控名单 {holding_count} 个标的。",
            },
            {
                "key": "watchlist_buy",
                "name": "自选股买入",
                "priority": "P2",
                "status": "active" if watchlist_count > 0 else "warning",
                "notify_rule": "只提醒尾盘击球区买入信号",
                "archive_path": file_map["stock_intraday_replay_ledger"]["path"],
                "detail": f"当前自选股 {watchlist_count} 个标的，非买入信号静默。",
            },
            {
                "key": "ic_shadow",
                "name": "IC/期权 Shadow",
                "priority": "P3",
                "status": "shadow",
                "notify_rule": "只归档，不提醒",
                "archive_path": file_map["ic_shadow_signals"]["path"],
                "detail": f"统一P3账本 {_count_jsonl(signal_dir / 'ic_shadow_events.jsonl')} 条，原始账本 {_count_jsonl(reports_dir / 'ic_m1_m2_shadow_monitoring_events.jsonl')} 条。",
            },
            {
                "key": "gemini_external",
                "name": "Gemini外部观点",
                "priority": "P4",
                "status": "silent" if file_map["gemini_daily"]["exists"] or file_map["gemini_black_swan"]["exists"] else "warning",
                "notify_rule": "只归档和对比，不提醒",
                "archive_path": file_map["gemini_external_signals"]["path"],
                "detail": f"P4观点账本 {_count_jsonl(signal_dir / 'gemini_external_views.jsonl')} 条。",
            },
            {
                "key": "sniper_point_guard",
                "name": "狙击点位保护",
                "priority": "control",
                "status": "active" if file_map["sniper_point_downgrade_summary"]["exists"] else "warning",
                "notify_rule": "点位异常只降级展示，不直接给可执行价格",
                "archive_path": file_map["sniper_point_downgrade_audit"]["path"],
                "detail": f"已拦截 {_count_jsonl(reports_dir / 'sniper_point_downgrade_audit.jsonl')} 条异常点位或行情上下文冲突。",
            },
            {
                "key": "nightly_outlook",
                "name": "22:30明日大盘预判",
                "priority": "scheduled",
                "status": "active" if bool(getattr(config, "nightly_market_outlook_enabled", False)) else "warning",
                "notify_rule": "定时生成并推送独立报告",
                "archive_path": file_map["latest_nightly_outlook"]["path"],
                "detail": f"计划时间 {getattr(config, 'nightly_market_outlook_time', '22:30')}",
            },
        ]

    def _build_recommendations(
        self,
        *,
        modules: List[Dict[str, Any]],
        scheduler: Dict[str, Any],
        services: List[Dict[str, Any]],
        data_warehouse: Dict[str, Any],
        alerts: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        recommendations: List[Dict[str, str]] = []
        if any(item.get("level") == "critical" for item in alerts):
            recommendations.append(
                {
                    "level": "warning",
                    "title": "存在需要马上处理的工作站异常",
                    "description": "先看上方失败提醒，确认桌面端、定时任务、数据库和磁盘是否正常。",
                }
            )
        if not scheduler.get("launch_agent_alive"):
            recommendations.append(
                {
                    "level": "warning",
                    "title": "后台调度未确认存活",
                    "description": "系统总览未检测到有效 LaunchAgent。真实运行巡检时需要先恢复定时任务。",
                }
            )
        health_service = next((item for item in services if item.get("key") == "workstation_health"), None)
        if health_service and health_service.get("status") != "active":
            recommendations.append(
                {
                    "level": "warning",
                    "title": "建议启用工作站健康巡检",
                    "description": "启用后会定时写入健康账本，服务停止或数据库异常时可主动提醒。",
                }
            )
        health_archive = data_warehouse.get("health_archive", {})
        if health_archive.get("status") != "active":
            recommendations.append(
                {
                    "level": "warning",
                    "title": "健康账本还没有稳定沉淀",
                    "description": "等待工作站巡检运行一到两轮后，这里应显示最近 45 分钟内的健康记录。",
                }
            )
        if any(item.get("status") == "warning" for item in modules):
            recommendations.append(
                {
                    "level": "warning",
                    "title": "存在未完全就绪模块",
                    "description": "优先检查状态为 warning 的模块，避免以为已运行但实际只停留在配置或缓存层。",
                }
            )
        if not recommendations:
            recommendations.append(
                {
                    "level": "success",
                    "title": "控制面状态清晰",
                    "description": "服务、数据沉淀、P0-P4 路由和健康巡检都处于可用状态。",
                }
            )
        return recommendations

    def _launch_agent_status(self, label: str) -> Dict[str, Any]:
        checked_at = datetime.now().isoformat(timespec="seconds")
        try:
            uid = subprocess.check_output(["id", "-u"], text=True).strip()
            result = subprocess.run(
                ["launchctl", "print", f"gui/{uid}/{label}"],
                capture_output=True,
                text=True,
                check=False,
                timeout=2,
            )
        except Exception as exc:
            return {
                "label": label,
                "running": False,
                "pid": None,
                "checked_at": checked_at,
                "error": str(exc),
            }

        output = (result.stdout or "") + (result.stderr or "")
        pid: Optional[int] = None
        for line in output.splitlines():
            stripped = line.strip()
            if stripped.startswith("pid = "):
                try:
                    pid = int(stripped.split("=", 1)[1].strip())
                except ValueError:
                    pid = None
                break
        return {
            "label": label,
            "running": result.returncode == 0 and ("state = running" in output or (pid is not None and _is_process_alive(pid))),
            "pid": pid,
            "checked_at": checked_at,
            "error": None if result.returncode == 0 else output.strip()[:500],
        }

    @staticmethod
    def _is_port_open(host: str, port: int) -> bool:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            return False

    @staticmethod
    def _find_process_pid(pattern: str) -> Optional[int]:
        current_command = " ".join([sys.executable, *sys.argv])
        try:
            if re.search(pattern, current_command):
                return os.getpid()
        except re.error:
            if pattern in current_command:
                return os.getpid()

        try:
            result = subprocess.run(
                ["pgrep", "-f", pattern],
                capture_output=True,
                text=True,
                check=False,
                timeout=2,
            )
        except Exception:
            return None
        for line in result.stdout.splitlines():
            try:
                return int(line.strip())
            except ValueError:
                continue
        return None
