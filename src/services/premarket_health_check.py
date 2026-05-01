from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from src.config import get_config
from src.services.system_overview_service import SystemOverviewService


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _status_from_alerts(alerts: Iterable[Dict[str, Any]]) -> str:
    levels = {str(item.get("level") or "").lower() for item in alerts}
    if "critical" in levels:
        return "critical"
    if "warning" in levels:
        return "warning"
    return "ok"


def _status_label(status: str) -> str:
    return {
        "ok": "系统已就绪",
        "warning": "存在非致命警告",
        "critical": "存在关键异常",
    }.get(status, "状态未知")


def _worse_status(left: str, right: str) -> str:
    rank = {"ok": 0, "warning": 1, "critical": 2}
    return left if rank.get(left, 0) >= rank.get(right, 0) else right


def _status_from_observation(observation: Dict[str, Any]) -> str:
    counts = observation.get("counts") or {}
    if int(counts.get("notification_rate_limit") or 0) > 0:
        return "warning"
    if int(counts.get("errors") or 0) >= 10 or int(counts.get("search_rate_limit") or 0) >= 10:
        return "warning"
    return "ok"


def _latest_file(directory: Path, patterns: Iterable[str]) -> Optional[Path]:
    files: List[Path] = []
    if directory.exists():
        for pattern in patterns:
            files.extend(path for path in directory.glob(pattern) if path.is_file())
    if not files:
        return None
    return max(files, key=lambda path: path.stat().st_mtime)


def _tail_text(path: Optional[Path], *, max_bytes: int = 400_000) -> str:
    if path is None or not path.exists():
        return ""
    try:
        size = path.stat().st_size
        with path.open("rb") as handle:
            if size > max_bytes:
                handle.seek(max(0, size - max_bytes))
            return handle.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _count_patterns(text: str) -> Dict[str, int]:
    lines = text.splitlines()
    notification_rate_limit = sum(
        1
        for line in lines
        if re.search(r"飞书|feishu|lark", line, flags=re.I)
        and re.search(r"frequency limited|rate limit|限流|频控", line, flags=re.I)
    )
    search_rate_limit = sum(
        1
        for line in lines
        if re.search(r"SearXNG|搜索|search", line, flags=re.I)
        and re.search(r"限流|退避|rate limit", line, flags=re.I)
    )
    return {
        "errors": len(re.findall(r"\b(ERROR|CRITICAL)\b|Traceback", text)),
        "feishu_success": text.count("飞书消息发送成功"),
        "notification_rate_limit": notification_rate_limit,
        "search_rate_limit": search_rate_limit,
        "after_close_skips": text.count("非A股盘中交易时段"),
        "stock_reminder_sent": len(re.findall(r"个股推送: True|StockIntradayReminder.*sent", text)),
    }


def _build_log_observation(project_root: Path, home_dir: Path) -> Dict[str, Any]:
    app_log_dir = home_dir / "Library" / "Application Support" / "Daily Stock Analysis" / "logs"
    latest_app_log = _latest_file(app_log_dir, ["stock_analysis_*.log"])
    latest_debug_log = _latest_file(app_log_dir, ["stock_analysis_debug_*.log"])
    latest_stdout = app_log_dir / "launchagent.stdout.log"
    latest_stderr = app_log_dir / "launchagent.stderr.log"

    combined = "\n".join(
        _tail_text(path)
        for path in (latest_app_log, latest_debug_log, latest_stdout, latest_stderr)
    )
    counts = _count_patterns(combined)

    route_state = project_root / "reports" / "stock_intraday_reminder_state_route_state.json"
    if not route_state.exists():
        route_state = project_root / "reports" / "stock_intraday_reminder_route_state.json"

    return {
        "latest_app_log": str(latest_app_log) if latest_app_log else "",
        "latest_debug_log": str(latest_debug_log) if latest_debug_log else "",
        "route_state_path": str(route_state),
        "route_state_exists": route_state.exists(),
        "counts": counts,
    }


def _format_services(services: List[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    for item in services:
        status = item.get("status") or "unknown"
        name = item.get("name") or item.get("key") or "未知服务"
        pid = item.get("pid")
        pid_text = f" PID {pid}" if pid else ""
        lines.append(f"- {name}: {status}{pid_text}")
    return lines


def _write_outputs(
    *,
    project_root: Path,
    report_text: str,
    payload: Dict[str, Any],
    now: datetime,
) -> Dict[str, str]:
    archive_dir = project_root / "reports" / "runtime_governance"
    archive_dir.mkdir(parents=True, exist_ok=True)
    date_prefix = now.strftime("%Y-%m-%d")
    report_path = archive_dir / f"{date_prefix}_盘前健康自检.md"
    json_path = archive_dir / f"{date_prefix}_runtime_observation.json"
    report_path.write_text(report_text, encoding="utf-8")
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {"report_path": str(report_path), "json_path": str(json_path)}


def build_premarket_health_check(
    *,
    config: Any = None,
    project_root: str | Path | None = None,
    home_dir: str | Path | None = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    current = now or datetime.now()
    root = Path(project_root) if project_root is not None else PROJECT_ROOT
    home = Path(home_dir) if home_dir is not None else Path.home()
    runtime_config = config or get_config()
    overview = SystemOverviewService(
        project_root=root,
        home_dir=home,
        config=runtime_config,
    ).build_overview()
    alerts = list(overview.get("alerts") or [])
    overview_status = _status_from_alerts(alerts)
    observation = _build_log_observation(root, home)
    observation_status = _status_from_observation(observation)
    status = _worse_status(overview_status, observation_status)
    scheduler = overview.get("scheduler") or {}
    files = {str(item.get("key")): item for item in (overview.get("files") or [])}

    report_lines = [
        f"# {current.strftime('%Y-%m-%d')} 盘前健康自检",
        "",
        f"## 结论: {_status_label(status)}",
        "",
        "## 核心状态",
        f"- 后台调度: {'active' if scheduler.get('launch_agent_alive') else 'inactive'}"
        f" | PID {scheduler.get('launch_agent_pid') or '无'}"
        f" | 09:40报告 {'开启' if scheduler.get('schedule_enabled') else '关闭'}"
        f" | 22:30预判 {'开启' if scheduler.get('nightly_market_outlook_enabled') else '关闭'}",
        *_format_services(list(overview.get("services") or [])),
        "",
        "## 过去日志观察",
        f"- 日志观察结论: {_status_label(observation_status)}",
        f"- 错误/Traceback: {observation['counts']['errors']}",
        f"- 飞书成功发送: {observation['counts']['feishu_success']}",
        f"- 通知限流/频控: {observation['counts']['notification_rate_limit']}",
        f"- 搜索源限流/退避: {observation['counts']['search_rate_limit']}",
        f"- 闭市跳过记录: {observation['counts']['after_close_skips']}",
        f"- 盘中个股推送记录: {observation['counts']['stock_reminder_sent']}",
        f"- 路由状态文件: {'存在' if observation['route_state_exists'] else '未生成'}",
        "",
        "## 关键文件",
        f"- Gemini日常缓存: {files.get('gemini_daily', {}).get('modified_at') or '未找到'}",
        f"- Gemini黑天鹅缓存: {files.get('gemini_black_swan', {}).get('modified_at') or '未找到'}",
        f"- 最新明日预判: {files.get('latest_nightly_outlook', {}).get('path') or '未生成'}",
        f"- 最新盘前总报告: {files.get('latest_daily_push', {}).get('path') or '未生成'}",
    ]

    if alerts:
        report_lines.extend(["", "## 需要处理"])
        report_lines.extend(
            f"- {item.get('level', 'warning')}: {item.get('title') or item.get('description') or item}"
            for item in alerts
        )
    elif observation_status != "ok":
        report_lines.extend(
            [
                "",
                "## 需要处理",
                "- 日志观察出现非致命警告：优先检查数据源失败、搜索限流或飞书频控是否仍在持续；若连续 2-3 个交易日仍出现，再调低调用频率或改数据源降级。",
            ]
        )
    else:
        report_lines.extend(["", "## 需要处理", "- 无。"])

    report_text = "\n".join(report_lines) + "\n"
    payload = {
        "generated_at": current.isoformat(timespec="seconds"),
        "status": status,
        "status_label": _status_label(status),
        "alerts": alerts,
        "scheduler": scheduler,
        "services": overview.get("services") or [],
        "observation": observation,
    }
    paths = _write_outputs(
        project_root=root,
        report_text=report_text,
        payload=payload,
        now=current,
    )
    payload.update(paths)
    payload["report_text"] = report_text
    return payload


def run_premarket_health_check(
    *,
    config: Any = None,
    notify: bool = True,
    project_root: str | Path | None = None,
    home_dir: str | Path | None = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    payload = build_premarket_health_check(
        config=config,
        project_root=project_root,
        home_dir=home_dir,
        now=now,
    )
    runtime_config = config or get_config()
    should_push_ok = bool(getattr(runtime_config, "premarket_health_check_push_ok", True))
    should_notify = bool(notify and (payload["status"] != "ok" or should_push_ok))
    if should_notify:
        from src.notification import NotificationBuilder, NotificationService

        title = f"盘前健康自检：{payload['status_label']}"
        alert_type = "success" if payload["status"] == "ok" else "warning"
        message = NotificationBuilder.build_simple_alert(
            title=title,
            content=payload["report_text"],
            alert_type=alert_type,
        )
        result = NotificationService().send(message)
        payload["notification_sent"] = True
        payload["notification_result"] = result
    else:
        payload["notification_sent"] = False
    return payload
