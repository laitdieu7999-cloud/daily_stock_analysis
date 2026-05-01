# -*- coding: utf-8 -*-
"""Daily portfolio review report built from local holdings and backtests."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config import get_config
from src.services.data_center_service import DataCenterService
from src.storage import DatabaseManager


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class PortfolioDailyReviewService:
    """Generate a local daily review for current holdings."""

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        *,
        config: Any = None,
        project_root: str | Path | None = None,
    ) -> None:
        self.db_manager = db_manager or DatabaseManager.get_instance()
        self.config = config or get_config()
        self.project_root = Path(project_root) if project_root is not None else self._default_project_root()

    def run(
        self,
        *,
        report_date: Optional[date] = None,
        run_backtests: bool = True,
        send_notification: Optional[bool] = None,
    ) -> Dict[str, Any]:
        today = report_date or date.today()
        data_center = DataCenterService(self.db_manager)
        overview = data_center.build_overview()
        backtest_payload = None
        if run_backtests:
            backtest_payload = data_center.run_portfolio_backtests(
                limit_per_symbol=int(getattr(self.config, "portfolio_daily_review_limit_per_symbol", 50)),
            )
        radar = data_center.get_portfolio_risk_radar()
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "report_date": today.isoformat(),
            "status": "ok",
            "portfolio": overview.get("portfolio") or {},
            "market_data": overview.get("market_data") or {},
            "backtest": backtest_payload,
            "radar": radar,
            "ai_routing": overview.get("ai_routing") or {},
        }
        markdown = self._render_markdown(payload)
        paths = self._write_report(today, payload, markdown)
        payload.update(paths)

        should_notify = bool(
            getattr(self.config, "portfolio_daily_review_notify_enabled", False)
            if send_notification is None
            else send_notification
        )
        payload["notification_sent"] = False
        if should_notify:
            payload["notification_sent"] = self._send_notification(markdown)
        return payload

    def latest_run(self) -> Dict[str, Any]:
        root = self._report_dir()
        if not root.exists():
            return {"status": "missing", "path": str(root), "generated_at": None}
        candidates = [path for path in root.glob("*_portfolio_review.json") if path.is_file()]
        if not candidates:
            return {"status": "missing", "path": str(root), "generated_at": None}
        latest = max(candidates, key=lambda item: item.stat().st_mtime)
        try:
            payload = json.loads(latest.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"status": "unreadable", "path": str(latest), "generated_at": None}
        return {
            "status": payload.get("status", "unknown"),
            "path": str(latest),
            "markdown_path": payload.get("markdown_path"),
            "generated_at": payload.get("generated_at"),
            "report_date": payload.get("report_date"),
            "holding_count": ((payload.get("radar") or {}).get("holding_count") or 0),
        }

    def _render_markdown(self, payload: Dict[str, Any]) -> str:
        radar_items = ((payload.get("radar") or {}).get("items") or [])
        backtest = payload.get("backtest") or {}
        market_data = payload.get("market_data") or {}
        warehouse = market_data.get("warehouse") or {}
        quality = market_data.get("quality") or {}

        lines = [
            f"# {payload['report_date']} 持仓复盘",
            "",
            "## 总览",
            f"- 当前持仓数量：{len(radar_items)}",
            f"- 行情数据：{market_data.get('bar_count', 0)} 条，最新 {market_data.get('latest_date') or '暂无'}",
            f"- 数据沉淀：{warehouse.get('status', 'unknown')}，成功 {((warehouse.get('totals') or {}).get('succeeded') or 0)} 只",
            f"- 数据质量：正常 {((quality.get('summary') or {}).get('fresh') or 0)}，滞后 {((quality.get('summary') or {}).get('stale') or 0)}，缺失 {((quality.get('summary') or {}).get('missing') or 0)}",
        ]

        if backtest:
            totals = backtest.get("totals") or {}
            lines.extend(
                [
                    f"- 本次回测：处理 {totals.get('processed', 0)} 条样本，新增 {totals.get('saved', 0)} 条结果",
                    "",
                ]
            )
        else:
            lines.append("")

        lines.append("## 持仓重点")
        if not radar_items:
            lines.append("- 暂无持仓。")
        for item in radar_items:
            lines.append(
                f"- {item.get('code')}: {item.get('label')}。{item.get('message')} "
                f"胜率 {self._format_pct(item.get('win_rate_pct'))}，平均模拟收益 {self._format_pct(item.get('avg_simulated_return_pct'))}。"
            )

        lines.extend(["", "## 下一步", "- 优先处理标记为“需要谨慎”的持仓。", "- 样本不足的股票先积累分析记录，不强行下结论。"])
        return "\n".join(lines).strip() + "\n"

    def _write_report(self, report_date: date, payload: Dict[str, Any], markdown: str) -> Dict[str, str]:
        root = self._report_dir()
        root.mkdir(parents=True, exist_ok=True)
        stem = f"{report_date.isoformat()}_portfolio_review"
        json_path = root / f"{stem}.json"
        markdown_path = root / f"{stem}.md"
        json_payload = {**payload, "markdown_path": str(markdown_path)}
        json_path.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        markdown_path.write_text(markdown, encoding="utf-8")
        return {"json_path": str(json_path), "markdown_path": str(markdown_path)}

    @staticmethod
    def _send_notification(markdown: str) -> bool:
        try:
            from src.notification import NotificationBuilder, NotificationService

            message = NotificationBuilder.build_simple_alert(
                title="每日持仓复盘",
                content=markdown[:3500],
                alert_type="info",
            )
            return bool(NotificationService().send(message))
        except Exception:
            return False

    def _report_dir(self) -> Path:
        return self.project_root / "reports" / "portfolio_daily_review"

    def _default_project_root(self) -> Path:
        database = getattr(self.db_manager._engine.url, "database", None)
        if database and database != ":memory:":
            db_path = Path(database).expanduser().resolve()
            if db_path.parent.name == "data":
                return db_path.parent.parent
        return PROJECT_ROOT

    @staticmethod
    def _format_pct(value: Any) -> str:
        return f"{float(value):.2f}%" if isinstance(value, (int, float)) else "暂无"
