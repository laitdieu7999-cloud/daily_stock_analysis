# -*- coding: utf-8 -*-
"""
===================================
History Query Service Layer
===================================

Responsibilities:
1. Encapsulate history record query logic
2. Provide pagination and filtering functionality
3. Generate detailed reports in Markdown format
"""
from __future__ import annotations
import json
import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, TYPE_CHECKING

from src.config import get_config, resolve_news_window_days
from src.report_language import (
    format_chip_summary,
    get_bias_status_emoji,
    get_localized_stock_name,
    get_report_labels,
    get_signal_level,
    localize_bias_status,
    localize_operation_advice,
    localize_trend_prediction,
    normalize_report_language,
)
from src.services.sniper_points import clean_sniper_value, refine_sniper_points_for_context
from src.storage import DatabaseManager
from src.utils.data_processing import normalize_model_used, parse_json_field

if TYPE_CHECKING:
    from src.analyzer import AnalysisResult

logger = logging.getLogger(__name__)


class MarkdownReportGenerationError(Exception):
    """Exception raised when Markdown report generation fails due to internal errors."""

    def __init__(self, message: str, record_id: str = None):
        self.message = message
        self.record_id = record_id
        super().__init__(self.message)


class HistoryService:
    """
    History Query Service
    
    Encapsulates query logic for historical analysis records.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize the history query service.
        
        Args:
            db_manager: Database manager (optional, defaults to singleton instance)
        """
        self.db = db_manager or DatabaseManager.get_instance()
    
    def get_history_list(
        self,
        stock_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page: int = 1,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get history analysis list.
        
        Args:
            stock_code: Stock code filter
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            page: Page number
            limit: Items per page
            
        Returns:
            Dictionary containing total count and items
        """
        try:
            # Parse date parameters
            start_dt = None
            end_dt = None
            
            if start_date:
                try:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
                except ValueError:
                    logger.warning(f"无效的 start_date 格式: {start_date}")
            
            if end_date:
                try:
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
                except ValueError:
                    logger.warning(f"无效的 end_date 格式: {end_date}")
            
            # Calculate offset
            offset = (page - 1) * limit
            
            # Use new paginated query method
            records, total = self.db.get_analysis_history_paginated(
                code=stock_code,
                start_date=start_dt,
                end_date=end_dt,
                offset=offset,
                limit=limit
            )
            
            # Convert to response format
            items = []
            for record in records:
                items.append({
                    "id": record.id,
                    "query_id": record.query_id,
                    "stock_code": record.code,
                    "stock_name": record.name,
                    "report_type": record.report_type,
                    "sentiment_score": record.sentiment_score,
                    "operation_advice": record.operation_advice,
                    "created_at": record.created_at.isoformat() if record.created_at else None,
                })
            
            return {
                "total": total,
                "items": items,
            }
            
        except Exception as e:
            logger.error(f"查询历史列表失败: {e}", exc_info=True)
            return {"total": 0, "items": []}

    def _resolve_record(self, record_id: str):
        """
        Resolve a record_id parameter to an AnalysisHistory object.

        Tries integer primary key first; falls back to query_id string lookup
        when the value is not a valid integer.

        Args:
            record_id: integer PK (as string) or query_id string

        Returns:
            AnalysisHistory object or None
        """
        try:
            int_id = int(record_id)
            record = self.db.get_analysis_history_by_id(int_id)
            if record:
                return record
        except (ValueError, TypeError):
            pass
        # Fall back to query_id lookup
        return self.db.get_latest_analysis_by_query_id(record_id)

    def resolve_and_get_detail(self, record_id: str) -> Optional[Dict[str, Any]]:
        """
        Resolve record_id (int PK or query_id string) and return history detail.

        Args:
            record_id: integer PK (as string) or query_id string

        Returns:
            Complete analysis report dict, or None
        """
        try:
            record = self._resolve_record(record_id)
            if not record:
                return None
            return self._record_to_detail_dict(record)
        except Exception as e:
            logger.error(f"resolve_and_get_detail failed for {record_id}: {e}", exc_info=True)
            return None

    def resolve_and_get_news(self, record_id: str, limit: int = 20) -> List[Dict[str, str]]:
        """
        Resolve record_id (int PK or query_id string) and return associated news.

        Args:
            record_id: integer PK (as string) or query_id string
            limit: max items to return

        Returns:
            List of news intel dicts
        """
        try:
            record = self._resolve_record(record_id)
            if not record:
                logger.warning(f"resolve_and_get_news: record not found for {record_id}")
                return []
            items = self.get_news_intel(query_id=record.query_id, limit=limit)
            if items:
                return items
            items = self._fallback_news_content_items(record, limit=limit)
            if items and not self._looks_like_empty_news_summary(items):
                return items
            live_items = self._live_search_news_items(record, limit=limit)
            return live_items or items
        except Exception as e:
            logger.error(f"resolve_and_get_news failed for {record_id}: {e}", exc_info=True)
            return []

    def get_history_detail_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        """
        Get history report detail.

        Uses database primary key for precise query, avoiding returning incorrect records 
        due to duplicate query_id in batch analysis.

        Args:
            record_id: Analysis history record primary key ID

        Returns:
            Complete analysis report dictionary, or None if not exists
        """
        try:
            record = self.db.get_analysis_history_by_id(record_id)
            if not record:
                return None
            return self._record_to_detail_dict(record)
        except Exception as e:
            logger.error(f"根据 ID 查询历史详情失败: {e}", exc_info=True)
            return None

    @staticmethod
    def _normalize_display_sniper_value(value: Any) -> Optional[str]:
        """Normalize sniper point values for history display."""
        if HistoryService._is_display_sniper_placeholder(value):
            return None
        if isinstance(value, (dict, list, tuple)):
            cleaned = clean_sniper_value(value)
            return None if cleaned == "N/A" else cleaned
        text = str(value).strip()
        if text.startswith(("{", "[")):
            cleaned = clean_sniper_value(text)
            return None if cleaned == "N/A" else cleaned
        return text

    @staticmethod
    def _is_display_sniper_placeholder(value: Any) -> bool:
        """Treat empty or non-actionable sniper values as missing for display fallback."""
        if value is None:
            return True
        text = str(value).strip()
        lowered = text.lower()
        if lowered in {
            "",
            "-",
            "—",
            "n/a",
            "na",
            "none",
            "not applicable",
            "数据缺失",
            "未知",
            "无",
            "暂无",
            "不适用",
        }:
            return True
        return text.startswith("无（") or text.startswith("暂无")

    @staticmethod
    def _coerce_display_number(value: Any) -> Optional[float]:
        """Parse a positive numeric value from history/context payloads."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            number = float(value)
            return number if number > 0 else None
        try:
            text = str(value).strip().replace(",", "")
            if not text:
                return None
            number = float(text)
        except (TypeError, ValueError):
            return None
        return number if number > 0 else None

    @classmethod
    def _first_display_number(cls, *values: Any) -> Optional[float]:
        """Return the first positive number from scalar or list-like values."""
        for value in values:
            if isinstance(value, (list, tuple)):
                for item in value:
                    number = cls._coerce_display_number(item)
                    if number is not None:
                        return number
                continue
            number = cls._coerce_display_number(value)
            if number is not None:
                return number
        return None

    @staticmethod
    def _format_display_price(value: Optional[float]) -> str:
        if value is None:
            return "数据不足"
        return f"{value:.3f}元" if abs(value) < 10 else f"{value:.2f}元"

    @classmethod
    def _build_display_sniper_points_from_context(
        cls,
        context_snapshot: Any,
        raw_result: Any,
    ) -> Dict[str, str]:
        """Build visible sniper points for old history rows that only stored placeholders."""
        if not isinstance(context_snapshot, dict):
            return {}

        enhanced = context_snapshot.get("enhanced_context") or {}
        if not isinstance(enhanced, dict):
            enhanced = {}

        today = enhanced.get("today") or {}
        realtime = enhanced.get("realtime") or {}
        trend = enhanced.get("trend_analysis") or {}
        raw_quote = context_snapshot.get("realtime_quote_raw") or {}

        if not isinstance(today, dict):
            today = {}
        if not isinstance(realtime, dict):
            realtime = {}
        if not isinstance(trend, dict):
            trend = {}
        if not isinstance(raw_quote, dict):
            raw_quote = {}

        current = cls._first_display_number(
            realtime.get("price"),
            trend.get("current_price"),
            today.get("close"),
            raw_quote.get("price"),
        )
        ma5 = cls._first_display_number(trend.get("ma5"), today.get("ma5"))
        ma10 = cls._first_display_number(trend.get("ma10"), today.get("ma10"))
        ma20 = cls._first_display_number(trend.get("ma20"), today.get("ma20"))
        support = cls._first_display_number(
            trend.get("support_levels"),
            trend.get("boll_lower"),
            today.get("low"),
        )

        resistance_values: List[float] = []
        raw_resistance = trend.get("resistance_levels")
        if isinstance(raw_resistance, (list, tuple)):
            resistance_values.extend(
                value
                for value in (cls._coerce_display_number(item) for item in raw_resistance)
                if value is not None
            )
        for candidate in (trend.get("boll_upper"),):
            number = cls._coerce_display_number(candidate)
            if number is not None:
                resistance_values.append(number)
        if current is not None:
            resistance_values = [item for item in resistance_values if item > current]
        decision_type = ""
        if isinstance(raw_result, dict):
            decision_type = str(raw_result.get("decision_type") or "").lower()
        if decision_type == "sell" and current is not None:
            near_resistance = [
                item for item in (ma5, ma10)
                if item is not None and item > current
            ]
            resistance_values = sorted([*near_resistance, *resistance_values])

        ideal = ma5 or current or support or ma10 or ma20
        secondary = ma10 or support or (ideal * 0.98 if ideal else None)
        stop_base = ma20 or support or (current * 0.93 if current else None)
        if current is not None and stop_base is not None and stop_base >= current:
            stop_base = current * 0.95
        stop_loss = stop_base * 0.98 if stop_base else None

        if resistance_values:
            take_profit = min(resistance_values)
        elif current is not None:
            take_profit = current * 1.08
        elif ideal is not None:
            take_profit = ideal * 1.08
        else:
            take_profit = None

        if decision_type == "sell":
            ideal_text = f"暂不接回；重新站回MA5附近 {cls._format_display_price(ideal)} 后再评估"
            secondary_text = f"确认转强：站稳MA10附近 {cls._format_display_price(secondary)} 且止跌后再看"
        else:
            ideal_text = f"买入区：{cls._format_display_price(ideal)}（MA5附近，优先等回踩或站稳确认）"
            secondary_text = f"加仓区：{cls._format_display_price(secondary)}（MA10/支撑附近，更保守）"

        return {
            "ideal_buy": ideal_text,
            "secondary_buy": secondary_text,
            "stop_loss": f"止损线：{cls._format_display_price(stop_loss)}（跌破MA20/支撑后风控）",
            "take_profit": (
                f"反抽出局线：{cls._format_display_price(take_profit)}附近（不是止盈目标）"
                if decision_type == "sell"
                else f"目标区：{cls._format_display_price(take_profit)}（压力位或约8%风险回报目标）"
            ),
        }

    def _get_display_sniper_points(
        self,
        record,
        raw_result: Any,
        context_snapshot: Any = None,
    ) -> Dict[str, Optional[str]]:
        """Prefer raw dashboard sniper strings for history display, then fall back to numeric DB columns."""
        raw_points: Dict[str, Any] = {}
        if isinstance(raw_result, dict):
            for candidate in (raw_result.get("dashboard"), raw_result):
                if not isinstance(candidate, dict):
                    continue
                raw_points = DatabaseManager._find_sniper_in_dashboard(candidate) or raw_points
                if any(raw_points.get(k) is not None for k in ("ideal_buy", "secondary_buy", "stop_loss", "take_profit")):
                    break

        display_points: Dict[str, Optional[str]] = {}
        context_fallback = self._build_display_sniper_points_from_context(context_snapshot, raw_result)
        for field in ("ideal_buy", "secondary_buy", "stop_loss", "take_profit"):
            raw_value = self._normalize_display_sniper_value(raw_points.get(field))
            if raw_value is not None:
                display_points[field] = raw_value
                continue
            db_value = getattr(record, field, None)
            normalized_db_value = self._normalize_display_sniper_value(db_value)
            display_points[field] = normalized_db_value or context_fallback.get(field)
        dashboard = raw_result.get("dashboard") if isinstance(raw_result, dict) else None
        trend_analysis = raw_result.get("trend_analysis") if isinstance(raw_result, dict) else None
        market_snapshot = raw_result.get("market_snapshot") if isinstance(raw_result, dict) else None
        current_price = raw_result.get("current_price") if isinstance(raw_result, dict) else None
        return refine_sniper_points_for_context(
            display_points,
            current_price=current_price,
            decision_type=raw_result.get("decision_type") if isinstance(raw_result, dict) else None,
            operation_advice=getattr(record, "operation_advice", None),
            trend_prediction=getattr(record, "trend_prediction", None),
            dashboard=dashboard,
            trend_analysis=trend_analysis,
            market_snapshot=market_snapshot,
            audit_context={
                "source": "history_detail",
                "record_id": getattr(record, "id", None),
                "code": getattr(record, "code", None),
                "name": getattr(record, "name", None),
                "query_id": getattr(record, "query_id", None),
            },
        )

    def _record_to_detail_dict(self, record) -> Dict[str, Any]:
        """
        Convert an AnalysisHistory ORM record to a detail response dict.
        """
        raw_result = parse_json_field(record.raw_result)

        model_used = (raw_result or {}).get("model_used") if isinstance(raw_result, dict) else None
        model_used = normalize_model_used(model_used)

        context_snapshot = None
        if record.context_snapshot:
            try:
                context_snapshot = json.loads(record.context_snapshot)
            except json.JSONDecodeError:
                context_snapshot = record.context_snapshot
        sniper_points = self._get_display_sniper_points(record, raw_result, context_snapshot)

        return {
            "id": record.id,
            "query_id": record.query_id,
            "stock_code": record.code,
            "stock_name": record.name,
            "report_type": record.report_type,
            "created_at": record.created_at.isoformat() if record.created_at else None,
            "model_used": model_used,
            "analysis_summary": record.analysis_summary,
            "operation_advice": record.operation_advice,
            "trend_prediction": record.trend_prediction,
            "sentiment_score": record.sentiment_score,
            "sentiment_label": self._get_sentiment_label(record.sentiment_score or 50),
            "ideal_buy": sniper_points.get("ideal_buy"),
            "secondary_buy": sniper_points.get("secondary_buy"),
            "stop_loss": sniper_points.get("stop_loss"),
            "take_profit": sniper_points.get("take_profit"),
            "news_content": record.news_content,
            "raw_result": raw_result,
            "context_snapshot": context_snapshot,
        }

    def delete_history_records(self, record_ids: List[int]) -> int:
        """
        Delete specified analysis history records.

        Args:
            record_ids: List of history record primary key IDs

        Returns:
            Number of records actually deleted

        Raises:
            Exception: Re-raises any storage-layer exception so the API caller
                       receives a proper 500 error instead of a silent success.
        """
        return self.db.delete_analysis_history_records(record_ids)

    def get_news_intel(self, query_id: str, limit: int = 20) -> List[Dict[str, str]]:
        """
        Get news intelligence associated with a specified query_id.

        Args:
            query_id: Unique analysis identifier
            limit: Result limit

        Returns:
            List of news intelligence (containing title, snippet, and url)
        """
        try:
            records = self.db.get_news_intel_by_query_id(query_id=query_id, limit=limit)

            if not records:
                records = self._fallback_news_by_analysis_context(query_id=query_id, limit=limit)

            items: List[Dict[str, str]] = []
            for record in records:
                snippet = (record.snippet or "").strip()
                if len(snippet) > 200:
                    snippet = f"{snippet[:197]}..."
                items.append({
                    "title": record.title,
                    "snippet": snippet,
                    "url": record.url,
                })

            return items

        except Exception as e:
            logger.error(f"查询新闻情报失败: {e}", exc_info=True)
            return []

    def get_news_intel_by_record_id(self, record_id: int, limit: int = 20) -> List[Dict[str, str]]:
        """
        Get associated news intelligence based on analysis history record ID.

        Parses record_id to query_id, then calls get_news_intel.

        Args:
            record_id: Analysis history primary key ID
            limit: Result limit

        Returns:
            List of news intelligence (containing title, snippet, and url)
        """
        try:
            # Look up the corresponding AnalysisHistory record by record_id
            record = self.db.get_analysis_history_by_id(record_id)
            if not record:
                logger.warning(f"No analysis record found for record_id={record_id}")
                return []

            # Get query_id from record, then call original method
            return self.get_news_intel(query_id=record.query_id, limit=limit)

        except Exception as e:
            logger.error(f"根据 record_id 查询新闻情报失败: {e}", exc_info=True)
            return []

    def _fallback_news_by_analysis_context(self, query_id: str, limit: int) -> List[Any]:
        """
        Fallback by analysis context when direct query_id lookup returns no news.

        Typical scenarios:
        - URL-level dedup keeps one canonical news row across repeated analyses.
        - Legacy records may have different historical query_id strategies.
        """
        records = self.db.get_analysis_history(query_id=query_id, limit=1)
        if not records:
            return []

        analysis = records[0]
        if not analysis.code or not analysis.created_at:
            return []

        # Narrow down to same-stock recent news, then filter by analysis time window.
        days = max(1, (datetime.now() - analysis.created_at).days + 1)
        candidates = self.db.get_recent_news(code=analysis.code, days=days, limit=max(limit * 5, 50))

        start_time = analysis.created_at - timedelta(hours=6)
        end_time = analysis.created_at + timedelta(hours=6)
        matched = [
            item for item in candidates
            if item.fetched_at and start_time <= item.fetched_at <= end_time
        ]

        # 历史兜底链路也做发布时间硬过滤，避免旧库脏数据重新冒出。
        cfg = get_config()
        window_days = resolve_news_window_days(
            news_max_age_days=getattr(cfg, "news_max_age_days", 3),
            news_strategy_profile=getattr(cfg, "news_strategy_profile", "short"),
        )
        # Anchor to analysis date instead of "today" to preserve historical context.
        anchor_date = analysis.created_at.date()
        latest_allowed = anchor_date + timedelta(days=1)
        earliest_allowed = anchor_date - timedelta(days=max(0, window_days - 1))

        filtered = []
        for item in matched:
            if not item.published_date:
                continue
            if isinstance(item.published_date, datetime):
                published = item.published_date.date()
            elif isinstance(item.published_date, date):
                published = item.published_date
            else:
                continue
            if earliest_allowed <= published <= latest_allowed:
                filtered.append(item)

        return filtered[:limit]

    def _fallback_news_content_items(self, record: Any, limit: int) -> List[Dict[str, str]]:
        """
        Build displayable news items from analysis_history.news_content when no
        structured news_intel rows are available.
        """
        raw_text = (getattr(record, "news_content", None) or "").strip()
        if not raw_text:
            return []

        lines = [line.rstrip() for line in raw_text.splitlines()]
        items: List[Dict[str, str]] = []
        current_title: Optional[str] = None
        current_body: List[str] = []

        def flush_current() -> None:
            nonlocal current_title, current_body
            if not current_title:
                current_body = []
                return

            snippet = "\n".join(part for part in current_body if part).strip()
            if snippet:
                items.append({
                    "title": current_title.strip(),
                    "snippet": snippet[:200],
                    "url": "",
                })
            current_title = None
            current_body = []

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("【") and stripped.endswith("】"):
                continue

            if stripped.endswith(":") or stripped.endswith("："):
                flush_current()
                current_title = stripped.rstrip(":：").lstrip("📰📈📊⚠️💰🏛️🔍 ").strip()
                continue

            if current_title:
                current_body.append(stripped)

        flush_current()

        if items:
            return items[:limit]

        fallback_lines = [
            line.strip() for line in lines
            if line.strip() and not (line.strip().startswith("【") and line.strip().endswith("】"))
        ]
        if not fallback_lines:
            return []

        return [{
            "title": "资讯摘要",
            "snippet": "\n".join(fallback_lines)[:200],
            "url": "",
        }]

    @staticmethod
    def _looks_like_empty_news_summary(items: List[Dict[str, str]]) -> bool:
        if not items:
            return True
        normalized = " ".join((item.get("snippet") or "") for item in items).strip()
        if not normalized:
            return True
        empty_markers = [
            "未找到相关信息",
            "所有搜索引擎都不可用",
            "搜索失败",
            "新闻结果为空",
        ]
        return any(marker in normalized for marker in empty_markers)

    def _live_search_news_items(self, record: Any, limit: int) -> List[Dict[str, str]]:
        code = (getattr(record, "code", None) or "").strip()
        name = (getattr(record, "name", None) or code).strip()
        if not code:
            return []

        try:
            from src.search_service import SearchService

            response = SearchService().search_stock_news(code, name, max_results=limit)
            if not response.success or not response.results:
                return self._latest_available_news_items(code=code, limit=limit)
            return [
                {
                    "title": item.title,
                    "snippet": item.snippet,
                    "url": item.url,
                }
                for item in response.results[:limit]
            ]
        except Exception as exc:
            logger.warning("实时补新闻失败 %s(%s): %s", name, code, exc)
            return self._latest_available_news_items(code=code, limit=limit)

    def _latest_available_news_items(self, *, code: str, limit: int) -> List[Dict[str, str]]:
        """Last-resort display fallback: latest available Eastmoney news without date-window filtering."""
        try:
            import akshare as ak

            df = ak.stock_news_em(symbol=code)
            if df is None or df.empty:
                return []

            items: List[Dict[str, str]] = []
            for _, row in df.head(limit).iterrows():
                title = str(row.get("新闻标题") or "").strip()
                content = str(row.get("新闻内容") or "").strip()
                published = str(row.get("发布时间") or "").strip()
                url = str(row.get("新闻链接") or "").strip()
                if not title:
                    continue
                snippet = content
                if published:
                    snippet = f"{published} | {snippet}" if snippet else published
                items.append({
                    "title": title,
                    "snippet": snippet[:200],
                    "url": url,
                })
            return items
        except Exception as exc:
            logger.warning("最新可用新闻兜底失败 %s: %s", code, exc)
            return []
    
    def _get_sentiment_label(self, score: int) -> str:
        """
        Get sentiment label based on score.

        Args:
            score: Sentiment score (0-100)

        Returns:
            Sentiment label
        """
        if score >= 80:
            return "极度乐观"
        elif score >= 60:
            return "乐观"
        elif score >= 40:
            return "中性"
        elif score >= 20:
            return "悲观"
        else:
            return "极度悲观"

    def get_markdown_report(self, record_id: str) -> Optional[str]:
        """
        Generate a Markdown report for a single analysis history record.

        This method reconstructs an AnalysisResult from the stored raw_result
        and generates a detailed Markdown report similar to the push notifications.

        Args:
            record_id: integer PK (as string) or query_id string

        Returns:
            Markdown formatted report string, or None if record not found

        Raises:
            MarkdownReportGenerationError: If report generation fails due to internal errors
        """
        record = self._resolve_record(record_id)
        if not record:
            logger.warning(f"get_markdown_report: record not found for {record_id}")
            return None

        # Rebuild AnalysisResult from raw_result
        raw_result = parse_json_field(record.raw_result)
        if not raw_result:
            logger.error(f"get_markdown_report: raw_result is empty for {record_id}")
            raise MarkdownReportGenerationError(
                f"raw_result is empty or invalid for record {record_id}",
                record_id=record_id
            )

        try:
            result = self._rebuild_analysis_result(raw_result, record)
        except Exception as e:
            logger.error(f"get_markdown_report: failed to rebuild AnalysisResult for {record_id}: {e}", exc_info=True)
            raise MarkdownReportGenerationError(
                f"Failed to rebuild AnalysisResult: {str(e)}",
                record_id=record_id
            ) from e

        if not result:
            logger.error(f"get_markdown_report: _rebuild_analysis_result returned None for {record_id}")
            raise MarkdownReportGenerationError(
                f"Failed to rebuild AnalysisResult from raw_result",
                record_id=record_id
            )

        # Generate Markdown report
        try:
            return self._generate_single_stock_markdown(result, record)
        except Exception as e:
            logger.error(f"get_markdown_report: failed to generate markdown for {record_id}: {e}", exc_info=True)
            raise MarkdownReportGenerationError(
                f"Failed to generate markdown report: {str(e)}",
                record_id=record_id
            ) from e

    def _rebuild_analysis_result(
        self,
        raw_result: Dict[str, Any],
        record
    ) -> Optional[AnalysisResult]:
        """
        Rebuild an AnalysisResult object from stored raw_result dict.

        Args:
            raw_result: The parsed raw_result JSON dict
            record: The AnalysisHistory ORM record

        Returns:
            AnalysisResult object or None
        """
        try:
            from src.analyzer import AnalysisResult
            # Extract dashboard data if available
            dashboard = raw_result.get("dashboard", {})

            # Build AnalysisResult with available data
            return AnalysisResult(
                code=raw_result.get("code", record.code),
                name=raw_result.get("name", record.name),
                sentiment_score=raw_result.get("sentiment_score", record.sentiment_score or 50),
                trend_prediction=raw_result.get("trend_prediction", record.trend_prediction or ""),
                operation_advice=raw_result.get("operation_advice", record.operation_advice or ""),
                decision_type=raw_result.get("decision_type", "hold"),
                confidence_level=raw_result.get("confidence_level", "中"),
                report_language=normalize_report_language(raw_result.get("report_language")),
                dashboard=dashboard,
                trend_analysis=raw_result.get("trend_analysis", ""),
                short_term_outlook=raw_result.get("short_term_outlook", ""),
                medium_term_outlook=raw_result.get("medium_term_outlook", ""),
                technical_analysis=raw_result.get("technical_analysis", ""),
                ma_analysis=raw_result.get("ma_analysis", ""),
                volume_analysis=raw_result.get("volume_analysis", ""),
                pattern_analysis=raw_result.get("pattern_analysis", ""),
                fundamental_analysis=raw_result.get("fundamental_analysis", ""),
                sector_position=raw_result.get("sector_position", ""),
                company_highlights=raw_result.get("company_highlights", ""),
                news_summary=raw_result.get("news_summary", record.news_content or ""),
                market_sentiment=raw_result.get("market_sentiment", ""),
                hot_topics=raw_result.get("hot_topics", ""),
                analysis_summary=raw_result.get("analysis_summary", record.analysis_summary or ""),
                key_points=raw_result.get("key_points", ""),
                risk_warning=raw_result.get("risk_warning", ""),
                buy_reason=raw_result.get("buy_reason", ""),
                market_snapshot=raw_result.get("market_snapshot"),
                search_performed=raw_result.get("search_performed", False),
                data_sources=raw_result.get("data_sources", ""),
                success=raw_result.get("success", True),
                error_message=raw_result.get("error_message"),
                current_price=raw_result.get("current_price"),
                change_pct=raw_result.get("change_pct"),
                model_used=raw_result.get("model_used"),
            )
        except Exception as e:
            logger.error(f"Failed to rebuild AnalysisResult: {e}", exc_info=True)
            return None

    def _generate_single_stock_markdown(
        self,
        result: AnalysisResult,
        record
    ) -> str:
        """
        Generate a Markdown report for a single stock analysis.

        This follows the same format as NotificationService.generate_dashboard_report()
        using dashboard structured data for detailed report.

        Args:
            result: The AnalysisResult object
            record: The AnalysisHistory ORM record

        Returns:
            Markdown formatted report string
        """
        report_date = record.created_at.strftime("%Y-%m-%d") if record.created_at else datetime.now().strftime("%Y-%m-%d")
        report_time = record.created_at.strftime("%H:%M:%S") if record.created_at else datetime.now().strftime("%H:%M:%S")
        report_language = normalize_report_language(getattr(result, "report_language", "zh"))
        labels = get_report_labels(report_language)
        analysis_date_label = "Analysis Date" if report_language == "en" else "分析日期"
        report_time_label = "Report Time" if report_language == "en" else "报告生成时间"
        compact_labels = self._get_execution_labels(report_language, result)

        # Escape markdown special characters in stock name
        name_escaped = self._escape_md(
            get_localized_stock_name(result.name, result.code, report_language)
        ) or result.code

        # Get signal level
        signal_text, signal_emoji, signal_tag = self._get_signal_level(result)
        dashboard = result.dashboard if hasattr(result, 'dashboard') and result.dashboard else {}

        report_lines = [
            f"# 📊 {name_escaped} ({result.code}) {labels['report_title']}",
            "",
            f"> {analysis_date_label}: **{report_date}** | {report_time_label}: {report_time}",
            "",
        ]

        market_line = self._format_market_snapshot_line(result, report_language, labels)
        if market_line:
            report_lines.extend([market_line, ""])

        # ========== 核心结论 ==========
        core = dashboard.get('core_conclusion', {}) if dashboard else {}
        one_sentence = self._annotate_ma_levels(
            core.get('one_sentence', result.analysis_summary),
            dashboard,
        )
        time_sense = core.get('time_sensitivity', labels['default_time_sensitivity'])
        pos_advice = core.get('position_advice', {})

        report_lines.extend([
            f"### 📌 {labels['core_conclusion_heading']}",
            "",
            f"**{signal_emoji} {signal_text}** | {localize_trend_prediction(result.trend_prediction, report_language)}",
            "",
            f"> **{labels['one_sentence_label']}**: {one_sentence}",
            "",
        ])
        battle = dashboard.get('battle_plan', {}) if dashboard else {}
        trend_analysis = getattr(result, "trend_analysis", None)
        raw_sniper = battle.get('sniper_points', {}) if battle else {}
        sniper = (
            refine_sniper_points_for_context(
                raw_sniper,
                current_price=getattr(result, "current_price", None),
                decision_type=getattr(result, "decision_type", None),
                operation_advice=getattr(result, "operation_advice", None),
                trend_prediction=getattr(result, "trend_prediction", None),
                dashboard=dashboard,
                trend_analysis=trend_analysis if isinstance(trend_analysis, dict) else None,
                market_snapshot=getattr(result, "market_snapshot", None),
                audit_context={
                    "source": "history_markdown",
                    "record_id": getattr(record, "id", None),
                    "code": getattr(result, "code", None),
                    "name": getattr(result, "name", None),
                },
            )
            if isinstance(raw_sniper, dict) and raw_sniper
            else {}
        )
        report_lines.extend([
            f"### {compact_labels['execution_heading']}",
            "",
            f"- 🆕 **{labels['no_position_label']}**: {self._truncate_text(self._annotate_ma_levels(pos_advice.get('no_position', localize_operation_advice(result.operation_advice, report_language)), dashboard), 120)}",
            f"- 💼 **{labels['has_position_label']}**: {self._truncate_text(self._annotate_ma_levels(pos_advice.get('has_position', labels['continue_holding']), dashboard), 120)}",
        ])
        if sniper.get('ideal_buy'):
            report_lines.append(f"- 🎯 **{compact_labels['entry_label']}**: {self._clean_sniper_value(sniper.get('ideal_buy'))}")
        if sniper.get('secondary_buy'):
            report_lines.append(f"- 🔵 **{compact_labels['secondary_label']}**: {self._clean_sniper_value(sniper.get('secondary_buy'))}")
        if sniper.get('stop_loss'):
            report_lines.append(f"- 🛑 **{compact_labels['stop_label']}**: {self._clean_sniper_value(sniper.get('stop_loss'))}")
        if sniper.get('take_profit'):
            report_lines.append(f"- 🎊 **{compact_labels['target_label']}**: {self._clean_sniper_value(sniper.get('take_profit'))}")
        if time_sense:
            report_lines.append(f"- ⏰ **{compact_labels['timing_label']}**: {time_sense}")
        report_lines.extend(["", f"### {compact_labels['veto_heading']}", ""])
        report_lines.append(f"- {self._extract_primary_veto_risk(result, dashboard, report_language)}")
        report_lines.append("")

        # ========== 如果没有 dashboard，显示传统格式 ==========
        if not dashboard:
            report_lines.append(f"- {self._truncate_text(result.risk_warning or result.buy_reason or compact_labels['fallback_veto'], 100)}")
            report_lines.append("")

        # ========== 底部 ==========
        report_lines.extend([
            "---",
            "",
            f"*{labels['generated_at_label']}: {report_time}*",
        ])

        return "\n".join(report_lines)

    @staticmethod
    def _escape_md(text: Optional[str]) -> str:
        """Escape markdown special characters."""
        if not text:
            return ""
        return text.replace('*', r'\*')

    @staticmethod
    def _clean_sniper_value(value: Any) -> str:
        """Clean sniper point value for display."""
        return clean_sniper_value(value)

    @staticmethod
    def _truncate_text(value: Any, limit: int = 90) -> str:
        """Trim verbose text blocks for execution-card style reports."""
        if value is None:
            return ""
        text = str(value).strip()
        if len(text) <= limit:
            return text
        return text[:limit].rstrip() + "..."

    @staticmethod
    def _annotate_ma_levels(text: Any, dashboard: Optional[Dict[str, Any]]) -> str:
        """Append concrete MA prices after MA labels inside short decision text."""
        if text is None:
            return ""
        sentence = str(text)
        if not dashboard:
            return sentence

        price_position = (dashboard.get("data_perspective", {}) or {}).get("price_position", {}) or {}
        replacements = {
            "MA5": price_position.get("ma5"),
            "MA10": price_position.get("ma10"),
            "MA20": price_position.get("ma20"),
        }

        for ma_label, value in replacements.items():
            if value in (None, "", "N/A"):
                continue
            value_text = str(value).strip()
            sentence = re.sub(
                rf"(?<![A-Za-z0-9]){re.escape(ma_label)}(?![0-9(（])",
                f"{ma_label}({value_text})",
                sentence,
            )
        return sentence

    def _get_signal_level(self, result: AnalysisResult) -> Tuple[str, str, str]:
        """Get signal level based on sentiment score and decision type."""
        return get_signal_level(
            result.operation_advice,
            result.sentiment_score,
            getattr(result, "report_language", "zh"),
        )

    _SOURCE_DISPLAY_NAMES = {
        "tencent": {"zh": "腾讯财经", "en": "Tencent Finance"},
        "akshare_em": {"zh": "东方财富", "en": "Eastmoney"},
        "akshare_sina": {"zh": "新浪财经", "en": "Sina Finance"},
        "akshare_qq": {"zh": "腾讯财经", "en": "Tencent Finance"},
        "efinance": {"zh": "东方财富(efinance)", "en": "Eastmoney (efinance)"},
        "tushare": {"zh": "Tushare Pro", "en": "Tushare Pro"},
        "sina": {"zh": "新浪财经", "en": "Sina Finance"},
        "stooq": {"zh": "Stooq", "en": "Stooq"},
        "longbridge": {"zh": "长桥", "en": "Longbridge"},
        "fallback": {"zh": "降级兜底", "en": "Fallback"},
    }

    def _get_source_display_name(self, source: Any, language: str) -> str:
        raw_source = str(source or "N/A")
        mapping = self._SOURCE_DISPLAY_NAMES.get(raw_source)
        if not mapping:
            return raw_source
        return mapping.get(language, raw_source)

    @staticmethod
    def _safe_format_number(value: Any, fmt: str = ".2f") -> str:
        """
        Safely format a numeric value that may be a string.

        Args:
            value: The value to format (may be int, float, or string like "12.34" or "N/A")
            fmt: Format string (default: ".2f")

        Returns:
            Formatted string or original string if not a valid number
        """
        if value is None:
            return "N/A"
        if isinstance(value, (int, float)):
            return f"{value:{fmt}}"
        if isinstance(value, str):
            value = value.strip()
            if not value or value in ("N/A", "-", "—", "None"):
                return "N/A"
            try:
                return f"{float(value):{fmt}}"
            except (ValueError, TypeError):
                return value
        return str(value)

    @staticmethod
    def _append_market_snapshot_to_report(
        lines: List[str],
        result: AnalysisResult,
        labels: Dict[str, str],
    ) -> None:
        """Append market snapshot data to report lines."""
        snapshot = getattr(result, 'market_snapshot', None)
        if not snapshot:
            return

        lines.extend([
            f"### 📈 {labels['market_snapshot_heading']}",
            "",
            f"| {labels['price_metrics_label']} | {labels['current_price_label']} |",
            "|------|------|",
        ])

        # Price info
        current_price = snapshot.get('price') or snapshot.get('current_price') or result.current_price
        change_pct = snapshot.get('change_pct') or snapshot.get('pct_chg') or result.change_pct
        if current_price is not None:
            current_str = HistoryService._safe_format_number(current_price, ".2f")
            if change_pct is not None:
                if isinstance(change_pct, str) and change_pct.strip().endswith("%"):
                    change_str = change_pct.strip()
                else:
                    change_str = f"{HistoryService._safe_format_number(change_pct, '+.2f')}%"
            else:
                change_str = "--"
            lines.append(f"| {labels['current_price_label']} | **{current_str}** ({change_str}) |")

        # Other metrics
        metrics = [
            (labels['open_label'], "open", ".2f"),
            (labels['high_label'], "high", ".2f"),
            (labels['low_label'], "low", ".2f"),
            (labels['volume_label'], "volume", ",.0f"),
            (labels['amount_label'], "amount", ",.0f"),
        ]
        for label, key, fmt in metrics:
            value = snapshot.get(key)
            if value is not None:
                formatted = HistoryService._safe_format_number(value, fmt)
                lines.append(f"| {label} | {formatted} |")

        lines.extend(["", "---", ""])

    @staticmethod
    def _get_execution_labels(report_language: str, result: Any | None = None) -> Dict[str, str]:
        """Localized headings for compact execution-card rendering."""
        is_bearish = HistoryService._is_bearish_execution_result(result)
        if report_language == "en":
            labels = {
                "execution_heading": "⚔️ Execution Plan",
                "veto_heading": "💣 Veto Risk",
                "entry_label": "Entry Zone",
                "secondary_label": "Add Zone",
                "stop_label": "Stop Line",
                "target_label": "Target Zone",
                "timing_label": "Window",
                "market_label": "Market",
                "fallback_veto": "Invalidate the plan immediately if price breaks the stop or market structure fails.",
            }
            if is_bearish:
                labels.update({
                    "entry_label": "Recheck Line",
                    "secondary_label": "Confirm Recovery",
                    "stop_label": "Reduce Now",
                    "target_label": "Exit on Bounce",
                })
            return labels
        labels = {
            "execution_heading": "⚔️ 执行计划",
            "veto_heading": "💣 一票否决风险",
            "entry_label": "买入区",
            "secondary_label": "加仓区",
            "stop_label": "止损线",
            "target_label": "目标区",
            "timing_label": "时效性",
            "market_label": "盘面",
            "fallback_veto": "一旦跌破止损位或趋势结构失效，当前计划立即作废。",
        }
        if is_bearish:
            labels.update({
                "entry_label": "重新评估线",
                "secondary_label": "确认转强线",
                "stop_label": "立即减仓区",
                "target_label": "反抽出局线",
            })
        return labels

    @staticmethod
    def _is_bearish_execution_result(result: Any | None) -> bool:
        if result is None:
            return False
        joined = " ".join(
            str(getattr(result, attr, "") or "")
            for attr in ("decision_type", "operation_advice", "trend_prediction")
        ).lower()
        return any(token in joined for token in ("sell", "reduce", "bear", "卖", "减仓", "清仓", "看空", "空头"))

    def _format_market_snapshot_line(self, result: AnalysisResult, report_language: str, labels: Dict[str, str]) -> str:
        """Return a single-line market snapshot instead of a table."""
        snapshot = getattr(result, "market_snapshot", None)
        if not snapshot:
            return ""

        if report_language == "en":
            price_label = "Px"
            change_label = "Chg"
            volume_ratio_label = "VolRatio"
            turnover_label = "Turnover"
            source_label = "Source"
        else:
            price_label = "现价"
            change_label = "涨跌"
            volume_ratio_label = "量比"
            turnover_label = "换手"
            source_label = "来源"

        price_value = snapshot.get('price') or snapshot.get('current_price') or result.current_price or 'N/A'
        price_text = self._safe_format_number(price_value, ".2f")
        if price_text != "N/A" and not price_text.endswith("元") and report_language == "zh":
            price_text = f"{price_text}元"

        parts = [
            f"{price_label} {price_text}",
        ]
        change_pct = snapshot.get('change_pct') or snapshot.get('pct_chg') or result.change_pct
        if change_pct not in (None, "", "N/A"):
            if isinstance(change_pct, str) and change_pct.strip().endswith("%"):
                change_str = change_pct.strip()
            else:
                change_str = f"{self._safe_format_number(change_pct, '+.2f')}%"
            parts.append(f"{change_label} {change_str}")
        if snapshot.get("volume_ratio") not in (None, "", "N/A"):
            parts.append(f"{volume_ratio_label} {self._safe_format_number(snapshot.get('volume_ratio'), '.2f')}")
        if snapshot.get("turnover_rate") not in (None, "", "N/A"):
            turnover_text = str(snapshot.get('turnover_rate')).strip()
            if turnover_text and turnover_text not in ("N/A", "-", "—", "None"):
                if not turnover_text.endswith("%"):
                    turnover_text = f"{self._safe_format_number(snapshot.get('turnover_rate'), '.2f')}%"
                parts.append(f"{turnover_label} {turnover_text}")
        source = snapshot.get("source")
        if source not in (None, "", "N/A"):
            parts.append(f"{source_label} {self._get_source_display_name(source, report_language)}")

        return f"**{self._get_execution_labels(report_language)['market_label']}**: " + " | ".join(parts)

    def _extract_primary_veto_risk(
        self,
        result: AnalysisResult,
        dashboard: Optional[Dict[str, Any]],
        report_language: str,
    ) -> str:
        """Pick the single highest-signal veto risk for compact reports."""
        extra_labels = self._get_execution_labels(report_language)
        if dashboard:
            intel = dashboard.get("intelligence", {}) or {}
            risks = intel.get("risk_alerts", []) or []
            for risk in risks:
                text = self._annotate_ma_levels(
                    self._truncate_text(risk, 100),
                    dashboard,
                )
                if text:
                    return text

            battle = dashboard.get("battle_plan", {}) or {}
            checklist = battle.get("action_checklist", []) or []
            for item in checklist:
                item_text = str(item).strip()
                if item_text.startswith(("❌", "⚠️")) and not self._is_non_veto_checklist_item(item_text):
                    return self._annotate_ma_levels(
                        self._truncate_text(item_text, 100),
                        dashboard,
                    )

        if getattr(result, "risk_warning", None):
            return self._annotate_ma_levels(
                self._truncate_text(result.risk_warning, 100),
                dashboard,
            )
        return extra_labels["fallback_veto"]

    @staticmethod
    def _is_non_veto_checklist_item(item_text: str) -> bool:
        """Filter out data-gap checklist items that should not occupy the veto-risk slot."""
        text = str(item_text or "").strip()
        if not text:
            return True
        data_gap_markers = (
            "数据缺失",
            "未知",
            "unknown",
            "not supported",
            "无筹码",
            "筹码结构未知",
            "筹码健康未知",
            "无法判断",
        )
        return any(marker.lower() in text.lower() for marker in data_gap_markers)
