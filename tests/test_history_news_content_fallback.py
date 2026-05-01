# -*- coding: utf-8 -*-
"""Tests for news_content fallback in history news API."""

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.services.history_service import HistoryService
from src.search_service import SearchResponse


class HistoryNewsContentFallbackTestCase(unittest.TestCase):
    def test_resolve_and_get_news_falls_back_to_news_content_sections(self) -> None:
        record = SimpleNamespace(
            query_id="q-1",
            news_content=(
                "【中泰证券 情报搜索结果】\n\n"
                "📰 最新消息 (来源: SearXNG):\n"
                "  未找到相关信息\n\n"
                "📈 机构分析 (来源: SearXNG):\n"
                "  券商板块短线承压，等待成交量修复。\n"
            ),
        )

        mock_db = MagicMock()
        mock_db.get_latest_analysis_by_query_id.return_value = record
        mock_db.get_news_intel_by_query_id.return_value = []
        mock_db.get_analysis_history.return_value = []

        svc = HistoryService(db_manager=mock_db)

        result = svc.resolve_and_get_news("q-1", limit=8)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["title"], "最新消息 (来源: SearXNG)")
        self.assertIn("未找到相关信息", result[0]["snippet"])
        self.assertEqual(result[1]["title"], "机构分析 (来源: SearXNG)")

    def test_resolve_and_get_news_uses_live_search_when_summary_is_empty_marker(self) -> None:
        record = SimpleNamespace(
            query_id="q-1",
            code="600519",
            name="贵州茅台",
            news_content=(
                "【贵州茅台 情报搜索结果】\n\n"
                "📰 最新消息 (来源: SearXNG):\n"
                "  未找到相关信息\n"
            ),
        )

        mock_db = MagicMock()
        mock_db.get_latest_analysis_by_query_id.return_value = record
        mock_db.get_news_intel_by_query_id.return_value = []
        mock_db.get_analysis_history.return_value = []

        svc = HistoryService(db_manager=mock_db)

        with patch.object(
            svc,
            "_live_search_news_items",
            return_value=[{"title": "实时新闻", "snippet": "真实抓取结果", "url": "https://example.com"}],
        ):
            result = svc.resolve_and_get_news("q-1", limit=8)

        self.assertEqual(result[0]["title"], "实时新闻")

    def test_resolve_and_get_news_uses_latest_available_news_when_live_search_empty(self) -> None:
        record = SimpleNamespace(
            query_id="q-1",
            code="600918",
            name="中泰证券",
            news_content="📰 最新消息 (来源: SearXNG):\n  未找到相关信息\n",
        )

        mock_db = MagicMock()
        mock_db.get_latest_analysis_by_query_id.return_value = record
        mock_db.get_news_intel_by_query_id.return_value = []
        mock_db.get_analysis_history.return_value = []

        svc = HistoryService(db_manager=mock_db)

        with patch(
            "src.search_service.SearchService.search_stock_news",
            return_value=SearchResponse(query="q", results=[], provider="None", success=False, error_message="empty"),
        ), patch.object(
            svc,
            "_latest_available_news_items",
            return_value=[{"title": "最近可用新闻", "snippet": "2026-04-24 | 兜底内容", "url": "https://example.com/latest"}],
        ):
            result = svc.resolve_and_get_news("q-1", limit=8)

        self.assertEqual(result[0]["title"], "最近可用新闻")


if __name__ == "__main__":
    unittest.main()
