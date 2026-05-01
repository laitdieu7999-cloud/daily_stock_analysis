# -*- coding: utf-8 -*-
"""Tests for AKShare Eastmoney news fallback."""

import sys
import unittest
from datetime import datetime
from types import ModuleType
from unittest.mock import patch

import pandas as pd

from src.search_service import SearchService


class _FakeAkshareModule(ModuleType):
    @staticmethod
    def stock_news_em(symbol: str):
        today = datetime.now().strftime("%Y-%m-%d 10:00:00")
        return pd.DataFrame(
            [
                {
                    "关键词": symbol,
                    "新闻标题": "贵州茅台一季度业绩更新",
                    "新闻内容": "公司披露最新经营数据，市场继续关注盈利表现。",
                    "发布时间": today,
                    "文章来源": "界面新闻",
                    "新闻链接": "https://example.com/moutai-news",
                }
            ]
        )


class SearchAkshareNewsFallbackTestCase(unittest.TestCase):
    def test_search_stock_news_falls_back_to_akshare_when_providers_fail(self) -> None:
        service = SearchService(searxng_public_instances_enabled=False)
        service._providers = []

        with patch.dict(sys.modules, {"akshare": _FakeAkshareModule("akshare")}):
            response = service.search_stock_news("600519", "贵州茅台", max_results=3)

        self.assertTrue(response.success)
        self.assertEqual(response.provider, "AKShare-Eastmoney")
        self.assertEqual(len(response.results), 1)
        self.assertEqual(response.results[0].title, "贵州茅台一季度业绩更新")
        self.assertEqual(response.results[0].source, "界面新闻")


if __name__ == "__main__":
    unittest.main()
